import tuyapower
import json
import time
import sys
from azure.data.tables import TableClient
from azure.core.exceptions import ResourceExistsError, HttpResponseError

MIN_WAIT_TIME_S = 5
SNAPSHOT_FILE_NAME=sys.argv[1] if len(sys.argv) >= 2 else "config/snapshot.json"
AZURE_CONFIG_FILE_NAME=sys.argv[2] if len(sys.argv) >= 3 else 'config/azure_config.json'


# Read and parse JSON file
def read_json_file(json_file):
    with open(json_file) as json_data:
        data = json.load(json_data)
        return data

def  read_azure_storage_account_info(filename):
    with open(filename) as json_data:
        data = json.load(json_data)
    return data.get('connection_string'), data.get('table_name')

# Create entity on Azure Table storage.
def create_entity(table_client, entity_data):
    try:
        resp = table_client.create_entity(entity=entity_data)
    except ResourceExistsError:
        print("Entity already exists")




sensors_data = read_json_file(SNAPSHOT_FILE_NAME)

CONNECTION_STRING, TABLE_NAME = read_azure_storage_account_info(AZURE_CONFIG_FILE_NAME)

sensor_last_read = {}
try:
    with TableClient.from_connection_string(CONNECTION_STRING, TABLE_NAME) as table_client:
        # Create a table in case it does not already exist
        try:
            table_client.create_table()
        except HttpResponseError:
            print("Table already exists")

        while True:
            for sensor in sensors_data["devices"]:
                start_timestamp = time.time()
                sensor_name = sensor["name"]
                
                # Get the last read time for this sensor, and check if it is too soon to read again.
                if sensor_name in sensor_last_read.keys():
                    difference = start_timestamp - sensor_last_read[sensor_name]
                    if difference < MIN_WAIT_TIME_S:
                        # Skipping sensor as it was read less than Min_Wait_Time seconds ago
                        time.sleep(MIN_WAIT_TIME_S - difference)
                        continue

                ## Update when the sensor was last read to current time.
                sensor_last_read[sensor_name] = start_timestamp
                (on, w, mA, V, err) = tuyapower.deviceInfo(sensor["id"],  sensor["ip"], sensor["key"], sensor["ver"])
                
                # Get current time
                current_time = time.strftime("%Y-%m-%d_%H:%M:%S", time.localtime())
                
                # Create a new entity
                entity = {
                "PartitionKey": sensor["name"],
                "RowKey": f"{sensor['name']}_{current_time}",
                "timestamp": start_timestamp,
                "name": sensor["name"],
                "isOn": on,
                "w": w,
                "mA": mA,
                "V": V,
                "err": err
                }
                # Insert the entity into the Table
                create_entity(table_client, entity)
except Exception as error:
    print(error)
    print('Error occurred. Exiting...')
