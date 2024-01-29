import os
import json
from base64 import b64encode 
from datetime import datetime, timezone
import time
import oci
from oci.functions import *
from dotenv import load_dotenv

load_dotenv()

message_endpoint = os.getenv('message_endpoint')   
stream_ocid = os.getenv('stream_ocid')
config = oci.config.from_file('~/.oci/config')

json_file_path = '../data/error_data'
json_files = [f for f in os.listdir(json_file_path) if f.endswith('.json')]

stream_client = oci.streaming.StreamClient(config=config, service_endpoint=message_endpoint)

def produce_messages(pub_data, client, stream_id, i):
    message_list = []
    key = "messageKey" + str(i)
    value = json.dumps(pub_data)
    encoded_key = b64encode(key.encode()).decode()
    encoded_value = b64encode(value.encode()).decode()
    message_list.append(oci.streaming.models.PutMessagesDetailsEntry(key=encoded_key, value=encoded_value))  

    print("Publishing {} messages to the stream {} ".format(len(message_list), stream_id))
    messages = oci.streaming.models.PutMessagesDetails(messages=message_list)
    put_message_result = client.put_messages(stream_id, messages)
    
    for entry in put_message_result.data.entries:
        if entry.error:
            print("Error ({}) : {}".format(entry.error, entry.error_message))
        else:
            print("Published message to partition {} , offset {}".format(entry.partition, entry.offset))
    time.sleep(1)
    
def edit_data(values):
    timestamp_value = datetime.now(timezone.utc)
    values[0]["timestamp"] = timestamp_value.strftime('%Y-%m-%dT%H:%M:%SZ')
    return values

for file_name in json_files:
    file_path = os.path.join(json_file_path, file_name)
    
    with open(file_path, 'r') as json_file:
        stream_body = json_file.read()
    pub_data = edit_data(json.loads(stream_body))
    produce_messages(pub_data, stream_client, stream_ocid, 1)
    print(pub_data)
    