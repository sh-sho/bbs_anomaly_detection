import io
import os
import json
import logging
import requests
from fdk import response
from base64 import b64encode 
from datetime import datetime, timezone

import oci
from oci.functions import *

ociMessageEndpoint = "https://cell-1.streaming.us-phoenix-1.oci.oraclecloud.com"  
ociStreamOcid = "ocid1.stream.oc1.phx.amaaaaaassl65iqap5ath7yuo7q6foymcme6omvrhprcgdwmk7zldsxsp5ea"  

function_endpoint = "https://7eabqyl3ymq.us-phoenix-1.functions.oci.oraclecloud.com"
compartment_id = "ocid1.compartment.oc1..aaaaaaaavu633dop4qlvss3ebdvrzo6hwnr4g5e7s42frmlfvlsjpnyss7xa"
app_id = "ocid1.fnapp.oc1.phx.aaaaaaaasxeuzsx5wzpgfuz5h7q42o6g2seq5pvxc3ctbnut47eabqyl3ymq"
function_ocid = "ocid1.fnfunc.oc1.phx.aaaaaaaa27mmpuv7pock7apbwgwtav7euldhgedwh73wb3gzrw6kjehzi2cq"
config = oci.config.from_file('~/.oci/config')

stream_client = oci.streaming.StreamClient(config=config, service_endpoint=ociMessageEndpoint)

json_file_path = "/home/opc/bbs_anomaly_detection/data/error_data"
json_files = [f for f in os.listdir(json_file_path) if f.endswith('.json')]

def produce_messages(pub_data, client, stream_id, i):
    # Build up a PutMessagesDetails and publish some messages to the stream
    message_list = []
    key = "messageKey" + str(i)
    #   data_bytes = bytes(data, 'utf-8')
    value = json.dumps(pub_data)
    encoded_key = b64encode(key.encode()).decode()
    encoded_value = b64encode(value.encode()).decode()
    message_list.append(oci.streaming.models.PutMessagesDetailsEntry(key=encoded_key, value=encoded_value))  

    print("Publishing {} messages to the stream {} ".format(len(message_list), stream_id))
    messages = oci.streaming.models.PutMessagesDetails(messages=message_list)
    put_message_result = client.put_messages(stream_id, messages)
    
    # The put_message_result can contain some useful metadata for handling failures
    for entry in put_message_result.data.entries:
        if entry.error:
            print("Error ({}) : {}".format(entry.error, entry.error_message))
        else:
            print("Published message to partition {} , offset {}".format(entry.partition, entry.offset))

def edit_data(values):
    # timestamp
    timestamp_value = datetime.now(timezone.utc)
    values[0]["timestamp"] = timestamp_value.strftime('%Y-%m-%dT%H:%M:%SZ')
    return values

for file_name in json_files:
    file_path = os.path.join(json_file_path, file_name)
    # print(file_path)
    
    with open(file_path, 'r') as json_file:
        stream_body = json_file.read()
    values = json.loads(stream_body)
    pub_data = edit_data(values)
    produce_messages(pub_data, stream_client, ociStreamOcid, 1)
    print(pub_data)
    




