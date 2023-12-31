import io
import json
import logging

from fdk import response
import oci
from base64 import b64encode 
from datetime import datetime, timezone
import time


ociMessageEndpoint = "https://cell-1.streaming.us-phoenix-1.oci.oraclecloud.com"  
ociStreamOcid = "ocid1.stream.oc1.phx.amaaaaaassl65iqap5ath7yuo7q6foymcme6omvrhprcgdwmk7zldsxsp5ea"  

# # API
# ociConfigFilePath = "/home/opc/.oci/config"  
# ociProfileName = "DEFAULT"
# config = oci.config.from_file(ociConfigFilePath, ociProfileName)
# stream_client = oci.streaming.StreamClient(config, service_endpoint=ociMessageEndpoint)

signer = oci.auth.signers.get_resource_principals_signer()
stream_client = oci.streaming.StreamClient(config={}, signer=signer, service_endpoint=ociMessageEndpoint)




def produce_messages(pub_data, client, stream_id, i):
    # Build up a PutMessagesDetails and publish some messages to the stream
    message_list = []
    key = "messageKey" + str(i)
    #   data_bytes = bytes(data, 'utf-8')
    value = json.dumps(pub_data)
    print(type(value))
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
    # timestampを追加
    timestamp_value = datetime.now(timezone.utc)
    values[0]["timestamp"] = timestamp_value.strftime('%Y-%m-%dT%H:%M:%SZ')
    return values



def handler(ctx, data: io.BytesIO = None):
    try:
        values = json.loads(data.getvalue())
        pub_data = edit_data(values)
        produce_messages(pub_data, stream_client, ociStreamOcid, 1)
        logging.getLogger().info(pub_data)
        
    except (Exception, ValueError) as ex:
        logging.getLogger().info('error parsing json payload: ' + str(ex))

    logging.getLogger().info("Inside Python Hello World function")
    return response.Response(
        ctx, response_data=json.dumps(
            {"message": "ack"}),
        headers={"Content-Type": "application/json"}
    )
