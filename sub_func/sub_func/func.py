import io
import json
import logging
from fdk import response
import pandas as pd
from datetime import datetime, date, timedelta 
import time
# import random
import base64
import os
import oci
from oci.ai_anomaly_detection.models import *
from oci.ai_anomaly_detection.anomaly_detection_client import AnomalyDetectionClient

def anomaly_detect(logs):
    ### functions
    signer = oci.auth.signers.get_resource_principals_signer()
    ad_client = AnomalyDetectionClient(config={}, signer=signer)
    
    # ### api key
    # CONFIG_FILENAME = "/Users/USERNAME/.oci/config"
    # SERVICE_ENDPOINT="https://anomalydetection.aiservice.us-phoenix-1.oci.oraclecloud.com" 
    # NAMESPACE = "orasejapan"
    # BUCKET_NAME = "bucket_sobata"
    # training_file_name="ad-demotraining-data.csv"
    # compartment_id = "ocid1.compartment.oc1..aaaaaaaavu633dop4qlvss3ebdvrzo6hwnr4g5e7s42frmlfvlsjpnyss7xa"
    
    # # config = from_file(CONFIG_FILENAME)
    # config = oci.config.from_file('~/.oci/config')
    # ad_client = AnomalyDetectionClient(config,service_endpoint=SERVICE_ENDPOINT)
    # ### end api key
    
    model_id = 'ocid1.aianomalydetectionmodel.oc1.phx.amaaaaaassl65iqaconi4dxm3imsy2ixv6fgxfod6npia4euvl3kntlhhu5a'
    num_rows = 10
    timeName = ["timestamp"]
    signalNames = ["temperature_1", "temperature_2", "temperature_3", "temperature_4", "temperature_5", "pressure_1", "pressure_2", "pressure_3", "pressure_4", "pressure_5"]
    df = pd.DataFrame()
    
    for i in range(num_rows):
        values_str = logs[i]['value']
        values_dict = json.loads(values_str)
        df_timestamp = pd.DataFrame(data = [values_dict['timestamp']], columns=timeName)
        df_values = pd.DataFrame(data = [values_dict['values']], columns=signalNames)
        df_cell = pd.concat([df_timestamp, df_values], axis=1)
        df = pd.concat([df, df_cell], axis=0)
    
    logging.getLogger().info(df)
    inline = InlineDetectAnomaliesRequest(model_id=model_id, request_type="INLINE", signal_names=signalNames, data=df)
    detect_res = ad_client.detect_anomalies(detect_anomalies_details=inline)
    return detect_res

### notification
def notification():
    ### resource principal
    signer = oci.auth.signers.get_resource_principals_signer()
    notificationClient = oci.ons.NotificationDataPlaneClient(config={}, signer=signer)
    ### end resource principal
    
    # ### api key
    # config = oci.config.from_file('~/.oci/config')
    # notificationClient = oci.ons.NotificationDataPlaneClient(config)
    # ### end api key
    
    topic_ocid = "ocid1.onstopic.oc1.phx.amaaaaaassl65iqa26skdp5ee2w6jn7zrja7pxqbvqlvf2roy3lom4qki63a"
    bodyMessage = "An anomaly has been detected in your system."
    notificationMessage = {"default": "Anomaly Detection", "body": bodyMessage, "title": "Notification of Anomaly Detection."}

    
    notificationClient.publish_message(topic_ocid, notificationMessage)
 

def base64_decode(encoded):
    print(type(encoded))
    base64_bytes = encoded.encode('utf-8')
    message_bytes = base64.b64decode(base64_bytes)
    return message_bytes.decode('utf-8')

def handler(ctx, data: io.BytesIO = None):
    message_result = "Success"
    try:
        logs = json.loads(data.getvalue())
        
        for item in logs:
            if 'value' in item:
                item['value'] = base64_decode(item['value'])

            if 'key' in item:
                item['key'] = base64_decode(item['key'])
        
        
        result = anomaly_detect(logs)
        logging.getLogger().info(result.data)
        # tmp = {"result":result}
        # print(result.data)
        # if values != None:
            # message_result = logs[0]['values']
        #     # message_result = logs['data'][0]['value']
        
        if result.data != None:
            notification()
            message_result = "Notificated"
            
    except (Exception, ValueError) as ex:
        logging.getLogger().info('error parsing json payload: ' + str(ex))
        message_result = "error"
        raise
    # logging.getLogger().info("Got incoming request")
    
    return response.Response(ctx, response_data=json.dumps({"status": message_result}), headers={"Content-Type": "application/json"})
    