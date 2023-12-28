import io
import json
import logging
from fdk import response
import pandas as pd
from datetime import datetime, date, timedelta 
import time
import base64
import os
import oci
from oci.ai_anomaly_detection.models import *
from oci.ai_anomaly_detection.anomaly_detection_client import AnomalyDetectionClient

### anomaly_detection
def anomaly_detect(logs_value):
    ### functions
    signer = oci.auth.signers.get_resource_principals_signer()
    ad_client = AnomalyDetectionClient(config={}, signer=signer)
    logging.getLogger().info(logs_value)
    
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
    
    # values_str = logs_value[0]['value']
    values_dict = json.loads(logs_value)
    logging.getLogger().debug(values_dict)
    value_time = values_dict[0]['timestamp']
    logging.getLogger().info(value_time)
    df_timestamp = pd.DataFrame(data = [values_dict[0]['timestamp']], columns=timeName)
    df_values = pd.DataFrame(data = [values_dict[0]['values']], columns=signalNames)
    
    df_cell = pd.concat([df_timestamp, df_values], axis=1)
    df = pd.concat([df, df_cell], axis=0)
    
    logging.getLogger().info('df ok')
    # df['timestamp'] = df['timestamp'].apply(lambda x: x.strftime('%Y-%m-%dT%H:%M:%SZ'))
    logging.getLogger().info('timestamp ok')
    
    # Now create the Payload from the dataframe
    payloadData = []
    for index, row in df.iterrows():
        timestamp = datetime.strptime(row['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
        values = list(row[signalNames])
        dItem = DataItem(timestamp=timestamp, values=values)
        payloadData.append(dItem)
    
    logging.getLogger().info(df.head())
    inline = InlineDetectAnomaliesRequest(model_id=model_id, request_type="INLINE", signal_names=signalNames, data=payloadData)
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
        
        logging.getLogger().info(item['value'])
        result = anomaly_detect(item['value'])
        logging.getLogger().info(result.data)
        
        if result.data != None:
            notification()
            message_result = "Notificated"
            
    except (Exception, ValueError) as ex:
        logging.getLogger().info('error parsing json payload: ' + str(ex))
        message_result = "error"
        raise
    
    return response.Response(ctx, response_data=json.dumps({"status": message_result}), headers={"Content-Type": "application/json"})
    