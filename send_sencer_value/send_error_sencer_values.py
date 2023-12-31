import io
import os
import json
import logging
import requests
from fdk import response

import oci
from oci.functions import *

function_endpoint = "https://7eabqyl3ymq.us-phoenix-1.functions.oci.oraclecloud.com"
compartment_id = "ocid1.compartment.oc1..aaaaaaaavu633dop4qlvss3ebdvrzo6hwnr4g5e7s42frmlfvlsjpnyss7xa"
app_id = "ocid1.fnapp.oc1.phx.aaaaaaaasxeuzsx5wzpgfuz5h7q42o6g2seq5pvxc3ctbnut47eabqyl3ymq"
function_ocid = "ocid1.fnfunc.oc1.phx.aaaaaaaa27mmpuv7pock7apbwgwtav7euldhgedwh73wb3gzrw6kjehzi2cq"
config = oci.config.from_file('~/.oci/config')



json_file_path = "/home/opc/bbs_anomaly_detection/data/error_data"
json_files = [f for f in os.listdir(json_file_path) if f.endswith('.json')]

for file_name in json_files:
    file_path = os.path.join(json_file_path, file_name)
    print(file_path)
    
    with open(file_path, 'r') as json_file:
        function_body = json_file.read()

    client = oci.functions.FunctionsInvokeClient(config=config, service_endpoint=function_endpoint)
    response = client.invoke_function(function_id=function_ocid, invoke_function_body=function_body)

print(function_body)
print(response.data.text, flush=True)
