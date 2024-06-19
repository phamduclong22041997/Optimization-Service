#
# @copyright
# Copyright (c) 2022 OVTeam
#
# All Rights Reserved
#
# Licensed under the MIT License;
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://choosealicense.com/licenses/mit/
#

import os
import requests
import json

def postFile(file_path,  options = {}):
    if os.environ["ROCKET_URL"] == "":
        return
    url = os.environ["ROCKET_URL"] + "/api/v2/upload/file"

    payload= options
    files=[
    ('file',(os.path.basename(file_path),open(file_path,'rb'), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))
    ]
    headers = {
    'Authorization': 'Bearer ' + os.environ["OPS_INTERNAL_TOKEN"]
    }

    response = requests.request("POST", url, headers=headers, data=payload, files=files)
    print(response.text)

def post_ops(url, data, timeout=10):
    if os.environ["OPS_URL"] == "":
        return
    warehouse_code = f"{data.get('WarehouseCode', '')}".lower()
    url = f"{os.environ['OPS_URL']}-{warehouse_code}/{url}"

    payload = json.dumps(data)
    headers = {
        'Authorization': 'Bearer ' + os.environ["OPS_INTERNAL_TOKEN"],
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload, timeout=timeout)
    return response.text

def post(url, body, header={}):
    headers = {
    # 'Authorization': 'Bearer '
    }
    response = requests.request("POST", url, data=body, headers=headers)
    return response.text