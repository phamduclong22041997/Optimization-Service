#
# @copyright
# Copyright (c) 2023 OVTeam
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

def post(api,  options = {}):
    if os.environ["ROCKET_URL"] == "":
        return
    url = os.environ["ROCKET_URL"] + api

    payload= options
    headers = {
    'Authorization': 'Bearer ' + os.environ["OPS_INTERNAL_TOKEN"]
    }

    print(44)

    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.text)