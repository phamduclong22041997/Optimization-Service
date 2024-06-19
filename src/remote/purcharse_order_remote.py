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

from . import request

def create_po_request(data):
    post_data = {
        "RocketCode": data.get("RocketCode"),
        "RequestBy": data.get("RequestBy")
    }
    uri = "/api/v2/rocket-planning/createPORequest"
    print(33)
    print(post_data)
    request.post(uri, post_data)
