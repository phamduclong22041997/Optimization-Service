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
from lib import db, constant
from datetime import datetime
import pytz
timeZone = pytz.timezone('Asia/Ho_Chi_Minh')
class Rocket3Set:
    def __init__(self, rocket_code) -> None:
        self.rocket_code = rocket_code
        self.db = db()

    def update(self,options,data): 
        filters = {
           "IsDeleted": 0, 
            "Code": self.rocket_code,    
        }
        if options["name"] != None: 
           filters["Name"] = options["name"]
        self.db.getCollection(os.getenv("BD_COLLECTION_DEMAND_SET")).update_one(filters, {"$set": data})

    def create(self,options): 
        self.db.getCollection(os.getenv("BD_COLLECTION_DEMAND_SET")).insert_one({
                "Name": options["Name"],
                "ClientCode": options["ClientCode"],
                "WarehouseCode": options["WarehouseCode"],
                "WarehouseSiteId": options["WarehouseSiteId"],
                "Code": self.rocket_code,
                "Status": options['Status'],
                "SourceType": "RocketV3",
                "CreatedBy": options["RequestBy"],
                "UpdatedBy": options["RequestBy"],
                "CreatedDate": datetime.now(timeZone),
                "UpdatedDate": datetime.now(timeZone),
                "IsDeleted": 0,
                "__vjob_priority": 889
            })
        return True

    