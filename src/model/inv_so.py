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
class INV_SO:
    def __init__(self) -> None:
        self.db = db()
        
    def update(self,options,data): 
        filters = {
           "IsDeleted": 0,
           "SOCode": {"$in" : options["STOList"] },
           "SiteId": options["StoreCode"]
        }
        self.db.getCollection(os.getenv("DB_COLLECTION_INV_SO")).update_many(filters, {"$set": data})