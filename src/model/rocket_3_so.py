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
class Rocket3SO:
    def __init__(self, rocket_code) -> None:
        self.rocket_code = rocket_code
        self.db = db()
        self.rocket_list = []
        self.sto_list = []

    def create(self, chunk): 
        self.db.getCollection(os.getenv("DB_COLLECTION_SO_BUBBLE")).insert_many(chunk)

    def load_data_by_store(self, store_code):
        self._maps = []
        filters = {
            "Session":   self.rocket_code,
            "StoreCode": store_code,
            "IsDeleted": 0,
            "SOCode": {"$in" : ['',None]},
            "Qty": {"$gt": 0},
            "IsSelected": True,
            "Status": constant.STATUS_CREATE_STO,
        }
        pipeline = [
            {"$match": filters},
            {"$group": {
                "_id": {"SKU": "$SKU", "PackageType": "$PackageType", "Indexing": "$Indexing"}, 
                "Qty": {"$sum": "$Qty"},
                "STOList": { "$addToSet": "$STOCode" }
            }},
            {"$sort": {"_id.Indexing": 1}}
        ]
        cursor = self.db.getCollection(os.getenv("DB_COLLECTION_SO_BUBBLE")).aggregate(pipeline=pipeline)

        data = []
        for item in cursor:
            data.append([item.get("_id").get("PackageType"),item.get("_id").get("SKU"),item.get("Qty"),item.get("STOList")])
            self._maps.append(item["_id"].get("SKU"))
        return data
    
    def load_stores(self):
        filters = {
            "Session":   self.rocket_code,
            "IsDeleted": 0,
            "SOCode": {"$in" : ['',None]},
            "Qty": {"$gt": 0},
            "IsSelected": True,
            "Status": constant.STATUS_CREATE_STO,
        }
        pipeline = [
            {"$match": filters},
            {"$group": {
                "_id": "$StoreCode",
                "RocketList": { "$addToSet": "$RocketCode" },
                "STOList": { "$addToSet": "$STOCode" }
                }
            }
        ]
        cursor = self.db.getCollection(
            os.getenv("DB_COLLECTION_SO_BUBBLE")).aggregate(pipeline=pipeline)
        stores = []
        for item in cursor:
            if len(self.rocket_list) == 0: 
                self.rocket_list = item.get("RocketList")
            else:
                self.rocket_list  = self.rocket_list + item.get("RocketList")
           
            if len(self.sto_list) == 0: 
                self.sto_list = item.get("STOList")
            else:
                self.sto_list  = self.sto_list + item.get("STOList")

            stores.append(item.get("_id"))
        self.rocket_list = list(set(self.rocket_list))
        self.sto_list = list(set(self.sto_list))

        return {
          "stores" : stores,
          "rocket_list": self.rocket_list,
          "sto_list": self.sto_list
        }

    def load_sku_line_maps(self):
        return self._maps

    def sync_so_code(self, chunk): 
        self.db.getCollection(os.getenv("DB_COLLECTION_SO_BUBBLE")).update_many(
            chunk.get("Filters"), 
            {"$set": {
                "SOCode": chunk.get("SOCode")
            }
        }) 

    
    def update_to_unselected(self, data): 
        self.db.getCollection(os.getenv("DB_COLLECTION_SO_BUBBLE")).update_many(
            data.get("Filters"), 
            {"$set": {
                 "IsSelected": False,
                 "Note": ["Không đủ điều kiện để gom STO"]
            }
        }) 

    def remove(self, filters): 
        self.db.getCollection(os.getenv("DB_COLLECTION_SO_BUBBLE")).delete_many(filters) 
    
    def load_stos(self):
        self._maps = []
        filters = {
            "RocketCode": self.rocket_code,
            "IsDeleted": 0,
            "IsSelected": False,
            "Status": constant.STATUS_NEW,
        }
        pipeline = [
            {"$match": filters},
            {"$group": {"_id": "$STOCode"}},
        ]
        cursor = self.db.getCollection(os.getenv("DB_COLLECTION_SO_BUBBLE")).aggregate(pipeline=pipeline)

        data = []
        for item in cursor:
            data.append(item.get("_id"))
        return data
    
    def update(self, data): 
        self.db.getCollection(os.getenv("DB_COLLECTION_SO_BUBBLE")).update_many(
            data.get("Filters"), 
            {"$set": data.get('SaveData')
        }) 
