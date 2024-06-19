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

class Rocket3STO:
    def __init__(self, rocket_code) -> None:
        self.rocket_code = rocket_code
        self.db = db()

    def sync_sto_code(self, chunk): 
        self.db.getCollection(os.getenv("DB_COLLECTION_STO_BUBBLE")).update_many(
            chunk.get("Filters"), 
            {"$set": {
                "Status": constant.STATUS_NEW,
                "IsSelected": True,
                "STOCode": chunk.get("Code")
            }
        }) 

    def load_sku_line_maps(self):
        return self._maps
    
    def load_data_by_sku(self, sku,type):
        self._maps = []
        filters = {
            "RocketCode": self.rocket_code,
            "SKU": sku,
            "IsSelected": True,
            "Status": {"$ne": constant.STATUS_ERR},
            "IsDeleted": 0
        }
        if type == 'REANALYZE_STO_DISTRIBUTION': 
            filters =  {
                "Session": self.rocket_code,
                "IsDeleted": 0,
                "IsSelected": True,
                "Status": {"$nin": [constant.STATUS_ERR,constant.STATUS_CANCEL]},
                "SKU": sku,
            }
        cursor = self.db.getCollection(os.getenv("DB_COLLECTION_STO_BUBBLE")).find(filters)

        data = []
        for item in cursor:
            data.append([item.get("PackageType"), int(item.get("Qty"))]) # [0, 10] - Package type, Pick qty
            self._maps.append({
                "STOCode": item.get("STOCode"),
                "StoreCode": item.get("StoreCode"),
                "SKU": item.get("SKU"),
                "Uom": item.get("Uom"),
                "PackageType": item.get("PackageType"),
                "RefData":  item.get("RefData")
            })
        return data
    
    def load_data_by_store(self, store_code):
        self._maps = []
        filters = {
            "RocketCode": self.rocket_code,
            "StoreCode": store_code,
            "Status": {"$ne": constant.STATUS_GROUP},
            "IsDeleted": 0
        }
        cursor = self.db.getCollection(os.getenv("DB_COLLECTION_STO_BUBBLE")).find(filters)
        
        data = []
        for item in cursor:
            data.append([item.get("PackageType"), item.get("Qty")])
            self._maps.append(item["_id"])
        return data
         
    def load_stores(self):
        filters = {
            "RocketCode": self.rocket_code,
            "IsDeleted": 0
        }
        pipeline = [
            {"$match": filters},
            {"$group": {"_id": "$StoreCode"}}
        ]
        cursor = self.db.getCollection(
            os.getenv("DB_COLLECTION_STO_BUBBLE")).aggregate(pipeline=pipeline)
        stores = []
        for item in cursor:
            stores.append(item.get("_id"))
        
        return stores
    
    def load_skus(self,type):
        filters = {
            "IsDeleted": 0,
            "RocketCode": self.rocket_code,
            "IsSelected": True,
            "Status": {"$nin": [constant.STATUS_ERR,constant.STATUS_CANCEL]}
        }
        if type == 'REANALYZE_STO_DISTRIBUTION': 
            filters =  {
                "IsDeleted": 0,
                "Session": self.rocket_code,
                "IsSelected": True,
                "Status": {"$nin": [constant.STATUS_ERR,constant.STATUS_CANCEL]}
            }
        pipeline = [
            {"$match": filters},
            {"$group": {"_id": "$SKU", "Total": {"$sum": "$Qty"}}}
        ]
        cursor = self.db.getCollection(
            os.getenv("DB_COLLECTION_STO_BUBBLE")).aggregate(pipeline=pipeline)
        stores = []
        for item in cursor:
            stores.append({"SKU": item.get("_id"), "TotalQty": item.get("Total")})
        
        return stores

    
