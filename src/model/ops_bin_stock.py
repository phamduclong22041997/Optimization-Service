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
from pickle import FALSE
from lib import db

class BinStock:
    def __init__(self, warehouse_code, options, client_code = "") -> None:
        self.warehouse_code = warehouse_code

        db_name = os.getenv("DB_NAME_{0}".format(warehouse_code))
        if client_code:
            _db_name = os.getenv("DB_NAME_{0}_{1}".format(warehouse_code, client_code))
            if _db_name:
                db_name = _db_name
        
        self.db = db(db_name)
        self.options = options
    
    def load_available_stock(self, warehouse_site_id, sku):
        filters = {
            # "WarehouseSiteId": warehouse_site_id,
            "SKU": sku,
            "Qty": {"$gt": 0},
            "IsDeleted": 0
        }
        pickable_locations = self.load_pickable_location(warehouse_site_id)
        cursor = self.db.getCollection(os.getenv("DB_COLLECTION_OPS_STORAGE")).find(filters)
        
        data = {}
        for item in cursor:
            loc = pickable_locations.get(item.get("LocationLabel"))
            if loc == None:
                continue
            
            key = "{0}_{1}".format(item.get("LocationLabel"), item.get("SubLocationLabel"))
            if key not in data:
                if(self.options['allow_analyze_distribution']) == False:
                    data[key] = {
                        "LocationLabel": item.get("LocationLabel"),
                        "LocationType": item.get("LocationType"),
                        "Qty": int(self.options['max_weight']),
                        "SubLocationLabel": item.get("SubLocationLabel"),
                        "LocationIndexing": loc
                    }
                else:
                    data[key] = {
                        "LocationLabel": item.get("LocationLabel"),
                        "LocationType": item.get("LocationType"),
                        "Qty": 0,
                        "SubLocationLabel": item.get("SubLocationLabel"),
                        "LocationIndexing": loc
                    }
                    data[key]["Qty"] += int(item.get("Qty"))

                if item.get("PendingOutQty") != None:
                    data[key]["Qty"] -= int(item.get("PendingOutQty"))
        data = list(data.values())
        if len(data) > 0: 
            data = sorted(data, key=lambda x: x["LocationIndexing"], reverse=True)
        return data
    
    def check_available_stock(self, warehouse_site_id, sku, request_qty):
        filters = {
            "WarehouseSiteId": warehouse_site_id,
            "SKU": sku,
            "Qty": {"$gt": 0},
            "IsDeleted": 0
        }
        pickable_locations = self.load_pickable_location(warehouse_site_id)

        cursor = self.db.getCollection(os.getenv("DB_COLLECTION_OPS_STORAGE")).find(filters)
        
        data = {}
        total_qty = 0
        for item in cursor:
            loc = pickable_locations.get(item.get("LocationLabel"))
            if loc == None:
                continue
            
            total_qty += item.get("Qty")
            if item.get("PendingOutQty") != None:
                total_qty -= item.get("PendingOutQty")

        return total_qty >= request_qty
    
    def load_pickable_location(self, warehouse_site_id):
        filters = {
            "Type": "Pickable",
            # "WarehouseSiteId": warehouse_site_id, // Rural Bin Point không theo SiteId
            "IsActived": 1,
            "IsDeleted": 0
        }
        if self.options['pick_zone'] != None and len(self.options['pick_zone'])> 0:
            filters['ZoneCode'] = {"$in": self.options['pick_zone']}
        data = {}
        cursor = self.db.getCollection(os.getenv("DB_COLLECTION_OPS_BIN")).find(filters, {"Code": 1, "__indexing": 1})
        for item in cursor:
            data[item.get("Code")] = item.get("__indexing") if item.get("__indexing") != None else 0

        cursor = self.db.getCollection(os.getenv("DB_COLLECTION_OPS_POINT")).find(filters, {"Code": 1, "__indexing": 1})
        for item in cursor:
            data[item.get("Code")] = item.get("__indexing") if item.get("__indexing") != None else 0
        
        return data
    
    def locked_by_location(self, data):
        self.db.getCollection(os.getenv("DB_COLLECTION_OPS_STORAGE")).update_many(
            data.get("Filters"), 
            {"$inc": {
                "PendingOutQty": data.get("Qty")
            }
        }) 

