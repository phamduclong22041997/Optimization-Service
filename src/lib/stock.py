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
from lib import db

def load_promotion_stock(promotion_code, po_code = None):
    collection = db().getCollection(os.getenv("DB_COLLECTION_PO"))
    filters = {
        "PromotionCode": promotion_code,
        "IsDeleted": 0,
        "IsAnalyzedForSTO": 0,
        "Status": "Finished"
    }

    if po_code:
        filters["POCode"] = po_code

    data = {}
    poFinished = []
    ref_key_list = []
    cursor = collection.find(filters, {"Details": 1, "POCode": 1,"Stocks": 1 ,"Status": 1,"RefKey": 1})
    for obj in cursor:
        if obj.get("Status") ==  'Finished':
            if obj.get("RefKey") != None:
                ref_key_list.append(obj.get("RefKey"))
                poFinished.append(obj.get("POCode"))
            for item,value in obj.get("Stocks").items():    
                key = "{0}".format(item)
                if key not in data:
                    data[key] = {
                        "Qty": 0,
                        "POList": [],
                        "PORef": ''
                    }
                qty = 0
                qty = value.get("ReceiptQty") 
                if qty != None:
                    data[key]["Qty"] += qty
                    data[key]["PORef"] = obj.get("POCode")
                    if obj.get("POCode") not in data[key]["POList"]:
                        data[key]["POList"].append(obj.get("POCode"))
        else:
            for item in obj.get("Details"):
                ref_key_list.append(obj.get("RefKey"))
                if item.get("SKU") not in data:
                    data[item.get("SKU")] = {
                        "RefKey": obj.get("Details"),
                        "Qty": 0,
                        "POList": []
                    }
                qty = 0
                qty = item.get("BaseQty") 
                if qty != None:
                    data[item.get("SKU")]["Qty"] += qty
                    if obj.get("POCode") not in data[item.get("SKU")]["POList"]:
                        data[item.get("SKU")]["POList"].append(obj.get("POCode"))
    return data,ref_key_list,poFinished