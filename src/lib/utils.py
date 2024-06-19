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
import random
import string
import pytz
from datetime import datetime
from lib import db

TIME_ZONE = pytz.timezone('Asia/Ho_Chi_Minh')

def load_vendor(skuList, warehouse_site_id, client_code):
    _db = db(db_name=os.getenv("DB_ADMIN"))
    collection = _db.getCollection(os.getenv("DB_COLLECTION_VENDOR"))
    filters = {
        "ClientCode": client_code,
        "SiteId": warehouse_site_id,
        "SKU": {"$in": skuList},
        "IsDeleted": 0
    }
    results = {}
    cursor = collection.find(filters)
    for item in cursor:
        key = "{0}".format(item.get("SKU"))
        results[key] = {
            "VendorId": item.get("Code"),
            "VendorName": item.get("Name"),
            "VSR": item.get("VSR")
        }
        if results[key]["VSR"] == None:
            results[key]["VSR"] = ""
    return results


def load_warehouses():
    _db = db(db_name=os.getenv("DB_ADMIN"))
    collection = _db.getCollection("WH.Warehouses")
    filters = {
        "Status": "Active",
        "IsDeleted": 0
    }
    results = {}
    cursor = collection.find(filters)
    for item in cursor:
        if 'Sites' in item:
            for obj in item.get('Sites'):
                if (obj.get("Code") in results) == False:
                    results[obj.get("Code")] = {
                        "WarehouseCode": item.get("Code"),
                        "WarehouseName": item.get("Name"),
                        "WarehouseType": obj.get("Type"),
                        "WarehouseSiteId": obj.get("Code"),
                        "WarehouseSiteName": obj.get("Name")
                    }
    _db.close()
    return results


def load_stores(data,client_code):
    _db = db(db_name=os.getenv("DB_ADMIN"))
    collection = _db.getCollection("GEO.Stores")
    filters = {
        "Code": {"$in": data},
        "IsDeleted": 0,
        "IsActived": 1
    }
    if client_code != None:
        filters['ClientCode'] = client_code
    results = {}
    cursor = collection.find(filters)
    for item in cursor:
        _address = item.get("Address")
        if _address == None:
            _address = {}
        results[item.get("Code")] = {
            "Name": item.get("Name"),
            "Address": {
                "Region": _address.get("Region"),
                "Province": _address.get("Province"),
                "District": _address.get("District"),
                "Ward": _address.get("Ward")
            }
        }
    _db.close()
    return results


def load_warehouse_coverage(data):
    _db = db(db_name=os.getenv("DB_ADMIN"))
    collection = _db.getCollection("GEO.Stores")
    filters = {
        "StoreCode": {"$in": data},
        "IsDeleted": 0,
        "Priority": 1
    }
    results = {}
    cursor = collection.find(filters)
    for item in cursor:
        results[item.get("StoreCode")] = {
            "WarehouseCode": item.get("WarehouseCode"),
            "WarehouseName": item.get("WarehouseName"),
            "WarehouseSiteId": item.get("WarehouseSiteId"),
            "WarehouseSiteName": item.get("WarehouseSiteName")
        }
    _db.close()

    return results


def load_mhu(sku, warehouse_site_id):
    _db = db(db_name=os.getenv("DB_ADMIN"))
    collection = _db.getCollection(os.getenv("DB_COLLECTION_PRODUCT"))
    filters = {
        "SKU": sku,
        "WarehouseSiteId": warehouse_site_id,
        "IsDeleted": 0
    }
    mhu = 20
    obj = collection.find_one(filter=filters)
    if obj != None:
        mhu = obj.get("MHU")
    return mhu


def load_convert_rate(skuList, client_code=None, warehouse_site_id=None):
    filters = {
        "SKU": {"$in": skuList},
        "IsDeleted": 0
    }
    if client_code != None:
        filters["ClientCode"] = client_code

    if warehouse_site_id != None:
        filters["WarehouseSiteId"] = warehouse_site_id
    cursor = db(db_name=os.getenv("DB_ADMIN")).getCollection(os.getenv("DB_COLLECTION_PRODUCT_UNIT")).find(
        filters, {"Numerator": 1, "Denominator": 1, "SKU": 1, "Uom": 1, "BaseUom": 1})
    results = {}
    for item in cursor:
        key = "{0}_{1}".format(item.get("SKU"), item.get("Uom"))
        results[key] = {
            "Rate": item.get("Numerator")/item.get("Denominator"),
            "Uom": item.get("BaseUom")
        }
    return results
   
def load_product_weight(skuList, client_code=None, warehouse_site_id=None):
    filters = {
        "SKU": {"$in": skuList},
        "IsDeleted": 0
    }
    if client_code != None:
        filters["ClientCode"] = client_code
    if warehouse_site_id != None:
        filters["WarehouseSiteId"] = warehouse_site_id
    cursor = db(db_name=os.getenv("DB_ADMIN")).getCollection(os.getenv("DB_COLLECTION_BARCODE")).find(
            filters,{"Volume" : 1,"GrossWeight" : 1,"SKU": 1,"Uom": 1})
    results = {}
    for item in cursor:
        key = "{0}_{1}".format(item.get("SKU"), item.get("Uom"))
        results[key] = {
            "Weight": item.get("GrossWeight"),
            "Volume": item.get("Volume")
        }
    return results

def load_products(skuList, client_code=None, warehouse_site_id=None):
    filters = {
        "SKU": {"$in": skuList},
        "IsDeleted": 0
    }
    if client_code != None:
        filters["ClientCode"] = client_code
    if warehouse_site_id != None:
        filters["WarehouseSiteId"] = warehouse_site_id
    cursor = db(db_name=os.getenv("DB_ADMIN")).getCollection(os.getenv("DB_COLLECTION_PRODUCT")).find(
            filters,{"SKU" : 1,"Name" : 1,"MHU": 1,"PCB":1})
    results = {}
    for item in cursor:
        key = "{0}".format(item.get("SKU"))
        results[key] = {
            "SKU": item.get("SKU"),
            "Name": item.get("Name"),
            "MHU": item.get("MHU"),
            "PCB": item.get("PCB")
        }
    return results

def is_int(x):
    if x == None or x == 0:
        return False
    if x % 1 == 0:
       return True
    else:
        return False
    
def parse_float(value):
    if value == None or value=='' or value == "":
        return 0
    return float(value)

def parse_int(value):
    if value == None or value == "" or value =='':
        return 0
    return int(value)

def generate_code(object_type="STO", digits=6):
    char_set = string.ascii_uppercase + string.digits
    current_time = datetime.now().strftime("%y%m%d%M")
    last_str = ''.join(random.sample(char_set * digits, digits))
    return object_type + current_time + last_str

def generate_sto_code(object_type="STO", digits=12):
    char_set = string.ascii_uppercase + string.digits
    last_str = ''.join(random.sample(char_set * digits, digits))
    return object_type + last_str

def generate_so_code(object_type="SO",client_code = "WIN" ,digits=10):
    char_set = string.ascii_uppercase + string.digits
    last_str = ''.join(random.sample(char_set * digits, digits))
    return object_type + client_code  + last_str

def current_date():
    return datetime.now(TIME_ZONE)

def calendar_day():
    return datetime.now(TIME_ZONE).strftime("%Y%m%d")

def load_rule(warehouse_site_id, type):
    config =  {
            "max_sku": 3,
            "max_unit": 150,
            "allow_packing_type": True,
            "pick_zone": []
        }
    filters = {
        "WarehouseSiteId": warehouse_site_id,
        "Name": type
    }
    obj = db(db_name=os.getenv("DB_NAME")).getCollection(os.getenv("DB_COLLECTION_RULE")).find_one(
            filters)
    if obj:
        config["max_sku"] = obj["Value"].get("MAX_SKU")
        config["min_sku"] = obj["Value"].get("MIN_SKU")
        config["max_unit"] = obj["Value"].get("MAX_UNIT")
        config["allow_packing_type"] = obj["Value"].get("ALLOW_PACKING_TYPE_SKU")
        config["pick_zone"] = obj["Value"].get("DEFAULT_PICK_ZONE")
        config["allow_group_inventory"] = obj["Value"].get("ALLOW_GROUP_INVENTORY")
        config['allow_analyze_distribution'] = obj["Value"].get("ALLOW_ANALYZE_DISTRIBUTION")
        config['max_weight'] = obj["Value"].get("MAX_WEIGHT")

        config['solid_by_zone'] = obj['Value'].get("SOLID_BY_ZONE")
        config['solid_by_sku_line'] = obj['Value'].get("SOLID_BY_SKU_LINE", False)
    return config

def gen_key(): 
    current_time = datetime.now(pytz.timezone(str(TIME_ZONE)))
    _keygen = current_time.strftime('%Y%m%d%H%M%S%f')
    return _keygen

def gen_code_time():
    current_time = datetime.now(pytz.timezone(str(TIME_ZONE)))
    year = str(current_time.year)[-2:]  # Lấy 2 chữ số cuối của năm
    month = '{:02d}'.format(current_time.month)  # Đảm bảo lấy 2 chữ số cho tháng
    day = '{:02d}'.format(current_time.day)  # Đảm bảo lấy 2 chữ số cho ngày
    hour = '{:02d}'.format(current_time.hour)  # Đảm bảo lấy 2 chữ số cho giờ
    minute = '{:02d}'.format(current_time.minute)  # Đảm bảo lấy 2 chữ số cho phút
    second = '{:02d}'.format(current_time.second)  # Đảm bảo lấy 2 chữ số cho giây
    code = year + month + day + hour + minute + second
    return code

def convertDateStringToDate(date, format =  "%Y-%m-%dT%H:%M:%S.%f%z"):
    if date == None:
        return None
    _date = datetime.strptime(date, format)
    return _date