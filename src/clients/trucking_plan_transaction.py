#
# @copyright
# Copyright (c) 2024 OVTeam
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
import pytz
import json
from datetime import datetime, timedelta
from lib import db, request, utils
import hashlib

timeZone = pytz.timezone('Asia/Ho_Chi_Minh')

class AnalyzeTransaction:
    def __init__(self, warehouse_code, pickwave_session, db_name, request_by):
        self.warehouse_code = warehouse_code
        self.pickwave_session = pickwave_session
        self.db_name = db_name
        self.request_by = request_by
        self.zones = []
        self.options = {}
        self.db = db()

    def set_options(self, options):
        self.options = options
    
    def send_remote_request(self):
        self.db.close()

    def load_init(self):
        # self.rules = utils.load_rule(self.warehouse_site_id, "TRUCKING_PLAN")
        self.zones = self.load_zones()

    def analyze_process(self):
        self.load_init()
        
        for zone in self.zones:
            # Xoa du lieu truoc
            self.clean_results(zone, self.pickwave_session)
            self.analyze_by_zone(zone)
    
    def analyze_by_zone(self, zone_code, request_by = ""):
        data_pickwave , _needed_units, location = self._get_pickwave_unit(zone_code, self.pickwave_session)
        if len(_needed_units) == 0:
            return
        
        _point_head = self._get_pickwave_head(zone_code,location)
        _point_heads = self._get_pickwave_heads(zone_code)

        if not _point_head:
            return
        
        if request_by:
            self.request_by = request_by
        
        _current_stocks = self._load_pickwave_head_stock(self.pickwave_session, _point_heads)

        # Tru di ton hien co
        # for sku in _current_stocks:
        #     if sku not in _needed_units:
        #         continue
        #     _current_qty = _current_stocks.get(sku)
        #     _qty = _needed_units.get(sku, 0)

        #     if _qty <= _current_qty:
        #         _needed_units[sku] = 0
        #         continue
        #     _needed_units[sku] = _qty - _current_qty

        for line in _needed_units:
            if _needed_units[line] <= 0:
                continue
            
            keys = line.split("_")
            if len(keys) == 1:
                if line in _current_stocks:
                    if _current_stocks[line] <= 0:
                        continue
                    
                    _current_qty = _current_stocks.get(line, 0)
                    _qty = _needed_units.get(line, 0)
                    if _qty <= _current_qty:
                        _needed_units[line] = 0
                        _current_stocks[line] = _current_qty - _qty
                        continue
                    _needed_units[line] = _qty - _current_qty
                    _current_stocks[line] = 0
                continue
            
            sku = keys[0]
            for po_code in keys[1:]:
                if _needed_units[line] <= 0:
                    continue
                
                key = f"{sku}_{po_code}"
                if key in _current_stocks:
                    if _current_stocks[key] <= 0:
                        continue
                    
                    _current_qty = _current_stocks.get(key, 0)
                    _qty = _needed_units.get(line, 0)
                    
                    if _qty <= _current_qty:
                        _needed_units[line] = 0
                        _current_stocks[key] = _current_qty - _qty
                        continue
                    
                    _needed_units[line] = _qty - _current_qty
                    _current_stocks[key] = 0
                continue

        post_data = {
            "WarehouseCode": self.warehouse_code,
            "DataZone": {},
            "DataSKU": []
        }

        for key in _needed_units:
            if _needed_units.get(key, 0) <= 0:
                continue

            keys = key.split("_")
            sku = keys[0]
            po_list = []
            
            if len(keys) > 1:
                po_list = keys[1:]

            _key = f"{zone_code}_{key}".replace(".", "_")

            if sku not in post_data["DataSKU"]:
                post_data["DataSKU"].append(sku)
            
            post_data["DataZone"][_key] = {
                "ClientCode": "WIN",
                "ZoneCode": zone_code,
                "SKU": sku,
                "POList": po_list,
                "Qty": _needed_units.get(key)
            }
        
        if len(post_data["DataSKU"]) > 0:
            self.save_results(self.get_location_storage(post_data), _point_head, data_pickwave)

    def save_results(self, data, point_head, data_pickwave):
        now = datetime.now(timeZone)
        _save_data = []
        for obj in data:
            for item in obj.get("BINS", []):
                _src_location_label = item.get("SubLocationLabel", "")
                # _src_location_type = item.get("SubLocationType", "")
                if not _src_location_label:
                    _src_location_label = item.get("BIN")
                    # _src_location_type = item.get("LocationType")
                _obj = {
                    "ClientCode" : obj.get("ClientCode", "WIN"),
                    "WarehouseCode": self.options.get("WarehouseCode", ""),
                    "WarehouseSiteId": self.options.get("WarehouseSiteId", ""),
                    "ZoneCode": obj.get("ZoneCode"),
                    "SplitSessionCode": self.pickwave_session,
                    "LocationLabel" : item.get("BIN"),
                    "LocationType" : item.get("LocationType"),
                    "SrcLocationLabel" : _src_location_label,
                    "DstLocationLabel" : point_head,
                    "DstLocationType" : "Point",
                    "DstSubLocationLabel": "",
                    "Barcode" : item.get("Barcode", ""),
                    "SKU" : obj.get("SKU"),
                    "Qty" : item.get("Qty"),
                    "SuggestQty":  item.get("Qty"),
                    "Uom" : item.get("Uom"),
                    "PCB": data_pickwave.get(obj.get("SKU"))['PCB'],
                    "Status": "New",
                    "POCode": item.get("POCode"),
                    "ExpiredDate" : item.get("ExpiredDate", None),
                    "BestBeforeDate" : item.get("BestBeforeDate", None),
                    "ManufactureDate" : item.get("ManufactureDate", None),
                    "ReceiveDate" : item.get("ReceiveDate", None),
                    "CalendarDay": now.strftime("%Y%m%d"),
                    "CreatedDate": now,
                    "UpdatedDate": now,
                    "Type": "SUGGESTION",
                    "Ordering": item.get("Indexing", 0),
                    "CreatedBy": self.request_by,
                    "UpdatedBy": self.request_by,
                    "IsDeleted" : 0,
                    "__v": 0
                }
                if _obj.get("ExpiredDate"):
                    _obj["ExpiredDate"] =  utils.convertDateStringToDate(_obj.get("ExpiredDate"))
                    
                if _obj.get("BestBeforeDate"):
                    _obj["BestBeforeDate"] =  utils.convertDateStringToDate(_obj.get("BestBeforeDate")) 
                    
                if _obj.get("ManufactureDate"):
                    _obj["ManufactureDate"] =  utils.convertDateStringToDate(_obj.get("ManufactureDate")) 

                if _obj.get("ReceiveDate"):
                    _obj["ReceiveDate"] =   utils.convertDateStringToDate(_obj.get("ReceiveDate")) 
                
                _save_data.append(_obj)

        self.save_data(_save_data)

    def save_data(self, data = []):
        if len(data) == 0:
            return
        self.db.getCollection(os.getenv("DB_COLLECTION_TRUCKING_PLAN_TRANSACTION"), db_name=self.db_name).insert_many(data)
    
    def clean_results(self, zone_code, pickwave_session):
        filters = {
            "SplitSessionCode" : pickwave_session,
            "ZoneCode": zone_code,
            "Type": "SUGGESTION",
            "Status": {"$in": ["New", "Processing"]},
            "IsDeleted": 0
        }
        save_data = {
            "Status": "Canceled",
            "UpdatedDate": datetime.now(timeZone)
        }
        self.db.getCollection(os.getenv("DB_COLLECTION_TRUCKING_PLAN_TRANSACTION"), db_name=self.db_name).update_many(filters, {"$set": save_data})

    # Lấy danh sách vị trí tồn kho
    def get_location_storage(self, post_data):
        url = "api/v1/splitSession/getProductStorage"
        resp = json.loads(request.post_ops(url, post_data))
        if resp.get("Status"):
            return list(resp.get("Data", {}).values())
        return None
    
    def get_transfer_session_code(self, pickwave_session):
        filters = {
            "SplitSessionCode": pickwave_session,
            "TransferSessionCode": {"$nin": ["", None]},
            "IsDeleted": 0
        }
        resp = []
        cursor = self.db.getCollection(os.getenv("DB_COLLECTION_TRUCKING_PLAN_TRANSACTION"), db_name=self.db_name).find(filters)
        for item in cursor:
            key = item.get("TransferSessionCode", "")
            if key == "":
                continue
            if key not in resp:
                resp.append(key)
        return resp

    
    # Lấy tồn hiện tại của point đầu hàng
    def _load_pickwave_head_stock(self, session_code, location_labels):
        trans = self.get_transfer_session_code(session_code)
        if len(trans) == 0:
            return {}
        
        last3Days = datetime.today() - timedelta(days=4)
        filters = {
            "CreatedDate": {"$gt": last3Days},
            "JobType": {"$in": ["JOB_TRANSFER_DIRECT", "JOB_TRANSFER_PALLET"]},
            "DstLocationLabel": {"$in": location_labels},
            "SessionCode": {"$in": trans},
            "IsDeleted": 0
        }
        pickedQtys = {}
        cursor = self.db.getCollection("INV.ProductItemTransactions", db_name=self.db_name).find(filters, {"SKU": 1, "Qty": 1, "BaseQty": 1, "POCode": 1})
        for item in cursor:
            _key = "{0}_{1}".format(item.get("SKU"), item.get("POCode"))
            if _key not in pickedQtys:
                pickedQtys[_key] = 0
            
            pickedQtys[_key] += item.get("BaseQty", 0)
        
        return pickedQtys
    
    # Lấy tất cả hàng cần chia
    def _get_pickwave_unit(self, zone_code, pickwave_session, restrict_po_line = True):
        filters = {
            "ZoneCode": zone_code,
            "SplitSessionCode": pickwave_session,
            "Status" : {"$ne": "Canceled"},
            "IsDeleted": 0
        }
        resp = {}
        data_sku = {}
        so_lists = []
        location = None
        cursor = self.db.getCollection(os.getenv("DB_COLLECTION_SPLITLIST_DETAILS"), db_name=self.db_name).find(filters, {"SKU": 1, "Qty": 1, "ScanQty": 1, "POList": 1, "ConvertionRate":1, "SOCode": 1, "LocationLabel": 1 })
        for item in cursor:
            _key = item.get("SKU")
            if _key not in data_sku:
                data_sku[_key] = {
                        "PCB": item.get('ConvertionRate')
                    }
            if not restrict_po_line:
                _po_list = item.get("POList", [])

                if len(_po_list) > 0:
                    _key = "{0}_{1}".format(item.get("SKU"), "_".join(_po_list))
                
                if _key not in resp:
                    resp[_key] = 0
                
                resp[_key] = resp[_key] + (item.get("Qty", 0) - item.get("ScanQty", 0))
            else:
                if item.get("SOCode") not in so_lists:
                    so_lists.append(item.get("SOCode"))
            
            if location == None:
               location = item.get("LocationLabel")
        
        if restrict_po_line == True and len(so_lists) > 0:
            resp = self._get_pickwave_so_units(so_lists)
    
        return data_sku, resp, location
    
    def _get_pickwave_so_units(self, sos):
        # last3Days = datetime.today() - timedelta(days=3)
        # filters = {
        #     "CreatedDate": {"$gt": last3Days},
        #     "JobType": "SO_SPLITING",
        #     "SOCode":  {"$in": sos},
        #     "IsDeleted": 0
        # }
        # pickedQtys = {}
        # cursor = self.db.getCollection("INV.ProductItemTransactions", db_name=self.db_name).find(filters, {"SKU": 1, "Qty": 1, "BaseQty": 1, "POCode": 1})
        # for item in cursor:
        #     _key = "{0}_{1}".format(item.get("SKU"), item.get("POCode"))
        #     if _key not in pickedQtys:
        #         pickedQtys[_key] = 0
            
        #     pickedQtys[_key] += item.get("BaseQty", 0)
        
        filters = {
            "SOCode": {"$in": sos},
            "IsDeleted": 0
        }
        resp = {}
        cursor = self.db.getCollection(os.getenv("DB_COLLECTION_INV_SO_DELIVERY")).find(filters, {"STOData": 1, "SOCode": 1})
        for item in cursor:
            for obj in item.get("STOData", {}).get("Details", []):
                _key = "{0}_{1}".format(obj.get("SKU"), obj.get("PORef"))
                if _key not in resp:
                    resp[_key] = 0
                resp[_key] += obj.get("Qty", 0)
        
        # for key in resp:
        #     resp[key] -= pickedQtys.get(key, 0)
        
        return resp
    
    # Lấy point đầu hàng của zone
    def _get_pickwave_head(self, zone_code, location):
        filters = {
            "ZoneCode": zone_code,
            "Type" : "PickwavePoint",
            "IsDeleted": 0,
            "Code": location
        }
        pick_point = self.db.getCollection(os.getenv("DB_COLLECTION_OPS_POINT"), db_name=self.db_name).find_one(filters, {"X": 1})
            
            
        filters = {
            "ZoneCode": zone_code,
            "Type" : "PickwaveHead",
            "IsDeleted": 0
        }
        if pick_point:
            filters['X'] = pick_point.get("X")
            
        obj = self.db.getCollection(os.getenv("DB_COLLECTION_OPS_POINT"), db_name=self.db_name).find_one(filters, {"Code": 1})
        
        if not obj:
            return None
        
        return obj.get("Code")
    
    def _get_pickwave_heads(self, zone_code):
        filters = {
            "ZoneCode": zone_code,
            "Type" : "PickwaveHead",
            "IsDeleted": 0
        }
        resp = []
            
        cursor = self.db.getCollection(os.getenv("DB_COLLECTION_OPS_POINT"), db_name=self.db_name).find(filters, {"Code": 1})
        
        for obj in cursor:
            resp.append(obj.get("Code"))
        
        return resp
    
    # Lấy tất cả zone chia hàng của kho
    def load_zones(self):
        result = []
        filters = {
            "Type": "PickwaveZone",
            "IsDeleted": 0
        }
        cursor = self.db.getCollection(os.getenv("DB_COLLECTION_ZONE"), db_name=self.db_name).find(filters, {"Code": 1}).sort("ZOrder", 1)
        for item in cursor:
            result.append(item.get("Code"))
        return result