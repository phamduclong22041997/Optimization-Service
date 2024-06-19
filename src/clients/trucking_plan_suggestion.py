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
import pandas as pd
import numpy as np
import json
from datetime import datetime
from lib import db, utils, request

timeZone = pytz.timezone('Asia/Ho_Chi_Minh')

# sort asc by Qty
def order_by_qty(item):
    return item["Qty"]

# sort asc by SKU
def order_by_sku(item):
    return item["SKU"]

class AnalyzePickwave:
    def __init__(self, warehouse_code, warehouse_site_id, rocket_code, request_by, keygen = None,picking_type = 0):
        self.warehouse_code = warehouse_code
        self.warehouse_site_id = warehouse_site_id
        self.rocket_code = rocket_code
        self.request_by = request_by
        self.keygen = keygen
        self.db = db()
        self.code_by_time = utils.gen_code_time(),
        self.code_by_time = str(self.code_by_time)
        self.picking_type = picking_type
        self.is_check_picking_type = picking_type in ['Even','Odd']
        self.solid_by_sku_line = False

    def send_remote_request(self):
        self.db.close()

    def load_init(self):
        self.rules = utils.load_rule(self.warehouse_site_id, "TRUCKING_PLAN")
        self.points = self._load_pickwave_point()
        self.solid_by_sku_line = self.rules.get("solid_by_sku_line", False)
        self.ex_points = self._load_pickwave_point(ver=2) if self.solid_by_sku_line else {}

        self.clean_results()

    def analyze_process(self):
        self.load_init()
        df = self.load_data()

        df2 = df.groupby(['StoreCode', 'SKU', 'PalQty'], as_index=False).agg({'Qty':'sum'}).rename(columns={'Qty':'TotalQty'})
        df2["Pal"] = df2['TotalQty'].div(df2["PalQty"])

        df2 = df2.groupby(['StoreCode'], as_index=False).agg({"Pal": "sum"}).rename(columns={"Pal": "TotalPal"})
        df2['TotalPal'] = df2['TotalPal'].apply(np.ceil)

        _pallets = {}

        for i in range(len(df2)):
            _pallets[df2.loc[i, "StoreCode"]] = df2.loc[i, "TotalPal"]
        
        df2 = None

        df["TotalPal"] = df['Qty'].div(df["PalQty"])
        # df = df.drop('PalQty', axis=1)

        # Loop và rãi pallet vào SO: NEO
        save_data = {}
        now = datetime.now(timeZone)

        for i in range(len(df)):
            _total_pal = _pallets.get(df.loc[i, "StoreCode"], 1)

            if df.loc[i, "ZoneCode"]:
                if not self.points[df.loc[i, "ZoneCode"]]:
                    raise Exception("Khu vực lấy hàng: [{}] Không hợp lệ".format(df.loc[i, "ZoneCode"]))
                
            if df.loc[i,"LocationLabel"] and df.loc[i, "ZoneCode"]:
                if not self.validate_point(df.loc[i, "ZoneCode"], df.loc[i,"LocationLabel"], df.loc[i,"StoreCode"]):
                    raise Exception(f"Vị trí chia hàng: [{df.loc[i,'LocationLabel']}] Không hợp lệ")
            
            _key = df.loc[i, "SOCode"] if not self.solid_by_sku_line else df.loc[i, "StoreCode"]
            
            tripCode = df.loc[i, "TripCode"]
            if not tripCode:
                tripCode = ""
                if df.loc[i, "RouteCode"] is not None:
                    tripCode += df.loc[i, "RouteCode"].replace("TUYEN", "T") 
                if df.loc[i, "SortCode"] is not None:
                    tripCode += df.loc[i, "SortCode"].replace("_", "")
                tripCode += self.code_by_time
                
            _pallet = df.loc[i,"LocationLabel"]   
            _zone = df.loc[i,"ZoneCode"]
            _pal = ("", "", 0, 0)

            if _pallet and _zone:
                _picked = self.get_point(_zone, _pallet)
                if len(_picked) > 0:
                    _pal = _picked[0]
            
            if not _pallet and not _zone:
                _picked = self.pick_point_v2(_total_pal, df.loc[i, "StoreCode"])
                if len(_picked) > 0:
                    _pal = _picked[0]
                    _zone = _pal[0]
                    _pallet = _pal[1]
            
            if not _pallet and _zone:
                _picked = self.pick_point_v2(_total_pal, df.loc[i, "StoreCode"], _zone)
                if len(_picked) > 0:
                    _pal = _picked[0]
                    _pallet = _pal[1]

            if _key not in save_data:
                save_data[_key] = {
                    "WarehouseCode": self.warehouse_code,
                    "WarehouseSiteId": self.warehouse_site_id,
                    "RocketCode": self.rocket_code,
                    "StoreCode": df.loc[i, "StoreCode"],
                    "SOCode": df.loc[i, "SOCode"],
                    "SOType": df.loc[i, "SOType"],
                    "Items": [],
                    "ZoneCode": _zone,
                    "LocationLabel": _pallet,
                    "TotalPallet": _total_pal,
                    "RouteCode": df.loc[i, "RouteCode"],
                    "LotCode": df.loc[i, "LotCode"],
                    "SortCode": df.loc[i, "SortCode"],
                    "TripCode": tripCode,
                    "Ordering": int(_pal[3]),
                    "Status": "New",
                    "CalendarDay": now.strftime("%Y%m%d"),
                    "CreatedDate": now,
                    "UpdatedDate": now,
                    "CreatedBy": self.request_by,
                    "UpdatedBy": self.request_by,
                    "IsDeleted": 0,
                }
            save_data[_key]["Items"].append({
                "SOCode": df.loc[i, "SOCode"],
                "SKU": df.loc[i, "SKU"],
                "Qty": int(df.loc[i, "Qty"]),
                "Uom": df.loc[i, "Uom"],
                "Uom": df.loc[i, "Uom"],
                "CaseQty": int(df.loc[i, "CaseQty"]),
                "PalQty": int(df.loc[i, "Qty"])/int(df.loc[i, "PalQty"]),
                "LocationLabel": "",
                "Ordering": 0
            })
        
        self.save_results(self.indexing_by_sky_line(save_data))

    def indexing_by_sky_line(self, data):
        if not self.solid_by_sku_line:
            return list(data.values())
        
        for idx in data:
            _idx = -1
            data[idx]["Items"].sort(key=order_by_qty)
            data[idx]["Items"].sort(key=order_by_sku)
            
            for obj in data[idx]["Items"]:
                _idx += 1
                _p = self.pick_point_v3(obj, data[idx].get("StoreCode"), obj["Qty"]/obj["PalQty"])
                data[idx]["Items"][_idx]["LocationLabel"] = _p[0]
                data[idx]["Items"][_idx]["Ordering"] = _p[1]
        
        # reverse data by SO
        results = {}
        for store_code in data:
            item = data[store_code]
            
            for obj in data[store_code].get("Items", []):
                key = obj.get("SOCode")
                if key not in results:
                    item["Items"] = []
                    item["SOCode"] = key
                    item["Ordering"] = obj.get("Ordering")
                    results[key] = item
                
                results[key]["Items"].append(obj)
        
        return list(results.values())


    def send_create_pickwave(self, picking_type = "Auto", file_name = None, hash = None,is_assign_zone = False):
        filters = {
            "RocketCode": self.rocket_code,
            "Status": "New",
            "IsDeleted": 0
        }
        postData = {
            "WarehouseCode": self.warehouse_code,
            "RocketCode": self.rocket_code,
            "PickingType": picking_type,
            "FileName": file_name,
            "RequestBy": self.request_by,
            "Hash": hash,
            "Keygen": str(self.keygen),
            "SOList": {}
        }
        _points = {}
        _indexing = 0
        store_so_type_map = {}
        storeError = []
        direction = 1
        cursor = self.db.getCollection(os.getenv("DB_COLLECTION_TRUCKING_PLAN_PICKWAVE")).find(filters).sort("Ordering", direction)
        for item in cursor:
            key_map = "{0}_{1}".format(item["StoreCode"],item["SOType"])
            if key_map in store_so_type_map:
                if  store_so_type_map[key_map] != item["SOCode"]:
                    if item["StoreCode"] not in storeError: 
                         storeError.append(item["StoreCode"])
            else:
                store_so_type_map[key_map] = item["SOCode"]
                            
            key = item.get("SOCode")
            if key not in postData["SOList"]:
                _indexing += 1
                postData["SOList"][key] = {}
            group_type = 0
            if picking_type == "Auto":
                group_type = 1 if item.get("SOType") == "Odd" else 2
            else:
                if item.get("SOType") != picking_type:
                     raise Exception(f"SO: {key} có SOType: {item['SOType']} Không hợp lệ với loại chia hàng: {self.picking_type}. Vui lòng kiểm tra lại!!")
            
            postData["SOList"][key] = {
                "RouteCode": item.get("RouteCode", ""),
                "LotCode": item.get("LotCode", ""),
                "LocationLabel": item.get("LocationLabel"),
                "ZoneCode": item.get("ZoneCode"),
                "RoutingIndex": _indexing,
                "GroupType": group_type,
                "Items": {}
            }

            if not item.get("LocationLabel"):
                raise Exception(f"SO [{key}] chưa xác định vị trí chia hàng.")
            
            if _points.get(item.get("LocationLabel")) == None:
                _points[item.get("LocationLabel")] = []

            if item.get("SOCode") not in _points[item.get("LocationLabel")]:
                _points[item.get("LocationLabel")].append(item.get("SOCode"))
            
            for obj in item.get("Items", []):
                _key = obj.get("SKU")
                if _key not in postData["SOList"][key]["Items"]:
                    postData["SOList"][key]["Items"][_key] = {
                        "CaseQty": obj.get("CaseQty", 1)
                    }

        if len(storeError) > 0: 
            raise Exception(f"Không thể tạo phiên chia hàng do CH: [{', '.join(storeError)}] có nhiều hơn 1 SO trong cùng 1 danh sách chia hàng!")
        
        resp = json.loads(request.post_ops(url="api/v1/splitSession/makeSplitSession", data=postData, timeout=30))

        if not resp.get("Status"):
            raise Exception(", ".join(resp.get("ErrorMessages", [])))
        
        if len(_points) > 0:
            self._move_pickwave_point_to_waiting(_points)

    def save_data(self, data):
        pass

    def save_results(self, data = []):
        if len(data) == 0:
            return
        self.db.getCollection(os.getenv("DB_COLLECTION_TRUCKING_PLAN_PICKWAVE")).insert_many(data)
    
    def clean_results(self):
        filters = {
            "RocketCode": self.rocket_code,
            "IsDeleted": 0
        }
        self.db.getCollection(os.getenv("DB_COLLECTION_TRUCKING_PLAN_PICKWAVE")).delete_many(filters)

    def validate_point(self, zone_code = "", location_label = "", store_code = ""):
        if zone_code not in self.points:
            return False
        
        data = self.points.get(zone_code, [])
        if len(data) == 0:
            return False
        
        valid = False
        empty_idx = -1
        idx = -1
        for item in data:
            idx += 1
            if item.get("CurrentStore") and  item.get("CurrentStore") == store_code and item.get("LocationLabel") == location_label:
                valid = True
                break
            if item.get("LocationLabel") == location_label and item.get("Status") == "Empty":
                empty_idx = idx

        if valid == False and empty_idx > -1:
            self.points[zone_code][empty_idx]["CurrentStore"] = store_code
            self.points[zone_code][empty_idx]["Status"] = "Waiting"
            valid = True

        return valid
    
    def get_point(self, zone_code = "", location_label = ""):
        _point = []
        if zone_code not in self.points:
            return []
        
        data = self.points.get(zone_code, [])
        if len(data) == 0:
            return []
        
        for item in data:
            if item.get("LocationLabel") == location_label:
                _point.append((zone_code, item.get("LocationLabel"), 0, item.get("Ordering")))
                break
        
        return _point

    def pick_point_v2(self, total_pal = 0, store_Code = None, zone_code = None):
        _point = []
        for idx in self.points:
            if zone_code != None and idx != zone_code:
                continue
            
            data = self.points.get(idx, [])
            if len(data) == 0:
                continue
            
            _idx = -1
            _selected_idx = -1

            for item in data:
                _idx += 1
                if _selected_idx == -1 and item.get("Status") == "Empty":
                    _selected_idx = _idx
                
                if item.get("CurrentStore") and item.get("CurrentStore") == store_Code:
                    _point.append((idx, item.get("LocationLabel"), total_pal, item.get("Ordering")))
                    break
            
            if len(_point) > 0:
                break

            if _selected_idx > -1:
                self.points[idx][_selected_idx]["CurrentStore"] = store_Code
                self.points[idx][_selected_idx]["Status"] = "Waiting"
                _point.append((idx, data[_selected_idx].get("LocationLabel"), total_pal, data[_selected_idx].get("Ordering")))
        
        return _point
    
    def pick_point_v3(self, item, store_code = "", total_pal = 0):
        _point = ["", 0]
        for idx in self.ex_points:
            data = self.ex_points.get(idx, [])
            if len(data) == 0:
                continue
            
            _idx = -1
            _selected_idx = -1

            for item in data:
                _idx += 1
                _store_code = item.get("StoreCode", "")
                if _selected_idx == -1 and item.get("TotalPal") < 1:
                    if _store_code == store_code or _store_code == "":
                        _selected_idx = _idx
            
            if _point[0] != "":
                break

            if _selected_idx > -1:
                self.ex_points[idx][_selected_idx]["TotalPal"] += total_pal
                self.ex_points[idx][_selected_idx]["Status"] = "Waiting"
                self.ex_points[idx][_selected_idx]["StoreCode"] = store_code
                _point = [data[_selected_idx].get("LocationLabel"), data[_selected_idx].get("Ordering")]
        
        return _point
    
    def pick_point(self, total_pal = 0):
        _point = []
        for idx in self.points:
            data = self.points.get(idx, [])
            if len(data) == 0:
                continue
            _idx = 0
            if data[_idx].get("MaxSlot") < total_pal:
                _point.append((idx, data[_idx].get("LocationLabel"), data[_idx].get("MaxSlot"), data[_idx].get("Ordering")))
                total_pal = total_pal - data[_idx].get("MaxSlot")
                data = data[1:]
            else:
                _point.append((idx, data[_idx].get("LocationLabel"), total_pal, data[_idx].get("Ordering")))
                total_pal = 0
                data = data[1:]
            
            self.points[idx] = data
            if total_pal == 0:
                break

        return _point
    
    def _move_pickwave_point_to_waiting(self, data):
        _db_name = os.getenv(f"DB_NAME_{self.warehouse_code}")
        for point_code in data:
            filters = {
                "Type": "PickwavePoint",
                # "Status": "Empty",
                "Code": point_code,
                "IsDeleted": 0
            }

            save_data = {
                "Status": "Waiting"
            }
            _slots = []
            for so_code in data.get(point_code, []):
                _slots.append({
                    "Code": so_code,
                    "Type": "SO"
                })
            
            self.db.getCollection(os.getenv("DB_COLLECTION_OPS_POINT"), db_name=_db_name).update_one(filters, {
                "$set": save_data,
                "$addToSet": {
                    "Slots": {
                        "$each": _slots
                    }
                }
            })

    def load_data(self) -> pd.DataFrame:
        filters = {
            "RocketCode": self.rocket_code,
            "IsDeleted": 0
        }
        df = pd.DataFrame([], columns=['StoreCode', 'SOCode', 'SKU', 'Qty', 'Uom', 'CaseQty', 'PalQty', 'RouteCode','LotCode','SortCode','TripCode', 'SOType','ZoneCode','LocationLabel'])
        cursor = self.db.getCollection(os.getenv("DB_COLLECTION_TRUCKING_PLAN_SUGGESS")).find(
            filters, {"SOCode": 1, "StoreCode": 1, "Items": 1, "RouteCode": 1, "LotCode": 1,"SortCode":1,"TripCode":1,"SOType":1,"ZoneCode": 1,"LocationLabel":1}).sort("Ordering", 1)
        for obj in cursor:
            for item in obj.get("Items", []):
                _pal_qty = item.get("PalQty", 0)
                if _pal_qty == 0:
                    _pal_qty = item.get("CaseQty") * 100
                df.loc[len(df)] = [obj.get("StoreCode", ""), obj.get("SOCode", ""), item.get("SKU"), item.get("Qty"), item.get("Uom"), item.get("CaseQty"), _pal_qty, obj.get("RouteCode"), obj.get("LotCode"), obj.get("SortCode"), obj.get("TripCode"), obj.get("SOType"),obj.get("ZoneCode"),obj.get("LocationLabel")]
        
        return df
    
    def _load_pickwave_point(self, ver = 1):
        filters = {
            "Type": "PickwavePoint",
            # "Status": "Empty",
            "IsDeleted": 0
        }

        # Luôn xử lý theo vùng chứa hàng, lấy vùng chứa hàng chưa xử lý
        if self.rules and self.rules.get("solid_by_zone"):
            zones = self._get_zone()
            if len(zones) == 0:
                return []
            filters["ZoneCode"] = {"$in": zones}
        
        results = {}
        _db_name = os.getenv(f"DB_NAME_{self.warehouse_code}")
        cursor = self.db.getCollection(os.getenv("DB_COLLECTION_OPS_POINT"), db_name=_db_name).find(filters, {"Code": 1, "ZoneCode": 1, "MaxSlot": 1, "__indexing": 1, "Slots": 1, "Status": 1}).sort("__indexing", 1)
        for item in cursor:
            _key = item.get("ZoneCode")
            if _key not in results:
                results[_key] = []
            
            current_store = ""
            slots = item.get("Slots", [])
            if len(slots) > 0:
                current_store = self._get_current_store(slots[0].get("Code"))

            results[_key].append({
                "LocationLabel": item.get("Code"),
                "MaxSlot": item.get("MaxSlot", 5),
                "Ordering": item.get("__indexing", 1),
                "Status": "Empty" if ver == 2 else item.get("Status"),
                "CurrentStore": current_store,
                "TotalPal": 0
            })
        return results
    
    def _get_current_store(self, soCode):
        filters = {
            "SOCode": soCode,
            "IsDeleted": 0
        }
        _db_name = os.getenv(f"DB_NAME_{self.warehouse_code}")
        obj = self.db.getCollection(os.getenv("DB_COLLECTION_INV_SO"), db_name=_db_name).find_one(filters, {"SiteId": 1})
        if not obj:
            return ""
        return obj.get("SiteId", "")

    def _get_zone(self):
        result = []
        filters = {
            "Status": "WAITING",
            "IsDeleted": 0
        }
        _db_name = os.getenv(f"DB_NAME_{self.warehouse_code}")
        cursor = self.db.getCollection(os.getenv("DB_COLLECTION_ZONE"), db_name=_db_name).find(filters, {"Code": 1}).sort("ZOrder", 1)
        for item in cursor:
            result.append(item.get("Code"))
        return result