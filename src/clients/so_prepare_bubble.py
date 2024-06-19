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

from lib import constant, utils
from model import Rocket3STO, Rocket3SO, BinStock, Rocket3Set
from model.rocket_autoprocess import AutoProcess
from survival import so_prepare_bubble
from datetime import datetime
import pytz
timeZone = pytz.timezone('Asia/Ho_Chi_Minh')

class AnalyzeLevel2Demand:
    def __init__(self, rocket_code, clientCode, warehouse_code, warehouse_site_id,request_by,trucking_plan,type):
        self.client_code = clientCode
        self.rocket_code = rocket_code
        self.warehouse_code = warehouse_code
        self.warehouse_site_id = warehouse_site_id
        self.request_by = request_by
        self.type = type
        self.trucking_plan = trucking_plan
        self.session = None
        self.__init()

    def __init(self):
        options = utils.load_rule(self.warehouse_site_id, "AUDO_GROUP_SO")
        self._db_sto_handle = Rocket3STO(self.rocket_code)
        self._db_so_handle = Rocket3SO(self.rocket_code)
        self._handle = so_prepare_bubble.SoBubble(options)
        self._ops_handle = BinStock(self.warehouse_code, options, self.client_code)
        self.rocket_set = Rocket3Set(self.rocket_code)
        self.auto_process = AutoProcess(self.rocket_code)
        if self.type == 'REANALYZE_STO_DISTRIBUTION':
            self.session = self.rocket_code
            


    def analyze_process(self): 
        skus = self._db_sto_handle.load_skus(self.type)
        for item in skus:
            self.analyze_distribution(item)
 
        if self.type == 'REANALYZE_STO_DISTRIBUTION': 
            self.rocket_set.update({"name": {"$in" : ['ANALYZE_SO_AUTOMATION','ANALYZE_SO_INVENTORY']}},{
            "Status":constant.STATUS_NEW
            })
        else:
        # createSTO
            _processSTO = []
            stos = self._db_so_handle.load_stos()
            for item in stos:
                _processSTO.append({
                    "__vjob_priority" : 99,
                    "IsDeleted" : 0,
                    "__vjob_status" : 0,
                    "Name" : "PROCESS_CREATE_STO_V3",
                    "Object" :item,
                    "SiteId" :  self.warehouse_site_id,
                    "IssueSite": self.warehouse_site_id,
                    "Keygen" :  self.rocket_code , 
                    "Data" : {
                        "SessionCode": self.rocket_code , 
                        "STOCode" : item,
                        "WarehouseSiteId" : self.warehouse_site_id,
                        "WarehouseCode" : self.warehouse_code,
                        "RequestBy" : self.request_by
                    },
                    "CalendarDay" : utils.calendar_day(),
                    "CreatedDate" :datetime.now(timeZone),
                    "UpdatedDate" : datetime.now(timeZone),
                })
            self.auto_process.create(_processSTO)

    def analyze_distribution(self, obj):
        sku = obj.get("SKU")
        stocks = self._ops_handle.load_available_stock(self.warehouse_site_id, sku)     
        capacities = {
            "total_qty": 0,
            "data" :[]
        }
        
        for item in stocks:
            if item.get("Qty") < 0:
                item["Qty"] = 0
            
            capacities["total_qty"] += item.get("Qty")
            capacities["data"].append(item.get("Qty"))

        if len(capacities["data"]) == 0:
            capacities["data"].append(0)
            stocks.append({
                "LocationLabel": "",
                "LocationType": "",
                "SubLocationLabel": "",
                "LocationIndexing": 0
            })

        rr = self._db_sto_handle.load_data_by_sku(sku,self.type)
        results = self._handle.analyze(rr, capacities)
        data = []
        _current_date = utils.current_date()
        _calendar_day = utils.calendar_day()
        _maps = self._db_sto_handle.load_sku_line_maps()
        status = constant.STATUS_NEW
        note = ''
        reanalyze_data = []
        if self.type == 'REANALYZE_STO_DISTRIBUTION':
            status  = constant.STATUS_CREATE_STO
        for item in results:
            loc = stocks[item[0]]   
            if not loc:
                note =  ["Không có tồn ở Pick Face !!!"]
                if self.type == "REANALYZE_STO_DISTRIBUTION":
                    reanalyze_data.append({
                        "SaveData" :{
                        "Qty": item[2],
                        "MissingQty": item[3],
                        "PackageType": _maps[item[1]].get("PackageType"),
                        "LocationLabel": "",
                        "SubLocationLabel": "",
                        "LocationType": "",
                        "Indexing": "",
                        "Note": note,
                        "IsSelected": False,
                        "BinStock": capacities["total_qty"]
                        },
                        "Filters": {
                            "Key": _maps[item[1]].get("STOCode") + "_" + _maps[item[1]].get("SKU"),
                            "Session":self.rocket_code,
                            "IsDeleted": 0
                            }
                        })
                continue
                
            if item[2] == 0:
                note = ["Không có tồn ở Pick Face"]
            is_selected = self.session != None and item[2] != 0 and item[2] != None and item[2] != ""
            if self.type == "REANALYZE_STO_DISTRIBUTION":
                reanalyze_data.append({
                    "SaveData" :{
                    "Qty": item[2],
                    "MissingQty": item[3],
                    "PackageType": _maps[item[1]].get("PackageType"),
                    "LocationLabel": loc.get("LocationLabel"),
                    "SubLocationLabel": loc.get("SubLocationLabel"),
                    "LocationType": loc.get("LocationType"),
                    "Indexing": loc.get("LocationIndexing"),
                    "Note": note,
                    "IsSelected": is_selected,
                    "BinStock": capacities["total_qty"]
                    },
                    "Filters": {
                        "Key": _maps[item[1]].get("STOCode") + "_" + _maps[item[1]].get("SKU"),
                        "Session":self.rocket_code,
                        "IsDeleted": 0
                        }
                    })
            else: 
                data.append({
                    "RocketCode": self.rocket_code,
                    "ClientCode": self.client_code,
                    "WarehouseCode": self.warehouse_code,
                    "WarehouseSiteId": self.warehouse_site_id,
                    "STOCode": _maps[item[1]].get("STOCode"),
                    "StoreCode": _maps[item[1]].get("StoreCode"),
                    "SKU": _maps[item[1]].get("SKU"),
                    "Qty": item[2],
                    "MissingQty": item[3],
                    "BinStock": capacities["total_qty"],
                    "Uom": _maps[item[1]].get("Uom"),
                    "PackageType": _maps[item[1]].get("PackageType"),
                    "LocationLabel": loc.get("LocationLabel"),
                    "SubLocationLabel": loc.get("SubLocationLabel"),
                    "LocationType": loc.get("LocationType"),
                    "Status": status,
                    "Indexing": loc.get("LocationIndexing"),
                    "CreatedDate": _current_date,
                    "CalendarDay": _calendar_day,
                    "IsSelected": is_selected,
                    "Session": self.session,
                    "Key": _maps[item[1]].get("STOCode") + "_" + _maps[item[1]].get("SKU"),
                    "IsDeleted": 0,
                    "Note": note,
                    "RefData":  _maps[item[1]].get("RefData"),
                })

        self.flush(data)
        self.update(reanalyze_data)

    def flush(self, chunks):
        if len(chunks) > 0:
            self._db_so_handle.create(chunks)
    
    def update(self,data):
        if len(data) > 0:
            for item in data:
                self._db_so_handle.update(item)
