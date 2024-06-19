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
from model import Rocket3STO, Rocket3SO, BinStock,AutoProcess
from model.rocket_3_set import Rocket3Set
from model.inv_so import INV_SO
from survival import so_bubble
from datetime import datetime
import pytz
timeZone = pytz.timezone('Asia/Ho_Chi_Minh')
class AnalyzeLevel3Demand:
    def __init__(self, client_code,rocket_code, warehouse_code,warehouse_site_id,request_by,type):
        self.rocket_code = rocket_code
        self.client_code = client_code
        self.warehouse_code = warehouse_code
        self.warehouse_site_id = warehouse_site_id
        self.request_by = request_by
        self.type = type
        self.trucking_plan = 1
        self.__init()

    def __init(self):
        if(self.type == 'ANALYZE_SO_INVENTORY'):
            options = utils.load_rule(self.warehouse_site_id, "AUDO_GROUP_SO_INVENTORY")
            self.trucking_plan = 0
        else:
            options = utils.load_rule(self.warehouse_site_id, "AUDO_GROUP_SO")
        self._db_sto_handle = Rocket3STO(self.rocket_code)
        self._db_so_handle = Rocket3SO(self.rocket_code)
        self._handle = so_bubble.SoBubble(options)
        self._ops_handle = BinStock(self.warehouse_code, options, self.client_code)
        self.auto_process = AutoProcess(self.rocket_code)
        self.rocket_set = Rocket3Set(self.rocket_code)
        self.inv_so = INV_SO()
        self.skus = []
        self.total_store = 0
        self.total_units = 0
        self.rocket_list = []
        self.sto_list = []

    def analyze_process(self):
        data_stores = self._db_so_handle.load_stores()
        self.rocket_list = data_stores["rocket_list"]
        # self.sto_list = data_stores["sto_list"]
        if len(data_stores["stores"]) == 0:
            roll_back_all_data =[{
                        "Filters": {
                            "Session":self.rocket_code,
                            "Qty": {"$gt": 0},
                            "IsSelected": True, # Issue Filter Rollback
                            "Status": constant.STATUS_CREATE_STO,
                            }
                        }]
            self.rollBack(roll_back_all_data)
            raise Exception("Không có dữ liệu hợp lệ. Vui lòng kiểm tra lại")
                
        for item in data_stores["stores"]:
            self.total_store += 1
            self.analyze_by_store(item)

    def analyze_by_store(self, store):
        data = self._db_so_handle.load_data_by_store(store)
        unique_val4 = set()
        if(len(data) == 0):
            roll_back_data_by_store =[{
                "Filters": {
                    "Session":self.rocket_code,
                    "StoreCode": store,
                     "Qty": {"$gt": 0},  #Issue Filter Rollback
                    "IsSelected": True,
                    "Status": constant.STATUS_CREATE_STO,
                    }
                }]
            self.rollBack(roll_back_data_by_store)
            return
            # raise Exception("Không có dữ liệu hợp lệ. Vui lòng kiểm tra lại!!")

        _maps = self._db_so_handle.load_sku_line_maps()
        results = self._handle.analyze(data)
        _data = []
        _processSO = []
        if(len(results[0]) > 0):
            for items in results[0]:
                so_code = utils.generate_so_code("SO",self.client_code,10)
                skus = []
                for idx in items:
                    for val in idx: 
                        if len(val) == 0:
                            continue
                        val4 = val[4]
                        if val4 is not None:
                            if isinstance(val4, list):
                                unique_val4.update(val4)

                        skus.append(_maps[val[3]])
                        if val[3] not in self.skus:
                            self.skus.append(val[3])
                        self.total_units += val[2]
                    filters = {
                                "IsDeleted": 0,
                                "Session":self.rocket_code,
                                "SOCode": {"$in" : ['',None]},
                                "Status":  constant.STATUS_CREATE_STO,
                                "IsSelected": True,
                                "Qty": {"$gt": 0},
                                "StoreCode": store,
                                "SKU": {"$in":skus},
                                "PackageType": val[0]
                              }
                    if self.trucking_plan == 0: 
                        filters = {
                                "IsDeleted": 0,
                                "Session":self.rocket_code,
                                "SOCode": {"$in" : ['',None]},
                                "Status":  constant.STATUS_CREATE_STO,
                                "IsSelected": True,
                                "Qty": {"$gt": 0},
                                "StoreCode": store,
                                "SKU": {"$in":skus}
                              }
                    _data.append({
                            "SOCode": so_code,
                            "Filters": filters
                        })
                _processSO.append({
                    "__vjob_priority" : 99,
                    "IsDeleted" : 0,
                    "__vjob_status" : 0,
                    "Name" : "PROCESS_CREATE_SO_V3",
                    "Object" :so_code,
                    "SiteId" :  self.warehouse_site_id,
                    "IssueSite": self.warehouse_site_id,
                    "Keygen" :  self.rocket_code ,
                    "Data" : {
                        "TruckingPlan": self.trucking_plan,
                        "SessionCode": self.rocket_code ,
                        "SOCode" : so_code,
                        "WarehouseSiteId" : self.warehouse_site_id,
                        "WarehouseCode" : self.warehouse_code,
                        "RequestBy" : self.request_by
                    },
                    "CalendarDay" : utils.calendar_day(),
                    "CreatedDate" :datetime.now(timeZone),
                    "UpdatedDate" : datetime.now(timeZone),
                })
        if unique_val4 is not None:
            unique_val4 = list(unique_val4)
            if len(unique_val4)>0:
                self.sto_list = unique_val4
    
        _rmdata= []
        if(len(results[1]) > 0):
            for items in results[1]:
                for idx in items:
                    for val in idx: 
                        skus = []
                        if len(val) == 0:
                            continue
                        skus.append(_maps[val[3]])
                    _rmdata.append({
                        "Filters": {
                            "Session":self.rocket_code,
                            "SOCode": {"$in" : ['',None]},
                            "StoreCode": store,
                            "SKU": {"$in":skus},
                            "PackageType": val[0],
                            "Qty": {"$gt": 0},  #Issue Filter Rollback
                            "IsSelected": True,
                            "Status":  constant.STATUS_CREATE_STO,
                            }
                        })
                
        self.flush(_data)
        self.rollBack(_rmdata)
        if len(_processSO):
            self.inv_so.update({"StoreCode":store, "STOList": self.sto_list},{ "Status":constant.SO_STATUS_WAITING_CREATE_DO})
            self.rocket_set.update({"name": {"$in" : ['ANALYZE_SO_AUTOMATION','ANALYZE_SO_INVENTORY']}},{
            "Status":constant.WAITING_FOR_CREATE_SO,
         	"TotalUnit" : self.total_units,
            "TotalStore" : self.total_store,
            "TotalSKU" : len(self.skus),
            "RefData": {
                "RocketList": self.rocket_list
                }
            })
            self.auto_process.create(_processSO)


    def flush(self, chunks):
        if len(chunks) > 0:
            for chunk in chunks:
                self._db_so_handle.sync_so_code(chunk)
        return True

    def rollBack(self,data):
        if len(data) > 0:
            for item in data:
                self._db_so_handle.update_to_unselected(item)
        return True

           
