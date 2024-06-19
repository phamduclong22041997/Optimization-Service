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
from model import Rocket3STO
from model.rocket_3_set import Rocket3Set
from model.rocket_autoprocess import AutoProcess
from survival import sto_bubble
from datetime import datetime
import pytz
timeZone = pytz.timezone('Asia/Ho_Chi_Minh')
class AnalyzeLevel1Demand:
    def __init__(self, rocket_code,client_code, warehouse_code, warehouse_site_id,request_by):
        self.client_code = client_code
        self.rocket_code = rocket_code
        self.warehouse_code = warehouse_code
        self.warehouse_site_id = warehouse_site_id
        self.request_by = request_by
        self.__init()

    def __init(self):
        options = utils.load_rule(self.warehouse_site_id, "AUDO_GROUP_STO")
        self._db_handle = Rocket3STO(self.rocket_code)
        self._handle = sto_bubble.StoBubble(options)
        self.rocket_set = Rocket3Set(self.rocket_code)
        self.auto_process = AutoProcess(self.rocket_code)
        
        self._maps = []
        self.load_stores()

    def analyze_process(self):
        # Duyệt qua tất cả CH của demand và analyze theo nhóm SKU
        stores = self._db_handle.load_stores()
        for store in stores:
            self.analyze_by_store(store)
        self.rocket_set.create({ 
            "ClientCode": self.client_code, 
            "WarehouseCode": self.warehouse_code,
            "WarehouseSiteId":self.warehouse_site_id,
            "Name": "ANALYZE_STO_DISTRIBUTION",
            "RequestBy": self.request_by,
            "Status":constant.STATUS_NEW
            })


    def analyze_by_store(self, store_code):
        results = self._handle.analyze(self._db_handle.load_data_by_store(store_code))
        
        #Khởi tạo mã STO và cập nhật vào demand
        data = []
        _maps = self._db_handle.load_sku_line_maps()
        # _processSTO = []
        for items in results:
            sto_code = utils.generate_sto_code()
            ids = []
            for val in items:
                ids.append(_maps[val[2]])
            data.append({
                "Code": sto_code,
                "Filters": {"_id": {"$in":ids}}
            })
            # _processSTO.append({
            #     "__vjob_priority" : 999,
            #     "IsDeleted" : 0,
            #     "__vjob_status" : 0,
            #     "Name" : "PROCESS_CREATE_STO_V3",
            #     "Object" :sto_code,
            #     "SiteId" :  self.warehouse_site_id,
            #     "Keygen" :  self.rocket_code , 
            #     "Data" : {
            #         "SessionCode": self.rocket_code , 
            #         "STOCode" : sto_code,
            #         "WarehouseSiteId" : self.warehouse_site_id,
            #         "WarehouseCode" : self.warehouse_code,
            #         "RequestBy" : self.request_by
            #     },
            #     "CreatedDate" :datetime.now(timeZone),
            #     "UpdatedDate" : datetime.now(timeZone),
            # })
        # self.rocket_set.update({"name": "ANALYZE_STO_AUTOMATION"},{
        #     "Status":constant.STATUS_NEW
        # })
        # self.auto_process.create(_processSTO)
        self.flush(data)
    
    def flush(self, chunks):
        for chunk in chunks:
            self._db_handle.sync_sto_code(chunk)
    
    def load_stores(self):
        self.stores = self._db_handle.load_stores()
