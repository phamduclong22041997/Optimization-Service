
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

import time
import os
import pytz
import gc
import traceback
from datetime import datetime
from time import perf_counter
from lib import db, usage
from clients import trucking_plan_suggestion

timeZone = pytz.timezone('Asia/Ho_Chi_Minh')

class AnalyzeTruckingPlan:
    def __init__(self):
        self.delay_time = 0
        self.db = db()

    def run(self):
        try:
            _queue = self.load_queue()
            for item in _queue:
                print("Start analyze: {0}".format(item.get("Code")))
                rs = self.process(item)
                gc.collect()
                print("End analyze: {0}".format(item.get("Code")))
                if rs == -1:
                    break
        except Exception as e:
            print(e)
            pass

        self.db.close()
        gc.collect()

    def process(self, item):
        start_time = datetime.now(timeZone)
        start_mem = usage.memory()
        message_log = None
        process_type = "ANALYZE"
        message_log =  'Phân tích thành công'
        try:
            if item.get("Status") != "Confirmed":
                self.processing(item.get("Id"))
            
            begin = perf_counter()
            _status = "Analyzed"

            obj = self._get_client(item)
            if obj != None:
                if item.get("Status") == "Confirmed":
                    process_type = "MAKE_PICKWAVE"
                    _status = "CreatedPickwave"
                    message_log =  'Tạo pickwave thành công'
                
                if process_type == "ANALYZE":
                    obj.analyze_process()
                else:
                    obj.send_create_pickwave(item.get("PickingType"), item.get("Source"), item.get("Hash"),item.get("IsAssignZone"))
            
            end = perf_counter()
            end_mem = usage.memory()

            self.finished(item.get("Id"), _status)

            if obj != None:
                obj.send_remote_request()

            self.write_logs({
                "ClientCode": item.get("ClientCode"),
                "WarehouseCode": item.get("WarehouseCode"),
                "WarehouseSiteId": item.get("WarehouseSiteId"),
                "RocketCode": item.get("RocketCode"),
                "Name": f"{process_type}",
                "Type": "TRUCKING_PLAN",
                "SourceFile": "",
                "FileName": item.get("Source"),
                "Hash": item.get("Hash"),
                "Status": "OK",
                "Message": message_log,
                "Error": "",
                "Description": "{0}_{1}".format(item.get("Type"), process_type),
                "TimeProcess": "{0:.2f}s".format(end - begin),
                "MemoryUsage": "{0:.2f}byte".format(abs(end_mem - start_mem)),
                "CpuUsage": "{0:.2f}s".format(usage.cpu(end - begin)),
                "StartTime": start_time,
                "EndTime": datetime.now(timeZone),
                "CalendarDay": datetime.now(timeZone).strftime("%Y%m%d"),
                "CreatedBy": item.get("UpdatedBy"),
                "CreatedDate": datetime.now(timeZone),
                "UpdatedDate": datetime.now(timeZone),
                "IsDeleted": 0
            })
        except Exception as e:
            print(traceback.format_exc())
            end = perf_counter()
            end_mem = usage.memory()
            status = 'AnalyzeError'
            if process_type == "ANALYZE":
                message_log =  "Phân tích thất bại."
            else:
                status = 'AnalyzePWError'
                message_log =  "Tạo pickwave thất bại."
           
            self.error(item.get("Id"),status)
            self.write_logs({
                "ClientCode": item.get("ClientCode"),
                "WarehouseCode": item.get("WarehouseCode"),
                "WarehouseSiteId": item.get("WarehouseSiteId"),
                "RocketCode": item.get("RocketCode"),
                "Name": f"{process_type}",
                "Type": "TRUCKING_PLAN",
                "SourceFile": "",
                "FileName": item.get("Source"),
                "Hash": item.get("Hash"),
                "Status": "ERR",
                "Message": message_log,
                "Error": str(e),
                "ExtendError": traceback.format_exc(),
                "Description": "{0}_{1}".format(item.get("Type"), process_type),
                "TimeProcess": "{0:.2f}s".format(end - begin),
                "MemoryUsage": "{0:.2f}byte".format(abs(end_mem - start_mem)),
                "CpuUsage": "{0:.2f}%".format(usage.cpu(end - begin)),
                "StartTime": start_time,
                "EndTime": datetime.now(timeZone),
                "CalendarDay": datetime.now(timeZone).strftime("%Y%m%d"),
                "CreatedBy": item.get("UpdatedBy"),
                "CreatedDate": datetime.now(timeZone),
                "UpdatedDate": datetime.now(timeZone),
                "IsDeleted": 0
            })
        return 1

    def _get_client(self, item):
        return trucking_plan_suggestion.AnalyzePickwave(
            item.get("WarehouseCode"),
            item.get("WarehouseSiteId"),
            item.get("Code") ,
            item.get("UpdatedBy"),
            item.get("Id")
        )
    
    def load_queue(self):
        filters = {
            "Name": "TRUCKING_PLAN_SUGGESTION",
            "Status": {"$in": ["New", "Confirmed"]},
            "IsDeleted": 0
        }
        results = []
        cursor = self.db.getCollection(os.getenv("BD_COLLECTION_DEMAND_SET2")).find(
            filters).sort("__vjob_priority", -1)
        for item in cursor:
            results.append({
                "Id": item.get("_id"),
                "ClientCode": item.get("ClientCode"),
                "WarehouseCode": item.get("WarehouseCode"),
                "WarehouseSiteId": item.get("WarehouseSiteId"),
                "RocketCode": item.get("Code"),
                "PromotionCode": item.get("PromotionCode"),
                "IsPromotion": item.get("IsPromotion"),
                "POCode": item.get("POCode"),
                "Code": item.get("Code"),
                "Type": item.get("Name"),
                "Hash": item.get("Hash"),
                "Source": item.get("Source"),
                "PickingType": item.get("PickingType"),
                "UpdatedBy": item.get("UpdatedBy"),
                "Status": item.get("Status"),
                "AllowAnalyzePromotion": item.get("AllowAnalyzePromotion"),
                "SourceType": item.get("SourceType"),
                "CommodityType": item.get("CommodityType"),
                "IsAssignZone": item.get("IsAssignZone")
            })
        return results

    def error(self, id, status = "AnalyzeError"):
        self.db.getCollection(os.getenv("BD_COLLECTION_DEMAND_SET2")).update_one(
            {"_id": id}, {"$set": {"Status": status}})

    def processing(self, id, status = "Analyzing"):
        self.db.getCollection(os.getenv("BD_COLLECTION_DEMAND_SET2")).update_one(
            {"_id": id}, {"$set": {"Status": status}})
        self.Status = status
        
    def finished(self, id, status="Analyzed"):
        self.db.getCollection(os.getenv("BD_COLLECTION_DEMAND_SET2")).update_one(
            {"_id": id}, {"$set": {"Status": status}})
    
    def write_logs(self, logs):
        self.db.getCollection(os.getenv("DB_COLLECTION_LOG")).insert_one(logs)

    def get_config(self):
        filters = {
            "Name": "ANALYZE_CONFIGS",
            "IsDeleted": 0
        }
        obj = self.db.getCollection(os.getenv("DB_COLLECTION_SETTING")).find_one(filters)
        if obj != None:
            return obj.get("Value")
        return {}

def main(event):
    is_stop = False
    while not is_stop:
        if event.is_set():
            break
        obj = AnalyzeTruckingPlan()
        obj.run()
        time.sleep(30)
