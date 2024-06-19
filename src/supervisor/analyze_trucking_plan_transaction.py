
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
from clients import trucking_plan_transaction

timeZone = pytz.timezone('Asia/Ho_Chi_Minh')

class AnalyzeTruckingPlanTransaction:
    def __init__(self):
        self.delay_time = 0
        self.db = db()

    def run(self):
        try:
            warehouses = ["HY2"]
            for warhouse in warehouses:
                _db_name = os.getenv(f"DB_NAME_{warhouse}")
                _queue = self.load_queue(_db_name)
                print(_queue)
                for item in _queue:
                    print("Start analyze: {0}".format(item.get("SessionCode")))
                    rs = self.process(item, _db_name)
                    gc.collect()
                    print("End analyze: {0}".format(item.get("SessionCode")))
                    if rs == -1:
                        break
        except Exception as e:
            print(e)
            pass

        self.db.close()
        gc.collect()

    def process(self, item, db_name):
        start_time = datetime.now(timeZone)
        start_mem = usage.memory()
        message_log = None
        process_type = "ANALYZE"
        message_log =  'Phân tích transactions thành công'
        try:
            self.processing(item.get("Id"), db_name)
            
            begin = perf_counter()

            obj = self._get_client(item, db_name)
            if obj != None:
                obj.analyze_process()
            
            end = perf_counter()
            end_mem = usage.memory()

            self.finished(item.get("Id"), db_name)

            if obj != None:
                obj.send_remote_request()

            self.write_logs({
                "ClientCode": item.get("ClientCode"),
                "WarehouseCode": item.get("WarehouseCode"),
                "WarehouseSiteId": item.get("WarehouseSiteId"),
                "RocketCode": item.get("SessionCode"),
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
            self.error(item.get("Id"), db_name)
            message_log =  "Phân tích transactions thất bại."
            
            self.write_logs({
                "ClientCode": item.get("ClientCode"),
                "WarehouseCode": item.get("WarehouseCode"),
                "WarehouseSiteId": item.get("WarehouseSiteId"),
                "RocketCode": item.get("SessionCode"),
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

    def _get_client(self, item, db_name):
        _client = trucking_plan_transaction.AnalyzeTransaction(
            item.get("WarehouseCode"),
            item.get("SessionCode"),
            db_name,
            item.get("UpdatedBy")
        )
        _client.set_options(item)
        
        return _client
    
    def load_queue(self, db_name):
        filters = {
            "Name": "TRUCKING_PLAN_SUGGESTION",
            "Status": "New",
            "IsDeleted": 0
        }
        results = []
        cursor = self.db.getCollection(os.getenv("DB_COLLECTION_TRUCKING_PLAN_SUGGESTION_SET"), db_name=db_name).find(
            filters).sort("__vjob_priority", -1)
        for item in cursor:
            results.append({
                "Id": item.get("_id"),
                "ClientCode": item.get("ClientCode"),
                "WarehouseCode": item.get("WarehouseCode"),
                "WarehouseSiteId": item.get("WarehouseSiteId"),
                "SessionCode": item.get("SessionCode"),
                "Type": item.get("Name"),
                "Hash": item.get("Hash"),
                "Source": item.get("Source"),
                "UpdatedBy": item.get("UpdatedBy"),
                "Status": item.get("Status")
            })
        return results

    def error(self, id, db_name):
        self.db.getCollection(os.getenv("DB_COLLECTION_TRUCKING_PLAN_SUGGESTION_SET"), db_name=db_name).update_one(
            {"_id": id}, {"$set": {"Status": "AnalyzeError"}})

    def processing(self, id, db_name):
        self.db.getCollection(os.getenv("DB_COLLECTION_TRUCKING_PLAN_SUGGESTION_SET"), db_name=db_name).update_one(
            {"_id": id}, {"$set": {"Status": "Analyzing"}})
        
    def finished(self, id, db_name):
        self.db.getCollection(os.getenv("DB_COLLECTION_TRUCKING_PLAN_SUGGESTION_SET"), db_name=db_name).update_one(
            {"_id": id}, {"$set": {"Status": "Analyzed"}})
    
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
        obj = AnalyzeTruckingPlanTransaction()
        obj.run()
        time.sleep(30)
