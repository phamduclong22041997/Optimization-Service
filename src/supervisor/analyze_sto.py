
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

import time
import os
import gc
import threading
import signal
from multiprocessing import Pool
from time import perf_counter
from lib import db, usage, utils, constant
from clients.sto_bubble import AnalyzeLevel1Demand

class Analysis:
    def __init__(self):
        self.delay_time = 60
        self.db = db()

    def run(self, item):
        try:
            print("Start analyze sto: {0}".format(item.get("Code")))
            self.process(item)
            gc.collect()
            print("End analyze sto: {0}".format(item.get("Code")))
        except Exception as e:
            print(e)

        self.db.close()
        gc.collect()

    def process(self, item):
        start_time = utils.current_date()
        start_mem = usage.memory()
        message_log = None
        try:
            self.processing(item.get("Id"))
            
            begin = perf_counter()
            print("X111")

            obj = self._get_client(item)
            if obj != None:
                print("X114")
                obj.analyze_process()
            print("X112")
            end = perf_counter()
            end_mem = usage.memory()

           

            self.finished(item.get("Id"))
            print("X113")

            message_log =  'Phân tích thành công'

            self.write_logs({
                "ClientCode": item.get("ClientCode"),
                "WarehouseCode": item.get("WarehouseCode"),
                "WarehouseSiteId": item.get("WarehouseSiteId"),
                "RocketCode": item.get("RocketCode"),
                "Name": "ANALYZE",
                "Type": item.get("Type"),
                "SourceFile": "",
                "FileName": item.get("Source"),
                "Hash": item.get("Hash"),
                "Status": "OK",
                "Message": message_log,
                "Error": "",
                "Description": "{0}_ANALYZE".format(item.get("Type")),
                "TimeProcess": "{0:.2f}s".format(end - begin),
                "MemoryUsage": "{0:.2f}byte".format(abs(end_mem - start_mem)),
                "CpuUsage": "{0:.2f}s".format(usage.cpu(end - begin)),
                "StartTime": start_time,
                "EndTime": utils.current_date(),
                "CalendarDay": utils.calendar_day(),
                "CreatedBy": item.get("CreatedBy"),
                "CreatedDate": utils.current_date(),
                "UpdatedDate": utils.current_date(),
                "IsDeleted": 0
            })
        except Exception as e:
            print(e)
            end = perf_counter()
            end_mem = usage.memory()
            self.error(item.get("Id")) 
            message_log =  "Phân tích thất bại."
            
            self.write_logs({
                "ClientCode": item.get("ClientCode"),
                "WarehouseCode": item.get("WarehouseCode"),
                "WarehouseSiteId": item.get("WarehouseSiteId"),
                "RocketCode": item.get("RocketCode"),
                "Name": "ANALYZE",
                "Type": item.get("Type"),
                "SourceFile": "",
                "FileName": item.get("Source"),
                "Hash": item.get("Hash"),
                "Status": "ERR",
                "Message": message_log,
                "Error": str(e),
                "Description": "{0}_ANALYZE".format(item.get("Type")),
                "TimeProcess": "{0:.2f}s".format(end - begin),
                "MemoryUsage": "{0:.2f}byte".format(abs(end_mem - start_mem)),
                "CpuUsage": "{0:.2f}%".format(usage.cpu(end - begin)),
                "StartTime": start_time,
                "EndTime": utils.current_date(),
                "CalendarDay": utils.calendar_day(),
                "CreatedBy": item.get("CreatedBy"),
                "CreatedDate": utils.current_date(),
                "UpdatedDate": utils.current_date(),
                "IsDeleted": 0
            })
        return 1

    def _get_client(self, item):
        return AnalyzeLevel1Demand(item.get("Code"),item.get("ClientCode") ,item.get("WarehouseCode"), item.get("WarehouseSiteId"),item.get("CreatedBy"))
    
    def load_queue(self):
        filters = {
            "Status": {"$in": [constant.STATUS_NEW]},
            "IsDeleted": 0
        }
        results = []
        cursor = self.db.getCollection(os.getenv("BD_COLLECTION_DEMAND_SET")).find(
            filters).sort("__vjob_priority", -1)
        for item in cursor:
            results.append({
                "Id": item.get("_id"),
                "ClientCode": item.get("ClientCode"),
                "WarehouseCode": item.get("WarehouseCode"),
                "WarehouseSiteId": item.get("WarehouseSiteId"),
                "RocketCode": item.get("Code"),
                "Code": item.get("Code"),
                "CreatedBy": item.get("CreatedBy")
            })
        return results

    def error(self, id):
        self.db.getCollection(os.getenv("BD_COLLECTION_DEMAND_SET")).update_one(
            {"_id": id}, {"$set": {"Status": constant.STATUS_ANALYZE_ERROR}})

    def processing(self, id):
        self.db.getCollection(os.getenv("BD_COLLECTION_DEMAND_SET")).update_one(
            {"_id": id}, {"$set": {"Status": constant.STATUS_ANALYZING}})
        self.Status = 'Analyzing'
        
    def finished(self, id):
        self.db.getCollection(os.getenv("BD_COLLECTION_DEMAND_SET")).update_one(
            {"_id": id,"Status" : {"$ne": constant.STATUS_CREATE_STO}}, {"$set": {"Status": constant.STATUS_ANALYZED}})
    
    def write_logs(self, logs):
        self.db.getCollection(os.getenv("DB_COLLECTION_LOG")).insert_one(logs)

def load_queue():
    _db = db()
    filters = {
        "Name": "ANALYZE_STO_AUTOMATION",
        "Status": {"$in": [constant.STATUS_NEW]},
        "IsDeleted": 0
    }
    results = []
    cursor = _db.getCollection(os.getenv("BD_COLLECTION_DEMAND_SET")).find(
        filters).sort("__vjob_priority", -1)
    for item in cursor:
        results.append({
            "Id": item.get("_id"),
            "ClientCode": item.get("ClientCode"),
            "WarehouseCode": item.get("WarehouseCode"),
            "WarehouseSiteId": item.get("WarehouseSiteId"),
            "RocketCode": item.get("Code"),
            "Type": item.get("Name"),
            "Code": item.get("Code"),
            "CreatedBy": item.get("CreatedBy")
        })
    _db.close()
    return results


def work_start(item):
    print(item.get("Code"))
    Analysis().run(item)

def main(event):
    is_stop = False
    while not is_stop:
        if event.is_set():
            break
        work_queue = load_queue()
        if len(work_queue) > 0:
            work_pool = Pool(3)
            work_pool.map(work_start, work_queue)
        time.sleep(10)