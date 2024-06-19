
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

from lib import box_resolve
from lib.constant import BubbleRule

half = lambda A: [A[:len(A)//2], A[len(A)//2:]]

class SoBubble:
    def __init__(self, options = None):
        self._active_rules:BubbleRule = [BubbleRule.MAX_SKU]

        self.__init_rule(options)
    
    def __init_rule(self, options):
        if options == None:
            return
        
        if "max_sku" in options:
            self.max_sku = options.get("max_sku")
        
        if "min_sku" in options:
            self.min_sku = options.get("min_sku")
            self._active_rules.append(BubbleRule.MIN_SKU)

        if "min_unit" in options:
            self.max_unit = options.get("min_unit")
            self._active_rules.append(BubbleRule.MIN_UNIT)
        
        if "max_unit" in options:
            self.max_unit = options.get("max_unit")
            self._active_rules.append(BubbleRule.MAX_UNIT)
        
        if options.get("allow_packing_type") == True:
            self._active_rules.append(BubbleRule.PACKAGE_TYPE)

        if "allow_group_inventory" in options:
            self.allow_group_inventory = options.get("allow_group_inventory")

    def analyze(self, data):
        #Phân hoạch theo loại đóng gói chẵn/lẻ
        tmp_data = self.package_type_rule(data)
        #Phân hoạch theo số lượng unit của SKU
        chunks = []
        chunksAbnormal = []
        roll_back_data = []
        for _data in tmp_data:
            if len(_data) > 0:
                resp = self.max_sku_rule(_data)
                if len(resp['NORMAL']) > 0:
                    chunks += resp['NORMAL']
                if len(resp['ABNORMAL']) > 0:
                    chunksAbnormal += resp['ABNORMAL']

        #khúc này em làm chưa được hop lý anh giúp em với
        resultsAbnormal = []
        for chunk in chunksAbnormal:
            rs = self.max_unit_rule(chunk)
            if len(rs['data']) > 0:
                resultsAbnormal += rs['data']
            if len(rs['data_roll_back']) > 0:
                roll_back_data += rs['data_roll_back']
                
        # Chuyển mỗi phần tử trong chunks thành một danh sách lồng
        chunks += resultsAbnormal
        return [chunks, roll_back_data]

    def min_rule(self, data):
        if BubbleRule.MAX_SKU not in self._active_rules:
            return [data]
        skus = {}
        idx = -1
        total = 0
        for val in data:
            idx += 1
            if skus.get(val[1]) == None:
                skus[val[1]] = []
            
            total += val[2]            
            skus[val[1]].append(val)
            if len(skus) == self.max_sku:
                skus = {}
        results = {
            "ABNORMAL": []
        }
        if len(skus) > 0:
            results["ABNORMAL"].append(list(skus.values()))
            skus = None

        return results
    
    def max_sku_rule(self, data):
        results = {
            "NORMAL": [],
            "ABNORMAL": []
        }
        if self.allow_group_inventory == True:
            results["NORMAL"] = [[data]]
            return results

        if BubbleRule.MAX_SKU not in self._active_rules:
            results["ABNORMAL"] = [[data]]
            return results
        
        skus = []
        for val in data:
            if val[1] not in skus:
                skus.append(val[1])     
        if len(skus) < self.max_sku:
            results["ABNORMAL"] = [[data]]
            return results
        

        skus = {}
        idx = -1
        
        for val in data:
            idx += 1
            if skus.get(val[1]) == None:
                skus[val[1]] = []

            skus[val[1]].append(val)
            if len(skus) == self.max_sku:
                results["NORMAL"].append(list(skus.values()))
                skus = {}

        if len(skus) > 0:
            results["ABNORMAL"].append(list(skus.values()))
            skus = None
        
        return results

    def max_unit_rule(self, data):
        results = {
            "total_qty" : 0,
            "data" : [],
            "data_roll_back": []
        } 
        if BubbleRule.MAX_UNIT not in self._active_rules:
            return results
        if BubbleRule.MIN_SKU not in self._active_rules:
            return results
        skus = {
            "total_unit" : 0,
            "data":{}
        }
        idx = -1
        results = {
            "total_qty" : 0,
            "data" : [],
            "data_roll_back": []
        } 
        for item in data:
            for val in item:
                idx += 1
                if skus['data'].get(val[1]) == None:
                    skus['data'][val[1]] = []
                skus['data'][val[1]].append(val)
                skus['total_unit'] += val[2]
                # skus = {}
        if len(skus['data']) >= self.min_sku or skus['total_unit'] >= self.max_unit:
            results["data"].append(list(skus['data'].values()))
        else:
            results["data_roll_back"].append(list(skus['data'].values()))
        

        return results
        


        # for item in data:
        #     results['data'].append(item)
        #     results['total_qty'] += item[2]
        # if results['total_qty'] >= self.max_unit:
        #     return results
        # else:
        #     return {
        #         "total_qty" : 0,
        #         "data": [],
        #         "data_roll_back" : results["data"]
        #     } 




    def package_type_rule(self, data):
        results = {
            "ODD": [],
            "EVEN": [],
            "NORMAL": []
        }
        idx = -1

        # [val[0], val[1], val[2], idx, val[4]] [Package Type, SKU, Qty, index, STOList]
        for val in data:
            idx += 1
            if BubbleRule.PACKAGE_TYPE not in self._active_rules:
                results["NORMAL"].append([val[0], val[1], val[2], idx,  val[3] ])
            else:
                if val[0] == 0:
                    results["ODD"].append([val[0], val[1], val[2], idx,  val[3]])
                
                if val[0] == 1:
                    results["EVEN"].append([val[0], val[1], val[2], idx,  val[3]])
        
        return [results["ODD"], results["EVEN"], results["NORMAL"]]
    