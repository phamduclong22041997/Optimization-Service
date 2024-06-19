
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
import numpy as np
from lib import box_resolve
from lib.constant import BubbleRule

half = lambda A: [A[:len(A)//2], A[len(A)//2:]]

class StoBubble:
    def __init__(self, options = None):
        self._active_rules:BubbleRule = [BubbleRule.MAX_SKU]

        self.__init_rule(options)
    
    def __init_rule(self, options):
        if options == None:
            return
        
        if "max_sku" in options:
            self.max_sku = options.get("max_sku")
        
        if "max_unit" in options:
            self.max_unit = options.get("max_unit")
            self._active_rules.append(BubbleRule.MAX_UNIT)
        
        if options.get("allow_packing_type") == True:
            self._active_rules.append(BubbleRule.PACKAGE_TYPE)

    def analyze(self, data):
        #Phân hoạch theo loại đóng gói chẵn/lẻ
        tmp_data = self.package_type_rule(data)

        #Phân hoạch theo số lượng unit của SKU
        chunks = []
        for _data in tmp_data:
            if len(_data) > 0:
                chunks  += self.max_unit_rule(_data)
        
        #Phân hoạch theo tổng số SKU
        results = []
        for chunk in chunks:
            rs = self.max_sku_rule(chunk, [])
            if len(rs) > 0:
                results += rs
        
        return results
    
    def sort_routing(self):
        pass

    def max_unit_rule(self, data):
        if BubbleRule.MAX_UNIT not in self._active_rules:
            return [data]
        results = []
        weights = []
        analyze_data = []
        for item in data:
            if item[1] >= self.max_unit:
                results.append([item])
            else:
                weights.append(item[1])
                analyze_data.append(item)

        # Sử dụng thuật toán Packing Problem để nhóm các SKU sao cho tổng của chúng <= tổng số lượng unit của STO
        if len(analyze_data) > 1:
            _classifs = box_resolve.calc(weights, self.max_unit)
            
            for item in _classifs:
                vals = []
                for idx in item:
                    vals.append(analyze_data[idx])
                results.append(vals)
        else:
            if len(analyze_data) > 0:
                results.append(analyze_data)
        return results

    def package_type_rule(self, data):
        results = {
            "ODD": [],
            "EVEN": [],
            "NORMAL": []
        }
        idx = -1
        for val in data:
            idx += 1
            if BubbleRule.PACKAGE_TYPE not in self._active_rules:
                results["NORMAL"].append([val[0], val[1], idx])
            else:
                if val[0] == 0:
                    results["ODD"].append([val[0], val[1], idx])
                
                if val[0] == 1:
                    results["EVEN"].append([val[0], val[1], idx])
        
        return [results["ODD"], results["EVEN"], results["NORMAL"]]
    
    def max_sku_rule(self, data, results = []):
        if len(data) == 0:
            return results
        
        l = len(data)/self.max_sku
        if l == 0:
            results.append(data)
            return results

        if l <= 1:
            results.append(data)
            return results

        if l <= 2:
            r = half(data)
            results.append(r[0])
            results.append(r[1])
            return results

        if l > 2:
            results.append(data[:self.max_sku])
            return self.max_sku_rule(data[self.max_sku:], results)