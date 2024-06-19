
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

from lib import bin_resolve
from lib.constant import BubbleRule

half = lambda A: [A[:len(A)//2], A[len(A)//2:]]

class SoBubble:
    def __init__(self, options = None):
        self._active_rules:BubbleRule = [BubbleRule.MAX_SKU]
        self.max_sku = 35

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

    def analyze(self, data, capacities):
        results = self.package_type_rule(data, capacities)
        
        return results

    def package_type_rule(self, data, capacities):
        minings = {
            "ODD": [],
            "EVEN": [],
            "NORMAL": []
        }
        idx = -1
        for val in data:
            idx += 1
            if BubbleRule.PACKAGE_TYPE not in self._active_rules:
                minings["NORMAL"].append([val[1],  idx])
            else:
                if val[0] == 0:
                    minings["ODD"].append([val[1], idx])
                
                if val[0] == 1:
                    minings["EVEN"].append([val[1], idx])
        resp = []
        for key in minings:
            chunk = minings[key]
            if len(chunk) == 0:
                continue
            
            _resp = self.smart_resolve(chunk, capacities)
            for item in _resp:
                sku_line_index = item[1]
                # Location index, SKU Line Index, Total Pick
                resp.append([item[0], chunk[sku_line_index][1], item[2], item[3]])
        return resp
        
    def smart_resolve(self, data, capacities):
        weights = []
        values = []
        for item in data:
            if(item[0]> capacities['total_qty']):
                capacities['data'] = [0]
            weights.append(item[0])
            values.append(1)
        result = bin_resolve.calc(weights, values, capacities['data'])
        # [bin, sku line, pick qty, value]

        return result