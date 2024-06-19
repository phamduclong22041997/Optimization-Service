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

#Ref: https://developers.google.com/optimization/pack/multiple_knapsack?hl=vi
from ortools.sat.python import cp_model

def make_data_model(weights, values, capacities):
    data = {}
    data['weights'] = weights # List qty picking of STO
    data['values'] = values # List total qty of sto
    data['num_items'] = len(data['weights'])
    data['all_items'] = range(data['num_items'])

    data['bin_capacities'] = capacities
    data['num_bins'] = len(data['bin_capacities'])
    data['all_bins'] = range(data['num_bins'])
    return data


def calc(weights, values, capacities):
    data = make_data_model(weights, values, capacities)

    model = cp_model.CpModel()

    # Variables.
    # x[i, b] = 1 if item i is packed in bin b.
    x = {}
    for i in data['all_items']:
        for b in data['all_bins']:
            x[i, b] = model.NewBoolVar(f'x_{i}_{b}')

    # Constraints.
    # Each item is assigned to at most one bin.
    for i in data['all_items']:
        model.AddAtMostOne(x[i, b] for b in data['all_bins'])

    # The amount packed in each bin cannot exceed its capacity.
    for b in data['all_bins']:
        model.Add(
            sum(x[i, b] * data['weights'][i]
                for i in data['all_items']) <= data['bin_capacities'][b])

    # Objective.
    # Maximize total value of packed items.
    objective = []
    for i in data['all_items']:
        for b in data['all_bins']:
            objective.append(
                cp_model.LinearExpr.Term(x[i, b], data['values'][i]))
    model.Maximize(cp_model.LinearExpr.Sum(objective))

    solver = cp_model.CpSolver()
    status = solver.Solve(model)

    results = []
    if status == cp_model.OPTIMAL:
        for b in data['all_bins']:
            for i in data['all_items']:
                if solver.Value(x[i, b]) > 0:
                    results.append([b, i, data['weights'][i], 0])
    
    distributed = []
    for item in results:
        capacities[item[0]] -= item[2]
        distributed.append(item[1])
    
    # Phân hoạch lại cho những SKU không resolve được
    if len(results) < len(weights):
        idx = -1
        for qty in weights:
            idx += 1
            if idx in distributed:
                continue
            
            total_qty = 0
            idx2 = -1
            is_picked = 0

            for _qty in capacities:
                idx2 += 1
                _pick_qty = 0
                if total_qty == qty:
                    break
                if _qty == 0:
                    continue

                if (total_qty + _qty) > qty:
                    _pick_qty = (qty - total_qty)
                    capacities[idx2] -= (qty - total_qty)
                    total_qty = qty
                else:
                    total_qty += _qty
                    _pick_qty = _qty
                    capacities[idx2] = 0
                
                missing_qty = 0

                if (idx2 + 1) == len(capacities):
                    if total_qty < qty:
                        missing_qty = qty - total_qty

                is_picked = 1
                results.append([idx2, idx, _pick_qty, missing_qty])
            
            if is_picked == 0:
                results.append([0, idx, 0, qty])

    return results