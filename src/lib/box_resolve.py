
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

# Gom nhóm các đối tượng vào các nhóm, sao cho tổng giá trị không vượt quá định mức

# Ref: https://developers.google.com/optimization/pack/bin_packing?hl=vi
from ortools.linear_solver import pywraplp

def make_data_model(weights, max_capacity):
    data = {}
    data['weights'] = weights
    data['items'] = list(range(len(weights)))
    data['bins'] = data['items']
    data['bin_capacity'] = max_capacity
    return data

def calc(weights, max_capacity):
    data = make_data_model(weights, max_capacity)
    print(data)
    # Create the mip solver with the SCIP backend.
    solver = pywraplp.Solver.CreateSolver('SCIP')
    
    # Variables
    # x[i, j] = 1 if item i is packed in bin j.
    x = {}
    for i in data['items']:
        for j in data['bins']:
            x[(i, j)] = solver.IntVar(0, 1, 'x_%i_%i' % (i, j))

    # y[j] = 1 if bin j is used.
    y = {}
    for j in data['bins']:
        y[j] = solver.IntVar(0, 1, 'y[%i]' % j)

    # Constraints
    # Each item must be in exactly one bin.
    for i in data['items']:
        solver.Add(sum(x[i, j] for j in data['bins']) == 1)

    # The amount packed in each bin cannot exceed its capacity.
    for j in data['bins']:
        solver.Add(
            sum(x[(i, j)] * data['weights'][i] for i in data['items']) <= y[j] *
            data['bin_capacity'])

    # Objective: minimize the number of bins used.
    solver.Minimize(solver.Sum([y[j] for j in data['bins']]))

    status = solver.Solve()

    results = []
    if status == pywraplp.Solver.OPTIMAL:
        for j in data['bins']:
            if y[j].solution_value() == 1:
                bin_items = []
                for i in data['items']:
                    if x[i, j].solution_value() > 0:
                        bin_items.append(i)
                if bin_items:
                    results.append(bin_items)
    else:
        print("XRRR")

    return results


 