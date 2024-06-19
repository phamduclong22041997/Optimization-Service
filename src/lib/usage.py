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

import psutil
import os


def cpu(seconds = 4):
    if seconds < 1:
        seconds = 1
    return psutil.cpu_percent(seconds)

# caculate = (total – available)/total * 100 
# - total: total memory excluding swap
# - available: available memory for processes
# - percent: memory usage in per cent
# - used: the memory used
# - free: memory not used at and is readily available
def memory():
    return psutil.Process(os.getpid()).memory_info().rss

