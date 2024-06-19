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

import sys
import os
from dotenv import load_dotenv
import asyncio
import threading

load_dotenv()

scriptpath = "src/"
sys.path.append(os.path.abspath(scriptpath))

from supervisor import analyze_sto, analyze_so, analyze_sto_distribution, analyze_trucking_plan, analyze_trucking_plan_transaction

def main(event):
    anlyze_sto_obj = threading.Thread(target=analyze_sto.main, args=(event,))
    analyze_distribution = threading.Thread(target=analyze_sto_distribution.main, args=(event,))
    analyze_so_obj = threading.Thread(target=analyze_so.main, args=(event,))
    analyze_trucking_obj = threading.Thread(target=analyze_trucking_plan.main, args=(event,))
    analyze_transation_obj = threading.Thread(target=analyze_trucking_plan_transaction.main, args=(event,))

    anlyze_sto_obj.start()
    analyze_distribution.start()
    analyze_so_obj.start()
    analyze_trucking_obj.start()
    analyze_transation_obj.start()
if __name__ == "__main__":
    event = threading.Event()
    main(event)
    
    loop = asyncio.get_event_loop()
    try:
        loop.run_forever()
    finally:
        event.set()
        loop.close()