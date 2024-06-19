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

import os
from lib import db, constant
from datetime import datetime
import pytz
timeZone = pytz.timezone('Asia/Ho_Chi_Minh')
class AutoProcess:
    def __init__(self, rocket_code) -> None:
        self.rocket_code = rocket_code
        self.db = db()
            
    def create(self,data): 
        self.db.getCollection(os.getenv("BD_COLLECTION_AUTO_PROCESS")).insert_many(data)
        return True

    