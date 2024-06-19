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

import os
from pymongo import MongoClient

class db:
    def __init__(self, db_name = None, alias = None):
        self.client = None
        self.mongo_uri = os.getenv("MONGO_URI")
        self.db_name = os.getenv('DB_NAME')
        if alias != None:
            self.mongo_uri = alias
        if db_name != None:
            self.db_name = db_name

    def getClient(self):
        return MongoClient(self.mongo_uri)

    def getCollection(self, name, db_name = None):
        if self.client == None:
            self.client = self.getClient()
        
        if db_name == None:
            db_name = self.db_name
        dbname = self.client[db_name]
        return dbname[name]

    def close(self):
        if self.client != None:
            self.client.close()
            self.client = None
