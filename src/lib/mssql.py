
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

import pymssql

class mssql:
    def __init__(self, optios ={}):
        # self.cusor = None
        self.driver = None
        self.username = None
        self.password = None
        self.host = None
        self.port = None
        self.database = "master"
        
    
    # def open_ssh(self, removeip, host, port, username, password ):
    #     server = SSHTunnelForwarder(
    #         removeip,
    #         ssh_username=username,
    #         ssh_password=password,
    #         remote_bind_address=(host, port)
    #     )
    #     server.start()
    #     print(server.local_bind_port)
    #     print(server)

        
    def connect_mssql(self):
        try:
           return pymssql.connect(server=self.host, port=self.port, user=self.username, password=self.password, database=self.database)
                       
        except Exception as ex:
            print(ex)
            return None

    def connect_mssql_with_config(self,config):
        try:
           return pymssql.connect(server=config.get("host"), port=config.get("port"), user=config.get("username"), password=config.get("password"), database=config.get("database"))
                       
        except Exception as ex:
            print(ex)
            return None

    
    def select(self, sql):
        conn = self.connect_mssql()
        cursor = conn.cursor(as_dict=True)
        cursor.execute(sql)
        data = cursor.fetchall()
        return data
    
    def select_with_config(self, sql, config):
        try:
            conn = self.connect_mssql_with_config(config)
            if conn == None:
                print('not connect sql')
                return None
            
            cursor = conn.cursor(as_dict=True)
            cursor.execute(sql)
            data = cursor.fetchall()
            return data
        except Exception as ex:
            print(ex)
            return None

           
