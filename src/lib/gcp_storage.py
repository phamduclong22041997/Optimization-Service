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

from google.cloud import storage
from datetime import datetime
import pytz

timeZone = pytz.timezone('Asia/Ho_Chi_Minh')

class download:
    def __init__(self, config):
        self.storage_client = None
        self.config = config

    def set_config(self, config):
        self.config = config

    def getClient(self):
        if self.storage_client == None:
            print(self.config['gcp_credentitals'])
            self.storage_client = storage.Client.from_service_account_json(self.config['gcp_credentitals'])
        return self.storage_client

    def list_file(self, dir):
        bucket_name = self.config['bucket_name']
        storage_client = self.getClient()
        return storage_client.list_blobs(bucket_name, prefix=dir)

    def get_file(self, src_file_path, dst_file_path):
        try:
            bucket_name = self.config['bucket_name']
            storage_client = self.getClient()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(src_file_path)
            blob.download_to_filename(dst_file_path)
            return dst_file_path;
        except:
            return "";

    def clean(self, blob_name):
        bucket_name = self.config['bucket_name']
        storage_client = self.getClient()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
