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

import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa
import shutil
import os
import gc


def lookup(t):
    if pa.types.is_decimal128(t):
        return pd.Float64Dtype()


def lookup_numeric(field):
    if pa.types.is_decimal128(field.type):
        return field.name
    if pa.types.is_decimal256(field.type):
        return field.name
    if pa.types.is_float64(field.type):
        return field.name
    if pa.types.is_float32(field.type):
        return field.name
    if pa.types.is_float16(field.type):
        return field.name
    if pa.types.is_decimal(field.type):
        return field.name
    if pa.types.is_floating(field.type):
        return field.name
    return ""


def convert_numeric_fr_column(df, columns):
    for col in columns:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors='coerce')


def convert_row(data, column, rs_data):
    idx = len(rs_data) - 1
    for val in data:
        idx += 1
        if column["IsNumberic"]:
            val = pd.to_numeric(val, errors='coerce')
        if idx not in rs_data:
            rs_data.append({})
        rs_data[idx][column["Name"]] = val
    return rs_data


class PQ:
    def __init__(self, file_path):
        self.file_path = file_path
        self.convert_numeric_columns = {}

    def make_rocket_code(self):
        file_name = os.path.basename(self.file_path).replace(".parquet", "")
        return file_name

    def read(self, parserObj=None):
        self._read(self.file_path, parserObj)
        if parserObj != None:
            parserObj.finish()

    def read_by_multi_file(self, parserObj = None):
        dir_path = self.split_multi_file(parserObj.group_fields)
        dir_list = []
        for (root, dirs, files) in os.walk(dir_path):
            for file in files:
                baseName, ext = os.path.splitext(file)
                if ext != ".parquet":
                    continue
                
                dc_dir = ""
                mch_dir = ""
                _dir = os.path.basename(root).split("=")
                if len(_dir) < 2:
                    continue

                if _dir[0] == "Ma_DC":
                    dc_dir = _dir[1].strip()
                else:
                    mch_dir = _dir[1].strip()
                
                _dir = os.path.basename(os.path.dirname(root)).split("=")
                if len(_dir) > 1:
                    dc_dir = _dir[1]
                
                if dc_dir != "":
                    dir_list.append({
                        "file_path": "{0}/{1}".format(root, file),
                        "dc_site": dc_dir,
                        "mch": mch_dir
                        })
                
        for file in dir_list:
            file_path = file['file_path']
            print("Reading file: {0}".format(file_path))
            self._read(file_path, parserObj, {"dc_site": file["dc_site"], "mch": file["mch"]})
            if os.path.exists(file_path):
                os.remove(file_path)
            gc.collect()
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
        
        if parserObj != None:
            parserObj.finish()
    def _read(self, file_path, parserObj=None, dcSite = None):
        pfile = pq.ParquetFile(file_path)
        print(pfile.metadata)

        if len(self.convert_numeric_columns) == 0:
            for field in pfile.schema_arrow:
                col = lookup_numeric(field)
                self.convert_numeric_columns[field.name] = {
                    "IsNumberic": col != ""
                }

        for table in pfile.iter_batches(batch_size = 30000, use_pandas_metadata=True):
            df = table.to_pandas()
            if parserObj != None:
                parserObj.process(df, self.convert_numeric_columns, dcSite)
            gc.collect()

    def split_multi_file(self, partition_cols = ['Ma_DC', 'Nganh_Hang_MCH3']):
        dir_path = self.file_path.replace(".parquet", "")
        
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)

        if os.path.exists(dir_path) == False:
            os.makedirs(dir_path)
        
        splitter = ParquetSplitter(
            src_parquet_path=self.file_path,
            target_dir=dir_path,
            num_chunks=3
        )
        splitter.split(partition_cols)
        return dir_path

    def move(self, new_dir):
        if os.path.exists(new_dir) == False:
            os.makedirs(new_dir)
        new_path = "{0}/{1}".format(new_dir, os.path.basename(self.file_path))
        shutil.move(self.file_path, new_path)


class ParquetSplitter:
    def __init__(self,
                 src_parquet_path: str,
                 target_dir: str,
                 num_chunks: int = 25
                 ):
        self._src_parquet_path = src_parquet_path
        self._target_dir = target_dir
        self._num_chunks = num_chunks

        self._src_parquet = pq.ParquetFile(
            self._src_parquet_path,
            memory_map=True,
        )

        self._total_group_num = self._src_parquet.num_row_groups
        self._schema = self._src_parquet.schema

        if self._num_chunks > self._src_parquet.num_row_groups:
            self._num_chunks = self._src_parquet.num_row_groups

    @property
    def num_row_groups(self):
        print(f'Total num of groups found: {self._total_group_num}')
        return self._src_parquet.num_row_groups

    @property
    def schema(self):
        return self._schema

    def read_rows(self):
        for elem in self._src_parquet.iter_batches():
            elem: pa.RecordBatch
            print(elem.to_pydict())

    def split(self, partition_cols):
        for chunk_num, chunk_range in self._next_chunk_range():
            table = self._src_parquet.read_row_groups(row_groups=chunk_range)
            print(f'Writing chunk #{chunk_num}...')
            pq.write_to_dataset(
                table=table,
                root_path=self._target_dir,
                partition_cols= partition_cols,
            )
        print(f'Finished chunk file.')

    def _next_chunk_range(self):
        upper_bound = self.num_row_groups

        chunk_size = upper_bound // self._num_chunks

        chunk_num = 0
        low, high = 0, chunk_size
        while low < upper_bound:
            group_range = list(range(low, high))

            yield chunk_num, group_range
            chunk_num += 1
            low, high = low + chunk_size, high + chunk_size
            if high > upper_bound:
                high = upper_bound

    @staticmethod
    def _get_row_hour(row: pa.RecordBatch):
        return row.to_pydict()['played_at'][0].hour
