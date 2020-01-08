#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author tom
import sys
import pandas as pd
import pymongo
import json
import os
import time


def import_content(filepath):
    mng_client = pymongo.MongoClient('mongodb://mts:123456@127.0.0.1:27017/big_csv')
    mng_db = mng_client['big_csv']
    collection_name = 'ip_dns_tongu'
    db_cm = mng_db[collection_name]
    cdir = os.path.dirname(__file__)
    file_res = os.path.join(cdir, filepath)
    for chunk in pd.read_csv(file_res, chunksize=20000):
        data_json = json.loads(chunk.to_json(orient='records'))
        db_cm.insert_many(data_json)


if __name__ == "__main__":
    start = time.time()
    import_content('file.csv')
    end = time.time()
    print(f'Finished in {end - start} seconds')
