#!/usr/bin/env python
# -*- coding: utf-8 -*-
#author tom
import sys
import pandas as pd
import json
import os
import time
import asyncio
import time
from motor.motor_asyncio import AsyncIOMotorClient


async def import_content(filepath):
    client = AsyncIOMotorClient('mongodb://mts:123456@127.0.0.1:27017/big_csv')
    db = client['big_csv']
    collection=db['ip_dns']
    for chunk in pd.read_csv(filepath, chunksize=20000):
        data_json = json.loads(chunk.to_json(orient='records'))
        await collection.insert_many(data_json)
    print('数据插入成功')

if __name__ == "__main__":
    start = time.time()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(import_content('file.csv'))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        end = time.time()
        print(f'Finished in {end - start} seconds')