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
import motor.motor_asyncio


async def import_content(filepath):
    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://127.0.0.1:27017')
    print(client)
    db = client['big_csv']
    data = pd.read_csv(filepath)
    print(data)
    print(type(data))
    data_json = json.loads(data.to_json(orient='records'))
    await db.test_collection.insert_many(data_json)

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