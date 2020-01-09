#!/usr/bin/env python
# -*- coding: utf-8 -*-
#author tom
import asyncio

import time
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
MONGODB_URL='mongodb://mts:123456@47.106.79.197:27017/{}'

class CombineShop:
    def __init__(self, db: str, collection_read: str, collection_write: str, batch: int = 2000):
        """
        初始化
        :param db: mongo database
        :param collection_read: mongo read collection
        :param collection_write: mongo write collection
        :param batch: bulk write
        """

        self.batch = batch

        self.db = db
        self.collection_read = collection_read
        self.collection_write = collection_write

        self.url = MONGODB_URL.format(self.db)
        self.queue = asyncio.Queue()  # 共享队列
        self.finished = False  # 完成标志

    async def run(self):
        """入口"""

        await asyncio.gather(self.read(), self.write())

    def connect(self):
        """连接"""

        connection = AsyncIOMotorClient(self.url)
        return connection[self.db]

    async def read(self):
        """读,生产数据
            这边只是把数据增加一个deleted字段
        """

        db = self.connect()

        async for row in db[self.collection_read].find({}, {'_id': 0}):
            row['deleted'] = 0
            await self.queue.put(row)

        #cursor = db.test_collection.find({ 'name': { '$lt': 5}}).sort( 'i')

        #for document in await cursor.to_list(length= 100):
            
        self.finished = True

    async def write(self):
        """写，消费数据"""

        db = self.connect()
        db[self.collection_write].create_index([('id', 1)])
        db[self.collection_write].create_index([('Name', 1)])
        db[self.collection_write].create_index([('phone', 1)])
        db[self.collection_write].create_index([('Province', 1)])
        requests = []

        while not self.finished or not self.queue.empty():
            try:
                kwargs = self.queue.get_nowait()
                requests.append(kwargs)
                print(len(requests))
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.1)

            if len(requests) == self.batch:
                await db[self.collection_write].insert_many(requests)
                requests.clear()
                print('插入2000条')
        if requests:
            await db[self.collection_write].insert_many(requests)
            print('插入完成')


if __name__ == '__main__':
    startTime=time.time()
    c = CombineShop(db='qidatas', collection_read='qidata', collection_write='qidata_async')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(c.run())
    endTime=time.time()
    print(f'异步读写花费时间{endTime-startTime}')