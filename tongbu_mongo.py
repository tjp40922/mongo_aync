#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author tom
import time
import pymongo


def tongbu_insert(collection, collection_new):
    collection_new.create_index([('id', 1)])
    collection_new.create_index([('Name', 1)])
    collection_new.create_index([('phone', 1)])
    collection_new.create_index([('Province', 1)])
    item_list = []
    for row in collection.find({}, {'_id': 0}):
        row['deleted'] = 0
        item_list.append(row)
        if len(item_list) == 2000:
            collection_new.insert_many(item_list)
            item_list.clear()
    if item_list:
        collection_new.insert_many(item_list)
        item_list.clear()


if __name__ == '__main__':
    startTime = time.time()
    client = pymongo.MongoClient(host='47.106.79.197', port=27017)
    database = client['qidatas']
    database.authenticate('mts','123456')
    collection = database['qidata']
    collection_new = database['qidata_tongbu']
    tongbu_insert(collection,collection_new)
    endTime = time.time()
    print(f'同步花费时间{endTime-startTime}')
