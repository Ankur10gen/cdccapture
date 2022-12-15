import os
import pymongo
import config
from bson.json_util import dumps

source_client = pymongo.MongoClient(config.source_cluster_conn_string)
dest_client = pymongo.MongoClient(config.dest_cluster_conn_string)

change_stream = source_client.watch()
for change in change_stream:
    print(dumps(change))
    dest_client.test.coll.insert_one(change)
    print('')  # for readability only
