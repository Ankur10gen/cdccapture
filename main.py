import os
import pymongo
import config
from bson.json_util import dumps

source_client = pymongo.MongoClient(config.source_cluster_conn_string)
dest_client = pymongo.MongoClient(config.dest_cluster_conn_string)

# Sample Change Stream Document
# {"_id":{"_data":"82639A908F000000012B022C0100296E5A1004F80578A4E6F74E52B4D1781C6C005F8146645F69640064639A908C3322D17839F4C3720004"},"operationType":"insert","clusterTime":{"$timestamp":{"t":1671073935,"i":1}},"fullDocument":{"_id":{"$oid":"639a908c3322d17839f4c372"},"a":{"$numberInt":"1"}},"ns":{"db":"ankur","coll":"raina"},"documentKey":{"_id":{"$oid":"639a908c3322d17839f4c372"}}}

change_stream = source_client.watch()
for change in change_stream:
    print(dumps(change))
    change["namespace"] = change.pop("ns")
    dest_client.persist.changestreams.insert_one(change)
    print('')  # for readability only
