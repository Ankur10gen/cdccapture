import os
import pymongo
import config
from bson.json_util import dumps
from bson import timestamp
from datetime import datetime
from datetime import timezone

# source is the BizProd system and dest is our CSPersistentStore
source_client = pymongo.MongoClient(config.source_cluster_conn_string)
dest_client = pymongo.MongoClient(config.dest_cluster_conn_string)

# Sample Change Stream Document
# {"_id":{"_data":"82639A908F000000012B022C0100296E5A1004F80578A4E6F74E52B4D1781C6C005F8146645F69640064639A908C3322D17839F4C3720004"},"operationType":"insert","clusterTime":{"$timestamp":{"t":1671073935,"i":1}},"fullDocument":{"_id":{"$oid":"639a908c3322d17839f4c372"},"a":{"$numberInt":"1"}},"ns":{"db":"ankur","coll":"raina"},"documentKey":{"_id":{"$oid":"639a908c3322d17839f4c372"}}}

# Record a start time in the milestone collection on dest
dest_client.persist.milestone.update_one(
    {}, {"$set": {"ts": datetime.now(timezone.utc), "batch": 1}}, upsert=True)

# Watch for the changes
change_stream = source_client.admin.aggregate(
    [{"$changeStream": {"allChangesForCluster": True, "showExpandedEvents": True}}])

for change in change_stream:
    # replace ns field with namespace
    # this field currently conflicts with Atlas Data Federation
    change["namespace"] = change.pop("ns")

    # convert internal BSON timestamp to Date format
    change["clusterTime"] = datetime.utcfromtimestamp(
        change["clusterTime"].time)

    print(dumps(change))

    # write the change in persistent store
    dest_client.persist.changestreams.insert_one(change)
    print('')  # for readability only
