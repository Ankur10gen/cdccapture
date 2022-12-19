import config
import requests
from requests.auth import HTTPDigestAuth
import json
import time

private_key = config.PRIVATE_KEY
public_key = config.PUBLIC_KEY
group_id = config.GROUP_ID
cluster_name = config.CLUSTER_NAME

auth = HTTPDigestAuth(public_key, private_key)

create_snapshot_url = f"https://cloud.mongodb.com/api/atlas/v1.0/groups/{group_id}/clusters/{cluster_name}/backup/snapshots"

data = {
    "description": "On Demand Snapshot",
    "retentionInDays": 1
}

headers = {
    "Accept": "application/json",
    "Content-Type": "application/json"
}

print(create_snapshot_url)

# create a snapshot
res_create_snapshot = requests.post(
    url=create_snapshot_url, data=json.dumps(data), headers=headers, auth=auth)
print(res_create_snapshot.json())

# get snapshot id
snapshot_id = res_create_snapshot.json()["id"]

# Start snapshot status checks
snapshot_status = "starting status checks"
print(snapshot_status)
while snapshot_status != "completed":
    # check snapshot status
    check_snapshot_status = requests.get(
        url=f"https://cloud.mongodb.com/api/atlas/v1.0/groups/{group_id}/clusters/{cluster_name}/backup/snapshots/{snapshot_id}", auth=auth).json()

    snapshot_status = check_snapshot_status["status"]
    print("checking status")
    print("status is: ", snapshot_status)
    if snapshot_status != "completed":
        time.sleep(30)

print("Snapshot Taken")
print("Now Exporting to S3")
print("Stay Tuned")

# Export Snapshot to S3
# Prerequisite - Setup the cloud bucket access
# Follow https://www.mongodb.com/docs/atlas/backup/cloud-backup/export/#prerequisites

# Get the S3 buckets
get_buckets_url = f"https://cloud.mongodb.com/api/atlas/v1.0/groups/{group_id}/backup/exportBuckets"
res_buckets_list = requests.get(auth=auth, url=get_buckets_url)
print(res_buckets_list.json())

# Get the access roles
cloud_provider_access_roles_url = f"https://cloud.mongodb.com/api/atlas/v1.0/groups/{group_id}/cloudProviderAccess"
cloud_provider_access_roles_list = requests.get(auth=auth,
                                                url=cloud_provider_access_roles_url)
iam_role_id = cloud_provider_access_roles_list.json()[
    "awsIamRoles"][0]["roleId"]
print(iam_role_id)

# Grant one aws bucket
grant_aws_bucket_url = f"https://cloud.mongodb.com/api/atlas/v1.0/groups/{group_id}/backup/exportBuckets"
grant_aws_bucket_data = {
    "bucketName": "ankurs-bizprod-bucket",
    "cloudProvider": "AWS",
    "iamRoleId": iam_role_id
}

# grant_aws_bucket_res = requests.post(
#     auth=auth, url=grant_aws_bucket_url, data=json.dumps(grant_aws_bucket_data), headers=headers)
# print(grant_aws_bucket_res.json())

# Get the aws bucket
get_aws_buckets_url = f"https://cloud.mongodb.com/api/atlas/v1.0/groups/{group_id}/backup/exportBuckets"
res = requests.get(auth=auth, url=get_aws_buckets_url)
print(res.json())

# Get first aws bucket
aws_bucket = res.json()["results"][0]["_id"]

# Export backup to bucket
export_backup_url = f"https://cloud.mongodb.com/api/atlas/v1.0/groups/{group_id}/clusters/{cluster_name}/backup/exports"
export_backup_data = {
    "customData": [
        {
            "key": "who",
            "value": "ankur"
        }
    ],
    "exportBucketId": aws_bucket,
    "snapshotId": snapshot_id}
res = requests.post(auth=auth, url=export_backup_url,
                    data=json.dumps(export_backup_data), headers=headers)
print(res.json())

export_job_state = "initial"
export_job_id = res.json()["id"]
print("Export Job Status: ", export_job_state)

while export_job_state != "Successful":
    # print("Current state is: ", export_job_state)
    time.sleep(1)
    res = requests.get(auth=auth, url=export_backup_url)
    export_job_state = res.json()["results"][0]["state"]
    print(export_job_state)

print("Export Job State: ", export_job_state)
print("Initial snapshot copied to S3")
