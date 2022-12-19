source_cluster_user_name = ""
source_cluster_user_password = ""
source_cluster_conn_string = "mongodb+srv://{0}:{1}@bizprod.u1yty.mongodb.net/?retryWrites=true&w=majority".format(
    source_cluster_user_name, source_cluster_user_password)

dest_cluster_user_name = ""
dest_cluster_user_password = ""
dest_cluster_conn_string = "mongodb+srv://{0}:{1}@cspersistentstore.p8wznsr.mongodb.net/?retryWrites=true&w=majority".format(
    dest_cluster_user_name, dest_cluster_user_password)
