import os

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 pyspark-shell"
)

import pyspark
from pyspark.sql import SparkSession
from App.utils import *
from graphframes import *

hdfs_host = "hdfs://localhost:9000"
hdfs_root_path = "/SafeEntry_Analytics/"

conf = pyspark.SparkConf().setAppName("Process Entry Record Graph").setMaster("local[*]")
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

resident_file_dest = "resident.parquet"
safe_entry_daily_file_dest = "entry_record.parquet"
contact_graph_edge_file_dest = "contact_graph_edge.parquet"
contact_graph_vertex_file_dest = "contact_graph_vertex.parquet"

# Step 1: read resident parquet file
resident_df = read_parquet_file(spark, hdfs_host + hdfs_root_path + resident_file_dest)
# resident_df.show()
# resident_df.printSchema()

# Step 2: read safe entry parquet file and build close contact graph
contact_list = []
f = open("in/tmp/entry_date.txt")
date_list = f.read().splitlines()

for i in range(len(date_list)):
    safe_entry_daily_df= read_parquet_file(spark,
                                      hdfs_host + hdfs_root_path + date_list[i] + '_' + safe_entry_daily_file_dest)
    safe_entry_daily_df.cache()
    print('------' + hdfs_host + hdfs_root_path + date_list[i] + '_' + safe_entry_daily_file_dest + '-------')
    #safe_entry_daily_df.show()
    print('number of rows', safe_entry_daily_df.count())

    data_collect = safe_entry_daily_df.rdd.collect()
    safe_entry_daily_df.unpersist()

    row_count = len(data_collect)

    for i in range(row_count):
        for j in range(i + 1, row_count):
            # print(i, j)
            minute_diff = (data_collect[i]['exit_time'] - data_collect[j]['entry_time']).total_seconds() / 60.0
            same_place = data_collect[i]['place_id'] == data_collect[j]['place_id']

            if (minute_diff > 5 and same_place):
                contact_tuple_dir1 = (data_collect[i]['resident_id'], data_collect[j]['resident_id'])
                contact_tuple_dir2 = (data_collect[j]['resident_id'], data_collect[i]['resident_id'])
                contact_list.append(contact_tuple_dir1)
                contact_list.append(contact_tuple_dir2)


print('contact list', contact_list)
print('number of close contact', len(contact_list))

v = resident_df.withColumnRenamed('resident_id', 'id')
e = spark.createDataFrame(contact_list, ['src', 'dst'])

g = GraphFrame(v, e)
g.cache()
g.edges.show()
g.vertices.show();

save_contact_vertex_hdfs_path = hdfs_host + hdfs_root_path + contact_graph_vertex_file_dest
save_contact_edge_hdsf_path = hdfs_host + hdfs_root_path + contact_graph_edge_file_dest
g.vertices.write.mode("Overwrite").parquet(save_contact_vertex_hdfs_path)
g.edges.write.mode("Overwrite").parquet(save_contact_edge_hdsf_path)
g.unpersist()