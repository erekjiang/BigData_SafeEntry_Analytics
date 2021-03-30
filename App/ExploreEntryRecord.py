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

conf = pyspark.SparkConf().setAppName("Explore Entry Record").setMaster("local[*]")
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

resident_file_dest = "residents.parquet"
place_file_dest = "place.parquet"
safe_entry_file_dest = "entry_records.parquet"
contact_graph_edge_file_dest = "contact_graph_edge.parquet"
contact_graph_vertex_file_dest = "contact_graph_vertex.parquet"

e = read_parquet_file(spark, hdfs_host + hdfs_root_path + contact_graph_edge_file_dest)
v = read_parquet_file(spark, hdfs_host + hdfs_root_path + contact_graph_vertex_file_dest)
g = GraphFrame(v, e)

g.edges.show()
g.vertices.show();