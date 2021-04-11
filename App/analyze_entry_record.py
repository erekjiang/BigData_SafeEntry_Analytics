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

resident_file_dest = "resident.parquet"
place_file_dest = "place.parquet"
safe_entry_file_dest = "entry_record.parquet"
contact_graph_edge_file_dest = "contact_graph_edge.parquet"
contact_graph_vertex_file_dest = "contact_graph_vertex.parquet"

# Step 1: retrieve graph data
e = read_parquet_file(spark, hdfs_host + hdfs_root_path + contact_graph_edge_file_dest)
v = read_parquet_file(spark, hdfs_host + hdfs_root_path + contact_graph_vertex_file_dest)
g = GraphFrame(v, e)

g.edges.show()
g.vertices.show();

# Step 2: visualize in NetworkX
g_pdf = g.edges.select("*").toPandas()

import networkx as nx
import matplotlib.pyplot as plt
g_nx = nx.from_pandas_edgelist(g_pdf, source='src', target='dst')
nx.draw(g_nx,node_size = 10)
plt.show()

# Step 3.1: Find Close Node
from pyspark.sql.functions import explode

confirmed_cases = ['rid_949']

shortest_path_df = g.shortestPaths(landmarks=confirmed_cases)
shortest_path_df.select("id", "distances").orderBy('id').show(1000,False)

shortest_path_df=shortest_path_df.select("id", explode("distances"))

print(f"============close contact as below============")
shortest_path_df.filter(shortest_path_df.value == 1).show()

# Step 3.2: Community Detection
# can not use SCC and CC

communities = g.labelPropagation(maxIter=10)
#communities.select('id', 'label').orderBy("label").show()

from pyspark.sql import functions as func
communities.sort("label").groupby("label").agg(func.collect_list("id")).show(50,truncate=False)