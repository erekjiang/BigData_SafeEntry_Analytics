import pyspark
from pyspark.sql import SparkSession
from App.utils import *

hdfs_host = "hdfs://localhost:9000"
hdfs_root_path = "/SafeEntry_Analytics/"

conf = pyspark.SparkConf().setAppName("Read Parquet files").setMaster("local[*]")
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

resident_file_dest = "resident.parquet"
place_file_dest = "place.parquet"
safe_entry_file_dest = "entry_record.parquet"
case_file_dest = "case.parquet"
contact_graph_edge_file_dest = "contact_graph_edge.parquet"
contact_graph_vertex_file_dest = "contact_graph_vertex.parquet"

# Step 1: read safe entry parquet file
safe_entry_df  = read_parquet_file(spark, hdfs_host+hdfs_root_path+safe_entry_file_dest)
safe_entry_df.show()
safe_entry_df.printSchema()
safe_entry_df.coalesce(1).write.format("csv").option("header","true").option("sep",",").mode("overwrite").save("hdfs_files/safe_entry")

# Step 2: read resident parquet file
resident_df = read_parquet_file(spark, hdfs_host+hdfs_root_path+resident_file_dest)
resident_df.show()
resident_df.printSchema()
resident_df.coalesce(1).write.format("csv").option("header","true").option("sep",",").mode("overwrite").save("hdfs_files/resident")

# Step 3: read place parquet file
place_df = read_parquet_file(spark, hdfs_host+hdfs_root_path+place_file_dest)
place_df.show()
place_df.printSchema()
place_df.coalesce(1).write.mode("overwrite").csv("hdfs_files/place")

# Step 4: read case parquet file
case_df = read_parquet_file(spark, hdfs_host+hdfs_root_path+case_file_dest)
case_df.show()
case_df.printSchema()
case_df.coalesce(1).write.format("csv").option("header","true").option("sep",",").mode("overwrite").save("hdfs_files/case")

# Step 5: read Contact Graph Edge parquet file
contact_graph_edge_df= read_parquet_file(spark, hdfs_host+hdfs_root_path+contact_graph_edge_file_dest)
contact_graph_edge_df.show()
contact_graph_edge_df.printSchema()
contact_graph_edge_df.coalesce(1).write.format("csv").option("header","true").option("sep",",").mode("overwrite").save("hdfs_files/contact_graph_edge")

# Step 6: read Contact Graph Edge parquet file
contact_graph_vertex_df = read_parquet_file(spark, hdfs_host+hdfs_root_path+contact_graph_vertex_file_dest)
contact_graph_vertex_df.show()
contact_graph_vertex_df.printSchema()
contact_graph_vertex_df.coalesce(1).write.format("csv").option("header","true").option("sep",",").mode("overwrite").save("hdfs_files/contact_graph_vertex")