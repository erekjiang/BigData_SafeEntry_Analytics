import pyspark
from pyspark.sql import SparkSession
from App.utils import *

hdfs_host = "hdfs://localhost:9000"
hdfs_root_path = "/SafeEntry_Analytics/"

conf = pyspark.SparkConf().setAppName("Find Cluster").setMaster("local[*]")
sc = pyspark.SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)


communities_file_dest = "communities.parquet"

communities_df= read_parquet_file(spark, hdfs_host+hdfs_root_path+communities_file_dest)

confirmed_case = 'F000006D'
label = communities_df.filter("id = '" + confirmed_case + "'" ).collect()[0]['label']

print('============Resident in the same cluster============')
communities_df.filter("label = '"+ str(label) +"'").show()