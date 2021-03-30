import pyspark
from pyspark.sql import SparkSession
from App.utils import *

hdfs_host = "hdfs://localhost:9000"
hdfs_root_path = "/SafeEntry_Analytics/"

conf = pyspark.SparkConf().setAppName("Read Parquet files").setMaster("local[*]")
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

resident_file_dest = "residents.parquet"
place_file_dest = "place.parquet"
safe_entry_file_dest = "entry_records.parquet"

# Step 1: read safe entry parquet file
parquetFile = read_parquet_file(spark, hdfs_host+hdfs_root_path+safe_entry_file_dest)
parquetFile.createOrReplaceTempView("safeEntryParquetFile")
safe_entry_df = spark.sql("SELECT * FROM safeEntryParquetFile")
safe_entry_df.show()
safe_entry_df.printSchema()

# Step 2: read resident parquet file
parquetFile = read_parquet_file(spark, hdfs_host+hdfs_root_path+resident_file_dest)
parquetFile.createOrReplaceTempView("residentParquetFile")
resident_df = spark.sql("SELECT * FROM residentParquetFile")
resident_df.show()
resident_df.printSchema()

# Step 3: read place parquet file
parquetFile = read_parquet_file(spark, hdfs_host+hdfs_root_path+place_file_dest)
parquetFile.createOrReplaceTempView("placeParquetFile")
place_df = spark.sql("SELECT * FROM placeParquetFile")
place_df.show()
place_df.printSchema()

