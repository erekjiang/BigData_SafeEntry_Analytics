import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pathlib import Path
from App.utils import *
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions

hdfs_host = "hdfs://localhost:9000"
hdfs_root_path = "/SafeEntry_Analytics/"

conf = pyspark.SparkConf()
spark = SparkSession.builder.appName("Read Parquet files").getOrCreate()
sc = pyspark.SparkContext.getOrCreate(conf=conf)

resident_file_dest = "residents.parquet"
place_file_dest = "place.parquet"
safe_entry_file_dest = "entry_records.parquet"

# Step 1: read safe entry parquet file
parquetFile = read_parquet_file(spark, hdfs_host+hdfs_root_path+safe_entry_file_dest)
parquetFile.createOrReplaceTempView("safeEntryParquetFile")
safe_entry_df = spark.sql("SELECT * FROM safeEntryParquetFile")
safe_entry_df.show()

# Step 2: read resident parquet file
parquetFile = read_parquet_file(spark, hdfs_host+hdfs_root_path+resident_file_dest)
parquetFile.createOrReplaceTempView("residentParquetFile")
resident_df = spark.sql("SELECT * FROM residentParquetFile")
resident_df.show()

# Step 3: read place parquet file
parquetFile = read_parquet_file(spark, hdfs_host+hdfs_root_path+place_file_dest)
parquetFile.createOrReplaceTempView("placeParquetFile")
place_df = spark.sql("SELECT * FROM placeParquetFile")
place_df.show()