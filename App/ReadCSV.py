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
spark = SparkSession.builder.appName("Read CSV files").getOrCreate()
sc = pyspark.SparkContext.getOrCreate(conf=conf)

resident_file_path = str(Path('in/resident.csv'))
place_file_path = str(Path('in/place.csv'))
safe_entry_file_path = str(Path('in/entry_records.csv'))

resident_file_dest = "residents.parquet"
place_file_dest = "place.parquet"
safe_entry_file_dest = "entry_records.parquet"

# Step 1: Read & Store resident file
resident_schema = StructType([StructField("resident_id", StringType(), False),
                              StructField("resident_name", StringType(), True),
                              StructField("nric", StringType(), False),
                              StructField("phone_number", StringType(), False),
                              StructField("last_update_dt", TimestampType(), False)])

resident_df = read_csv_file(spark, resident_file_path,resident_schema)
resident_df.show(100, False)
resident_df.printSchema()
print("new resident count: ",resident_df.count())

resident_hdsf_path = hdfs_host+hdfs_root_path+resident_file_dest
is_resident_hdfs_exist = is_hdfs_file_exist(hdfs_root_path+resident_file_dest)
if is_resident_hdfs_exist:
    existing_resident_df = spark.read.schema(resident_schema).parquet(resident_hdsf_path).cache()
    print("existing resident count: ",existing_resident_df.count())
    merged_resident_df = resident_df.union(existing_resident_df)
    merged_resident_df = merged_resident_df.sort('last_update_dt', ascending=True).dropDuplicates(subset=['nric'])
    print("merged resident count: ",merged_resident_df.count())
    merged_resident_df.write.mode("Overwrite").parquet(resident_hdsf_path)
else:
    resident_df.write.mode("Overwrite").parquet(resident_hdsf_path)

print(f"============saved: {resident_file_dest} to hdfs============")


# Step 2: Read & Store place file
place_schema = StructType([StructField("place_id", StringType(), False),
                              StructField("place_name", StringType(), True),
                              StructField("url", StringType(), True),
                              StructField("postal_code", StringType(), False),
                              StructField("address", StringType(), False),
                              StructField("lat", DoubleType(), True),
                              StructField("lon", DoubleType(), True),
                              StructField("place_category", StringType(), True),
                              StructField("last_update_dt", TimestampType(), False)])


place_df = read_csv_file(spark, place_file_path,place_schema)
place_df.show(100, False)
place_df.printSchema()

place_df.write.mode("Overwrite").parquet(hdfs_host+hdfs_root_path+place_file_dest)
print(f"============saved: {place_file_dest} to hdfs============")


# Step 3: Read & Store safe entry file
safe_entry_schema = StructType([StructField("record_id", StringType(), False),
                                StructField("resident_id", StringType(), False),
                              StructField("place_id", StringType(), True),
                              StructField("entry_time", TimestampType(), False),
                              StructField("exit_time", TimestampType(), True),
                              StructField("last_update_dt", TimestampType(), False)])

safe_entry_df = read_csv_file(spark, safe_entry_file_path,safe_entry_schema)
safe_entry_df.show(100, False)
safe_entry_df.printSchema()

safe_entry_df.write.mode("Overwrite").parquet(hdfs_host+hdfs_root_path+safe_entry_file_dest)
print(f"============saved: {safe_entry_file_dest} to hdfs============")



