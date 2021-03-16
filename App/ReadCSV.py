from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pathlib import Path
from App.utils import *


hdfs_host = "hdfs://localhost:9000"
hdfs_root_path = "/SafeEntry_Analytics/"

spark = SparkSession.builder.appName("Read CSV files").getOrCreate()

resident_file_path = str(Path('in/resident.csv'))
place_file_path = str(Path('in/place.csv'))
safe_entry_file_path = str(Path('in/entry_records.csv'))

resident_file_dest = "residents.parquet"
place_file_dest = "place.parquet"
safe_entry_file_dest = "entry_records.parquet"

# Read & Store resident file
resident_schema = StructType([StructField("resident_id", StringType(), False),
                              StructField("resident_name", StringType(), True),
                              StructField("nric", StringType(), False),
                              StructField("phone_number", StringType(), False)])

resident_df = read_csv_file(spark, resident_file_path,resident_schema)
resident_df.show(100, False)
resident_df.printSchema()

resident_df.write.mode("Overwrite").parquet(hdfs_host+hdfs_root_path+resident_file_dest)
print(f"============saved: {resident_file_dest} to hdfs============")


# Read & Store place file
place_schema = StructType([StructField("place_id", StringType(), False),
                              StructField("place_name", StringType(), True),
                              StructField("url", StringType(), True),
                              StructField("postal_code", StringType(), False),
                              StructField("address", StringType(), False),
                              StructField("lat", DoubleType(), True),
                              StructField("lon", DoubleType(), True),
                              StructField("place_category", StringType(), True)
                         ])

place_df = read_csv_file(spark, place_file_path,place_schema)
place_df.show(100, False)
place_df.printSchema()

place_df.write.mode("Overwrite").parquet(hdfs_host+hdfs_root_path+place_file_dest)
print(f"============saved: {place_file_dest} to hdfs============")


# Read & Store safe entry file
safe_entry_schema = StructType([StructField("record_id", StringType(), False),
                                StructField("resident_id", StringType(), False),
                              StructField("place_id", StringType(), True),
                              StructField("entry_time", TimestampType(), False),
                              StructField("exit_time", TimestampType(), True)
                         ])

safe_entry_df = read_csv_file(spark, safe_entry_file_path,safe_entry_schema)
safe_entry_df.show(100, False)
safe_entry_df.printSchema()

safe_entry_df.write.mode("Overwrite").parquet(hdfs_host+hdfs_root_path+safe_entry_file_dest)
print(f"============saved: {safe_entry_file_dest} to hdfs============")