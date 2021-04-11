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
spark = SparkSession.builder.appName("Ingest Batch Data").getOrCreate()
sc = pyspark.SparkContext.getOrCreate(conf=conf)

resident_file_path = str(Path('in/resident.csv'))
place_file_path = str(Path('in/place.csv'))
safe_entry_file_path = str(Path('in/entry_record.csv'))
case_file_path = str(Path('in/case.csv'))
case_daily_file_path = str(Path('in/sg_covid_daily_sum.csv'))

resident_file_dest = "resident.parquet"
place_file_dest = "place.parquet"
safe_entry_file_dest = "entry_record.parquet"
case_file_dest = "case.parquet"
case_daily_file_dest = "case_daily_summary.parquet"

# Step 1: Read & Store resident file
resident_schema = StructType([StructField("resident_id", StringType(), False),
                              StructField("resident_name", StringType(), True),
                              StructField("nric", StringType(), False),
                              StructField("phone_number", StringType(), False),
                              StructField("last_update_dt", TimestampType(), False)])

resident_df = read_csv_file(spark, resident_file_path, resident_schema)
resident_df.show(100, False)
resident_df.printSchema()
print("new resident count: ", resident_df.count())

resident_hdfs_path = hdfs_host + hdfs_root_path + resident_file_dest
is_resident_hdfs_exist = is_hdfs_file_exist(hdfs_root_path + resident_file_dest)
if is_resident_hdfs_exist:
    existing_resident_df = spark.read.schema(resident_schema).parquet(resident_hdfs_path).cache()
    print("existing resident count: ", existing_resident_df.count())
    merged_resident_df = resident_df.union(existing_resident_df)
    merged_resident_df = merged_resident_df.sort('last_update_dt', ascending=True).dropDuplicates(subset=['nric'])
    print("merged resident count: ", merged_resident_df.count())
    merged_resident_df.write.mode("Overwrite").parquet(resident_hdfs_path)
else:
    resident_df.write.mode("Overwrite").parquet(resident_hdfs_path)

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

place_df = read_csv_file(spark, place_file_path, place_schema)
place_df.show(100, False)
place_df.printSchema()

place_hdfs_path = hdfs_host + hdfs_root_path + place_file_dest
is_place_hdfs_exist = is_hdfs_file_exist(hdfs_root_path + place_file_dest)
if is_place_hdfs_exist:
    existing_place_df = spark.read.schema(place_schema).parquet(place_hdfs_path).cache()
    print("existing place count: ", existing_place_df.count())
    merged_place_df = place_df.union(existing_place_df)
    merged_place_df = merged_place_df.sort('last_update_dt', ascending=True).dropDuplicates(subset=['place_id'])
    print("merged place count: ", merged_place_df.count())
    merged_place_df.write.mode("Overwrite").parquet(place_hdfs_path)
else:
    place_df.write.mode("Overwrite").parquet(place_hdfs_path)

print(f"============saved: {place_file_dest} to hdfs============")

# Step 3: Read & Store safe entry file
safe_entry_schema = StructType([StructField("record_id", StringType(), False),
                                StructField("resident_id", StringType(), False),
                                StructField("place_id", StringType(), True),
                                StructField("entry_time", TimestampType(), False),
                                StructField("exit_time", TimestampType(), True),
                                StructField("last_update_dt", TimestampType(), False)])

safe_entry_df = read_csv_file(spark, safe_entry_file_path, safe_entry_schema)
safe_entry_df.show(100, False)
safe_entry_df.printSchema()

save_entry_hdsf_path = hdfs_host + hdfs_root_path + safe_entry_file_dest
is_safe_entry_hdfs_exist = is_hdfs_file_exist(hdfs_root_path + safe_entry_file_dest)
if is_safe_entry_hdfs_exist:
    existing_safe_entry_df = spark.read.schema(safe_entry_schema).parquet(save_entry_hdsf_path).cache()
    print("existing safe entry count: ", existing_safe_entry_df.count())
    merged_safe_entry_df = safe_entry_df.union(existing_safe_entry_df)
    merged_safe_entry_df = merged_safe_entry_df.sort('last_update_dt', ascending=True).dropDuplicates(
        subset=['record_id'])
    print("merged safe entry count: ", merged_safe_entry_df.count())
    merged_safe_entry_df.write.mode("Overwrite").parquet(save_entry_hdsf_path)
else:
    safe_entry_df.write.mode("Overwrite").parquet(save_entry_hdsf_path)
print(f"============saved: {safe_entry_file_dest} to hdfs============")

# Step 4: Read & Store case file
case_schema = StructType([StructField("caseId", StringType(), False),
                          StructField("nric", StringType(), True),
                          StructField("passType", StringType(), True),
                          StructField("nationality", StringType(), True),
                          StructField("race", StringType(), True),
                          StructField("name", StringType(), True),
                          StructField("birthDt", TimestampType(), True),
                          StructField("age", StringType(), True),
                          StructField("gender", StringType(), True),
                          StructField("diagnosedDate", TimestampType(), True),
                          StructField("active", StringType(), True),
                          StructField("activeStatus", StringType(), True),
                          StructField("importedCase", StringType(), True),
                          StructField("importedFromCountry", StringType(), True),
                          StructField("hospitalizedHospital", StringType(), True),
                          StructField("admittedDt", TimestampType(), True),
                          StructField("dischargedDt", TimestampType(), True),
                          StructField("deceased", StringType(), True),
                          StructField("deceasedDt", TimestampType(), True),
                          StructField("createdDttm", TimestampType(), True),
                          StructField("lastUpdatedDttm", TimestampType(), True)])

case_df = read_csv_file(spark, case_file_path, case_schema)
case_df.show(100, False)
case_df.printSchema()

case_hdfs_path = hdfs_host + hdfs_root_path + case_file_dest
is_case_hdfs_exist = is_hdfs_file_exist(hdfs_root_path + case_file_dest)
if is_case_hdfs_exist:
    existing_case_df = spark.read.schema(case_schema).parquet(case_hdfs_path).cache()
    print("existing case count: ", existing_case_df.count())
    merged_case_df = case_df.union(existing_case_df)
    merged_case_df = merged_case_df.sort('lastUpdatedDttm', ascending=True).dropDuplicates(
        subset=['caseId'])
    print("merged case count: ", merged_case_df.count())
    merged_case_df.write.mode("Overwrite").parquet(case_hdfs_path)
else:
    case_df.write.mode("Overwrite").parquet(case_hdfs_path)
print(f"============saved: {case_file_dest} to hdfs============")

# Step 5: Read & Store case daily summary file
case_daily_schema = StructType([StructField("date", StringType(), False),
                                StructField("dailyConfirmed", IntegerType(), True)
                                ])

case_daily_df = read_csv_file(spark, case_daily_file_path, case_daily_schema)
case_daily_df.show(100, False)
case_daily_df.printSchema()

case_daily_hdfs_path = hdfs_host + hdfs_root_path + case_daily_file_dest
case_daily_df.write.mode("Overwrite").parquet(case_daily_hdfs_path)
print(f"============saved: {case_daily_file_dest} to hdfs============")