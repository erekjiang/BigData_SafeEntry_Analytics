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
case_daily_summary_file_dest = "case_daily_summary.parquet"

safe_entry_export_dest = "hdfs_files/safe_entry"
resident_export_dest ="hdfs_files/resident"
place_export_dest ="hdfs_files/place"
case_export_dest ="hdfs_files/case"
case_daily_summary_export_dest ="hdfs_files/case_daily_summary"

def view_export_hdfs_files(file_dest, export_dest):
    df = read_parquet_file(spark, hdfs_host + hdfs_root_path + file_dest)
    df.cache()
    df.show()
    df.printSchema()
    df.coalesce(1).write.format("csv").option("header", "true").option("sep", ",").mode("overwrite").save(export_dest)
    df.unpersist()

# Step 1: read safe entry parquet file
view_export_hdfs_files(safe_entry_file_dest, safe_entry_export_dest)

# Step 2: read resident parquet file
view_export_hdfs_files(resident_file_dest, resident_export_dest)

# Step 3: read place parquet file
view_export_hdfs_files(place_file_dest, place_export_dest)

# Step 4: read case parquet file
view_export_hdfs_files(case_file_dest, case_export_dest)

# Step 5: read case daily summary parquet file
view_export_hdfs_files(case_daily_summary_file_dest, case_daily_summary_export_dest)