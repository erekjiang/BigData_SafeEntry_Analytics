import pyspark
from pyspark.sql import SparkSession
from App.utils import *

hdfs_host = "hdfs://localhost:9000"
hdfs_root_path = "/SafeEntry_Analytics/"

conf = pyspark.SparkConf().setAppName("Process Case Data").setMaster("local[*]")
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

case_file_dest = "case.parquet"
case_daily_file_dest = "case_daily.parquet"

# Step 1: read case file
case_df = read_parquet_file(spark, hdfs_host + hdfs_root_path + case_file_dest)

case_df.createOrReplaceTempView("case_df")
case_daily_df =spark.sql('SELECT CAST(diagnosedDate as DATE) as Date, count(caseId) as count FROM case_df WHERE group by diagnosedDate order by diagnosedDate')
case_daily_df.show()

save_case_daily_hdfs_path = hdfs_host + hdfs_root_path + case_daily_file_dest
case_daily_df.write.mode("Overwrite").parquet(save_case_daily_hdfs_path)