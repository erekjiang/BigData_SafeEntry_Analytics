import os

from pyspark.sql.functions import to_date

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 pyspark-shell"
)

import pyspark
from pyspark.sql import SparkSession
from App.utils import *
from pyspark.sql.functions import *
from graphframes import *

hdfs_host = "hdfs://localhost:9000"
hdfs_root_path = "/SafeEntry_Analytics/"

conf = pyspark.SparkConf().setAppName("Process Entry Record").setMaster("local[*]")
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

safe_entry_file_dest = "entry_record.parquet"
safe_entry_daily_file_dest = "entry_record.parquet"

# Step 2: read safe entry parquet file
safe_entry_df= read_parquet_file(spark, hdfs_host + hdfs_root_path + safe_entry_file_dest)
safe_entry_df.show()
safe_entry_df.printSchema()

from_pattern = 'yyyy-MM-dd h:mm:ss'
to_pattern = 'yyyy-MM-dd'

safe_entry_df = safe_entry_df.withColumn('entry_date', from_unixtime(unix_timestamp(safe_entry_df['entry_time'], from_pattern), to_pattern))

#safe_entry_df.show()

entry_date = safe_entry_df.select('entry_date').distinct().collect()
entry_date_list = []

for i in range(len(entry_date)):
    date = entry_date[i][0]
    entry_date_list.append(date)
    safe_entry_daily_df = safe_entry_df.filter(safe_entry_df.entry_date == date)

    print(date)
    safe_entry_daily_df.show()
    print('number of rows',safe_entry_daily_df.count())

    case_daily_hdfs_path = hdfs_host + hdfs_root_path + date +"_"+ safe_entry_daily_file_dest
    safe_entry_daily_df.write.mode("Overwrite").parquet(case_daily_hdfs_path)

f = open("tmp/entry_date.txt", "w")
f.write('\n'.join(entry_date_list))
f.close()
