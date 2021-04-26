import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pathlib import Path
from App.utils import *
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions
from datetime import datetime
from datetime import timedelta


hdfs_host = "hdfs://localhost:9000"
hdfs_root_path = "/SafeEntry_Analytics/"

conf = pyspark.SparkConf()
spark = SparkSession.builder.appName("Ingest Batch Data").getOrCreate()
sc = pyspark.SparkContext.getOrCreate(conf=conf)

output = spark.createDataFrame([
        ("2021-04-24",30,0,0,200)
        ]).toDF("date", "dailyConfirmed", "cumulativeConfirmed", "dayOfWeek", "index")
lastDate = output.limit(1).select("date").collect()[0][0]
lastIndex = output.limit(1).select("index").collect()[0][0]

forcastDataList = []

def addDate(dateStr,dateFormat="%Y-%m-%d", addDays=0):
    datetime_obj = datetime.strptime(dateStr, dateFormat)
    if (addDays!=0):
        anotherTime = datetime_obj + timedelta(days=addDays)
    else:
        anotherTime = datetime_obj
    return anotherTime.strftime(dateFormat)

for x in range(30):
        newDate = lastDate
        date = [addDate(lastDate,"%Y-%m-%d",x+1),0,0,0,lastIndex+x+1]
        forcastDataList.append(date)

print(forcastDataList)