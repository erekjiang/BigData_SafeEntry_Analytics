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

def read_and_store_file(schema, file_path, file_dest, key):
    df = read_csv_file(spark, file_path, schema)
    df.cache()
    df.show(20, False)
    df.printSchema()
    print("new file ", file_path," count: ", df.count())

    hdfs_path = hdfs_host + hdfs_root_path + file_dest
    is_in_hdfs_exist = is_hdfs_file_exist(hdfs_root_path + file_dest)
    if is_in_hdfs_exist:
        existing_df = spark.read.schema(schema).parquet(hdfs_path).cache()
        print("existing dataframe count: ", df.count())
        merged_df = df.union(existing_df)

        sort_key = 'last_update_dt'
        if file_dest == case_file_dest:
            sort_key = 'lastUpdatedDttm'
        elif file_dest == case_daily_file_dest:
            sort_key = 'date'

        merged_df = merged_df.sort(sort_key, ascending=True).dropDuplicates(subset=[key])

        print("merged resident count: ", merged_df.count())
        merged_df.write.mode("Overwrite").parquet(hdfs_path)
    else:
        df.write.mode("Overwrite").parquet(hdfs_path)

    print(f"============saved: {file_dest} to hdfs============")
    df.unpersist()

# Step 1: Read & Store resident file
resident_schema = StructType([StructField("resident_id", StringType(), False),
                              StructField("resident_name", StringType(), True),
                              StructField("nric", StringType(), False),
                              StructField("phone_number", StringType(), False),
                              StructField("last_update_dt", TimestampType(), False)])
key = 'nric'
read_and_store_file(resident_schema,resident_file_path, resident_file_dest, key)

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
key = 'place_id'
read_and_store_file(place_schema, place_file_path,place_file_dest,key)

# Step 3: Read & Store safe entry file
safe_entry_schema = StructType([StructField("record_id", StringType(), False),
                                StructField("resident_id", StringType(), False),
                                StructField("place_id", StringType(), True),
                                StructField("entry_time", TimestampType(), False),
                                StructField("exit_time", TimestampType(), True),
                                StructField("last_update_dt", TimestampType(), False)])
key = 'record_id'
read_and_store_file(safe_entry_schema,safe_entry_file_path,safe_entry_file_dest,key)


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

key = 'caseId'
read_and_store_file(case_schema,case_file_path,case_file_dest, key)

# Step 5: Read & Store case daily summary file
case_daily_schema = StructType([StructField("date", StringType(), False),
                                StructField("dailyConfirmed", IntegerType(), True),
                                StructField("falsePositiveFound", IntegerType(), True),
                                StructField("cumulativeConfirmed", IntegerType(), True),
                                StructField("dailyDischarged", IntegerType(), True),
                                StructField("passedButNotDueToCovid", IntegerType(), True),
                                StructField("cumulativeDischarged", IntegerType(), True),
                                StructField("dischargedToIsolation", IntegerType(), True),
                                StructField("stillHospitalized", IntegerType(), True),
                                StructField("dailyDeaths", IntegerType(), True),
                                StructField("cumulativeDeaths", IntegerType(), True),
                                StructField("testedPositiveDemise", IntegerType(), True),
                                StructField("dailyImported", IntegerType(), True),
                                StructField("dailyLocalTransmission", IntegerType(), True),
                                StructField("localCaseResidingInDorms", IntegerType(), True),
                                StructField("localCaseNotResidingInDorms", IntegerType(), True),
                                StructField("intensiveCareUnitCases", IntegerType(), True),
                                StructField("generalWardsMOHReport", IntegerType(), True),
                                StructField("inIsolationMOHReport", IntegerType(), True),
                                StructField("totalCompletedIsolationMOHReport", IntegerType(), True),
                                StructField("totalHospitalDischargedMOHReport", IntegerType(), True)
                                ])

key = 'date'
read_and_store_file(case_daily_schema, case_daily_file_path, case_daily_file_dest,key)

