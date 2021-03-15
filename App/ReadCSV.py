from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pathlib import Path
from utils import *

spark = SparkSession.builder.appName("Read CSV files").getOrCreate()

resident_file_path = str(Path('in/resident.csv'))
place_file_path = str(Path('in/place.csv'))
safe_entry_file_path = str(Path('in/entry_records.csv'))

# Read resident file
resident_schema = StructType([StructField("resident_id", StringType(), False),
                              StructField("resident_name", StringType(), True),
                              StructField("nric", StringType(), False),
                              StructField("phone_number", StringType(), False)])

resident_df = read_csv_file(spark, resident_file_path,resident_schema)
resident_df.show(100, False)
resident_df.printSchema()









