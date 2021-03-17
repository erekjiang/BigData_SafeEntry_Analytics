from pyspark.sql.types import StructType, StructField, FloatType, BooleanType
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pathlib import Path
import pyspark
from pyspark.sql import SparkSession

from pyspark import SQLContext
import os
#os.environ["PYSPARK_PYTHON"]="/Library/Frameworks/Python.framework/Versions/3.9/bin/python3"

conf = pyspark.SparkConf()
spark = SparkSession.builder.appName("Read CSV files").getOrCreate()

sc = pyspark.SparkContext.getOrCreate(conf=conf)
sqlcontext = SQLContext(sc)

schema = StructType([
    StructField("sales", IntegerType(), True),
    StructField("sales person", StringType(), True)
])

data = ([(10, 'Walker'),
         (20, 'Stepher')
         ])

df = sqlcontext.createDataFrame(data, schema=schema)
df.show()