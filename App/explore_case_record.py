import pyspark
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from App.utils import *
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, LongType, IntegerType

hdfs_host = "hdfs://localhost:9000"
hdfs_root_path = "/SafeEntry_Analytics/"

conf = pyspark.SparkConf().setAppName("Explore Case Record").setMaster("local[*]")
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

case_daily_file_dest = "case_daily.parquet"

# Step 1: retrieve case daily data
case_daily_df = read_parquet_file(spark, hdfs_host + hdfs_root_path + case_daily_file_dest)
case_daily_df.show(100, False)

# Step 2: explore trend
case_daily_pdf = case_daily_df.select("*").toPandas()
case_daily_pdf['mov_avg'] = case_daily_pdf['count'].rolling(6).mean()
import plotly.graph_objs as go

fig = go.Figure()
fig.add_trace(go.Scatter(x=case_daily_pdf['Date'], y=case_daily_pdf['mov_avg'], name="moving_avg"))
fig.add_trace(go.Scatter(x=case_daily_pdf['Date'], y=case_daily_pdf['count'], name="count"))
fig.show()


# Step 3: build model
row_with_index = Row(
    "Date"
    , "count"
    , "index"
)

new_schema = StructType(case_daily_df.schema.fields[:] + [StructField("index", IntegerType(), False)])
case_daily_rdd = case_daily_df.rdd.zipWithIndex()
case_daily_df = (case_daily_rdd.map(lambda ri: row_with_index(*list(ri[0]) + [ri[1]])).toDF(new_schema))

print(type(case_daily_df))
case_daily_df.show(truncate=False)

from pyspark.ml.feature import PolynomialExpansion, VectorAssembler

assembler = VectorAssembler().setInputCols(['index']).setOutputCol('features')
output = assembler.transform(case_daily_df)

output.show()
polyExpansion = PolynomialExpansion(degree=1, inputCol="features", outputCol="polyFeatures")
polyDF = polyExpansion.transform(output)
polyDF.show(100)

final_data = polyDF.select("polyFeatures","count")
final_data.show()

# Split into training and testing datasets
train_data, test_data = final_data.randomSplit([0.7,0.3])
train_data.describe().show()
test_data.describe().show()

# Create a linear regression model object
lr = LinearRegression(labelCol='count',featuresCol='polyFeatures')

# Fit the model to the data and call this model lrModel
lrModel = lr.fit(train_data,)
# print the coefficients and intercept for linear regression
print("Coefficients: {} Intercept {}".format(lrModel.coefficients, lrModel.intercept))

test_result = lrModel.evaluate(test_data)
test_result.residuals.show()
print("RMSE:{}".format(test_result.rootMeanSquaredError))
print("MSE: {}".format(test_result.meanSquaredError))