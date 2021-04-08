from pyspark.sql import SparkSession
from pathlib import Path
from App.utils import *
import pyspark.sql.functions as func
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder.appName('lr_example').getOrCreate()

# Load data
cases_file_path = str(Path('in/case.csv'))
all_data = read_csv_file(spark,cases_file_path)
all_data.printSchema()

cases_group_by_date = all_data[['diagnosedDate']]\
                        .groupby(['diagnosedDate'])\
                        .count()\
                        .orderBy('diagnosedDate',asending=True)

cases_group_by_date = cases_group_by_date.withColumn("dayOfWeek", func.dayofweek(cases_group_by_date["diagnosedDate"]))

cases_group_by_date = add_index_to_dataframe(cases_group_by_date)
cases_group_by_date.show()
cases_group_by_date.printSchema()


# Split into training data and test data
total_count = cases_group_by_date.count()
seventy_percent_index = round(total_count * 0.7)
cases_group_by_date.registerTempTable("daily_cases")
train_data = spark.sql("select * from daily_cases where index <= {}".format(seventy_percent_index))
test_data = spark.sql("select * from daily_cases where index > {}".format(seventy_percent_index))
train_data.show()
test_data.show()

# These are the default values for the featuresCol, labelCol, predictionCol
vectorAssembler = VectorAssembler(inputCols=["index","dayOfWeek"], outputCol="features")
train_sub = vectorAssembler.transform(train_data)
test_sub = vectorAssembler.transform(test_data)

lr = LinearRegression(featuresCol='features',labelCol='count',predictionCol='prediction')
# Fit the model
lr_model = lr.fit(train_sub)
# Print the coefficients and intercept training data
print("Coefficients: {}".format(str(lr_model.coefficients)))
print("Intercept: {}".format(str(lr_model.intercept)))
# Testing result
test_result = lr_model.evaluate(test_sub)
test_result.residuals.show()
print("RMSE: {}".format(test_result.rootMeanSquaredError))

# Prediction
predictions = lr_model.transform(test_sub)
predictions = predictions.withColumn("prediction",func.round(predictions["prediction"]))
predictions.show()

