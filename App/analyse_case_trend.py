from pyspark.sql import SparkSession
from pathlib import Path
from App.utils import *
import pyspark.sql.functions as func
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
import matplotlib.pyplot as plt
from pyspark.sql.functions import sum as _sum

spark = SparkSession.builder.appName('case_trend_predict').getOrCreate()

# Load data
hdfs_host = "hdfs://localhost:9000"
hdfs_root_path = "/SafeEntry_Analytics/"
case_file_dest = "case_daily_summary.parquet"

all_data = read_parquet_file(spark, hdfs_host+hdfs_root_path+case_file_dest)
all_data.printSchema()

#select key columns and append dayOfWeek & Index column
cases_group_by_date = all_data["date","dailyConfirmed","cumulativeConfirmed",
                               "dailyImported","localCaseNotResidingInDorms","localCaseResidingInDorms"]\
                      .withColumn("dayOfWeek", func.dayofweek(all_data["date"]))\
                      .na.fill(0)
cases_group_by_date = add_index_to_dataframe(cases_group_by_date)
cases_group_by_date.show()
cases_group_by_date.printSchema()

#Show daily confirmed cases linechart
cases_group_by_date.toPandas().plot(x="date", y="dailyConfirmed")
plt.title("Daily cases")
plt.xlabel("Date")
plt.ylabel("Count")
plt.rcParams["figure.figsize"] = (30,5)


#Show cumulative confirmed cases by day
cases_group_by_date.toPandas().plot(x="date", y="cumulativeConfirmed")
plt.title("Daily Cumulative Confirmed Cases")
plt.xlabel("Date")
plt.ylabel("Count")
plt.rcParams["figure.figsize"] = (30,5)


#Show dailyConfirmed cases sum group by dayOfWeek
cases_group_by_day_of_week = cases_group_by_date\
                        .groupby(['dayOfWeek'])\
                        .agg(_sum('dailyConfirmed'))\
                        .orderBy('dayOfWeek',asending=True)
cases_group_by_day_of_week.show()

cases_group_by_day_of_week.toPandas().plot(kind='bar')
plt.rcParams["figure.figsize"] = (5,5)

#Show cases group by categories as imported, local cases not residing dorm, local cases residing dorms
case_categories_columns = ["Case Type","Count"]
case_categories = [("Imported Cases",cases_group_by_date.groupBy().sum('dailyImported').collect()[0][0]),
        ("Local Transmission Cases",cases_group_by_date.groupBy().sum('localCaseNotResidingInDorms').collect()[0][0]),
        ("Local Case Reside Dorms",cases_group_by_date.groupBy().sum('localCaseResidingInDorms').collect()[0][0])]
case_categories_df = spark.createDataFrame(data=case_categories, schema = case_categories_columns)
case_categories_df.printSchema()
case_categories_df.show(truncate=False)

case_categories_df.toPandas().plot.bar(x="Case Type")
plt.rcParams["figure.figsize"] = (5,5)

#only fetch data after 2020-09-15 for modeling
cases_group_by_date=cases_group_by_date.filter(cases_group_by_date["Date"]>='2020-09-15').drop("index","dailyImported","localCaseNotResidingInDorms","localCaseResidingInDorms")
cases_group_by_date = add_index_to_dataframe(cases_group_by_date)
cases_group_by_date.show()


# Split into training data and test data
total_count = cases_group_by_date.count()
print(total_count)
seventy_percent_index = round(total_count * 0.7)
cases_group_by_date.registerTempTable("daily_cases")
train_data = spark.sql("select * from daily_cases where index <= {}".format(seventy_percent_index))
test_data = spark.sql("select * from daily_cases where index > {}".format(seventy_percent_index))
train_data.orderBy(desc("index")).show()
test_data.show()

#show train data set in linechart
train_data.toPandas().plot(x="date", y="cumulativeConfirmed")
plt.title("Daily cases")
plt.xlabel("Date")
plt.ylabel("Count")

# These are the default values for the featuresCol, labelCol, predictionCol
vectorAssembler = VectorAssembler(inputCols=["index"], outputCol="features")
train_sub = vectorAssembler.transform(train_data)
test_sub = vectorAssembler.transform(test_data)

lr = LinearRegression(featuresCol='features',labelCol='cumulativeConfirmed',predictionCol='prediction_cumulativeConfirmed')
# Fit the model
lr_model = lr.fit(train_sub)
# Print the coefficients and intercept training data
print("Coefficients: {}".format(str(lr_model.coefficients)))
print("Intercept: {}".format(str(lr_model.intercept)))

# Testing result
test_result = lr_model.evaluate(test_sub)
test_result.residuals.show()
print("RMSE: {}".format(test_result.rootMeanSquaredError))

# Prediction execution
predictions = lr_model.transform(test_sub)
predictions = predictions.withColumn("prediction_cumulativeConfirmed",func.round(predictions["prediction_cumulativeConfirmed"]))

### Defining the window
Windowspec=Window.orderBy("index")

### Calculating lag of prediction_cumulativeConfirmed at each day level
predictions= predictions.withColumn('prev_day_prediction_cumulativeConfirmed',
                        func.lag(predictions['prediction_cumulativeConfirmed'])
                                .over(Windowspec))

#compare predicted dailyConfimred vs actual
predictionsPandas = predictions.toPandas()
plt.plot( 'date', 'dailyConfirmed', data=predictionsPandas, marker='')
plt.plot( 'date', 'prediction_dailyConfirmed', data=predictionsPandas, marker='')
plt.legend()
plt.show()


#compare predicted dailyConfimred vs actual
predictionsPandas = predictions.toPandas()
plt.plot( 'date', 'dailyConfirmed', data=predictionsPandas, marker='')
plt.plot( 'date', 'prediction', data=predictionsPandas, marker='')
# show legend
plt.legend()
# show graph
plt.show()