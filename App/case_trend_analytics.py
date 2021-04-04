from pyspark.sql import SparkSession
from pathlib import Path
from App.utils import *

spark = SparkSession.builder.appName('lr_example').getOrCreate()
from pyspark.ml.regression import LinearRegression

# Load data
cases_file_path = str(Path('in/case.csv'))
all_data = read_csv_file(spark,cases_file_path)
all_data.printSchema()

groupByDate = all_data[['diagnosedDate']].groupby(['diagnosedDate']).count()
groupByDate.show()
# Split into training data and test data
train_data, test_data = all_data.randomSplit([0.7,0.3])
train_data.show()
test_data.show()
# unlabeled_data = test_data.select("features")
# unlabeled_data.show()
# # These are the default values for the featuresCol, labelCol, predictionCol
# lr = LinearRegression(featuresCol='features',labelCol='label',predictionCol='prediction')
# # Fit the model
# lr_model = lr.fit(train_data)
# # Print the coefficients and intercept training data
# print("Coefficients: {}".format(str(lr_model.coefficients)))
# print("Intercept: {}".format(str(lr_model.intercept)))
# # Testing result
# test_result = lr_model.evaluate(test_data)
# test_result.residuals.show()
# print("RMSE: {}".format(test_result.rootMeanSquaredError))
#
# # Prediction
# predictions = lr_model.transform(unlabeled_data)
# predictions.show()
