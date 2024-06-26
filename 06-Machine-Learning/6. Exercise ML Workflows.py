# Databricks notebook source
# MAGIC %md
# MAGIC ## Exercise: ML Workflows
# MAGIC 
# MAGIC Do a train/test split on a Dataset, create a baseline model, and evaluate the result.  Optionally, try to beat this baseline model by training a linear regression model.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to set up our environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Train/Test Split
# MAGIC 
# MAGIC Import the bike sharing dataset and take a look at what's in it.  This dataset contains number of bikes rented (`cnt`) by season, year, month, and hour and for a number of weather conditions.

# COMMAND ----------

bikeDF = (spark
  .read
  .option("header", True)
  .option("inferSchema", True)
  .csv("/mnt/training/bikeSharing/data-001/hour.csv")
  .drop("instant", "dteday", "casual", "registered", "holiday", "weekday") # Drop unnecessary features
)

display(bikeDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Perform a train/test split.  Put 70% of the data into `trainBikeDF` and 30% into `testBikeDF`.  Use a seed of `42` so you have the same split every time you perform the operation.

# COMMAND ----------

trainBikeDF, testBikeDF = bikeDF.randomSplit([0.7, 0.3], seed=42)

# COMMAND ----------

# TEST - Run this cell to test your solution
_traincount = trainBikeDF.count()
_testcount = testBikeDF.count()

dbTest("ML1-P-03-01-01", True, _traincount < 13000 and _traincount > 12000)
dbTest("ML1-P-03-01-02", True, _testcount < 5500 and _testcount > 4800)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create a Baseline Model
# MAGIC 
# MAGIC Calculate the average of the column `cnt` and save it to the variable `trainCnt`.  Then create a new DataFrame `bikeTestPredictionDF` that appends a new column `prediction` that's the value of `trainCnt`.

# COMMAND ----------

from pyspark.sql.functions import avg, lit

# COMMAND ----------

avgTrainCnt = trainBikeDF.select(avg('cnt')).first()[0]
bikeTestPredictionDF = testBikeDF.withColumn('prediction', lit(avgTrainCnt))

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ML1-P-03-02-01", True, avgTrainCnt < 195 and avgTrainCnt > 180)
dbTest("ML1-P-03-02-02", True, "prediction" in bikeTestPredictionDF.columns)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 3: Evaluate the Result
# MAGIC 
# MAGIC Evaluate the result using `mse` as the error metric.  Save the result to `testError`.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Your baseline prediction will not be very accurate.  Be sure to take the square root of the MSE to return the results to the proper units (that is, bike counts).

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='cnt', metricName = 'mse')

# COMMAND ----------

testError = evaluator.evaluate(bikeTestPredictionDF)

# COMMAND ----------

testError

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ML1-P-03-03-01", True, testError > 33000 and testError < 35000)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 (Optional): Beat the Baseline
# MAGIC 
# MAGIC Use a linear regression model (explored in the previous lesson) to beat the baseline model score.

# COMMAND ----------

display(bikeDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.1: VectorAssembler on the training DF

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

assembler = VectorAssembler(inputCols=bikeDF.drop('cnt').columns, outputCol='features')

# COMMAND ----------

trainBikeFeaturizedDF = assembler.transform(trainBikeDF)

# COMMAND ----------

display(trainBikeFeaturizedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.2: Create the regression model

# COMMAND ----------

from pyspark.ml.regression import LinearRegression

# COMMAND ----------

lr = LinearRegression(featuresCol='features',
                     labelCol='cnt',
                     predictionCol='lr_prediction')

# COMMAND ----------

lrModel = lr.fit(trainBikeFeaturizedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.3: Predict on the test site

# COMMAND ----------

display(bikeTestPredictionDF)

# COMMAND ----------

bikeTestPredictionDF = assembler.transform(bikeTestPredictionDF)

# COMMAND ----------

bikeTestPredictionDF = lrModel.transform(bikeTestPredictionDF)

# COMMAND ----------

display(bikeTestPredictionDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.4: Evaluate

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

# COMMAND ----------

evaluator = RegressionEvaluator(predictionCol='lr_prediction',
                           labelCol='cnt', metricName='mse')

# COMMAND ----------

BestModelError = evaluator.evaluate(bikeTestPredictionDF)

# COMMAND ----------

print(f"The new model result is: {BestModelError}")

# COMMAND ----------

np.sqrt(testError)

# COMMAND ----------

np.sqrt(BestModelError)

# COMMAND ----------


