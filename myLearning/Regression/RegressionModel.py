# Databricks notebook source
# MAGIC %md
# MAGIC ### Building a simple Regression Machine Learning Model with PySpark and SparkML

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0 Create the enviroment variables

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.0 Libraries

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vectors

# COMMAND ----------

bostonDF = (spark.read
            .option('header', 'true')
            .option('inferSchema', 'true')
            .csv('/mnt/training/bostonhousing/bostonhousing/bostonhousing.csv')
           )

# COMMAND ----------

featureCols = ["rm", "crim", "lstat"]
assembler = VectorAssembler(inputCols=featureCols, outputCol="features")

bostonFeaturizedDF = assembler.transform(bostonDF)

display(bostonFeaturizedDF)

# COMMAND ----------

lr = LinearRegression(labelCol="medv", featuresCol="features")

# COMMAND ----------

trainDF, testDF = bostonDF.randomSplit([0.8, 0.2], seed=42)

print("We have {} training examples and {} test examples.".format(trainDF.count(), testDF.count()))

# COMMAND ----------

lrModel = lr.fit(trainDF)

# COMMAND ----------

print("Coefficients: {0:.1f}, {1:.1f}, {2:.1f}".format(*lrModel.coefficients))
print("Intercept: {0:.1f}".format(lrModel.intercept))

# COMMAND ----------

subsetDF = (bostonFeaturizedDF
  .limit(10)
  .select("features", "medv")
)

display(subsetDF)

# COMMAND ----------

predictionDF = lrModel.transform(subsetDF)

display(predictionDF)

# COMMAND ----------



data = [(Vectors.dense([6., 3.6, 12.]), )]              # Creates our hypothetical data point
predictDF = spark.createDataFrame(data, ["features"])

display(lrModel.transform(predictDF))

# COMMAND ----------

evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='medv', metricName='mse')
testError = evaluator.evaluate(testPredictionDF)

print("Error on the test set for the baseline model: {}".format(testError))
