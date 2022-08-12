# Databricks notebook source
# MAGIC %md
# MAGIC ## Exercise: Train a Model and Create Predictions
# MAGIC 
# MAGIC Train a model using the Boston dataset and a different set of input features.  Predict on new data.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Import the Boston dataset, which contains median house values in 1000's (`medv`) for a variety of different features.  Since this dataset is "supervised" my the median value, this is a supervised machine learning use case.

# COMMAND ----------

bostonDF = (spark.read
  .option("HEADER", True)
  .option("inferSchema", True)
  .csv("/mnt/training/bostonhousing/bostonhousing/bostonhousing.csv")
)

display(bostonDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create the Features
# MAGIC 
# MAGIC Using `bostonDF`, use a `VectorAssembler` object `assembler` to create a new column `newFeatures` that has the following three variables:<br><br>
# MAGIC 
# MAGIC 1. `indus`: proportion of non-retail business acres per town
# MAGIC 2. `age`: proportion of owner-occupied units built prior to 1940
# MAGIC 3. `dis`: weighted distances to five Boston employment centers
# MAGIC 
# MAGIC Save the results to `bostonFeaturizedDF2`

# COMMAND ----------

# TODO
from pyspark.ml.feature import VectorAssembler

assembler = # FILL_IN
bostonFeaturizedDF2 = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ML1-P-02-01-01", True, set(assembler.getInputCols()) == {'indus', 'age', 'dis'})
dbTest("ML1-P-02-01-02", True, bool(bostonFeaturizedDF2.schema['newFeatures'].dataType))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Train the Model
# MAGIC 
# MAGIC Instantiate a linear regression model `lrNewFeatures`.  Save the trained model to `lrModelNew`.

# COMMAND ----------

# TODO
from pyspark.ml.regression import LinearRegression

lrNewFeatures = # FILL_IN
lrModelNew = # FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ML1-P-02-02-01", True, lrNewFeatures.getFeaturesCol() == "newFeatures")
dbTest("ML1-P-02-02-02", True, lrNewFeatures.getLabelCol() == "medv")
dbTest("ML1-P-02-02-03", True, lrModelNew.hasSummary)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create Predictions
# MAGIC 
# MAGIC Create the DataFrame `predictionsDF` for the following values, created for you in `newDataDF`:
# MAGIC 
# MAGIC | Feature | Datapoint 1 | Datapoint 2 | Datapoint 3 |
# MAGIC |:--------|:------------|:------------|:------------|
# MAGIC | `indus` | 11          | 6           | 19          |
# MAGIC | `age`   | 68          | 35          | 74          |
# MAGIC | `dis`   | 4           | 2           | 8           |

# COMMAND ----------

# TODO
from pyspark.ml.linalg import Vectors

data = [(Vectors.dense([11., 68., 4.]), ),
        (Vectors.dense([6., 35., 2.]), ),
        (Vectors.dense([19., 74., 8.]), )]
newDataDF = spark.createDataFrame(data, ["newFeatures"])
predictionsDF = # FILL_IN

display(predictionsDF)

# COMMAND ----------

# TEST - Run this cell to test your solution
predicitions = [i.prediction for i in predictionsDF.select("prediction").collect()]

dbTest("ML1-P-02-02-01", True, predicitions[0] > 20 and predicitions[0] < 23)
dbTest("ML1-P-02-02-01", True, predicitions[1] > 30 and predicitions[1] < 34)
dbTest("ML1-P-02-02-01", True, predicitions[2] > 7 and predicitions[2] < 11)

print("Tests passed!")
