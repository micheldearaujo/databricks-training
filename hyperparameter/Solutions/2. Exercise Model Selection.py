# Databricks notebook source
# MAGIC %md
# MAGIC ## Exercise: Select Optimal Model by Tuning Hyperparameters
# MAGIC 
# MAGIC Use grid search and cross-validation to tune the hyperparameters from a logistic regression model.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to set up our environment.

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Import the Data
# MAGIC 
# MAGIC Import the data and perform a train/test split.

# COMMAND ----------

from pyspark.sql.functions import col

cols = ["index",
 "sample-code-number",
 "clump-thickness",
 "uniformity-of-cell-size",
 "uniformity-of-cell-shape",
 "marginal-adhesion",
 "single-epithelial-cell-size",
 "bare-nuclei",
 "bland-chromatin",
 "normal-nucleoli",
 "mitoses",
 "class"]

cancerDF = (spark.read  # read the data
  .option("HEADER", True)
  .option("inferSchema", True)
  .csv("/mnt/training/cancer/biopsy/biopsy.csv")
)

cancerDF = (cancerDF    # Add column names and drop nulls
  .toDF(*cols)
  .withColumn("bare-nuclei", col("bare-nuclei").isNotNull().cast("integer"))
)

display(cancerDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Perform a train/test split to create `trainCancerDF` and `testCancerDF`.  Put 80% of the data in `trainCancerDF` and use the seed that is set for you.

# COMMAND ----------

# ANSWER
seed = 42
trainCancerDF, testCancerDF = cancerDF.randomSplit([0.8, 0.2], seed=seed)

display(trainCancerDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create a Pipeline
# MAGIC 
# MAGIC Create a pipeline `cancerPipeline` that consists of the following stages:<br>
# MAGIC 
# MAGIC 1. `indexer`: a `StringIndexer` that takes `class` as an input and outputs the column `is-malignant`
# MAGIC 2. `assembler`: a `VectorAssembler` that takes all of the other columns as an input and outputs  the column `features`
# MAGIC 3. `logr`: a `LogisticRegression` that takes `features` as the input and `is-malignant` as the output variable

# COMMAND ----------

# ANSWER
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, VectorAssembler

indexer = StringIndexer(inputCol="class", outputCol="is-malignant")
assembler = VectorAssembler(inputCols=cols[2:-1], outputCol="features")
logr = LogisticRegression(featuresCol="features", labelCol="is-malignant")

cancerPipeline = Pipeline(stages = [indexer, assembler, logr])

# logrModel = cancerPipeline.fit(trainCancerDF) # To fit without cross-validation

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, VectorAssembler

dbTest("ML1-P-08-02-01", True, type(indexer) == type(StringIndexer()))
dbTest("ML1-P-08-02-02", True, indexer.getInputCol() == 'class')
dbTest("ML1-P-08-02-03", True, indexer.getOutputCol() == 'is-malignant')

dbTest("ML1-P-08-02-04", True, type(assembler) == type(VectorAssembler()))
dbTest("ML1-P-08-02-05", True, assembler.getInputCols() == cols[2:-1])
dbTest("ML1-P-08-02-06", True, assembler.getOutputCol() == 'features')

dbTest("ML1-P-08-02-07", True, type(logr) == type(LogisticRegression()))
dbTest("ML1-P-08-02-08", True, logr.getLabelCol() == "is-malignant")
dbTest("ML1-P-08-02-09", True, logr.getFeaturesCol() == 'features')

dbTest("ML1-P-08-02-10", True, type(cancerPipeline) == type(Pipeline()))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create Grid Search Parameters
# MAGIC 
# MAGIC Take a look at the parameters for our `LogisticRegression` object.  Use this to build the inputs to grid search.

# COMMAND ----------

print(logr.explainParams())

# COMMAND ----------

# MAGIC %md
# MAGIC Create a `ParamGridBuilder` object with two grids:<br><br>
# MAGIC 
# MAGIC 1. A regularization parameter `regParam` of `[0., .2, .8, 1.]`
# MAGIC 2. Test both with and without an intercept using `fitIntercept`

# COMMAND ----------

# ANSWER
from pyspark.ml.tuning import ParamGridBuilder

cancerParamGrid = (ParamGridBuilder()
  .addGrid(logr.regParam, [0., .2, .8, 1.])
  .addGrid(logr.fitIntercept, [True, False])
  .build()
)

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ML1-P-08-03-01", True, type(cancerParamGrid) == list)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Perform 3-Fold Cross-Validation
# MAGIC 
# MAGIC Create a `BinaryClassificationEvaluator` object and use it to perform 3-fold cross-validation.

# COMMAND ----------

# ANSWER
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator

binaryEvaluator = BinaryClassificationEvaluator(
  labelCol = "is-malignant", 
  metricName = "areaUnderROC"
)

cancerCV = CrossValidator(
  estimator = cancerPipeline,             # Estimator (individual model or pipeline)
  estimatorParamMaps = cancerParamGrid,   # Grid of parameters to try (grid search)
  evaluator=binaryEvaluator,              # Evaluator
  numFolds = 3,                           # Set k to 3
  seed = 42                               # Seed to sure our results are the same if ran again
)

cancerCVModel = cancerCV.fit(trainCancerDF)

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator

dbTest("ML1-P-08-04-01", True, type(binaryEvaluator) == type(BinaryClassificationEvaluator()))
dbTest("ML1-P-08-04-02", True, type(cancerCV) == type(CrossValidator()))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Examine the results
# MAGIC 
# MAGIC Take a look at the results.  Which combination of hyperparameters learned the most from the data?

# COMMAND ----------

for params, score in zip(cancerCVModel.getEstimatorParamMaps(), cancerCVModel.avgMetrics):
  print("".join([param.name+"\t"+str(params[param])+"\t" for param in params]))
  print("\tScore: {}".format(score))
