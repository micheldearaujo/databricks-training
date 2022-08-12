# Databricks notebook source
# MAGIC %md
# MAGIC ## Exercise: Improve the Regression Model
# MAGIC 
# MAGIC Improve on the model trained in the **Regression Modeling** unit by adding features and interpreting the results.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to set up our environment.

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Import the Boston dataset.

# COMMAND ----------

bostonDF = (spark.read
  .option("HEADER", True)
  .option("inferSchema", True)
  .csv("/mnt/training/bostonhousing/bostonhousing/bostonhousing.csv")
  .drop("_c0")
)

display(bostonDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Prepare the Features for a New Model
# MAGIC 
# MAGIC Prepare a new column `allFeatures` for a new model that uses all of the features in `bostonDF` except for the label `medv`.  Create the following variables:<br><br>
# MAGIC 
# MAGIC 1. `allFeatures`: a list of all the column names
# MAGIC 2. `assemblerAllFeatures`: A `VectorAssembler` that uses `allFeatures` to create the output column `allFeatures`
# MAGIC 3. `bostonFeaturizedAllFeaturesDF`: The transformed `bostonDF`

# COMMAND ----------

# ANSWER
from pyspark.ml.feature import VectorAssembler

allFeatures = bostonDF.columns[:-1]
assemblerAllFeatures = VectorAssembler(inputCols=allFeatures, outputCol="allFeatures")

bostonFeaturizedAllFeaturesDF = assemblerAllFeatures.transform(bostonDF)

display(bostonFeaturizedAllFeaturesDF)

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.ml.feature import VectorAssembler

_features = ['crim',
  'zn',
  'indus',
  'chas',
  'nox',
  'rm',
  'age',
  'dis',
  'rad',
  'tax',
  'ptratio',
  'black',
  'lstat'
]

dbTest("ML1-P-06-01-01", _features, allFeatures)
dbTest("ML1-P-06-01-02", True, type(assemblerAllFeatures) == type(VectorAssembler()))
dbTest("ML1-P-06-01-03", True, assemblerAllFeatures.getOutputCol() == 'allFeatures')
dbTest("ML1-P-06-01-04", True, "allFeatures" in bostonFeaturizedAllFeaturesDF.columns)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Train the Model
# MAGIC 
# MAGIC Create a linear regression model `lrAllFeatures`.  Save the trained model to lrModelAllFeatures.

# COMMAND ----------

# ANSWER
from pyspark.ml.regression import LinearRegression

lrAllFeatures = (LinearRegression()
  .setLabelCol("medv")
  .setFeaturesCol("allFeatures")
)

lrModelAllFeatures = lrAllFeatures.fit(bostonFeaturizedAllFeaturesDF)

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.ml.regression import LinearRegression

dbTest("ML1-P-06-02-01", True, type(lrAllFeatures) == type(LinearRegression()))
dbTest("ML1-P-06-02-02", True, lrAllFeatures.getLabelCol() == 'medv')
dbTest("ML1-P-06-02-03", True, lrAllFeatures.getFeaturesCol() == 'allFeatures')
dbTest("ML1-P-06-02-04", True, "LinearRegressionModel" in str(type(lrModelAllFeatures)))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Interpret the Coefficients and Variance Explained
# MAGIC 
# MAGIC Take a look at the coefficients and variance explained.  What do these mean?

# COMMAND ----------

# ANSWER
print("β0 (intercept): {}".format(lrModelAllFeatures.intercept))
for i, (col, coef) in enumerate(zip(allFeatures, lrModelAllFeatures.coefficients)):
  print("β{} (coefficient for {}): {}".format(i+1, col, coef))
  
print("\nR2 score: {}".format(lrModelAllFeatures.summary.r2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Interpret the Statistical Significance of the Coefficients
# MAGIC 
# MAGIC Print out the p-values associated with each coefficient and the intercept.  Which were statistically significant?

# COMMAND ----------

# ANSWER
'''
Using the 5% rule of thumb on P-values, the values listed below that are below .05
would be considered statistically significant 
'''
for feat, pval in zip(allFeatures+["intercept"], lrModelAllFeatures.summary.pValues):
  print("P-value for {}:\t{}".format(feat, pval))
