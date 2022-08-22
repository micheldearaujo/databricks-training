# Databricks notebook source
# MAGIC %md
# MAGIC ## Exercise: Finish Featurizing the Dataset
# MAGIC 
# MAGIC One common way of handling categorical data is to divide it into bins, a process technically known as discretizing.  For instance, the dataset contains a number of rating scores that can be translated into a value of `1` if they are a highly rated host or `0` if not.
# MAGIC 
# MAGIC Finish featurizing the dataset by binning the review scores rating into high versus low rated hosts.  Also filter the extreme values and clean the column `price`.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to set up our environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC **Restore the Dataset from the Featurization module**

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import Imputer

airbnbDF = spark.read.parquet("/mnt/training/airbnb/sf-listings/sf-listings-correct-types.parquet")

indexer = StringIndexer(inputCol="room_type", outputCol="room_type_index")
encoder = OneHotEncoder(inputCols=["room_type_index"], outputCols=["encoded_room_type"])
imputeCols = [
  "host_total_listings_count",
  "bathrooms",
  "beds", 
  "review_scores_rating",
  "review_scores_accuracy",
  "review_scores_cleanliness",
  "review_scores_checkin",
  "review_scores_communication",
  "review_scores_location",
  "review_scores_value"
]
imputer = Imputer(strategy="median", inputCols=imputeCols, outputCols=imputeCols)

pipeline = Pipeline(stages=[
  indexer, 
  encoder, 
  imputer
])

pipelineModel = pipeline.fit(airbnbDF)
transformedDF = pipelineModel.transform(airbnbDF)

display(transformedDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 1: Binning `review_scores_rating`
# MAGIC 
# MAGIC Divide the hosts by whether their `review_scores_rating` is above 97.  Do this using the transformer `Binarizer` with the output column `high_rating`.  This should create the objects `binarizer` and the transformed DataFrame `transformedBinnedDF`.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Note that `Binarizer` is a transformer, so it does not have a `.fit()` method<br>
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** See the <a href="http://spark.apache.org/docs/latest/api/python/pyspark.ml.html?highlight=binarizer#pyspark.ml.feature.Binarizer" target="_blank">Binarizer Docs</a> for more details.</a>

# COMMAND ----------

from pyspark.ml.feature import Binarizer
binarizer = Binarizer(inputCol='review_scores_rating', outputCol='high_rating', threshold=97)
transformedBinnedDF = binarizer.transform(airbnbDF)

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.ml.feature import Binarizer

dbTest("ML1-P-05-01-01", True, type(binarizer) == type(Binarizer()))
dbTest("ML1-P-05-01-02", True, binarizer.getInputCol() == 'review_scores_rating')
dbTest("ML1-P-05-01-03", True, binarizer.getOutputCol() == 'high_rating')
dbTest("ML1-P-05-01-04", True, "high_rating" in transformedBinnedDF.columns)

print("Tests passed!")

# COMMAND ----------

display(transformedBinnedDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2: Regular Expressions on Strings
# MAGIC 
# MAGIC Clean the column `price` by creating two new columns:<br><br>
# MAGIC 
# MAGIC 1. `price`: a new column that contains a cleaned version of price.  This can be done using the regular expression replacement of `"[\$,]"` with `""`.  Cast the column as a decimal.
# MAGIC 2. `raw_price`: the collumn `price` in its current form
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** See the <a href="http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=regexp_replace#pyspark.sql.functions.regexp_replace" target="_blank">`regex_replace` Docs</a> for more details.

# COMMAND ----------

from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col, regexp_replace
transformedBinnedRegexDF = transformedBinnedDF.withColumn('raw_price', col('price'))
transformedBinnedRegexDF = transformedBinnedRegexDF.withColumn('price', regexp_replace('price', r"[\$,]", ""))
transformedBinnedRegexDF = transformedBinnedRegexDF.withColumn('price', col('price').cast(DecimalType()))

# COMMAND ----------

display(transformedBinnedRegexDF)

# COMMAND ----------

# TEST - Run this cell to test your solution
from pyspark.sql.types import DecimalType

dbTest("ML1-P-05-02-01", True, type(transformedBinnedRegexDF.schema["price"].dataType) == type(DecimalType()))
dbTest("ML1-P-05-02-02", True, "raw_price" in transformedBinnedRegexDF.columns)
dbTest("ML1-P-05-02-03", True, "price" in transformedBinnedRegexDF.columns)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Filter Extremes
# MAGIC 
# MAGIC The dataset contains extreme values, including negative prices and minimum stays of over one year.  Filter out all prices of $0 or less and all `minimum_nights` of 365 or higher.  Save the results to `transformedBinnedRegexFilteredDF`.

# COMMAND ----------

# TODO
transformedBinnedRegexFilteredDF = transformedBinnedRegexDF.where((transformedBinnedRegexDF.price > 0) | (transformedBinnedRegexDF.minimum_nights <= 365))

# COMMAND ----------

transformedBinnedRegexFilteredDF = transformedBinnedRegexDF.filter(col('price') >0).filter(col('minimum_nights') <= 365)

# COMMAND ----------

# TEST - Run this cell to test your solution
dbTest("ML1-P-05-03-01", 4789, transformedBinnedRegexFilteredDF.count())

print("Tests passed!")

# COMMAND ----------


