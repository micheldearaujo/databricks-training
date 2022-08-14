# Databricks notebook source
# MAGIC %md
# MAGIC # Featurization
# MAGIC 
# MAGIC Cleaning data and adding features creates the inputs for machine learning models, which are only as strong as the data they are fed.  This lesson examines the process of featurization including common tasks such as handling categorical features and normalization, imputing missing data, and creating a pipeline of featurization steps.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC * Differentiate Spark transformers, estimators, and pipelines
# MAGIC * One-hot encode categorical features
# MAGIC * Impute missing data
# MAGIC * Combine different featurization stages into a pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/9j0djq95kk?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/9j0djq95kk?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Transformers, Estimators, and Pipelines
# MAGIC 
# MAGIC Spark's machine learning library, `MLlib`, has three main abstractions:<br><br>
# MAGIC 
# MAGIC 1. A **transformer** takes a DataFrame as an input and returns a new DataFrame with one or more columns appended to it.  
# MAGIC   - Transformers implement a `.transform()` method.  
# MAGIC 2. An **estimator** takes a DataFrame as an input and returns a model, which itself is a transformer.
# MAGIC   - Estimators implements a `.fit()` method.
# MAGIC 3. A **pipeline** combines together transformers and estimators to make it easier to combine multiple algorithms.
# MAGIC   - Pipelines implement a `.fit()` method.
# MAGIC 
# MAGIC These basic building blocks form the machine learning process in Spark from featurization through model training and deployment.  
# MAGIC 
# MAGIC Machine learning models are only as strong as the data they see and can only work on numerical data.  **Featurization is the process of creating this input data for a model.**  There are a number of common featurization approaches:<br><br>
# MAGIC 
# MAGIC * Encoding categorical variables
# MAGIC * Normalizing
# MAGIC * Creating new features
# MAGIC * Handling missing values
# MAGIC * Binning/discretizing
# MAGIC 
# MAGIC This lesson builds a pipeline of transformers and estimators in order to featurize a dataset.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/pipeline.jpg" style="height: 400px; margin: 20px"/></div>
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> `MLlib` can refer to both the general machine learning library in Spark or the RDD-specific API.  `SparkML` refers to the DataFrame-specific API, which is preferred over working on RDD's wherever possible.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Categorical Features and One-Hot Encoding
# MAGIC 
# MAGIC Categorical features refer to a discrete number of groups.  In the case of the AirBnB dataset we'll use in this lesson, one categorical variable is room type.  There are three types of rooms: `Private room`, `Entire home/apt`, and `Shared room`.
# MAGIC 
# MAGIC A machine learning model does not know how to handle these room types.  Instead, we must first *encode* each unique string into a number.  Second, we must *one-hot encode* each of those values to a location in an array.  This allows our machine learning algorithms to model effects of each category.
# MAGIC 
# MAGIC | Room type       | Room type index | One-hot encoded room type index |
# MAGIC |-----------------|-----------------|---------------------------------|
# MAGIC | Private room    | 0               | [1, 0 ]                         |
# MAGIC | Entire home/apt | 1               | [0, 1]                          |
# MAGIC | Shared room     | 2               | [0, 0]                          |

# COMMAND ----------

# MAGIC %md
# MAGIC Import the AirBnB dataset.

# COMMAND ----------

airbnbDF = spark.read.parquet("/mnt/training/airbnb/sf-listings/sf-listings-correct-types.parquet")

display(airbnbDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Take the unique values of `room_type` and index them to a numerical value.  Fit the `StringIndexer` estimator to the unique room types using the `.fit()` method and by passing in the data.
# MAGIC 
# MAGIC The trained `StringIndexer` model then becomes a transformer.  Use it to transform the results using the `.transform()` method and by passing in the data.

# COMMAND ----------

display(uniqueTypesDF)

# COMMAND ----------

from pyspark.ml.feature import StringIndexer


uniqueTypesDF = airbnbDF.select("room_type").distinct() # Use distinct values to demonstrate how StringIndexer works

indexer = StringIndexer(inputCol="room_type", outputCol="room_type_index") # Set input column and new output column
indexerModel = indexer.fit(uniqueTypesDF)                                  # Fit the indexer to learn room type/index pairs
indexedDF = indexerModel.transform(uniqueTypesDF)                          # Append a new column with the index

display(indexedDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Now each room has a unique numerical value assigned.  While we could pass the new `room_type_index` into a machine learning model, it would assume that `Shared room` is twice as much as `Entire home/apt`, which is not the case.  Instead, we need to change these values to a binary yes/no value if a listing is for a shared room, entire home, or private room.
# MAGIC 
# MAGIC Do this by training and fitting the `OneHotEncoderEstimator`, which only operates on numerical values (this is why we needed to use `StringIndexer` first).
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Certain models, such as random forest, do not need one-hot encoding (and can actually be negatively affected by the process).  The models we'll explore in this course, however, do need this process.

# COMMAND ----------

from pyspark.ml.feature import OneHotEncoder

encoder = OneHotEncoder(inputCols=["room_type_index"], outputCols=["encoded_room_type"])
encoderModel = encoder.fit(indexedDF)
encodedDF = encoderModel.transform(indexedDF)

display(encodedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC The new column `encoded_room_type` is a vector.  The difference between a sparse and dense vector is whether Spark records all of the empty values.  In a sparse vector, like we see here, Spark saves space by only recording the places where the vector has a non-zero value.  The value of 0 in the first position indicates that it's a sparse vector.  The second value indicates the length of the vector.
# MAGIC 
# MAGIC Here's how to read the mapping above:<br><br>
# MAGIC 
# MAGIC * `Shared room` maps to the vector `[0, 0]`
# MAGIC * `Entire home/apt` maps to the vector `[0, 1]`
# MAGIC * `Private room` maps to the vector `[1, 0]`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Imputing Null or Missing Data
# MAGIC 
# MAGIC Null values refer to unknown or missing data as well as irrelevant responses. Strategies for dealing with this scenario include:<br><br>
# MAGIC 
# MAGIC * **Dropping these records:** Works when you do not need to use the information for downstream workloads
# MAGIC * **Adding a placeholder (e.g. `-1`):** Allows you to see missing data later on without violating a schema
# MAGIC * **Basic imputing:** Allows you to have a "best guess" of what the data could have been, often by using the mean of non-missing data
# MAGIC * **Advanced imputing:** Determines the "best guess" of what data should be using more advanced strategies such as clustering machine learning algorithms or oversampling techniques <a href="https://jair.org/index.php/jair/article/view/10302" target="_blank">such as SMOTE.</a>
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Try to determine why a value is null.  This can provide information that can be helpful to the model.

# COMMAND ----------

# MAGIC %md
# MAGIC Describe the dataset and take a look at the `count` values.  There's a fair amount of missing data in this dataset.

# COMMAND ----------

display(airbnbDF.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC Try dropping missing values.

# COMMAND ----------

countWithoutDropping = airbnbDF.count()
countWithDropping = airbnbDF.na.drop(subset=["zipcode", "host_is_superhost"]).count()

print("Count without dropping nulls:\t", countWithoutDropping)
print("Count with dropping nulls:\t", countWithDropping)

# COMMAND ----------

# MAGIC %md
# MAGIC Another common option for working with missing data is to impute the missing values with a best guess for their value.  Try imputing a list of columns with their median.

# COMMAND ----------

from pyspark.ml.feature import Imputer

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

imputer = Imputer(strategy='median', inputCols=imputeCols, outputCols = imputeCols)
imputerModel = imputer.fit(airbnbDF)
imputedDF = imputerModel.transform(airbnbDF)

display(imputedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a Pipeline
# MAGIC 
# MAGIC Passing around estimator objects, trained estimators, and transformed dataframes quickly becomes cumbersome.  Spark uses the convention established by `scikit-learn` to combine each of these steps into a single pipeline.
# MAGIC We can now combine all of these steps into a single pipeline.

# COMMAND ----------

from pyspark.ml import Pipeline

pipeline = Pipeline(stages=[
    indexer,
    encoder,
    imputer
])

# COMMAND ----------

# MAGIC %md
# MAGIC The pipeline is itself is now an estimator.  Train the model with its `.fit()` method and then transform the original dataset.  We've now combined all of our featurization steps into one pipeline with three stages.

# COMMAND ----------

pipelineModel = pipeline.fit(airbnbDF)
transformedDF = pipelineModel.transform(airbnbDF)
display(transformedDF)

# COMMAND ----------

transformedDF.count()
