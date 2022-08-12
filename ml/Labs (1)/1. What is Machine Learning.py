# Databricks notebook source
# MAGIC %md
# MAGIC # What is Machine Learning?
# MAGIC 
# MAGIC Machine learning discovers patterns within data without being explicitly programmed.  This lesson introduces machine learning, explores the main topics in the field, and builds an end-to-end pipeline in Spark.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC * Define machine learning
# MAGIC * Differentiate supervised and unsupervised tasks
# MAGIC * Identify regression and classification tasks
# MAGIC * Train a model, interpret the results, and create predictions

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/h8cqzugdrf?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/h8cqzugdrf?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Learning from Data
# MAGIC 
# MAGIC Machine learning refers to a diverse set of tools for understanding data.  More technically, **machine learning is the process of _learning from data_ without being _explicitly programmed_**.  Let's unpack what that means.
# MAGIC 
# MAGIC Take a dataset of Boston home values in the 1970's for example.  The dataset consists of the value of homes as well as the number of rooms, crime per capita, and percent of the population considered lower class.  Home value is the _output variable_, also known as the _label_.  The other variables are known as _input variables_ or _features_.
# MAGIC 
# MAGIC A machine learning model would _learn_ the relationship between housing price and the various features without being explicitly programmed.  There are many possible functions that would map input features to the output variable.  In more technical terms, our model would learn a function `f()` that maps the relationship between input features and the output variable.
# MAGIC 
# MAGIC The following image shows the relation between our features and house value.  A good model `f()` would learn from the data that the number of rooms in a home is positively correlated to the house value while crime and percent of the neighborhood that is lower class is negatively correlated.  
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/boston-housing.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 
# MAGIC The lines above represent the best fit for the data where our model's best guess for a house price given a feature value on the X axis is the corresponding point on the line.
# MAGIC 
# MAGIC **Machine learning is the set of approaches for estimating this function `f()` that maps features to an output.**  The inputs to this function can range from stock prices and customer information to images and DNA sequences.  Many of the same statistical techniques apply regardless of the domain.  This makes machine learning a generalizable skill set that drives decision-making in modern businesses.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Remember to attach your notebook to a cluster. Click <b>Detached</b> in the upper left hand corner and then select your preferred cluster.
# MAGIC 
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/attach-to-cluster.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Supervised vs Unsupervised Learning
# MAGIC 
# MAGIC Machine learning problems are roughly categorized into two main types:<br><br>
# MAGIC 
# MAGIC * **Supervised learning** looks to predict the value of some outcome based on one or more input measures
# MAGIC   - Our example of the Boston Housing Dataset is an example of supervised learning
# MAGIC   - In this case, the output is the price of a home and the input is features such as number of rooms
# MAGIC * **Unsupervised learning** describes associations and patterns in data without a known outcome
# MAGIC   - An example of this would be clustering customer data to find the naturally occurring customer segments
# MAGIC   - In this case, no known output is used as an input.  Instead, the goal is to discover how the data are organized into natural segments or clusters
# MAGIC 
# MAGIC This course will cover supervised learning, which is the vast majority of machine learning use cases in industry.  Later courses will look at unsupervised approaches.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/regression.png" style="height: 400px; margin: 20px"/><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/clustering.png" style="height: 400px; margin: 20px"/></div>

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

# MAGIC %md-sandbox
# MAGIC Spark differs from many other machine learning frameworks in that we train our model on a single column that contains a vector of all of our features.  Prepare the data by creating one column named `features` that has the average number of rooms, crime rate, and poverty percentage.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See the <a href="http://spark.apache.org/docs/latest/api/python/pyspark.ml.html?highlight=vectorassembler#pyspark.ml.feature.VectorAssembler" target="_blank">`VectorAssembler` documentation for more details.</a>
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> `VectorAssembler` is a tranformer, which implements a `.transform()` method.  Estimators, by contrast, need to learn from the data using a `.fit()` method before they can transform data.

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

featureCols = ["rm", "crim", "lstat"]
assembler = VectorAssembler(inputCols=featureCols, outputCol="features")

bostonFeaturizedDF = assembler.transform(bostonDF)

display(bostonFeaturizedDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC We now have the input features and the output, median value, which appears in the data as `medv`.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See <a href="https://spark.apache.org/docs/latest/mllib-data-types.html" target="_blank">the MLlib documentation for more details</a> on the sparse vector notation seen in the `features` column, which will be covered in more detail in the Featurization lesson.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Regression vs Classification
# MAGIC 
# MAGIC Variables can either be quantitative or qualitative:<br><br>
# MAGIC 
# MAGIC * **Quantitative** values are numeric and generally unbounded, taking any positive or negative value
# MAGIC * **Qualitative** values take on a set number of classes or categories
# MAGIC 
# MAGIC | Variable type    | Also known as         | Examples                                                          |
# MAGIC |:-----------------|:----------------------|:------------------------------------------------------------------|
# MAGIC | quantitative     | continuous, numerical | age, salary, temperature                                          |
# MAGIC | qualitative      | categorical, discrete | gender, whether or a not a patient has cancer, state of residence |
# MAGIC 
# MAGIC Machine learning models operate on numbers so a qualitative variable like gender, for instance, would need to be encoded as `0` for male or `1` for female.  In this case, female isn't "one more" than male, so this variable is handled differently compared to a quantitative variable.
# MAGIC 
# MAGIC Generally speaking, **a supervised model learning a quantitative variable is called regression and a model learning a qualitative variable is called classification.**
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/classification_v_regression.jpg" style="height: 400px; margin: 20px"/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Import linear regression, one way of modeling continuous variables.  Set the output to be the `medv` variable and the input to be the `features` made above.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See the <a href="http://spark.apache.org/docs/latest/api/python/pyspark.ml.html?highlight=vectorassembler#pyspark.ml.regression.LinearRegression" target="_blank">LinearRegression documentation for more details.</a>

# COMMAND ----------

from pyspark.ml.regression import LinearRegression

lr = LinearRegression(labelCol="medv", featuresCol="features")

# COMMAND ----------

# MAGIC %md
# MAGIC Fit the model to the data using the `.fit()` method.  This returns a trained model that has learned from our data and will be stored in `lrModel`

# COMMAND ----------

lrModel = lr.fit(bostonFeaturizedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at what the model learned from our data.  In the case of linear regression, this comes in the form of an equation that describes the relationship the model has just learned.  This will be covered in more detail in later lessons.

# COMMAND ----------

print("Coefficients: {0:.1f}, {1:.1f}, {2:.1f}".format(*lrModel.coefficients))
print("Intercept: {0:.1f}".format(lrModel.intercept))

# COMMAND ----------

# MAGIC %md
# MAGIC The coefficients of our model are a multiplier against our original features and the intercept is a constant.  To interpret our model, use the following equation:
# MAGIC 
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;`predicted home value = (5.2 x number of rooms) - (.1 x crime rate) - (.6 x % lower class) - 2.6`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prediction
# MAGIC 
# MAGIC So far, we've accomplished the following:<br><br>
# MAGIC 
# MAGIC * Prepared data for a machine learning model
# MAGIC * Trained a linear regression model
# MAGIC * Interpreted what it learned about our data
# MAGIC 
# MAGIC Now, let's see how it predicts on data it has already seen as well as new data.

# COMMAND ----------

# MAGIC %md
# MAGIC Create a subset of the first 10 rows of the `features` and `medv` target variable.

# COMMAND ----------

subsetDF = (bostonFeaturizedDF
  .limit(10)
  .select("features", "medv")
)

display(subsetDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Use the `transform` method on the trained model to see its prediction.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Now that `lrModel` is a trained estimator, we can transform data using its `.transform()` method.

# COMMAND ----------

predictionDF = lrModel.transform(subsetDF)

display(predictionDF)

# COMMAND ----------

# MAGIC %md
# MAGIC How did the model do?  While there's a difference between the prediction and the true value (also known as the model error), generally the model seems to have learned something about our data.
# MAGIC 
# MAGIC Now predict off of a hypothetical data point of a 6 bedroom home with a 3.6 crime rate and 12 % average lower class.  According to our formula, the model should predict about 21:
# MAGIC 
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;`predicted home value = (5.2 x number of rooms) - (.1 x crime rate) - (.6 x % lower class) - 2.6`<br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;`predicted home value = (5.2 x 6) - (.1 x 3.6) - (.6 x 12) - 2.6`<br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;`predicted home value = 31.2 - .4 - 7.2 - 2.6`<br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;`predicted home value = 21`<br>

# COMMAND ----------

from pyspark.ml.linalg import Vectors

data = [(Vectors.dense([6., 3.6, 12.]), )]              # Creates our hypothetical data point
predictDF = spark.createDataFrame(data, ["features"])

display(lrModel.transform(predictDF))
