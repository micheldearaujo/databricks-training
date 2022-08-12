# Databricks notebook source
# MAGIC %md
# MAGIC # Machine Learning Workflows
# MAGIC 
# MAGIC Machine learning practitioners generally follow an iterative workflow.  This lesson walks through that workflow at a high level before exploring train/test splits, a baseline model, and evaluation.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC * Define the data analytics development cycle
# MAGIC * Motivate and perform a split between training and test data
# MAGIC * Train a baseline model
# MAGIC * Evaluate a baseline model's performance and improve it

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/qimsc8jn4a?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/qimsc8jn4a?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### The Development Cycle
# MAGIC 
# MAGIC Data scientists follow an iterative workflow that keeps their work closely aligned to both business problems and their data.  This cycle begins with a thorough understanding of the business problem and the data itself, a process called _exploratory data analysis_.  Once the motivating business question and data are understood, the next step is preparing the data for modeling.  This includes removing or imputing missing values and outliers as well as creating features to train the model on.  The majority of a data scientist's work is spent in these earlier steps.
# MAGIC 
# MAGIC After preparing the features in a way that the model can benefit from, the modeling stage uses those features to determine the best way to represent the data.  The various models are then evaluated and this whole process is repeated until the best solution is developed and deployed into production.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/CRISP-DM.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 
# MAGIC The above model addresses the high-level development cycle of data products.  This lesson addresses how to implement this at more practical level.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> <a href="https://en.wikipedia.org/wiki/Cross-industry_standard_process_for_data_mining" target="_blank">See the Cross-Industry Standard Process for Data Mining</a> for details on the method above.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to set up our environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Train/Test Split
# MAGIC 
# MAGIC To implement the development cycle detailed above, data scientists first divide their data randomly into two subsets.  This allows for the evaluation of the model on unseen data.<br><br>
# MAGIC 
# MAGIC 1. The **training set** is used to train the model on
# MAGIC 2. The **test set** is used to test how well the model performs on unseen data
# MAGIC 
# MAGIC This split avoids the memorization of data, known as **overfitting**.  Overfitting occurs when our model learns patterns caused by random chance rather than true signal.  By evaluating our model's performance on unseen data, we can minimize overfitting.
# MAGIC 
# MAGIC Splitting training and test data should be done so that the amount of data in the test set is a good sample of the overall data.  **A split of 80% of your data in the training set and 20% in the test set is a good place to start.**
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/train-test-split.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC Import the Boston dataset.

# COMMAND ----------

bostonDF = (spark.read
  .option("HEADER", True)
  .option("inferSchema", True)
  .csv("/mnt/training/bostonhousing/bostonhousing/bostonhousing.csv")
)

display(bostonDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Split the dataset into two DataFrames:<br><br>
# MAGIC 
# MAGIC 1. `trainDF`: our training data
# MAGIC 2. `testDF`: our test data
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Using a seed ensures that the random split we conduct will be the same split if we rerun the code again.  Reproducible experiments are a hallmark of good science.<br>
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Conventions using other machine learning tools often entail creating 4 objects: `X_train`, `y_train`, `X_test`, and `y_test` where your features `X` are separate from your label `y`.  Since Spark is distributed, the Spark convention keeps the features and labels together when the split is performed.

# COMMAND ----------

trainDF, testDF = bostonDF.randomSplit([0.8, 0.2], seed=42)

print("We have {} training examples and {} test examples.".format(trainDF.count(), testDF.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Baseline Model
# MAGIC 
# MAGIC A **baseline model** offers an educated best guess to improve upon as different models are trained and evaluated.  It represents the simplest model we can create.  This is generally approached as the center of the data.  In the case of regression, this could involve predicting the average of the outcome regardless of the features it sees.  In the case of classification, the center of the data is the mode, or the most common class.  
# MAGIC 
# MAGIC A baseline model could also be a random value or a preexisting model.  Through each new model, we can track improvements with respect to this baseline.

# COMMAND ----------

# MAGIC %md
# MAGIC Create a baseline model by calculating the average housing value in the training dataset.

# COMMAND ----------

from pyspark.sql.functions import avg

trainAvg = trainDF.select(avg("medv")).first()[0]

print("Average home value: {}".format(trainAvg))

# COMMAND ----------

# MAGIC %md
# MAGIC Take the average calculated on the training dataset and append it as the column `prediction` on the test dataset.

# COMMAND ----------

from pyspark.sql.functions import lit

testPredictionDF = testDF.withColumn("prediction", lit(trainAvg))

display(testPredictionDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Evaluation and Improvement
# MAGIC 
# MAGIC Evaluation offers a way of measuring how well predictions match the observed data.  In other words, an evaluation metric measures how closely predicted responses are to the true response.
# MAGIC 
# MAGIC There are a number of different evaluation metrics.  The most common evaluation metric in regression tasks is **mean squared error (MSE)**.  This is calculated by subtracting each predicted response from the corresponding true response and squaring the result.  This assures that the result is always positive.  The lower the MSE, the better the model is performing.  Technically:
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/mse.png" style="height: 100px; margin: 20px"/></div>
# MAGIC 
# MAGIC Since we care about how our model performs on unseen data, we are more concerned about the test error, or the MSE calculated on the unseen data.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Define the evaluator with the prediction column, label column, and MSE metric.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We'll explore various model parameters in later lessons.

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="medv", metricName="mse")

# COMMAND ----------

# MAGIC %md
# MAGIC Evaluate `testPredictionDF` using the `.evaluator()` method.

# COMMAND ----------

testError = evaluator.evaluate(testPredictionDF)

print("Error on the test set for the baseline model: {}".format(testError))

# COMMAND ----------

# MAGIC %md
# MAGIC This score indicates that the average squared distance between the true home value and the prediction of the baseline is about 79.  Taking the square root of that number gives us the error in the units of the quantity being estimated.  In other words, taking the square root of 79 gives us an average error of about $8,890.  That's not great, but it's also not too bad for a naive approach.
