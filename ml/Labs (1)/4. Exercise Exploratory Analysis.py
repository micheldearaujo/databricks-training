# Databricks notebook source
# MAGIC %md
# MAGIC ## Exercise: EDA on the Bike Sharing Dataset
# MAGIC 
# MAGIC Do exploratory analysis on the bike sharing dataset by calculating and interpreting summary statistics, creating basic plots, and calculating correlations.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to set up our environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Summary Statistics
# MAGIC 
# MAGIC Calculate the count, mean, and standard deviation for each variable in the dataset.  What does each variable signify?  What is the spread of the data?

# COMMAND ----------

# MAGIC %md
# MAGIC Import the data.

# COMMAND ----------

bikeDF = (spark
  .read
  .option("header", True)
  .option("inferSchema", True)
  .csv("/mnt/training/bikeSharing/data-001/hour.csv")
  .drop("instant", "dteday", "casual", "registered", "holiday", "weekday")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate count, mean, and standard deviation.

# COMMAND ----------

# TODO
FILL_IN

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Plotting
# MAGIC 
# MAGIC Create the following plots:<br><br>
# MAGIC 
# MAGIC 1. A histogram of the dependent variable `cnt`
# MAGIC 2. A barplot of counts by hour
# MAGIC 3. A scattermatrix

# COMMAND ----------

# MAGIC %md
# MAGIC Create a histogram of the variable `cnt`.

# COMMAND ----------

# TODO
FILL_IN

# COMMAND ----------

# MAGIC %md
# MAGIC Create a barplot of counts by hour.

# COMMAND ----------

# TODO
FILL_IN

# COMMAND ----------

# MAGIC %md
# MAGIC Create a scattermatrix.  This can be done in Python or with the built-in Databricks functionality.

# COMMAND ----------

# TODO
FILL_IN

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Correlations
# MAGIC 
# MAGIC Calculate the correlations of the different variables.  Start by using `VectorAssembler` to put all the variables into a single column `features`.

# COMMAND ----------

# TODO
from pyspark.ml.feature import VectorAssembler
assembler = # FILL_IN

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate the correlation on the transformed `bikeDF`

# COMMAND ----------

# TODO
from pyspark.ml.stat import Correlation

pearsonCorr = FILL_IN
pandasDF = FILL_IN
