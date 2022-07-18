# Databricks notebook source
# MAGIC %md
# MAGIC # Writing Data
# MAGIC 
# MAGIC Just as there are many ways to read data, we have just as many ways to write data.
# MAGIC 
# MAGIC In this notebook, we will take a quick peek at how to write data back out to Parquet files.
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC - Writing data to Parquet files

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Writing Data
# MAGIC 
# MAGIC Let's start with one of our original CSV data sources, **pageviews_by_second.tsv**:

# COMMAND ----------

from pyspark.sql.types import *

csvSchema = StructType([
  StructField("timestamp", StringType(), False),
  StructField("site", StringType(), False),
  StructField("requests", IntegerType(), False)
])

csvFile = "/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv"

csvDF = (spark.read
  .option('header', 'true')
  .option('sep', "\t")
  .schema(csvSchema)
  .csv(csvFile)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have a `DataFrame`, we can write it back out as Parquet files or other various formats.

# COMMAND ----------

fileName = userhome + '/pageviews_by_second.parquet'
print("Output Location: " + fileName)

(csvDF.write
  .option('compression', 'snappy')
  .mode('overwrite')
  .parquet(fileName)
)

# COMMAND ----------

fileName = userhome + "/pageviews_by_second.parquet"
print("Output location: " + fileName)

(csvDF.write                       # Our DataFrameWriter
  .option("compression", "snappy") # One of none, snappy, gzip, and lzo
  .mode("overwrite")               # Replace existing files
  .parquet(fileName)               # Write DataFrame to Parquet files
)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that the file has been written out, we can see it in the DBFS:

# COMMAND ----------

display(
  dbutils.fs.ls(fileName)
)

# COMMAND ----------

# MAGIC %fs ls /user/michel.araujo@artefact.com

# COMMAND ----------

# MAGIC %md
# MAGIC And lastly we can read that same parquet file back in and display the results:

# COMMAND ----------

display(
  spark.read.parquet(fileName)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC 
# MAGIC Start the next lesson, [Reading Data - Lab]($./6.Reading%20Data%20-%20Lab)
