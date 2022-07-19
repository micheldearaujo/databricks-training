# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction to DataFrames Lab
# MAGIC ## Distinct Articles

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
# MAGIC 
# MAGIC In the cell provided below, write the code necessary to count the number of distinct articles in our data set.
# MAGIC 0. Copy and paste all you like from the previous notebook.
# MAGIC 0. Read in our parquet files.
# MAGIC 0. Apply the necessary transformations.
# MAGIC 0. Assign the count to the variable `totalArticles`
# MAGIC 0. Run the last cell to verify that the data was loaded correctly.
# MAGIC 
# MAGIC **Bonus**
# MAGIC 
# MAGIC If you recall from the beginning of the previous notebook, the act of reading in our parquet files will trigger a job.
# MAGIC 0. Define a schema that matches the data we are working with.
# MAGIC 0. Update the read operation to use the schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Show Your Work

# COMMAND ----------

(source, sasEntity, sasToken) = getAzureDataSource()
spark.conf.set(sasEntity, sasToken)

path = source + "/wikipedia/pagecounts/staging_parquet_en_only_clean/"

# COMMAND ----------

# TODO
# Replace <<FILL_IN>> with your code. 

df = (spark                    # Our SparkSession & Entry Point
  .read                        # Our DataFrameReader
  .parquet(path)                  # Read in the parquet files
  .select('article')                  # Reduce the columns to just the one
  .distinct()                  # Produce a unique set of values
)
totalArticles = df.count() # Identify the total number of records remaining.

print("Distinct Articles: {0:,}".format(totalArticles))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Verify Your Work
# MAGIC Run the following cell to verify that your `DataFrame` was created properly.

# COMMAND ----------

expected = 1783138
assert totalArticles == expected, "Expected the total to be " + str(expected) + " but found " + str(totalArticles)


# COMMAND ----------


