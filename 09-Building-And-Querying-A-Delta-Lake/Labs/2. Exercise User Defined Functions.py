# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Exercise User Defined Functions
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
# MAGIC 
# MAGIC In this exercise, we're doing ETL. Instead of using built-in functions, use UDFs to complete the exercise. 
# MAGIC 
# MAGIC As a reminder, that file contains data about people, including:
# MAGIC 
# MAGIC 
# MAGIC * first, middle and last names
# MAGIC * gender
# MAGIC * birth date
# MAGIC * Social Security number
# MAGIC * salary
# MAGIC 
# MAGIC But, as is unfortunately common in data we get from this customer, the file contains some duplicate records. Worse:
# MAGIC 
# MAGIC * In some of the records, the names are mixed case (e.g., "Carol"), while in others, they are uppercase (e.g., "CAROL"). 
# MAGIC * The Social Security numbers aren't consistent, either. Some of them are hyphenated (e.g., "992-83-4829"), while others are missing hyphens ("992834829").
# MAGIC 
# MAGIC The name fields are guaranteed to match, if you disregard character case, and the birth dates will also match. (The salaries will match, as well,
# MAGIC and the Social Security Numbers *would* match, if they were somehow put in the same format).
# MAGIC 
# MAGIC Your job is to remove the duplicate records. The specific requirements of your job are:
# MAGIC 
# MAGIC * Remove duplicates. It doesn't matter which record you keep; it only matters that you keep one of them.
# MAGIC * Preserve the data format of the columns. For example, if you write the first name column in all lower-case, you haven't met this requirement.
# MAGIC * Write the result as a Parquet file, as designated by *destFile*.
# MAGIC * The final Parquet "file" must contain 8 part files (8 files ending in ".parquet").
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** The initial dataset contains 103,000 records.<br/>
# MAGIC The de-duplicated result haves 100,000 records.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Hints
# MAGIC 
# MAGIC * Use the <a href="http://spark.apache.org/docs/latest/api/python/index.html" target="_blank">API docs</a>. Specifically, you might find 
# MAGIC   <a href="http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame" target="_blank">DataFrame</a> and
# MAGIC   <a href="http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions" target="_blank">functions</a> to be helpful.
# MAGIC * It's helpful to look at the file first, so you can check the format. `dbutils.fs.head()` (or just `%fs head`) is a big help here.

# COMMAND ----------

# TODO

sourceFile = "dbfs:/mnt/training/dataframes/people-with-dups.txt"
destFile = userhome + "/people.parquet"

# In case it already exists
dbutils.fs.rm(destFile, True)

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs head dbfs:/mnt/training/dataframes/people-with-dups.txt

# COMMAND ----------

df = (spark.read
     .option('header', 'true')
     .option('sep', ':')
     .csv(sourceFile)
     )

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 8)

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.0 Correct the Names: Put everything with "Title" 

# COMMAND ----------

def titleString(string):
    return string.title()

titleStringUDF = udf(titleString)

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

df = df.withColumn('firstName', titleStringUDF(F.col('firstName')))
df = df.withColumn('middleName', titleStringUDF(F.col('middleName')))
df = df.withColumn('lastName', titleStringUDF(F.col('lastName')))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.0 Correct the SSN: Remove the hyphen

# COMMAND ----------

def removeHyphen(string):
    return string.replace('-', '')

removeHyphenUDF = udf(removeHyphen)

# COMMAND ----------

df = df.withColumn('ssn', removeHyphenUDF(F.col('ssn')))

# COMMAND ----------

final_df = df.dropDuplicates(subset=['firstName', 'lastName', 'middleName', 'ssn'])

# COMMAND ----------

final_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.0 Save the File

# COMMAND ----------



# COMMAND ----------

final_df.write.mode('overwrite').format('parquet').save(destFile)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer
# MAGIC 
# MAGIC At the bare minimum, we can verify that you wrote the parquet file out to **destFile** and that you have the right number of records.
# MAGIC 
# MAGIC Running the following cell to confirm your result:

# COMMAND ----------

display(dbutils.fs.ls(destFile))

# COMMAND ----------

partFiles = len(list(filter(lambda f: f.path.endswith(".parquet"), dbutils.fs.ls(destFile))))

finalDF = spark.read.parquet(destFile)
finalCount = finalDF.count()

clearYourResults()
validateYourAnswer("01 Parquet File Exists", 1276280174, partFiles)
validateYourAnswer("02 Expected 100000 Records", 972882115, finalCount)
summarizeYourResults()

# COMMAND ----------


