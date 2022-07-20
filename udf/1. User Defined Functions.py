# Databricks notebook source
# MAGIC %md
# MAGIC # User Defined Functions
# MAGIC 
# MAGIC If you can use the built-in functions in `sql.functions`, you certainly should use them as they less likely to have bugs and can be optimized by Catalyst.
# MAGIC 
# MAGIC However, the built-in functions don't contain everything you need, and sometimes you have to write custom functions, known as User Defined Functions.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:</br>
# MAGIC 
# MAGIC * Create User Defined Functions (UDFs)
# MAGIC * Articulate performance advantages of Vectorized UDFs in Python

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

filePath = "dbfs:/mnt/training/airbnb/sf-listings/sf-listings-2019-03-06.csv"
rawDF = spark.read.csv(filePath, header=True, inferSchema=True, multiLine=True, escape='"')
display(rawDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's select the columns`id`, `host_name`, `bedrooms`, `neighbourhood_cleansed`, and `price`, as well as filter out all the rows that contain nulls.

# COMMAND ----------

airbnbDF = rawDF.select("id", "host_name", "bedrooms", "neighbourhood_cleansed", "price").dropna()
display(airbnbDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### User Defined Functions
# MAGIC 
# MAGIC We've seen many built-in functions (e.g. `avg`, `lit`, `col`, etc.). However, sometimes you might need a specific function that is not provided, so let's look at how to define your own **User Defined Function**.
# MAGIC 
# MAGIC For example, let's say we want to get the first initial from our `host_name` field. Let's start by writing that function in local Python/Scala.

# COMMAND ----------

def firstInitialFunction(name):
  return name[0]

firstInitialFunction("Jane")

# COMMAND ----------

# MAGIC %md
# MAGIC Now we have to define a UDF that wraps the function. This will serialize our function and send it to the executors so that we can use it in our DataFrame.

# COMMAND ----------

firstInitialUDF = udf(firstInitialFunction)

# COMMAND ----------

from pyspark.sql.functions import col
display(airbnbDF.select(firstInitialUDF(col("host_name"))))

# COMMAND ----------

# MAGIC %md
# MAGIC We can also create a UDF using `spark.sql.register`, which will create the UDF in the SQL namespace.

# COMMAND ----------

airbnbDF.createOrReplaceTempView("airbnbDF")

spark.udf.register("sql_udf", firstInitialFunction)

# COMMAND ----------

# MAGIC %sql
# MAGIC select sql_udf(host_name) as firstInitial from airbnbDF

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decorator Syntax [Python Only]
# MAGIC 
# MAGIC Alternatively, you can define a UDF using decorator syntax in Python with the dataType the function will return. 
# MAGIC 
# MAGIC However, you cannot call the local Python function anymore (e.g. `decoratorUDF("Jane")` will not work)

# COMMAND ----------

# MAGIC %python
# MAGIC # Our input/output is a sting
# MAGIC @udf("string")
# MAGIC def decoratorUDF(name):
# MAGIC   return name[0]

# COMMAND ----------

display(airbnbDF.select(decoratorUDF(col("host_name"))))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC UDFs provide even more functionality, but it is best to use a built in function wherever possible.
# MAGIC 
# MAGIC **UDF Drawbacks:**
# MAGIC * UDFs cannot be optimized by the Catalyst Optimizer
# MAGIC * The function **has to be serialized** and sent out to the executors
# MAGIC * In the case of Python, there is even more overhead - we have to **spin up a Python interpreter** on every Executor to run the UDF (e.g. Python UDFs much slower than Scala UDFs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vectorized UDF
# MAGIC 
# MAGIC As of Spark 2.3, there are Vectorized UDFs available in Python to help speed up the computation.
# MAGIC 
# MAGIC * [Blog post](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)
# MAGIC * [Documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html#pyspark-usage-guide-for-pandas-with-apache-arrow)
# MAGIC 
# MAGIC ![Benchmark](https://databricks.com/wp-content/uploads/2017/10/image1-4.png)
# MAGIC 
# MAGIC Vectorized UDFs utilize Apache Arrow to speed up computation. Let's see how that helps improve our processing time.
# MAGIC 
# MAGIC [Apache Arrow](https://arrow.apache.org/), is an in-memory columnar data format that is used in Spark to efficiently transfer data between JVM and Python processes. See more [here](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html).

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import pandas_udf
# MAGIC 
# MAGIC # We have a string input/output
# MAGIC @pandas_udf("string")
# MAGIC def vectorizedUDF(name):
# MAGIC   return name.str[0]

# COMMAND ----------

display(airbnbDF.select(vectorizedUDF(col("host_name"))))

# COMMAND ----------

# MAGIC %md
# MAGIC We can also register these Vectorized UDFs to the SQL namespace.

# COMMAND ----------

# MAGIC %python
# MAGIC spark.udf.register("sql_vectorized_udf", vectorizedUDF)
