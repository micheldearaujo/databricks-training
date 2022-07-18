# Databricks notebook source
# MAGIC %md
# MAGIC # Reading Data - Tables and Views
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC * Demonstrate how to pre-register data sources in Azure Databricks.
# MAGIC * Introduce temporary views over files.
# MAGIC * Read data from tables/views.
# MAGIC * Regarding `printRecordsPerPartition(..)`, it 
# MAGIC   * converts the specified `DataFrame` to an RDD
# MAGIC   * counts the number of records in each partition
# MAGIC   * prints the results to the console.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %run "./Includes/Utility-Methods"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Registering Tables in Databricks
# MAGIC 
# MAGIC So far we've seen purely programmatic methods for reading in data.
# MAGIC 
# MAGIC Databricks allows us to "register" the equivalent of "tables" so that they can be easily accessed by all users. 
# MAGIC 
# MAGIC It also allows us to specify configuration settings such as secret keys, tokens, username & passwords, etc without exposing that information to all users.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register a Table/View
# MAGIC * Databrick's UI has built in support for working with a number of different data sources
# MAGIC * New ones are being added regularly
# MAGIC * In our case we are going to upload the file <a href="http://files.training.databricks.com/static/data/pageviews_by_second_example.tsv">pageviews_by_second_example.tsv</a>
# MAGIC * .. and then use the UI to create a table.

# COMMAND ----------

# MAGIC %md
# MAGIC There are several benefits to this strategy:
# MAGIC * Once setup, it never has to be done again
# MAGIC * It is available for any user on the platform (permissions permitting)
# MAGIC * Minimizes exposure of credentials
# MAGIC * No real overhead to reading the schema (no infer-schema)
# MAGIC * Easier to advertise available datasets to other users

# COMMAND ----------

# MAGIC %md
# MAGIC ## Follow these steps to register a new Table
# MAGIC 
# MAGIC **NOTE:** *It may be easiest for you to duplicate this browser tab so you can refer back to these steps.*
# MAGIC 
# MAGIC 1. Download the [pageviews_by_second_example.tsv](http://files.training.databricks.com/static/data/pageviews_by_second_example.tsv) file to your computer.
# MAGIC 2. Select **Data** in the left-hand menu.
# MAGIC 3. Select the database with your username.
# MAGIC 4. Select **Add Data** to create a new Table.
# MAGIC 
# MAGIC   ![The Data menu item and Add Data button are both highlighted.](https://databricksdemostore.blob.core.windows.net/images/03-de-learning-path/data-add-data.png)
# MAGIC 
# MAGIC 5. In the Create New Table form, make sure **Upload File** is selected, then click on browse and select the [pageviews_by_second_example.tsv](http://files.training.databricks.com/static/data/pageviews_by_second_example.tsv) file is highlighted, or drag and drop it into the File box.
# MAGIC 6. Select **Create Table with UI**.
# MAGIC 
# MAGIC   ![The previously listed form options are shown.](https://databricksdemostore.blob.core.windows.net/images/03-de-learning-path/create-new-table-1.png)
# MAGIC 
# MAGIC 7. Select your cluster, then select **Preview Table**.
# MAGIC 8. Under **Create in Database**, select the database with your username in the list. It is **important** that you do not skip this step. You can find the database name in the output of `cell 3` above.
# MAGIC 9. Select **Create Table**.
# MAGIC 
# MAGIC   ![The previously listed form options are shown.](https://databricksdemostore.blob.core.windows.net/images/03-de-learning-path/create-new-table-2.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Reading from a Table/View
# MAGIC 
# MAGIC We can now read in the "table" **pageviews_by_seconds_example** as a `DataFrame` with one simple command (and then print the schema):

# COMMAND ----------

pageviewsBySecondsExampleDF = spark.read.table("pageviews_by_second_example_tsv")

pageviewsBySecondsExampleDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC And of course we can now view that data as well:

# COMMAND ----------

display(pageviewsBySecondsExampleDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review: Reading from Tables
# MAGIC * No job is executed - the schema is stored in the table definition on Databricks.
# MAGIC * The data types shown here are those we defined when we registered the table.
# MAGIC * In our case, the file was uploaded to Databricks and is stored on the DBFS.
# MAGIC   * If we used JDBC, it would open the connection to the database and read it in.
# MAGIC   * If we used an object store (like what is backing the DBFS), it would read the data from source.
# MAGIC * The "registration" of the table simply makes future access, or access by multiple users easier.
# MAGIC * The users of the notebook cannot see username and passwords, secret keys, tokens, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at some of the other details of the `DataFrame` we just created for comparison sake.

# COMMAND ----------

print("Partitions: " + str(pageviewsBySecondsExampleDF.rdd.getNumPartitions()))
printRecordsPerPartition(pageviewsBySecondsExampleDF)
print("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Temporary Views
# MAGIC 
# MAGIC Tables that are loadable by the call `spark.read.table(..)` are also accessible through the SQL APIs.
# MAGIC 
# MAGIC For example, we already used Databricks to expose **pageviews_by_second_example_tsv** as a table/view.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pageviews_by_second_example_tsv limit(5)

# COMMAND ----------

# MAGIC %md
# MAGIC You can also take an existing `DataFrame` and register it as a view exposing it as a table to the SQL API.
# MAGIC 
# MAGIC If you recall from earlier, we have an instance called `parquetDF`.
# MAGIC 
# MAGIC We can create a [temporary] view with this call...

# COMMAND ----------

# create a DataFrame from a parquet file
parquetFile = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"
parquetDF = spark.read.parquet(parquetFile)

# create a temporary view from the resulting DataFrame
parquetDF.createOrReplaceTempView("parquet_table")

# COMMAND ----------

# MAGIC %md
# MAGIC And now we can use the SQL API to reference that same `DataFrame` as the table **parquet_table**.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet_table order by requests desc limit(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ** *Note #1:* ** *The method createOrReplaceTempView(..) is bound to the SparkSession meaning it will be discarded once the session ends.*
# MAGIC 
# MAGIC ** *Note #2:* ** On the other hand, the method createOrReplaceGlobalTempView(..) is bound to the spark application.*
# MAGIC 
# MAGIC *Or to put that another way, I can use createOrReplaceTempView(..) in this notebook only. However, I can call createOrReplaceGlobalTempView(..) in this notebook and then access it from another.*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC 
# MAGIC Start the next lesson, [Writing Data]($./5.Writing%20Data)
