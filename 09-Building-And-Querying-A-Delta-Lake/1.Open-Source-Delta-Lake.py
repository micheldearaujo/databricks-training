# Databricks notebook source
# MAGIC %md
# MAGIC # <img src="https://files.training.databricks.com/images/DeltaLake-logo.png" width=80px> Open Source Delta Lake
# MAGIC 
# MAGIC [Delta Lake](https://delta.io/) is an open-source storage layer that brings ACID transactions to Apache Spark™ and big data workloads.
# MAGIC 
# MAGIC <img src="https://www.evernote.com/l/AAF4VIILJtFNZLuvZjGGhZTr2H6Z0wh6rOYB/image.png" width=900px>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Features
# MAGIC 
# MAGIC [Quick start intro to Delta Lake.](https://docs.delta.io/latest/quick-start.html#)
# MAGIC 
# MAGIC **ACID Transactions**:
# MAGIC Data lakes typically have multiple data pipelines reading and writing data concurrently, and data engineers have to go through a tedious process to ensure data integrity, due to the lack of transactions. Delta Lake brings ACID transactions to your data lakes. It provides serializability, the strongest level of isolation level.
# MAGIC 
# MAGIC **Scalable Metadata Handling**:
# MAGIC In big data, even the metadata itself can be "big data". Delta Lake treats metadata just like data, leveraging Spark's distributed processing power to handle all its metadata. As a result, Delta Lake can handle petabyte-scale tables with billions of partitions and files at ease.
# MAGIC 
# MAGIC **Time Travel (data versioning)**:
# MAGIC Delta Lake provides snapshots of data enabling developers to access and revert to earlier versions of data for audits, rollbacks or to reproduce experiments.
# MAGIC 
# MAGIC **Open Format**:
# MAGIC All data in Delta Lake is stored in Apache Parquet format enabling Delta Lake to leverage the efficient compression and encoding schemes that are native to Parquet.
# MAGIC 
# MAGIC **Unified Batch and Streaming Source and Sink**:
# MAGIC A table in Delta Lake is both a batch table, as well as a streaming source and sink. Streaming data ingest, batch historic backfill, and interactive queries all just work out of the box.
# MAGIC 
# MAGIC **Schema Enforcement**:
# MAGIC Delta Lake provides the ability to specify your schema and enforce it. This helps ensure that the data types are correct and required columns are present, preventing bad data from causing data corruption.
# MAGIC 
# MAGIC **Schema Evolution**:
# MAGIC Big data is continuously changing. Delta Lake enables you to make changes to a table schema that can be applied automatically, without the need for cumbersome DDL.
# MAGIC 
# MAGIC **100% Compatible with Apache Spark API**:
# MAGIC Developers can use Delta Lake with their existing data pipelines with minimal change as it is fully compatible with Spark, the commonly used big data processing engine.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC 
# MAGIC You will notice that throughout this course, there is a lot of context switching between PySpark/Scala and SQL.
# MAGIC 
# MAGIC This is because:
# MAGIC * `read` and `write` operations are performed on DataFrames using PySpark or Scala
# MAGIC * table creates and queries are performed directly off Delta Lake tables using SQL
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Key Concepts: Delta Lake Architecture</h2>
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We'll touch on this further in future notebooks.
# MAGIC 
# MAGIC Throughout our Delta Lake discussions, we'll often refer to the concept of Bronze/Silver/Gold tables. These levels refer to the state of data refinement as data flows through a processing pipeline.
# MAGIC 
# MAGIC **These levels are conceptual guidelines, and implemented architectures may have any number of layers with various levels of enrichment.** Below are some general ideas about the state of data in each level.
# MAGIC 
# MAGIC * **Bronze** tables
# MAGIC   * Raw data (or very little processing)
# MAGIC   * Data will be stored in the Delta format (can encode raw bytes as a column)
# MAGIC * **Silver** tables
# MAGIC   * Data that is directly queryable and ready for insights
# MAGIC   * Bad records have been handled, types have been enforced
# MAGIC * **Gold** tables
# MAGIC   * Highly refined views of the data
# MAGIC   * Aggregate tables for BI
# MAGIC   * Feature tables for data scientists
# MAGIC 
# MAGIC For different workflows, things like schema enforcement and deduplication may happen in different places.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake Batch Operations - Create
# MAGIC 
# MAGIC Creating Delta Lakes is as easy as changing the file type while performing a write. 
# MAGIC 
# MAGIC In this section, we'll read from a CSV and write to Delta.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/adbcore/AAFxQkg_SzRC06GvVeatDBnNbDL7wUUgCg4B.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Set up relevant paths to the online retail datasets from `/mnt/training/online_retail`

# COMMAND ----------

inputPath = "/mnt/training/online_retail/data-001/data.csv"
DataPath = userhome + "/delta/customer-data/"

#remove directory if it exists
dbutils.fs.rm(DataPath, True)

# COMMAND ----------

# MAGIC %md
# MAGIC Read the data into a DataFrame. We supply the schema.
# MAGIC 
# MAGIC Use overwrite mode so that there will not be an issue in rewriting the data in case you end up running the cell again.
# MAGIC 
# MAGIC Partition on `Country` because there are only a few unique countries and because we will use `Country` as a predicate in a `WHERE` clause.
# MAGIC 
# MAGIC More information on the how and why of partitioning is contained in the links at the bottom of this notebook.
# MAGIC 
# MAGIC Then write the data to Delta Lake.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType

inputSchema = StructType([
  StructField("InvoiceNo", IntegerType(), True),
  StructField("StockCode", StringType(), True),
  StructField("Description", StringType(), True),
  StructField("Quantity", IntegerType(), True),
  StructField("InvoiceDate", StringType(), True),
  StructField("UnitPrice", DoubleType(), True),
  StructField("CustomerID", IntegerType(), True),
  StructField("Country", StringType(), True)
])

rawDataDF = (spark.read
  .option("header", "true")
  .schema(inputSchema)
  .csv(inputPath)
)

# write to Delta Lake
rawDataDF.write.mode("overwrite").format("delta").partitionBy("Country").save(DataPath)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> While we show creating a table in the next section, Spark SQL queries can run directly on a directory of data, for delta use the following syntax: 
# MAGIC ```
# MAGIC SELECT * FROM delta.`/path/to/delta_directory`
# MAGIC ```

# COMMAND ----------

display(spark.sql("SELECT * FROM delta.`{}` LIMIT 5".format(DataPath)))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### CREATE A Table Using Delta Lake
# MAGIC 
# MAGIC Create a table called `customer_data_delta` using `DELTA` out of the above data.
# MAGIC 
# MAGIC The notation is:
# MAGIC > `CREATE TABLE <table-name>` <br>
# MAGIC   `USING DELTA` <br>
# MAGIC   `LOCATION <path-do-data> ` <br>
# MAGIC   
# MAGIC Tables created with a specified `LOCATION` are considered unmanaged by the metastore. Unlike a managed table, where no path is specified, an unmanaged table’s files are not deleted when you `DROP` the table. However, changes to either the registered table or the files will be reflected in both locations.
# MAGIC 
# MAGIC <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Managed tables require that the data for your table be stored in DBFS. Unmanaged tables only store metadata in DBFS. 
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Since Delta Lake stores schema (and partition) info in the `_delta_log` directory, we do not have to specify partition columns!

# COMMAND ----------

spark.sql("""
  DROP TABLE IF EXISTS customer_data_delta
""")
spark.sql("""
  CREATE TABLE customer_data_delta
  USING DELTA
  LOCATION '{}'
""".format(DataPath))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Perform a simple `count` query to verify the number of records.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Notice how the count is right off the bat; no need to worry about table repairs.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM customer_data_delta

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Metadata
# MAGIC 
# MAGIC Since we already have data backing `customer_data_delta` in place,
# MAGIC the table in the Hive metastore automatically inherits the schema, partitioning,
# MAGIC and table properties of the existing data.
# MAGIC 
# MAGIC Note that we only store table name, path, database info in the Hive metastore,
# MAGIC the actual schema is stored in the `_delta_log` directory as shown below.

# COMMAND ----------

display(dbutils.fs.ls(DataPath + "/_delta_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC Metadata is displayed through `DESCRIBE DETAIL <tableName>`.
# MAGIC 
# MAGIC As long as we have some data in place already for a Delta Lake table, we can infer schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL customer_data_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Takeaways
# MAGIC 
# MAGIC Saving to Delta Lake is as easy as saving to Parquet, but creates an additional log file.
# MAGIC 
# MAGIC Using Delta Lake to create tables is straightforward and you do not need to specify schemas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake Batch Operations - Append
# MAGIC 
# MAGIC In this section, we'll load a small amount of new data and show how easy it is to append this to our existing Delta table.
# MAGIC 
# MAGIC We'll start start by setting up our relevant path and loading new consumer product data.

# COMMAND ----------

miniDataInputPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv"

newDataDF = (spark
  .read
  .option("header", "true")
  .schema(inputSchema)
  .csv(miniDataInputPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Do a simple count of number of new items to be added to production data.

# COMMAND ----------

newDataDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### APPEND Using Delta Lake
# MAGIC 
# MAGIC Adding to our existing Delta Lake is as easy as modifying our write statement and specifying the `append` mode. 
# MAGIC 
# MAGIC Here we save to our previously created Delta Lake at `delta/customer-data/`.

# COMMAND ----------

(newDataDF
  .write
  .format("delta")
  .partitionBy("Country")
  .mode("append")
  .save(DataPath)
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Perform a simple `count` query to verify the number of records and notice it is correct.
# MAGIC 
# MAGIC Should be `65535`.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The changes to our files have been immediately reflected in the table that we've registered.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM customer_data_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Takeaways
# MAGIC With Delta Lake, you can easily append new data without schema-on-read issues.
# MAGIC 
# MAGIC Changes to Delta Lake files will immediately be reflected in registered Delta tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake Batch Operations - Upsert
# MAGIC 
# MAGIC To UPSERT means to "UPdate" and "inSERT". In other words, UPSERT is literally TWO operations. It is not supported in traditional data lakes, as running an UPDATE could invalidate data that is accessed by the subsequent INSERT operation.
# MAGIC 
# MAGIC Using Delta Lake, however, we can do UPSERTS. Delta Lake combines these operations to guarantee atomicity to
# MAGIC - INSERT a row 
# MAGIC - if the row already exists, UPDATE the row.
# MAGIC 
# MAGIC ### Scenario
# MAGIC You have a small amount of batch data to write to your Delta table. This is currently staged in a JSON in a mounted blob store.

# COMMAND ----------

upsertDF = spark.read.format("json").load("/mnt/training/enb/commonfiles/upsert-data.json")
display(upsertDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We'll register this as a temporary view so that this table doesn't persist in DBFS (but we can still use SQL to query it).

# COMMAND ----------

upsertDF.createOrReplaceTempView("upsert_data")

# COMMAND ----------

# MAGIC %md
# MAGIC Included in this data are:
# MAGIC - Some new orders for customer 20993
# MAGIC - An update to a previous order correcting the country for customer 20993 to Iceland
# MAGIC - Corrections to some records for StockCode 22837 where the Description was incorrect
# MAGIC 
# MAGIC We can use UPSERT to simultaneously INSERT our new data and UPDATE our previous records.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO customer_data_delta
# MAGIC USING upsert_data
# MAGIC ON customer_data_delta.InvoiceNo = upsert_data.InvoiceNo
# MAGIC   AND customer_data_delta.StockCode = upsert_data.StockCode
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC Notice how this data is seamlessly incorporated into `customer_data_delta`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customer_data_delta WHERE CustomerID=20993

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(Description) 
# MAGIC FROM customer_data_delta 
# MAGIC WHERE StockCode = 22837

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC In this Lesson, we:
# MAGIC - Saved files using Delta Lake
# MAGIC - Used Delta Lake to UPSERT data into existing Delta Lake tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="https://docs.databricks.com/delta/delta-batch.html#" target="_blank">Table Batch Read and Writes</a>
