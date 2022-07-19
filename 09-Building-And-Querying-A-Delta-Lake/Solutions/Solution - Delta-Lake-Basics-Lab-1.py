# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Basics Lab Solution
# MAGIC 
# MAGIC Databricks&reg; Delta allows you to read, write and query data in data lakes in an efficient manner.
# MAGIC 
# MAGIC ## Learning Objectives:
# MAGIC In this lab, you will:
# MAGIC * Create a new Delta Lake from aggregate data of an existing Delta Lake
# MAGIC * UPSERT records into a Delta lake
# MAGIC * Append new data to an existing Delta Lake
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers
# MAGIC * Secondary Audience: Data Analysts and Data Scientists
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and
# MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
# MAGIC * Databricks Runtime 4.2 or greater
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC We will use online retail datasets from `/mnt/training/online_retail`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> **This lab depends upon the complete execution of the notebook titled "Open-Source-Delta-Lake" which registered the `customer_data_delta` table. If this table doesn't exist, run the cell below.**

# COMMAND ----------

# MAGIC %run "./Includes/Delta-Lab-1-Prep"

# COMMAND ----------

# MAGIC %md
# MAGIC Because we'll be calculating some aggregates in this notebook, we'll change our partitions after shuffle from the default `200` to `8` (which is a good number for the 8 node cluster we're currently working on).

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC sqlContext.setConf("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## What is a table? 
# MAGIC Before we continue, we need to address a semantic concern addressed by the [Databricks docs](https://docs.databricks.com/user-guide/tables.html#view-databases-and-tables):
# MAGIC 
# MAGIC > A Databricks table is a collection of structured data. Tables are equivalent to Apache Spark DataFrames.
# MAGIC 
# MAGIC Generally, the distinction between tables and DataFrames in Spark can be summarized by discussing scope and persistence:
# MAGIC - Tables are defined at the **workspace** level and **persist** between notebooks.
# MAGIC - DataFrames are defined at the **notebook** level and are **ephemeral**.
# MAGIC 
# MAGIC When we discuss **Delta tables**, we are always talking about collections of structured data that persist between notebooks. Importantly, we do not need to register a directory of files to Spark SQL in order to refer to them as a table. The directory of files itself _is_ the table; registering it with a useful name to Spark SQL just gives us easy accessing to querying these underlying data.
# MAGIC 
# MAGIC A **Delta Lake** can be thought of as a collection of one or many Delta tables. Generally, an entire elastic storage container will be dedicated to a single Delta Lake, and data will be enriched and cleaned as it is promoted through pre-defined logic.
# MAGIC 
# MAGIC <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> To make Delta tables easily accessible, register them using Spark SQL. Use table ACLs to control access in workspaces shared by many diverse parties within an organization.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Creating a new Delta table
# MAGIC 
# MAGIC You business intelligence team wants to create a dashboard to track the total number of orders made by customers globally. Many of your customers are international retailers, and have the same customer ID.
# MAGIC 
# MAGIC Because you batch process your data each day, you've decided to create a workflow that will update their numbers when you run your reports each night.
# MAGIC 
# MAGIC In this notebook, we'll start by transforming existing data stored in Delta to create a new Delta table for your BI team's dashboard. Then, we'll create processes to append new data to our full records as well as updating the Delta table for the BI team.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load in Delta table
# MAGIC 
# MAGIC The path to our existing Delta table is provided.

# COMMAND ----------

DeltaPath = userhome + "/delta/customer-data/"

# COMMAND ----------

# MAGIC %md
# MAGIC Note that because we registered a global table associated with this Delta table, you should already be able to query this data with SQL. To see all your currently registered tables, click the `Data` icon on the left navigation bar.
# MAGIC 
# MAGIC <img src=https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/adbcore/data-button.png width=100px>

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM customer_data_delta

# COMMAND ----------

# MAGIC %md
# MAGIC Because we stored our data in Delta, our schema and partions are preserved. All we'll need to do is specify the format and the path.

# COMMAND ----------

# ANSWER

deltaDF = (spark.read
  .format("delta")
  .load(DeltaPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate our BI table
# MAGIC We'll start out by just looking at our aggregate counts. Here, we group by both `"CustomerID"` and `"Country"`, as it is the combination of these two fields that is of interest to our BI team.

# COMMAND ----------

customerCounts = (deltaDF.groupBy("CustomerID", "Country")
  .count()
  .withColumnRenamed("count", "total_orders"))

display(customerCounts)

# COMMAND ----------

# MAGIC %md
# MAGIC Clicking on the names of the various columns will allow us to quickly sort on different fields. You may notice that we have a large number of entries that are `null` for both `"CustomerID"` and `"Country"`. While in production, we would like to explore _why_ we are seeing these missing values, for now we'll just leave them as is and save out this DataFrame as a new Delta table.
# MAGIC 
# MAGIC Here, the path is provided for you.

# COMMAND ----------

CustomerCountsPath = userhome + "/delta/customer_counts/"

dbutils.fs.rm(CustomerCountsPath, True) #deletes Delta table if previously created

# COMMAND ----------

# MAGIC %md
# MAGIC Here we'll write out our Delta table to the path provided above. Make sure the following settings are provided:
# MAGIC - `overwrite` (so that this code will work if you run it again)
# MAGIC - format as `delta`
# MAGIC - partition by `"Country"`
# MAGIC - save to `CustomerCountsPath`

# COMMAND ----------

# ANSWER

(customerCounts.write
  .mode("overwrite")
  .format("delta")
  .partitionBy("Country")
  .save(CustomerCountsPath))

# COMMAND ----------

# MAGIC %md
# MAGIC We'll also register this Delta table as a Spark SQL table.

# COMMAND ----------

spark.sql("""
  DROP TABLE IF EXISTS customer_counts
""")

spark.sql("""
  CREATE TABLE customer_counts
  USING DELTA
  LOCATION '{}'
""".format(CustomerCountsPath))

# COMMAND ----------

# MAGIC %md
# MAGIC Now our BI team can quickly query those data points they've expressed interest in.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM customer_counts

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Looking at your existing Delta table, you know that a large number of recent orders haven't been loaded in yet.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###  READ updated CSV data
# MAGIC 
# MAGIC Read the data into a DataFrame. We'll use the same schema that we used when creating our Delta table, which is supplied for you.

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

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Read data in `newDataPath`. Re-use `inputSchema` as defined above. We'll name our DataFrame `newDataDF`.

# COMMAND ----------

# ANSWER

newDataPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-small.csv"
newDataDF = (spark
  .read
  .option("header", "true")
  .schema(inputSchema)
  .csv(newDataPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's do the same aggregate count as above, on `"Country"` and `"CustomerID"`.

# COMMAND ----------

# ANSWER

newCustomerCounts = (newDataDF.groupBy("CustomerID", "Country")
  .count()
  .withColumnRenamed("count", "total_orders"))

# COMMAND ----------

display(newCustomerCounts)

# COMMAND ----------

# MAGIC %md
# MAGIC ### UPSERT new customer counts
# MAGIC 
# MAGIC Now that we've successfully loaded and aggregated our new data, we can upsert it in our existing Delta Lake.
# MAGIC 
# MAGIC First, we'll register it as a temp view.

# COMMAND ----------

newCustomerCounts.createOrReplaceTempView("new_customer_counts")

# COMMAND ----------

# MAGIC %md
# MAGIC And now we can merge these new counts into our existing data.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO customer_counts
# MAGIC USING new_customer_counts
# MAGIC ON customer_counts.Country = new_customer_counts.Country
# MAGIC AND customer_counts.CustomerID = new_customer_counts.CustomerID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET total_orders = customer_counts.total_orders + new_customer_counts.total_orders
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC We can write a simple SQL query to confirm that this has worked.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT SUM(total_orders) FROM customer_counts

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update full records using append
# MAGIC 
# MAGIC We want to retain the full records being generated from our batch report in our existing non-aggregated Delta table.
# MAGIC 
# MAGIC In this case, we're assuming that the records we process at the end of each day are correct, and that batch processing will result in correct, stable records. We can safely write our table to the same file path using the append mode to insert these records.
# MAGIC 
# MAGIC **Note**: If our reports included changes to line items from previous days, we would want to write an UPSERT which would allow us to simultaneously update our changed records and insert new data.

# COMMAND ----------

# ANSWER

(newDataDF.write
  .format("delta")
  .mode("append")
  .save(DeltaPath))

# COMMAND ----------

# MAGIC %md
# MAGIC Querying our table again shows that we've immediately updated.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM customer_data_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="https://docs.databricks.com/delta/delta-batch.html#" target="_blank">Table Batch Read and Writes</a>
