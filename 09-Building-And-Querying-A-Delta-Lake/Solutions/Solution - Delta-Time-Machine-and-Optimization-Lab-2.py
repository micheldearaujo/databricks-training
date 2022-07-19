# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Delta Time Machine & Optimization Lab Solution
# MAGIC 
# MAGIC Databricks&reg; Delta allows you to read, write and query data in data lakes in an efficient manner.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC In this lab, you will:
# MAGIC * Compare different versions of a Delta table using Time Machine
# MAGIC * Optimize your Delta Lake to increase speed and reduce number of files
# MAGIC * Describe how VACUUM handles invalid files
# MAGIC 
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

# MAGIC %md
# MAGIC Because we'll be calculating some aggregates in this notebook, we'll change our partitions after shuffle from the default `200` to `8` (which is a good number for the 8 node cluster we're currently working on).

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC sqlContext.setConf("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for our previous Delta Lake tables
# MAGIC 
# MAGIC This lab relies upon some tables created in previous Delta Lake lessons and labs. 
# MAGIC 
# MAGIC If you get an error from either of the next two SQL queries, running the solution code for the "2.Delta-Lake-Basics-Lab-1" will build all necessary tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM customer_counts;
# MAGIC SELECT COUNT(*) FROM customer_data_delta;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> **The following cell will take several minutes to execute, and is only necessary to run if you got an error in the previous cell.**

# COMMAND ----------

# MAGIC %run "./Includes/Delta-Lab-2-Prep"

# COMMAND ----------

# MAGIC %md
# MAGIC For convenience later in this lab, the paths to the files defining our existing Delta tables are provided. You can use these paths to load the data into DataFrames, if desired, though this entire lab can be completed using SQL on the existant tables.

# COMMAND ----------

DeltaPath = userhome + "/delta/customer-data/"
CustomerCountsPath = userhome + "/delta/customer_counts/"

# COMMAND ----------

# MAGIC %md
# MAGIC **Note: This lab depends upon the complete exectuion of the notebook titled "Open-Source-Delta-Lake" and the "Delta-Lake-Basics" lab. If these tables don't exist, go back and run all cells in these notebook.**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Travel
# MAGIC Because Delta Lake is version controlled, you have the option to query past versions of the data. Let's look at the history of our current Delta table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customer_data_delta

# COMMAND ----------

# MAGIC %md
# MAGIC Querying an older version is as easy as adding `VERSION AS OF desired_version`. Let's verify that our table from one version back still exists.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM customer_data_delta
# MAGIC VERSION AS OF 1

# COMMAND ----------

# MAGIC %md
# MAGIC Using a single file storage system, you now have access to every version of your historical data, ensuring that your data analysts will be able to replicate their reports (and compare aggregate changes over time) and your data scientists will be able to replicate their experiments.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check difference between versions
# MAGIC 
# MAGIC You want to compare how many orders from Sweden were added by your recent UPSERT to your BI table.
# MAGIC 
# MAGIC Let's start by getting the total sum of our `total_orders` column where our country is Sweden.

# COMMAND ----------

# ANSWER
count = spark.sql("SELECT SUM(total_orders) FROM customer_counts WHERE Country='Sweden'").collect()[0][0]

print(count)

# COMMAND ----------

# MAGIC %md
# MAGIC Again, we can look at the history of our Delta table here.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customer_counts

# COMMAND ----------

# MAGIC %md
# MAGIC Our original table will be version `0`. Let's write a SQL query to see how many orders we originally had from Sweden.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(total_orders)
# MAGIC FROM customer_counts
# MAGIC VERSION AS OF 0
# MAGIC WHERE Country='Sweden'

# COMMAND ----------

# MAGIC %md
# MAGIC We can combine these two queries and get our difference, which represents our new entries.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(total_orders) - (
# MAGIC   SELECT SUM(total_orders)
# MAGIC   FROM customer_counts
# MAGIC   VERSION AS OF 0
# MAGIC   WHERE Country='Sweden') AS new_entries
# MAGIC FROM customer_counts
# MAGIC WHERE Country='Sweden'

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTIMIZE and ZORDER
# MAGIC 
# MAGIC Let's apply some of these optimizations to `../delta/customer-data/`.
# MAGIC 
# MAGIC Our data is partitioned by `Country`.
# MAGIC 
# MAGIC We want to query the data for `StockCode` equal to `22301`.
# MAGIC 
# MAGIC We expect this query to be slow because we have to examine ALL OF `../delta/customer-data/` to find the desired `StockCode` and not just in one or two partitions.
# MAGIC 
# MAGIC First, let's time the above query: you will need to form a DataFrame to pass to `preZorderQuery`.

# COMMAND ----------

# ANSWER
%timeit preZorderQuery = spark.sql("SELECT * FROM customer_data_delta WHERE StockCode=22301 ").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Compact the files and re-order by `StockCode`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC OPTIMIZE customer_data_delta
# MAGIC ZORDER by (StockCode)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's time the above query again: you will need to form a DataFrame to pass to `postZorderQuery`.

# COMMAND ----------

# ANSWER
%timeit postZorderQuery = spark.sql("SELECT * FROM customer_data_delta WHERE StockCode=22301").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTIMIZE your BI table
# MAGIC 
# MAGIC Here we'll optimize our `customer_counts` table so that we can quickly query on our `CustomerID` column.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE customer_counts
# MAGIC ZORDER by (CustomerID)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can easily look at which of our customers have made the most orders.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CustomerID, SUM(total_orders) AS total
# MAGIC FROM customer_counts
# MAGIC GROUP BY CustomerID
# MAGIC ORDER BY total DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Or we can see examine those customers that operate in the most countries.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CustomerID, COUNT(Country) AS num_countries
# MAGIC FROM customer_counts
# MAGIC GROUP BY CustomerID
# MAGIC SORT BY num_countries DESC

# COMMAND ----------

# MAGIC %md
# MAGIC And then look at how many orders a customer made in each of these countries.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Country, total_orders
# MAGIC FROM customer_counts
# MAGIC WHERE CustomerID = 20059

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using VACUUM to clean up small files
# MAGIC 
# MAGIC After we run OPTIMIZE, we have a number of uncompacted files that are no longer necessary. Running VACUUM will remove these files for us.
# MAGIC 
# MAGIC Let's go ahead and VACUUM our `customer_data_delta` table, which points at the files in our `DeltaPath` variable.
# MAGIC 
# MAGIC Count number of files before `VACUUM` for `Country=Sweden`.

# COMMAND ----------

# ANSWER
preNumFiles = len(dbutils.fs.ls(DeltaPath + "/Country=Sweden"))
print(preNumFiles)

# COMMAND ----------

# MAGIC %md
# MAGIC If you try to perform an immediate `VACUUM` (using `RETAIN 0 HOURS` to clean up recently optimized files), you will get an error.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC VACUUM customer_data_delta RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC This is a helfpul error. Remember that `VACUUM` is intended for occasional garbage collection. Here we'll just demonstrating that we _can_ use it to clean up files, so we'll set our configuration to allow this operation.

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we won't get an error when we run `VACUUM`.

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM customer_data_delta RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC Count how many files there are for `Country=Sweden`.

# COMMAND ----------

# ANSWER
postNumFiles = len(dbutils.fs.ls(DeltaPath + "/Country=Sweden"))
print(postNumFiles)

# COMMAND ----------

# MAGIC %md
# MAGIC Comparing our `preNumFiles` to `postNumFiles`, we can see that this number has reduced.
