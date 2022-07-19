# Databricks notebook source
# MAGIC %md
# MAGIC # <img src="https://files.training.databricks.com/images/DeltaLake-logo.png" width=80px> Managed Delta Lake
# MAGIC 
# MAGIC Delta Lake&reg; managed and queried via the Databricks platform includes additional features and optimizations.
# MAGIC 
# MAGIC These include:
# MAGIC 
# MAGIC - **Optimize**
# MAGIC 
# MAGIC - **Data skipping**
# MAGIC 
# MAGIC - **Z-Order**
# MAGIC 
# MAGIC - **Caching**
# MAGIC 
# MAGIC <img src="https://www.evernote.com/l/AAGv1SuWeRNJM4TI4bIOyGNPm0CTHa17PLwB/image.png" width=900px>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %run ./Includes/Delta-Optimization-Setup

# COMMAND ----------

spark.sql("""
    DROP TABLE IF EXISTS iot_data
  """)
spark.sql("""
    CREATE TABLE iot_data
    USING DELTA
    LOCATION '{}/delta/iot-events/'
  """.format(userhome))

# COMMAND ----------

# MAGIC %md
# MAGIC Set up relevant paths.

# COMMAND ----------

iotPath = userhome + "/delta/iot-events/"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SMALL FILE PROBLEM
# MAGIC 
# MAGIC Historical and new data is often written in very small files and directories.
# MAGIC 
# MAGIC This data may be spread across a data center or even across the world (that is, not co-located).
# MAGIC 
# MAGIC The result is that a query on this data may be very slow due to
# MAGIC * network latency
# MAGIC * volume of file metatadata
# MAGIC 
# MAGIC The solution is to compact many small files into one larger file.
# MAGIC Delta Lake has a mechanism for compacting small files.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### OPTIMIZE
# MAGIC Delta Lake supports the `OPTIMIZE` operation, which performs file compaction.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Small files are compacted together into new larger files up to 1GB.
# MAGIC Thus, at this point the number of files increases!
# MAGIC 
# MAGIC The 1GB size was determined by the Databricks optimization team as a trade-off between query speed and run-time performance when running Optimize.
# MAGIC 
# MAGIC `OPTIMIZE` is not run automatically because you must collect many small files first.
# MAGIC 
# MAGIC * Run `OPTIMIZE` more often if you want better end-user query performance
# MAGIC * Since `OPTIMIZE` is a time consuming step, run it less often if you want to optimize cost of compute hours
# MAGIC * To start with, run `OPTIMIZE` on a daily basis (preferably at night when spot prices are low), and determine the right frequency for your particular business case
# MAGIC * In the end, the frequency at which you run `OPTIMIZE` is a business decision
# MAGIC 
# MAGIC The easiest way to see what `OPTIMIZE` does is to perform a simple `count(*)` query before and after and compare the timing!

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the `iotPath + "/date=2018-06-01/" ` directory.
# MAGIC 
# MAGIC Notice, in particular files like `../delta/iot-events/date=2018-07-26/part-xxxx.snappy.parquet`. There are hundreds of small files!

# COMMAND ----------

display(dbutils.fs.ls(iotPath + "/date=2016-07-26"))

# COMMAND ----------

# MAGIC %md
# MAGIC CAUTION: Run this query. Notice it is very slow, due to the number of small files.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM iot_data where deviceId=92

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Data Skipping and ZORDER
# MAGIC 
# MAGIC Delta Lake uses two mechanisms to speed up queries.
# MAGIC 
# MAGIC <b>Data Skipping</b> is a performance optimization that aims at speeding up queries that contain filters (WHERE clauses).
# MAGIC 
# MAGIC For example, we have a data set that is partitioned by `date`.
# MAGIC 
# MAGIC A query using `WHERE date > 2016-07-26` would not access data that resides in partitions that correspond to dates prior to `2016-07-26`.
# MAGIC 
# MAGIC <b>ZOrdering</b> is a technique to colocate related information in the same set of files.
# MAGIC 
# MAGIC ZOrdering maps multidimensional data to one dimension while preserving locality of the data points.
# MAGIC 
# MAGIC Given a column that you want to perform ZORDER on, say `OrderColumn`, Delta
# MAGIC * takes existing parquet files within a partition
# MAGIC * maps the rows within the parquet files according to `OrderColumn` using the algorithm described <a href="https://en.wikipedia.org/wiki/Z-order_curve" target="_blank">here</a>
# MAGIC * (in the case of only one column, the mapping above becomes a linear sort)
# MAGIC * rewrites the sorted data into new parquet files
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You cannot use the partition column also as a ZORDER column.

# COMMAND ----------

# MAGIC %md
# MAGIC #### ZORDER Technical Overview
# MAGIC 
# MAGIC A brief example of how this algorithm works (refer to [this blog](https://databricks.com/blog/2018/07/31/processing-petabytes-of-data-in-seconds-with-databricks-delta.html) for more details):
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/adbcore/zorder.png)
# MAGIC 
# MAGIC Legend:
# MAGIC - Gray dot = data point e.g., chessboard square coordinates
# MAGIC - Gray box = data file; in this example, we aim for files of 4 points each
# MAGIC - Yellow box = data file that’s read for the given query
# MAGIC - Green dot = data point that passes the query’s filter and answers the query
# MAGIC - Red dot = data point that’s read, but doesn’t satisfy the filter; “false positive”

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC 
# MAGIC #### ZORDER example
# MAGIC In the image below, table `Students` has 4 columns:
# MAGIC * `gender` with 2 distinct values
# MAGIC * `Pass-Fail` with 2 distinct values
# MAGIC * `Class` with 4 distinct values
# MAGIC * `Student` with many distinct values
# MAGIC 
# MAGIC Suppose you wish to perform the following query:
# MAGIC 
# MAGIC ```SELECT Name FROM Students WHERE gender = 'M' AND Pass_Fail = 'P' AND Class = 'Junior'```
# MAGIC 
# MAGIC ```ORDER BY Gender, Pass_Fail```
# MAGIC 
# MAGIC The most effective way of performing that search is to order the data starting with the largest set, which is `Gender` in this case.
# MAGIC 
# MAGIC If you're searching for `gender = 'M'`, then you don't even have to look at students with `gender = 'F'`.
# MAGIC 
# MAGIC Note that this technique only works if all `gender = 'M'` values are co-located.
# MAGIC 
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/zorder.png" style="height: 300px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC #### ZORDER usage
# MAGIC 
# MAGIC With Delta Lake the notation is:
# MAGIC 
# MAGIC > `OPTIMIZE Students`<br>
# MAGIC `ZORDER BY Gender, Pass_Fail`
# MAGIC 
# MAGIC This will ensure all the data backing `Gender = 'M' ` is colocated, then data associated with `Pass_Fail = 'P' ` is colocated.
# MAGIC 
# MAGIC See References below for more details on the algorithms behind ZORDER.
# MAGIC 
# MAGIC Using ZORDER, you can order by multiple columns as a comma separated list; however, the effectiveness of locality drops.
# MAGIC 
# MAGIC In streaming, where incoming events are inherently ordered (more or less) by event time, use `ZORDER` to sort by a different column, say 'userID'.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE iot_data
# MAGIC ZORDER by (deviceId)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM iot_data WHERE deviceId=92

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## VACUUM
# MAGIC 
# MAGIC To save on storage costs you should occasionally clean up invalid files using the `VACUUM` command.
# MAGIC 
# MAGIC Invalid files are small files compacted into a larger file with the `OPTIMIZE` command.
# MAGIC 
# MAGIC The  syntax of the `VACUUM` command is
# MAGIC >`VACUUM name-of-table RETAIN number-of HOURS;`
# MAGIC 
# MAGIC The `number-of` parameter is the <b>retention interval</b>, specified in hours.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Databricks does not recommend you set a retention interval shorter than seven days because old snapshots and uncommitted files can still be in use by concurrent readers or writers to the table.
# MAGIC 
# MAGIC The scenario here is:
# MAGIC 0. User A starts a query off uncompacted files, then
# MAGIC 0. User B invokes a `VACUUM` command, which deletes the uncompacted files
# MAGIC 0. User A's query fails because the underlying files have disappeared
# MAGIC 
# MAGIC Invalid files can also result from updates/upserts/deletions.
# MAGIC 
# MAGIC More details are provided here: <a href="https://docs.databricks.com/delta/optimizations.html#garbage-collection" target="_blank"> Garbage Collection</a>.

# COMMAND ----------

len(dbutils.fs.ls(iotPath + "date=2016-07-26"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> In the example below we set off an immediate `VACUUM` operation with an override of the retention check so that all files are cleaned up immediately.
# MAGIC 
# MAGIC Do not do this in production!
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> If using Databricks Runtime 5.1, in order to use a retention time of 0 hours, the following flag must be set.

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC VACUUM iot_data RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Notice how the directory looks vastly cleaned up!

# COMMAND ----------

len(dbutils.fs.ls(iotPath + "date=2016-07-26"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC Delta Lake offers key features that allow for query optimization and garbage collection, resulting in improved performance.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="https://docs.databricks.com/delta/optimizations.html#" target="_blank">Optimizing Performance and Cost</a>
# MAGIC * <a href="http://parquet.apache.org/documentation/latest/" target="_blank">Parquet Metadata</a>
# MAGIC * <a href="https://en.wikipedia.org/wiki/Z-order_curve" target="_blank">Z-Order Curve</a>
