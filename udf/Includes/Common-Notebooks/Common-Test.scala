// Databricks notebook source
// MAGIC %md
// MAGIC # Integration Tests
// MAGIC The purpose of this notebook is to faciliate testing of our systems.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC spark.conf.set("com.databricks.training.module-name", "common-notebooks")
// MAGIC 
// MAGIC val currentVersion = System.getenv().get("DATABRICKS_RUNTIME_VERSION")
// MAGIC println(currentVersion)
// MAGIC 
// MAGIC spark.conf.set("com.databricks.training.expected-dbr", currentVersion)

// COMMAND ----------

// MAGIC %python
// MAGIC import os
// MAGIC 
// MAGIC spark.conf.set("com.databricks.training.module-name", "common-notebooks")
// MAGIC 
// MAGIC currentVersion = os.environ["DATABRICKS_RUNTIME_VERSION"]
// MAGIC print(currentVersion)
// MAGIC 
// MAGIC spark.conf.set("com.databricks.training.expected-dbr", currentVersion)

// COMMAND ----------

// MAGIC %run ./Common

// COMMAND ----------

// MAGIC %scala
// MAGIC allDone(courseAdvertisements)

// COMMAND ----------

// MAGIC %python
// MAGIC allDone(courseAdvertisements)
