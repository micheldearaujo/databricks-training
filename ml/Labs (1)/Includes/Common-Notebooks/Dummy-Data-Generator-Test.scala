// Databricks notebook source
// MAGIC %md
// MAGIC # Dummy-Data-Generator-Test
// MAGIC The purpose of this notebook is to faciliate testing of the dummy data generator.

// COMMAND ----------

// MAGIC %scala
// MAGIC spark.conf.set("com.databricks.training.module-name", "common-notebooks")

// COMMAND ----------

// MAGIC %python
// MAGIC spark.conf.set("com.databricks.training.module-name", "common-notebooks")

// COMMAND ----------

// MAGIC %run ./Class-Utility-Methods

// COMMAND ----------

// MAGIC %run ./Assertion-Utils

// COMMAND ----------

// MAGIC %scala
// MAGIC val courseType = "test"
// MAGIC val moduleName = getModuleName()
// MAGIC val lessonName = getLessonName()
// MAGIC val username = getUsername()
// MAGIC val userhome = getUserhome()
// MAGIC val workingDir = getWorkingDir(courseType)
// MAGIC val databaseName = createUserDatabase(courseType, username, moduleName, lessonName)

// COMMAND ----------

// MAGIC %python
// MAGIC courseType = "test"
// MAGIC moduleName = getModuleName()
// MAGIC lessonName = getLessonName()
// MAGIC username = getUsername()
// MAGIC userhome = getUserhome()
// MAGIC workingDir = getWorkingDir(courseType)
// MAGIC databaseName = createUserDatabase(courseType, username, moduleName, lessonName)

// COMMAND ----------

// MAGIC %run ./Dummy-Data-Generator

// COMMAND ----------

// MAGIC %scala
// MAGIC // Initialize the username
// MAGIC spark.conf.set("com.databricks.training.username", "test")
// MAGIC spark.conf.set("com.databricks.training.userhome", "dbfs:/user/test")

// COMMAND ----------

// MAGIC %python
// MAGIC # name: String = "Name", 
// MAGIC # id: String = "ID", 
// MAGIC # password: String = "Password", 
// MAGIC # amount: String = "Amount", 
// MAGIC # percent: String = "Percent", 
// MAGIC # probability: String = "Probability", 
// MAGIC # yesNo: String = "YesNo", 
// MAGIC # UTCTime: String = "UTCTime"
// MAGIC from pyspark.sql import Row
// MAGIC from pyspark.sql.types import *
// MAGIC import os
// MAGIC 
// MAGIC # default rows
// MAGIC testDefaultRowsDF = DummyData("test_1_python").toDF()
// MAGIC assert testDefaultRowsDF.count() == 300
// MAGIC 
// MAGIC # custom rows
// MAGIC testCustomRowsDF = DummyData("test_2_python", numRows=50).toDF()
// MAGIC assert testCustomRowsDF.count() == 50
// MAGIC 
// MAGIC # custom field names
// MAGIC testCustomField1DF = DummyData("test_3_python").addNames("EmployeeName").toDF()
// MAGIC fields1 = testCustomField1DF.schema.names 
// MAGIC assert "EmployeeName" in fields1
// MAGIC assert testCustomField1DF.schema.fields[1].dataType == StringType()
// MAGIC 
// MAGIC testCustomField2DF = DummyData("test_4_python").renameId("EmployeeID").toDF()
// MAGIC fields2 = testCustomField2DF.schema.names 
// MAGIC assert "EmployeeID" in fields2
// MAGIC assert testCustomField2DF.schema.fields[0].dataType == LongType()
// MAGIC 
// MAGIC testCustomField3DF = DummyData("test_5_python").addPasswords("Yak").toDF()
// MAGIC fields3 = testCustomField3DF.schema.names
// MAGIC assert testCustomField3DF.schema.fields[1].dataType == StringType()
// MAGIC assert "Yak" in fields3
// MAGIC 
// MAGIC testCustomField4DF = DummyData("test_6_python").addDoubles("Salary").toDF()
// MAGIC fields4 = testCustomField4DF.schema.names 
// MAGIC assert testCustomField4DF.schema.fields[1].dataType == DoubleType()
// MAGIC assert "Salary" in fields4
// MAGIC 
// MAGIC testCustomField5DF = DummyData("test_7_python").addIntegers("ZeroTo100").toDF()
// MAGIC fields5 = testCustomField5DF.schema.names 
// MAGIC assert testCustomField5DF.schema.fields[1].dataType == IntegerType()
// MAGIC assert "ZeroTo100" in fields5
// MAGIC 
// MAGIC testCustomField6DF = DummyData("test_8_python").addProportions("Prob").toDF()
// MAGIC fields6 = testCustomField6DF.schema.names 
// MAGIC assert testCustomField6DF.schema.fields[1].dataType == DoubleType()
// MAGIC assert "Prob" in fields6 
// MAGIC 
// MAGIC testCustomField7DF = DummyData("test_9_python").addBooleans("isOK").toDF()
// MAGIC fields7 = testCustomField7DF.schema.names 
// MAGIC assert testCustomField7DF.schema.fields[1].dataType == BooleanType()
// MAGIC assert "isOK" in fields7
// MAGIC 
// MAGIC testCustomField8DF = DummyData("test_10_python").addTimestamps("Timestamp").toDF()
// MAGIC fields8 = testCustomField8DF.schema.names 
// MAGIC assert testCustomField8DF.schema.fields[1].dataType == IntegerType()
// MAGIC assert "Timestamp" in fields8
// MAGIC 
// MAGIC # all custome field names, custom rows
// MAGIC testCustomFieldsDF = (DummyData("test_11_python", numRows=50)
// MAGIC     .addNames("EmployeeName")
// MAGIC     .renameId("EmployeeID")
// MAGIC     .addPasswords("Yak")
// MAGIC     .addDoubles("Salary")
// MAGIC     .addIntegers("ZeroTo100")
// MAGIC     .addProportions("Prob")
// MAGIC     .addBooleans("isOK")
// MAGIC     .addTimestamps("Timestamp")
// MAGIC     .toDF())
// MAGIC fields = testCustomFieldsDF.schema.names
// MAGIC assert fields == ['EmployeeID', 'EmployeeName', 'Yak', 'Salary', 'ZeroTo100', 'Prob', 'isOK', 'Timestamp']
// MAGIC assert testCustomFieldsDF.count() == 50
// MAGIC assert testCustomFieldsDF.schema.fields[0].dataType == LongType()
// MAGIC assert testCustomFieldsDF.schema.fields[1].dataType == StringType()
// MAGIC assert testCustomFieldsDF.schema.fields[2].dataType == StringType()
// MAGIC assert testCustomFieldsDF.schema.fields[3].dataType == DoubleType()
// MAGIC assert testCustomFieldsDF.schema.fields[4].dataType == IntegerType()
// MAGIC assert testCustomFieldsDF.schema.fields[5].dataType == DoubleType()
// MAGIC assert testCustomFieldsDF.schema.fields[6].dataType == BooleanType()
// MAGIC assert testCustomFieldsDF.schema.fields[7].dataType == IntegerType()
// MAGIC 
// MAGIC # needs to be done: restructure testing format to match class-utility-methods
// MAGIC # needs to be done: add unit testing for the DummyData.add*() methods and their parameters
// MAGIC # needs to be done: add value testing

// COMMAND ----------

// MAGIC %scala
// MAGIC // name: String = "Name", 
// MAGIC // id: String = "ID", 
// MAGIC // password: String = "Password", 
// MAGIC // amount: String = "Amount", 
// MAGIC // percent: String = "Percent", 
// MAGIC // probability: String = "Probability", 
// MAGIC // yesNo: String = "YesNo", 
// MAGIC // UTCTime: String = "UTCTime"
// MAGIC import org.apache.spark.sql.types.{StringType, DoubleType, IntegerType, BooleanType, LongType}
// MAGIC 
// MAGIC // default rows
// MAGIC val testDefaultRowsDF = new DummyData("test_1_scala").toDF
// MAGIC assert(testDefaultRowsDF.count == 300)
// MAGIC 
// MAGIC // custom rows
// MAGIC val testCustomRowsDF = new DummyData("test_2_scala", numRows=50).toDF
// MAGIC assert(testCustomRowsDF.count == 50)
// MAGIC 
// MAGIC // custom field names
// MAGIC val testCustomField1DF = new DummyData("test_3_scala").addNames("EmployeeName").toDF
// MAGIC val fields1 = testCustomField1DF.schema.names 
// MAGIC assert(fields1.contains("EmployeeName"))
// MAGIC assert(testCustomField1DF.schema.fields(1).dataType == StringType)
// MAGIC 
// MAGIC val testCustomField2DF = new DummyData("test_4_scala").renameId("EmployeeID").toDF
// MAGIC val fields2 = testCustomField2DF.schema.names 
// MAGIC assert(fields2.contains("EmployeeID"))
// MAGIC assert(testCustomField2DF.schema.fields(0).dataType == LongType)
// MAGIC 
// MAGIC val testCustomField3DF = new DummyData("test_5_scala").addPasswords("Yak").toDF
// MAGIC val fields3 = testCustomField3DF.schema.names
// MAGIC assert(testCustomField3DF.schema.fields(1).dataType == StringType)
// MAGIC assert(fields3.contains("Yak"))
// MAGIC 
// MAGIC val testCustomField4DF = new DummyData("test_6_scala").addDoubles("Salary").toDF
// MAGIC val fields4 = testCustomField4DF.schema.names 
// MAGIC assert(testCustomField4DF.schema.fields(1).dataType == DoubleType)
// MAGIC assert(fields4.contains("Salary"))
// MAGIC 
// MAGIC val testCustomField5DF = new DummyData("test_7_scala").addIntegers("ZeroTo100").toDF
// MAGIC val fields5 = testCustomField5DF.schema.names 
// MAGIC assert(testCustomField5DF.schema.fields(1).dataType == IntegerType)
// MAGIC assert(fields5.contains("ZeroTo100"))
// MAGIC 
// MAGIC val testCustomField6DF = new DummyData("test_8_scala").addProportions("Prob").toDF
// MAGIC val fields6 = testCustomField6DF.schema.names 
// MAGIC assert(testCustomField6DF.schema.fields(1).dataType == DoubleType)
// MAGIC assert(fields6.contains("Prob")) 
// MAGIC 
// MAGIC val testCustomField7DF = new DummyData("test_9_scala").addBooleans("isOK").toDF
// MAGIC val fields7 = testCustomField7DF.schema.names 
// MAGIC assert(testCustomField7DF.schema.fields(1).dataType == BooleanType)
// MAGIC assert(fields7.contains("isOK"))
// MAGIC 
// MAGIC val testCustomField8DF = new DummyData("test_10_scala").addTimestamps("Timestamp").toDF
// MAGIC val fields8 = testCustomField8DF.schema.names 
// MAGIC assert(testCustomField8DF.schema.fields(1).dataType == IntegerType)
// MAGIC assert(fields8.contains("Timestamp"))
// MAGIC 
// MAGIC // all custome field names, custom rows
// MAGIC val testCustomFieldsDF = new DummyData("test_11_scala", numRows=50)
// MAGIC     .addNames("EmployeeName")
// MAGIC     .renameId("EmployeeID")
// MAGIC     .addPasswords("Yak")
// MAGIC     .addDoubles("Salary")
// MAGIC     .addIntegers("ZeroTo100")
// MAGIC     .addProportions("Prob")
// MAGIC     .addBooleans("isOK")
// MAGIC     .addTimestamps("Timestamp")
// MAGIC     .toDF
// MAGIC val fields = testCustomFieldsDF.schema.names
// MAGIC assert(fields.deep == Array("EmployeeID", "EmployeeName", "Yak", "Salary", "ZeroTo100", "Prob", "isOK", "Timestamp").deep)
// MAGIC assert(testCustomFieldsDF.count == 50)
// MAGIC assert(testCustomFieldsDF.schema.fields(0).dataType == LongType)
// MAGIC assert(testCustomFieldsDF.schema.fields(1).dataType == StringType)
// MAGIC assert(testCustomFieldsDF.schema.fields(2).dataType == StringType)
// MAGIC assert(testCustomFieldsDF.schema.fields(3).dataType == DoubleType)
// MAGIC assert(testCustomFieldsDF.schema.fields(4).dataType == IntegerType)
// MAGIC assert(testCustomFieldsDF.schema.fields(5).dataType == DoubleType)
// MAGIC assert(testCustomFieldsDF.schema.fields(6).dataType == BooleanType)
// MAGIC assert(testCustomFieldsDF.schema.fields(7).dataType == IntegerType)
// MAGIC 
// MAGIC // needs to be done: restructure testing format to match class-utility-methods
// MAGIC // needs to be done: add unit testing for the DummyData.add*() methods and their parameters
// MAGIC // needs to be done: add value testing

// COMMAND ----------

// MAGIC %scala
// MAGIC spark.sql(s"DROP DATABASE IF EXISTS $databaseName CASCADE")

// COMMAND ----------

// MAGIC %python
// MAGIC spark.sql(f"DROP DATABASE IF EXISTS {databaseName} CASCADE")

// COMMAND ----------


