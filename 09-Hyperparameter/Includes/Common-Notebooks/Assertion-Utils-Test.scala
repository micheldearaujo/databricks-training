// Databricks notebook source
// MAGIC %md
// MAGIC # Assertion-Utils-Test
// MAGIC The purpose of this notebook is to faciliate testing of assertions.

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

// MAGIC %md # TestSuite

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC println(s"Score:      ${TestResultsAggregator.score}")
// MAGIC println(s"Max Score:  ${TestResultsAggregator.maxScore}")
// MAGIC println(s"Percentage: ${TestResultsAggregator.percentage}")
// MAGIC 
// MAGIC println("-"*80)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC print(f"Score:      {TestResultsAggregator.score}")
// MAGIC print(f"Max Score:  {TestResultsAggregator.maxScore}")
// MAGIC print(f"Percentage: {TestResultsAggregator.percentage}")
// MAGIC 
// MAGIC print("-"*80)
// MAGIC 
// MAGIC # Once accessed, it must be reset
// MAGIC TestResultsAggregator = __TestResultsAggregator()

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val suiteA = TestSuite()
// MAGIC suiteA.test("ScalaTest-1", s"My first scala test",  testFunction = () => true)
// MAGIC suiteA.test("ScalaTest-2", s"My second scala test", testFunction = () => false)
// MAGIC suiteA.test("ScalaTest-3", null,                    testFunction = () => true)
// MAGIC suiteA.test("ScalaTest-4", s"My fourth scala test", testFunction = () => false)
// MAGIC 
// MAGIC for (testResult <- suiteA.testResults) {
// MAGIC   println(s"${testResult.test.id}: ${testResult.status}")
// MAGIC }
// MAGIC println("-"*80)
// MAGIC 
// MAGIC println(s"Score:      ${suiteA.score}")
// MAGIC println(s"Max Score:  ${suiteA.maxScore}")
// MAGIC println(s"Percentage: ${suiteA.percentage}")
// MAGIC 
// MAGIC println("-"*80)
// MAGIC 
// MAGIC assert(suiteA.score == 2, s"A.score: ${suiteA.score}")
// MAGIC assert(suiteA.maxScore == 4, s"A.maxScmaxScoreore: ${suiteA.maxScore}")
// MAGIC assert(suiteA.percentage == 50, s"A.percentage: ${suiteA.percentage}")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC suiteA = TestSuite()
// MAGIC suiteA.test("PythonTest-1", f"My first python test",  lambda: True)
// MAGIC suiteA.test("PythonTest-2", f"My second python test", lambda: False)
// MAGIC suiteA.test("PythonTest-3", None,                     lambda: True)
// MAGIC suiteA.test("PythonTest-4", f"My fourth python test", lambda: False)
// MAGIC 
// MAGIC for testResult in list(suiteA.testResults):
// MAGIC   print(f"{testResult.test.id}: {testResult.status}")
// MAGIC 
// MAGIC print("-"*80)
// MAGIC 
// MAGIC print(f"Score:      {suiteA.score}")
// MAGIC print(f"Max Score:  {suiteA.maxScore}")
// MAGIC print(f"Percentage: {suiteA.percentage}")
// MAGIC 
// MAGIC print("-"*80)
// MAGIC 
// MAGIC assert suiteA.score == 2, f"A.score: {suiteA.score}"
// MAGIC assert suiteA.maxScore == 4, f"A.maxScmaxScoreore: {suiteA.maxScore}"
// MAGIC assert suiteA.percentage == 50, f"A.percentage: {suiteA.percentage}"

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val suiteB = TestSuite()
// MAGIC suiteB.test("ScalaTest-5", "My fifth scala test",   () => true, points=3)
// MAGIC suiteB.testEquals("ScalaTest-6", "My sixth scala test",   "cat", "doc", points=3)
// MAGIC suiteB.testEquals("ScalaTest-7", "My seventh scala test", 99, 100-1,  points=3)
// MAGIC 
// MAGIC suiteB.testContains("List-Pass", "A list contains",          Seq("dog","bird","cat"), "cat",  points=3)
// MAGIC suiteB.testContains("List-Fail", "A list does not contain",  Seq("dog","bird","cat"), "cow",  points=3)
// MAGIC 
// MAGIC suiteB.testFloats("Floats-Pass", "Floats that match",        1.001, 1.002, 0.01,  points=3)
// MAGIC suiteB.testFloats("Floats-Fail", "Floats that do not match", 1.001, 1.002, 0.001, points=3)
// MAGIC 
// MAGIC val dfA = Seq(("Duck", 10000), ("Mouse", 60000)).toDF("LastName", "MaxSalary")
// MAGIC val dfB = Seq(("Duck", 10000), ("Chicken", 60000)).toDF("LastName", "MaxSalary")
// MAGIC val dfC = Seq(("Duck", 10000), ("Mouse", 60000)).toDF("LastName", "MaxSalary")
// MAGIC 
// MAGIC suiteB.testRows("Rows-Pass", "Rows that match",        dfA.collect()(0), dfB.collect()(0) )
// MAGIC suiteB.testRows("Rows-Fail", "Rows that do not match", dfA.collect()(1), dfB.collect()(1) )
// MAGIC 
// MAGIC suiteB.testDataFrames("DF-Pass", "DataFrames that match",        dfA, dfC, true, true)
// MAGIC suiteB.testDataFrames("DF-Fail", "DataFrames that do not match", dfA, dfB, true, true )
// MAGIC 
// MAGIC //////////////////////////////////////////
// MAGIC // Print the reulsts
// MAGIC println("-"*80)
// MAGIC for (testResult <- suiteB.testResults) {
// MAGIC   println(s"${testResult.test.id}: ${testResult.status} (${testResult.points}/${testResult.test.points})")
// MAGIC }
// MAGIC 
// MAGIC println("-"*80)
// MAGIC 
// MAGIC println(s"Score:      ${suiteB.score}")
// MAGIC println(s"Max Score:  ${suiteB.maxScore}")
// MAGIC println(s"Percentage: ${suiteB.percentage}")
// MAGIC 
// MAGIC println("-"*80)
// MAGIC 
// MAGIC assert(suiteB.score == 14, s"B.score: ${suiteB.score}")
// MAGIC assert(suiteB.maxScore == 25, s"B.maxScore: ${suiteB.maxScore}")
// MAGIC assert(suiteB.percentage == 56, s"B.percentage: ${suiteB.percentage}")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC suiteB = TestSuite()
// MAGIC suiteB.test("PythonTest-5", "My fifth python test",   lambda: True,  points=3)
// MAGIC suiteB.testEquals("PythonTest-6", "My sixth scala test", "cat", "doc", points=3)
// MAGIC suiteB.testEquals("PythonTest-7", "My seventh scala test", 99, 100-1,  points=3)
// MAGIC 
// MAGIC suiteB.testContains("List-Pass", "A list contains",          ["dog","bird","cat"], "cat", points=3)
// MAGIC suiteB.testContains("List-Fail", "A list does not contain",  ["dog","bird","cat"], "cow", points=3)
// MAGIC 
// MAGIC suiteB.testFloats("Floats-Pass", "Floats that match",        1.001, 1.002, 0.01,  points=3)
// MAGIC suiteB.testFloats("Floats-Fail", "Floats that do not match", 1.001, 1.002, 0.001, points=3)
// MAGIC 
// MAGIC listA = [("Duck", 10000), ("Mouse", 60000)]
// MAGIC dfA = spark.createDataFrame(listA)
// MAGIC 
// MAGIC listB = [("Duck", 10000), ("Chicken", 60000)]
// MAGIC dfB = spark.createDataFrame(listB)
// MAGIC 
// MAGIC listC = [("Duck", 10000), ("Mouse", 60000)]
// MAGIC dfC = spark.createDataFrame(listC)
// MAGIC 
// MAGIC suiteB.testRows("Rows-Pass", "Rows that match",        dfA.collect()[0], dfB.collect()[0] )
// MAGIC suiteB.testRows("Rows-Fail", "Rows that do not match", dfA.collect()[1], dfB.collect()[1] )
// MAGIC 
// MAGIC suiteB.testDataFrames("DF-Pass", "DataFrames that match",        dfA, dfC, True, True)
// MAGIC suiteB.testDataFrames("DF-Fail", "DataFrames that do not match", dfA, dfB, True, True)
// MAGIC 
// MAGIC ##########################################
// MAGIC # Print the results
// MAGIC for testResult in list(suiteB.testResults):
// MAGIC   print(f"{testResult.test.id}: {testResult.status} ({testResult.points}/{testResult.test.points}): {testResult.exception}")
// MAGIC 
// MAGIC print("-"*80)
// MAGIC 
// MAGIC print(f"Score:      {suiteB.score}")
// MAGIC print(f"Max Score:  {suiteB.maxScore}")
// MAGIC print(f"Percentage: {suiteB.percentage}")
// MAGIC 
// MAGIC print("-"*80)
// MAGIC 
// MAGIC assert suiteB.score == 14, f"B.score: {suiteB.score}"
// MAGIC assert suiteB.maxScore == 25, f"B.maxScore: {suiteB.maxScore}"
// MAGIC assert suiteB.percentage == 56, f"B.percentage: {suiteB.percentage}"

// COMMAND ----------

// MAGIC %scala
// MAGIC val suiteC = TestSuite()
// MAGIC 
// MAGIC println(s"Score:      ${suiteC.score}")
// MAGIC println(s"Max Score:  ${suiteC.maxScore}")
// MAGIC println(s"Percentage: ${suiteC.percentage}")
// MAGIC 
// MAGIC println("-"*80)
// MAGIC 
// MAGIC assert(suiteC.score == 0, s"C.score: ${suiteC.score}")
// MAGIC assert(suiteC.maxScore == 0, s"C.maxScore: ${suiteC.maxScore}")
// MAGIC assert(suiteC.percentage == 0, s"C.percentage: ${suiteC.percentage}")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC suiteC = TestSuite()
// MAGIC 
// MAGIC print(f"Score:      {suiteC.score}")
// MAGIC print(f"Max Score:  {suiteC.maxScore}")
// MAGIC print(f"Percentage: {suiteC.percentage}")
// MAGIC 
// MAGIC print("-"*80)
// MAGIC 
// MAGIC assert suiteC.score == 0, f"C.score: {suiteC.score}"
// MAGIC assert suiteC.maxScore == 0, f"C.maxScore: {suiteC.maxScore}"
// MAGIC assert suiteC.percentage == 0, f"C.percentage: {suiteC.percentage}"

// COMMAND ----------

// MAGIC %scala
// MAGIC suiteA.displayResults()

// COMMAND ----------

// MAGIC %python
// MAGIC suiteA.displayResults()

// COMMAND ----------

// MAGIC %scala
// MAGIC suiteB.displayResults()

// COMMAND ----------

// MAGIC %python
// MAGIC suiteB.displayResults()

// COMMAND ----------

// MAGIC %scala
// MAGIC suiteC.displayResults()

// COMMAND ----------

// MAGIC %python
// MAGIC suiteC.displayResults()

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC for ( (key, testResult) <- TestResultsAggregator.testResults) {
// MAGIC   println(s"${testResult.test.id}: ${testResult.status} (${testResult.points})")
// MAGIC }
// MAGIC println("-"*80)
// MAGIC 
// MAGIC println(s"Score:      ${TestResultsAggregator.score}")
// MAGIC println(s"Max Score:  ${TestResultsAggregator.maxScore}")
// MAGIC println(s"Percentage: ${TestResultsAggregator.percentage}")
// MAGIC 
// MAGIC println("-"*80)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC for key in TestResultsAggregator.testResults:
// MAGIC   testResult = TestResultsAggregator.testResults[key]
// MAGIC   print(f"""{testResult.test.id}: {testResult.status} ({testResult.points})""")
// MAGIC 
// MAGIC print("-"*80)
// MAGIC 
// MAGIC print(f"Score:      {TestResultsAggregator.score}")
// MAGIC print(f"Max Score:  {TestResultsAggregator.maxScore}")
// MAGIC print(f"Percentage: {TestResultsAggregator.percentage}")
// MAGIC 
// MAGIC print("-"*80)

// COMMAND ----------

// MAGIC %scala
// MAGIC TestResultsAggregator.displayResults

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC TestResultsAggregator.displayResults()

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC assert(TestResultsAggregator.score == 16, s"TR.score: ${TestResultsAggregator.score}")
// MAGIC assert(TestResultsAggregator.maxScore == 29, s"TR.maxScore: ${TestResultsAggregator.maxScore}")
// MAGIC assert(TestResultsAggregator.percentage == 55, s"TR.percentage: ${TestResultsAggregator.percentage}")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC assert TestResultsAggregator.score == 16, f"TR.score: {TestResultsAggregator.score}"
// MAGIC assert TestResultsAggregator.maxScore == 29, f"TR.maxScore: {TestResultsAggregator.maxScore}"
// MAGIC assert TestResultsAggregator.percentage == 55, f"TR.percentage: {TestResultsAggregator.percentage}"

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val suiteX = TestSuite()
// MAGIC 
// MAGIC try {
// MAGIC   suiteX.test(null, null, () => true)
// MAGIC   throw new Exception("Expexted an IllegalArgumentException")
// MAGIC } catch {
// MAGIC   case e:IllegalArgumentException => ()
// MAGIC }
// MAGIC 
// MAGIC try {
// MAGIC   suiteX.test("abc", null, () => true)
// MAGIC   suiteX.test("abc", null, () => true)
// MAGIC   throw new Exception("Expexted an IllegalArgumentException")
// MAGIC } catch {
// MAGIC   case e:IllegalArgumentException => ()
// MAGIC }

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC suiteX = TestSuite()
// MAGIC 
// MAGIC try:
// MAGIC   suiteX.test(None, None, lambda:True)
// MAGIC   raise Exception("Expexted a value error")
// MAGIC except ValueError:
// MAGIC   pass
// MAGIC 
// MAGIC try:
// MAGIC   suiteX.test("abc", None, lambda:True)
// MAGIC   suiteX.test("abc", None, lambda:True)
// MAGIC   raise Exception("Expected a value error")
// MAGIC except ValueError:
// MAGIC   pass

// COMMAND ----------

// MAGIC %md # dbTest()

// COMMAND ----------

// MAGIC %scala
// MAGIC dbTest("ScalaTest-1", "cat", "cat")
// MAGIC 
// MAGIC try {
// MAGIC   dbTest("ScalaTest-2", "cat", "dog")  
// MAGIC } catch {
// MAGIC   case _: AssertionError => ()
// MAGIC }
// MAGIC // dbTest("ScalaTest-3", 999, 666)  

// COMMAND ----------

// MAGIC %python
// MAGIC dbTest("PythonTest-1", "cat", "cat")
// MAGIC 
// MAGIC try:
// MAGIC   dbTest("PythonTest-2", "cat", "dog")
// MAGIC except AssertionError:
// MAGIC   pass
// MAGIC 
// MAGIC # dbTest("PythonTest-3", 999, 666)

// COMMAND ----------

// MAGIC %md ## Testing compareFloats
// MAGIC 
// MAGIC ```compareFloats(floatA, floatB, tolerance)```

// COMMAND ----------

// MAGIC %scala
// MAGIC assert(compareFloats(1, 1.toInt) == true)
// MAGIC 
// MAGIC assert(compareFloats(100.001, 100.toInt) == true)
// MAGIC assert(compareFloats(100.001, 100.toLong) == true)
// MAGIC assert(compareFloats(100.001, 100.toFloat) == true)
// MAGIC assert(compareFloats(100.001, 100.toDouble) == true)
// MAGIC 
// MAGIC assert(compareFloats(100.toInt, 100.001) == true)
// MAGIC assert(compareFloats(100.toLong, 100.001) == true)
// MAGIC assert(compareFloats(100.toFloat, 100.001) == true)
// MAGIC assert(compareFloats(100.toDouble, 100.001) == true)
// MAGIC 
// MAGIC assert(compareFloats(100.001, "blah") == false)
// MAGIC assert(compareFloats("blah", 100.001) == false)
// MAGIC 
// MAGIC assert(compareFloats(1.0, 1.0) == true)
// MAGIC 
// MAGIC assert(compareFloats(1.0, 1.2, .0001) == false)
// MAGIC assert(compareFloats(1.0, 1.02, .0001) == false)
// MAGIC assert(compareFloats(1.0, 1.002, .0001) == false)
// MAGIC assert(compareFloats(1.0, 1.0002, .0001) == false)
// MAGIC assert(compareFloats(1.0, 1.00002, .0001) == true)
// MAGIC assert(compareFloats(1.0, 1.000002, .0001) == true)
// MAGIC 
// MAGIC assert(compareFloats(1.2, 1.0, .0001) == false)
// MAGIC assert(compareFloats(1.02, 1.0, .0001) == false)
// MAGIC assert(compareFloats(1.002, 1.0, .0001) == false)
// MAGIC assert(compareFloats(1.0002, 1.0, .0001) == false)
// MAGIC assert(compareFloats(1.00002, 1.0, .0001) == true)
// MAGIC assert(compareFloats(1.000002, 1.0, .0001) == true)
// MAGIC 
// MAGIC assert(compareFloats(1, null) == false)
// MAGIC assert(compareFloats(null, 1) == false)
// MAGIC assert(compareFloats(null, null) == true)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC assert compareFloats(1, 1) == True
// MAGIC 
// MAGIC # No long or double equivilent to Scala
// MAGIC assert compareFloats(100.001, int(100)) == True
// MAGIC assert compareFloats(100.001, float(100)) == True
// MAGIC 
// MAGIC assert compareFloats(int(100), 100.001) == True
// MAGIC assert compareFloats(float(100), 100.001) == True
// MAGIC 
// MAGIC assert compareFloats(100.001, "blah") == False
// MAGIC assert compareFloats("blah", 100.001) == False
// MAGIC 
// MAGIC assert compareFloats(1.0, 1.0) == True
// MAGIC 
// MAGIC assert compareFloats(1.0, 1.2, .0001) == False
// MAGIC assert compareFloats(1.0, 1.02, .0001) == False
// MAGIC assert compareFloats(1.0, 1.002, .0001) == False
// MAGIC assert compareFloats(1.0, 1.0002, .0001) == False
// MAGIC assert compareFloats(1.0, 1.00002, .0001) == True
// MAGIC assert compareFloats(1.0, 1.000002, .0001) == True
// MAGIC 
// MAGIC assert compareFloats(1.2, 1.0, .0001) == False
// MAGIC assert compareFloats(1.02, 1.0, .0001) == False
// MAGIC assert compareFloats(1.002, 1.0, .0001) == False
// MAGIC assert compareFloats(1.0002, 1.0, .0001) == False
// MAGIC assert compareFloats(1.00002, 1.0, .0001) == True
// MAGIC assert compareFloats(1.000002, 1.0, .0001) == True
// MAGIC 
// MAGIC assert compareFloats(1, None) == False
// MAGIC assert compareFloats(None, 1) == False
// MAGIC assert compareFloats(None, None) == True

// COMMAND ----------

// MAGIC %md ## Testing compareSchemas
// MAGIC 
// MAGIC ```compareSchemas(schemaA, schemaB, testColumnOrder=True, testNullable=False)```

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.types._ 
// MAGIC val _expectedSchema = StructType(List(
// MAGIC   StructField("LastName", StringType, true),
// MAGIC   StructField("MaxSalary", DoubleType, true)
// MAGIC   ))
// MAGIC 
// MAGIC val _studentSchema1 = StructType(List(
// MAGIC   StructField("MaxSalary", DoubleType, true),
// MAGIC   StructField("LastName", StringType, true)
// MAGIC   ))
// MAGIC 
// MAGIC val _studentSchema2 = StructType(List(
// MAGIC   StructField("LastName", StringType, true),
// MAGIC   StructField("MaxSalary", BooleanType, true)
// MAGIC   ))
// MAGIC 
// MAGIC val _studentSchema3 = StructType(List(
// MAGIC   StructField("LastName", StringType, false),
// MAGIC   StructField("MaxSalary", DoubleType, true)
// MAGIC ))
// MAGIC 
// MAGIC val _studentSchema4 = StructType(List(
// MAGIC   StructField("LastName", StringType, true),
// MAGIC   StructField("MaxSalary", DoubleType, true),
// MAGIC   StructField("Country", StringType, true)
// MAGIC   ))
// MAGIC 
// MAGIC assert(compareSchemas(_expectedSchema, _studentSchema1, testColumnOrder=false, testNullable=true) == true,  "out of order, ignore order")
// MAGIC assert(compareSchemas(_expectedSchema, _studentSchema1, testColumnOrder=true, testNullable=true) == false,  "out of order, preserve order")
// MAGIC assert(compareSchemas(_expectedSchema, _studentSchema2, testColumnOrder=false, testNullable=true) == false, "different types")
// MAGIC 
// MAGIC assert(compareSchemas(_expectedSchema, _studentSchema3, testColumnOrder=true, testNullable=true) == false,  "check nullable")  
// MAGIC assert(compareSchemas(_expectedSchema, _studentSchema3, testColumnOrder=true, testNullable=false) == true,  "don't check nullable")
// MAGIC 
// MAGIC assert(compareSchemas(_expectedSchema, _studentSchema4, testColumnOrder=true, testNullable=true) == false,  "left side < right size")
// MAGIC assert(compareSchemas(_studentSchema4, _expectedSchema, testColumnOrder=true, testNullable=true) == false,  "left side > right side")
// MAGIC 
// MAGIC assert(compareSchemas(null,            _studentSchema3, testColumnOrder=true, testNullable=true) == false, "Null schemaA")
// MAGIC assert(compareSchemas(null,            null,            testColumnOrder=true, testNullable=true) == true,  "Null schemaA and schemaB")
// MAGIC assert(compareSchemas(_studentSchema2, null,            testColumnOrder=true, testNullable=true) == false, "Null schemaB")

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.types import *
// MAGIC _expectedSchema = StructType([
// MAGIC   StructField("LastName", StringType(), True),
// MAGIC   StructField("MaxSalary", DoubleType(), True)
// MAGIC ])
// MAGIC 
// MAGIC _studentSchema1 = StructType([
// MAGIC   StructField("MaxSalary", DoubleType(), True),
// MAGIC   StructField("LastName", StringType(), True)
// MAGIC ])
// MAGIC 
// MAGIC _studentSchema2 = StructType([
// MAGIC   StructField("LastName", StringType(), True),
// MAGIC   StructField("MaxSalary", BooleanType(), True)
// MAGIC ])
// MAGIC 
// MAGIC _studentSchema3 = StructType([
// MAGIC   StructField("LastName", StringType(), False),
// MAGIC   StructField("MaxSalary", DoubleType(), True)
// MAGIC ])
// MAGIC 
// MAGIC _studentSchema4 = StructType([
// MAGIC   StructField("LastName", StringType(), True),
// MAGIC   StructField("MaxSalary", DoubleType(), True),
// MAGIC   StructField("Country", StringType(), True)
// MAGIC ])
// MAGIC 
// MAGIC assert compareSchemas(_expectedSchema, _studentSchema1, testColumnOrder=False, testNullable=True) == True, "out of order, ignore order"
// MAGIC assert compareSchemas(_expectedSchema, _studentSchema1, testColumnOrder=True, testNullable=True) == False, "out of order, preserve order"
// MAGIC 
// MAGIC assert compareSchemas(_expectedSchema, _studentSchema2, testColumnOrder=True, testNullable=True) == False, "different types"
// MAGIC 
// MAGIC assert compareSchemas(_expectedSchema, _studentSchema3, testColumnOrder=True, testNullable=True) == False, "check nullable"
// MAGIC assert compareSchemas(_expectedSchema, _studentSchema3, testColumnOrder=True, testNullable=False) == True, "don't check nullable"
// MAGIC 
// MAGIC assert compareSchemas(_expectedSchema, _studentSchema4, testColumnOrder=True, testNullable=True) == False, "left side < right size"
// MAGIC assert compareSchemas(_studentSchema4, _expectedSchema, testColumnOrder=True, testNullable=True) == False, "left side > right side"
// MAGIC 
// MAGIC assert compareSchemas(None,            _studentSchema3, testColumnOrder=True, testNullable=False) == False, "Null schemaA"
// MAGIC assert compareSchemas(None,            None,            testColumnOrder=True, testNullable=False) == True,  "Null schemaA and schemaB"
// MAGIC assert compareSchemas(_studentSchema2, None,            testColumnOrder=True, testNullable=False) == False, "Null schemaB"

// COMMAND ----------

// MAGIC %md ## Testing compareRows
// MAGIC 
// MAGIC ```compareRows(rowA, rowB)```

// COMMAND ----------

val bla = Row("Duck", 10000.0)
bla.schema

// COMMAND ----------

// MAGIC %scala
// MAGIC val df1 = Seq(("Duck", 10000.0), ("Mouse", 60000.0)).toDF("LastName", "MaxSalary")
// MAGIC val _rowA = df1.collect()(0)
// MAGIC val _rowB = df1.collect()(1)
// MAGIC 
// MAGIC val df2 = Seq((10000.0, "Duck")).toDF("MaxSalary", "LastName")
// MAGIC val _rowC = df2.collect()(0)
// MAGIC 
// MAGIC val df3 = Seq(("Duck", 10000.0, "Anaheim")).toDF("LastName", "MaxSalary", "City")
// MAGIC val _rowD = df3.collect()(0)
// MAGIC 
// MAGIC // no schema!
// MAGIC val _rowE =  Row("Duck", 10000.0)
// MAGIC val _rowF =  Row("Mouse", 60000.0)
// MAGIC 
// MAGIC assert(compareRows(_rowA, _rowB) == false)                   // different schemas
// MAGIC assert(compareRows(_rowA, _rowA) == true)                    // compare to self
// MAGIC assert(compareRows(_rowA, _rowC) == true)                    // order reversed
// MAGIC assert(compareRows(null, _rowA) == false, "null _rowA")      // Null rowA
// MAGIC assert(compareRows(null, null) == true, "null _rowA, rowB")  // Null rowA and rowB
// MAGIC assert(compareRows(_rowA, null) == false, "null _rowB")      // Null rowB
// MAGIC assert(compareRows(_rowA, _rowD) == false)                   // _rowA smaller than _rowD
// MAGIC assert(compareRows(_rowD, _rowA) == false)                   // _rowD smaller than _rowA 
// MAGIC assert(compareRows(_rowE, _rowF) == false)                   // no schemas, different
// MAGIC assert(compareRows(_rowE, _rowE) == true)                    // no schemas, same
// MAGIC assert(compareRows(_rowE, _rowA) == true)                    // no schemas / schema, same content
// MAGIC assert(compareRows(_rowE, _rowB) == false)                    // no schemas / schema, different content

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql import Row
// MAGIC _rowA = Row(LastName="Duck", MaxSalary=10000)
// MAGIC _rowB = Row(LastName="Mouse", MaxSalary=60000)
// MAGIC _rowC = Row(MaxSalary=10000, LastName="Duck")
// MAGIC _rowD = Row(LastName="Duck", MaxSalary=10000, City="Anaheim")
// MAGIC 
// MAGIC assert compareRows(_rowA, _rowB) == False       # different schemas
// MAGIC assert compareRows(_rowA, _rowA) == True        # compare to self
// MAGIC assert compareRows(_rowA, _rowC) == True        # order reversed
// MAGIC assert compareRows(None, _rowA) == False        # Null rowA
// MAGIC assert compareRows(None, None) == True          # Null rowA and rowB
// MAGIC assert compareRows(_rowA, None) == False        # Null rowB
// MAGIC assert compareRows(_rowA, _rowD) == False       # _rowA smaller than _rowD
// MAGIC assert compareRows(_rowD, _rowA) == False       # _rowD bigger than _rowA
// MAGIC 
// MAGIC # note Python doesn't allow you to define rows without a schema
// MAGIC # _rowE = Row("Duck", 10000)
