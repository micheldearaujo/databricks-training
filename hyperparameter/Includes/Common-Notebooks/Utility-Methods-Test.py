# Databricks notebook source
# MAGIC %scala
# MAGIC spark.conf.set("com.databricks.training.module-name", "common-notebooks")
# MAGIC 
# MAGIC val courseAdvertisements = scala.collection.mutable.Map[String,(String,String,String)]()

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.set("com.databricks.training.module-name", "common-notebooks")
# MAGIC 
# MAGIC courseAdvertisements = dict()

# COMMAND ----------

# MAGIC %run ./Utility-Methods

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import scala.collection.mutable.ArrayBuffer
# MAGIC val scalaTests = ArrayBuffer.empty[Boolean]
# MAGIC def functionPassed(result: Boolean) = {
# MAGIC   if (result) {
# MAGIC     scalaTests += true
# MAGIC   } else {
# MAGIC     scalaTests += false
# MAGIC   } 
# MAGIC }

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC pythonTests = []
# MAGIC def functionPassed(result):
# MAGIC   if result:
# MAGIC     pythonTests.append(True)
# MAGIC   else:
# MAGIC     pythonTests.append(False)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `printRecordsPerPartition`

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def testPrintRecordsPerPartition(): Boolean = {
# MAGIC   
# MAGIC   // Import Data
# MAGIC   val peopleDF = spark.read.parquet("/mnt/training/dataframes/people-10m.parquet")
# MAGIC   
# MAGIC   // Get printed results
# MAGIC   import java.io.ByteArrayOutputStream
# MAGIC   val printedOutput = new ByteArrayOutputStream
# MAGIC   Console.withOut(printedOutput) {printRecordsPerPartition(peopleDF)}
# MAGIC   
# MAGIC   // Setup tests
# MAGIC   import scala.collection.mutable.ArrayBuffer
# MAGIC   val testsPassed = ArrayBuffer.empty[Boolean]
# MAGIC   
# MAGIC   def passedTest(result: Boolean, message: String = null) = {
# MAGIC     if (result) {
# MAGIC       testsPassed += true
# MAGIC     } else {
# MAGIC       testsPassed += false
# MAGIC       println(s"Failed Test: $message")
# MAGIC     } 
# MAGIC   }
# MAGIC   
# MAGIC   
# MAGIC   // Test if correct number of partitions are printing
# MAGIC   try {
# MAGIC     assert(
# MAGIC       (for (element <- printedOutput.toString.split("\n") if element.startsWith("#")) yield element).size == peopleDF.rdd.getNumPartitions
# MAGIC     )
# MAGIC     passedTest(true)
# MAGIC   } catch {
# MAGIC     case a: AssertionError => {
# MAGIC       passedTest(false, "The correct number of partitions were not identified for printRecordsPerPartition")
# MAGIC     }
# MAGIC     case _: Exception => {
# MAGIC       passedTest(false, "non-descript error for the correct number of partitions were not identified for printRecordsPerPartition")
# MAGIC     }
# MAGIC   }
# MAGIC   
# MAGIC   // Test if each printed partition has a record number associated
# MAGIC   try {
# MAGIC     val recordMap = for (element <- printedOutput.toString.split("\n") if element.startsWith("#")) 
# MAGIC       yield Map(
# MAGIC         element.slice(element.indexOf("#") + 1, element.indexOf(":") - element.indexOf("#")) -> element.split(" ")(1).replace(",", "").toInt
# MAGIC       )
# MAGIC     val recordCounts = (for (map <- recordMap if map.keySet.toSeq(0).toInt.isInstanceOf[Int]) yield map.getOrElse(map.keySet.toSeq(0), -1))
# MAGIC     recordCounts.foreach(x => assert(x.isInstanceOf[Int]))
# MAGIC     recordCounts.foreach(x => assert(x != -1))
# MAGIC     passedTest(true)
# MAGIC   } catch {
# MAGIC     case a: AssertionError => {
# MAGIC       passedTest(false, "Not every partition has an associated record count")
# MAGIC     }
# MAGIC     case _: Exception => {
# MAGIC       passedTest(false, "non-descript error for not every partition having an associated record count")
# MAGIC     }
# MAGIC   }
# MAGIC   
# MAGIC   // Test if the sum of the printed number of records per partition equals the total number of records
# MAGIC   try {
# MAGIC     val printedSum = (
# MAGIC       for (element <- printedOutput.toString.split("\n") if element.startsWith("#")) yield element.split(" ")(1).replace(",", "").toInt
# MAGIC     ).sum
# MAGIC     assert(printedSum == peopleDF.count)
# MAGIC     passedTest(true)
# MAGIC   } catch {
# MAGIC     case a: AssertionError => {
# MAGIC       passedTest(false, "The sum of the number of records per partition does not match the total number of records")
# MAGIC     }
# MAGIC     case _: Exception => {
# MAGIC       passedTest(false, "non-descript error for the sum of the number of records per partition not matching the total number of records")
# MAGIC     }
# MAGIC   }
# MAGIC   
# MAGIC 
# MAGIC   val numTestsPassed = testsPassed.groupBy(identity).mapValues(_.size)(true)
# MAGIC   if (numTestsPassed == testsPassed.length) {
# MAGIC     println(s"All $numTestsPassed tests for printRecordsPerPartition passed")
# MAGIC     true
# MAGIC   } else {
# MAGIC     println(s"$numTestsPassed of ${testsPassed.length} tests for printRecordsPerPartition passed")
# MAGIC     false
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC functionPassed(testPrintRecordsPerPartition())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def testPrintRecordsPerPartition():
# MAGIC   
# MAGIC     # Import data
# MAGIC     peopleDF = spark.read.parquet("/mnt/training/dataframes/people-10m.parquet")
# MAGIC     
# MAGIC     # Get printed results
# MAGIC     import io
# MAGIC     from contextlib import redirect_stdout
# MAGIC 
# MAGIC     f = io.StringIO()
# MAGIC     with redirect_stdout(f):
# MAGIC         printRecordsPerPartition(peopleDF)
# MAGIC     out = f.getvalue()
# MAGIC   
# MAGIC     # Setup tests
# MAGIC     testsPassed = []
# MAGIC     
# MAGIC     def passedTest(result, message = None):
# MAGIC         if result:
# MAGIC             testsPassed[len(testsPassed) - 1] = True
# MAGIC         else:
# MAGIC             testsPassed[len(testsPassed) - 1] = False
# MAGIC             print('Failed Test: {}'.format(message))
# MAGIC     
# MAGIC     # Test if correct number of partitions are printing
# MAGIC     testsPassed.append(None)
# MAGIC     try:
# MAGIC         assert int(out[out.rfind('#') + 1]) == peopleDF.rdd.getNumPartitions()
# MAGIC         passedTest(True)
# MAGIC     except:
# MAGIC         passedTest(False, "The correct number of partitions were not identified for printRecordsPerPartition")
# MAGIC         
# MAGIC     # Test if each printed partition has a record number associated
# MAGIC     testsPassed.append(None)
# MAGIC     try:
# MAGIC         output_list = [
# MAGIC           {val.split(" ")[0].replace("#", "").replace(":", ""): int(val.split(" ")[1].replace(",", ""))} 
# MAGIC           for val in out.split("\n") if val and val[0] == "#"
# MAGIC         ]
# MAGIC         assert all([isinstance(x[list(x.keys())[0]], int) for x in output_list])
# MAGIC         passedTest(True)
# MAGIC     except:
# MAGIC         passedTest(False, "Not every partition has an associated record count")
# MAGIC         
# MAGIC     # Test if the sum of the printed number of records per partition equals the total number of records
# MAGIC     testsPassed.append(None)
# MAGIC     try:
# MAGIC         printedSum = sum([
# MAGIC           int(val.split(" ")[1].replace(",", ""))
# MAGIC           for val in out.split("\n") if val and val[0] == "#"
# MAGIC         ])
# MAGIC       
# MAGIC         assert printedSum == peopleDF.count()
# MAGIC         passedTest(True)
# MAGIC     except:
# MAGIC         passedTest(False, "The sum of the number of records per partition does not match the total number of records")
# MAGIC     
# MAGIC     # Print final info and return
# MAGIC     if all(testsPassed):
# MAGIC         print('All {} tests for printRecordsPerPartition passed'.format(len(testsPassed)))
# MAGIC         return True
# MAGIC     else:
# MAGIC         print('{} of {} tests for printRecordsPerPartition passed'.format(testsPassed.count(True), len(testsPassed)))
# MAGIC         return False
# MAGIC 
# MAGIC functionPassed(testPrintRecordsPerPartition()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `computeFileStats`

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def testComputeFileStats(): Boolean = {
# MAGIC   
# MAGIC   // Set file path
# MAGIC   val filePath = "/mnt/training/global-sales/transactions/2017.parquet"
# MAGIC   
# MAGIC   // Run and get output
# MAGIC   val output = computeFileStats(filePath)
# MAGIC   
# MAGIC   // Setup tests
# MAGIC   import scala.collection.mutable.ArrayBuffer
# MAGIC   val testsPassed = ArrayBuffer.empty[Boolean]
# MAGIC   
# MAGIC   def passedTest(result: Boolean, message: String = null) = {
# MAGIC     if (result) {
# MAGIC       testsPassed += true
# MAGIC     } else {
# MAGIC       testsPassed += false
# MAGIC       println(s"Failed Test: $message")
# MAGIC     } 
# MAGIC   }
# MAGIC   
# MAGIC   
# MAGIC   // Test if result is correct structure
# MAGIC   try {
# MAGIC     assert(output.getClass.getName.startsWith("scala.Tuple"))
# MAGIC     assert(output.productArity == 2)
# MAGIC     assert(output._1.isInstanceOf[Long])
# MAGIC     assert(output._2.isInstanceOf[Long])
# MAGIC     passedTest(true)
# MAGIC   } catch {
# MAGIC     case a: AssertionError => {
# MAGIC       passedTest(false, "The incorrect structure is returned for computeFileStats")
# MAGIC     }
# MAGIC     case _: Exception => {
# MAGIC       passedTest(false, "non-descript error for the incorrect structure being returned for computeFileStats")
# MAGIC     }
# MAGIC   }
# MAGIC   
# MAGIC   // Test that correct result is returned
# MAGIC   try {
# MAGIC     assert(output._1 == 6276)
# MAGIC     assert(output._2 == 1269333224)
# MAGIC     passedTest(true)
# MAGIC   } catch {
# MAGIC     case a: AssertionError => {
# MAGIC       passedTest(false, "The incorrect result is returned for computeFileStats")
# MAGIC     }
# MAGIC     case _: Exception => {
# MAGIC       passedTest(false, "non-descript error for the incorrect result being returned for computeFileStats")
# MAGIC     }
# MAGIC   }
# MAGIC   
# MAGIC   // Test that nonexistent file path throws error
# MAGIC   try {
# MAGIC     computeFileStats("alkshdahdnoinscoinwincwinecw/cw/cw/cd/c/wcdwdfobnwef")
# MAGIC     passedTest(false, "A nonexistent file path did not throw an error for computeFileStats")
# MAGIC   } catch {
# MAGIC     case a: java.io.FileNotFoundException => {
# MAGIC       passedTest(true)
# MAGIC     }
# MAGIC     case _: Exception => {
# MAGIC       passedTest(false, "non-descript error for a nonexistent file path throwing an error for computeFileStats")
# MAGIC     }
# MAGIC   }
# MAGIC   
# MAGIC 
# MAGIC   val numTestsPassed = testsPassed.groupBy(identity).mapValues(_.size)(true)
# MAGIC   if (numTestsPassed == testsPassed.length) {
# MAGIC     println(s"All $numTestsPassed tests for computeFileStats passed")
# MAGIC     true
# MAGIC   } else {
# MAGIC     println(s"$numTestsPassed of ${testsPassed.length} tests for computeFileStats passed")
# MAGIC     false
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC functionPassed(testComputeFileStats())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def testComputeFileStats():
# MAGIC   
# MAGIC     # Set file path
# MAGIC     filePath = "/mnt/training/global-sales/transactions/2017.parquet"
# MAGIC   
# MAGIC     # Run and get output
# MAGIC     output = computeFileStats(filePath)
# MAGIC   
# MAGIC     # Setup tests
# MAGIC     testsPassed = []
# MAGIC     
# MAGIC     def passedTest(result, message = None):
# MAGIC         if result:
# MAGIC             testsPassed[len(testsPassed) - 1] = True
# MAGIC         else:
# MAGIC             testsPassed[len(testsPassed) - 1] = False
# MAGIC             print('Failed Test: {}'.format(message))
# MAGIC     
# MAGIC     # Test if correct structure is returned
# MAGIC     testsPassed.append(None)
# MAGIC     try:
# MAGIC         assert isinstance(output, tuple)
# MAGIC         assert len(output) == 2
# MAGIC         assert isinstance(output[0], int)
# MAGIC         assert isinstance(output[1], int)
# MAGIC         passedTest(True)
# MAGIC     except:
# MAGIC         passedTest(False, "The incorrect structure is returned for computeFileStats")
# MAGIC         
# MAGIC     # Test that correct result is returned
# MAGIC     testsPassed.append(None)
# MAGIC     try:
# MAGIC         assert output[0] == 6276
# MAGIC         assert output[1] == 1269333224
# MAGIC         passedTest(True)
# MAGIC     except:
# MAGIC         passedTest(False, "The incorrect result is returned for computeFileStats")
# MAGIC         
# MAGIC     # Test that nonexistent file path throws error
# MAGIC     testsPassed.append(None)
# MAGIC     try:
# MAGIC         computeFileStats("alkshdahdnoinscoinwincwinecw/cw/cw/cd/c/wcdwdfobnwef")
# MAGIC         passedTest(False, "A nonexistent file path did not throw an error for computeFileStats")
# MAGIC     except:
# MAGIC         passedTest(True)
# MAGIC      
# MAGIC     # Print final info and return
# MAGIC     if all(testsPassed):
# MAGIC         print('All {} tests for computeFileStats passed'.format(len(testsPassed)))
# MAGIC         return True
# MAGIC     else:
# MAGIC         print('{} of {} tests for computeFileStats passed'.format(testsPassed.count(True), len(testsPassed)))
# MAGIC         return False
# MAGIC 
# MAGIC functionPassed(testComputeFileStats()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `cacheAs`

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def testCacheAs(): Boolean = {
# MAGIC   
# MAGIC   import org.apache.spark.storage.StorageLevel
# MAGIC   // Import DF
# MAGIC   val inputDF = spark.read.parquet("/mnt/training/global-sales/transactions/2017.parquet").limit(100)
# MAGIC   
# MAGIC   // Setup tests
# MAGIC   import scala.collection.mutable.ArrayBuffer
# MAGIC   val testsPassed = ArrayBuffer.empty[Boolean]
# MAGIC   
# MAGIC   def passedTest(result: Boolean, message: String = null) = {
# MAGIC     if (result) {
# MAGIC       testsPassed += true
# MAGIC     } else {
# MAGIC       testsPassed += false
# MAGIC       println(s"Failed Test: $message")
# MAGIC     } 
# MAGIC   }
# MAGIC   
# MAGIC   
# MAGIC   // Test uncached table gets cached
# MAGIC   try {
# MAGIC     cacheAs(inputDF, "testCacheTable12344321", StorageLevel.MEMORY_ONLY)
# MAGIC     assert(spark.catalog.isCached("testCacheTable12344321"))
# MAGIC     passedTest(true)
# MAGIC   } catch {
# MAGIC     case a: AssertionError => {
# MAGIC       passedTest(false, "Uncached table was not cached for cacheAs")
# MAGIC     }
# MAGIC     case _: Exception => {
# MAGIC       passedTest(false, "non-descript error for uncached table being cached for cacheAs")
# MAGIC     }
# MAGIC   }
# MAGIC   
# MAGIC   // Test cached table gets recached
# MAGIC   try {
# MAGIC     cacheAs(inputDF, "testCacheTable12344321", StorageLevel.MEMORY_ONLY)
# MAGIC     assert(spark.catalog.isCached("testCacheTable12344321"))
# MAGIC     spark.catalog.uncacheTable("testCacheTable12344321")
# MAGIC     passedTest(true)
# MAGIC   } catch {
# MAGIC     case a: AssertionError => {
# MAGIC       passedTest(false, "Cached table was not recached for cacheAs")
# MAGIC     }
# MAGIC     case _: Exception => {
# MAGIC       passedTest(false, "non-descript error for cached table being recached for cacheAs")
# MAGIC     }
# MAGIC   }
# MAGIC 
# MAGIC   val numTestsPassed = testsPassed.groupBy(identity).mapValues(_.size)(true)
# MAGIC   if (numTestsPassed == testsPassed.length) {
# MAGIC     println(s"All $numTestsPassed tests for cacheAs passed")
# MAGIC     true
# MAGIC   } else {
# MAGIC     println(s"$numTestsPassed of ${testsPassed.length} tests for cacheAs passed")
# MAGIC     false
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC functionPassed(testCacheAs())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def testCacheAs():
# MAGIC   
# MAGIC     # Import DF
# MAGIC     inputDF = spark.read.parquet("/mnt/training/global-sales/transactions/2017.parquet").limit(100)
# MAGIC   
# MAGIC     # Setup tests
# MAGIC     testsPassed = []
# MAGIC     
# MAGIC     def passedTest(result, message = None):
# MAGIC         if result:
# MAGIC             testsPassed[len(testsPassed) - 1] = True
# MAGIC         else:
# MAGIC             testsPassed[len(testsPassed) - 1] = False
# MAGIC             print('Failed Test: {}'.format(message))
# MAGIC     
# MAGIC     # Test uncached table gets cached
# MAGIC     testsPassed.append(None)
# MAGIC     try:
# MAGIC         cacheAs(inputDF, "testCacheTable12344321")
# MAGIC         assert spark.catalog.isCached("testCacheTable12344321")
# MAGIC         passedTest(True)
# MAGIC     except:
# MAGIC         passedTest(False, "Uncached table was not cached for cacheAs")
# MAGIC         
# MAGIC     # Test cached table gets recached
# MAGIC     testsPassed.append(None)
# MAGIC     try:
# MAGIC         cacheAs(inputDF, "testCacheTable12344321")
# MAGIC         assert spark.catalog.isCached("testCacheTable12344321")
# MAGIC         passedTest(True)
# MAGIC     except:
# MAGIC         passedTest(False, "Cached table was not recached for cacheAs")
# MAGIC         
# MAGIC     # Test wrong level still gets cached
# MAGIC     testsPassed.append(None)
# MAGIC     try:
# MAGIC         spark.catalog.uncacheTable("testCacheTable12344321")
# MAGIC         cacheAs(inputDF, "testCacheTable12344321", "WRONG_LEVEL")
# MAGIC         assert spark.catalog.isCached("testCacheTable12344321")
# MAGIC         spark.catalog.uncacheTable("testCacheTable12344321")
# MAGIC         passedTest(True)
# MAGIC     except:
# MAGIC         passedTest(False, "Invalid storage level stopping caching for cacheAs")
# MAGIC         
# MAGIC      
# MAGIC     # Print final info and return
# MAGIC     if all(testsPassed):
# MAGIC         print('All {} tests for cacheAs passed'.format(len(testsPassed)))
# MAGIC         return True
# MAGIC     else:
# MAGIC         print('{} of {} tests for cacheAs passed'.format(testsPassed.count(True), len(testsPassed)))
# MAGIC         return False
# MAGIC 
# MAGIC functionPassed(testCacheAs()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `benchmarkCount()`

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def testBenchmarkCount(): Boolean = {
# MAGIC   
# MAGIC   def testFunction() = {
# MAGIC     spark.createDataFrame(Seq((1, 2, 3, 4), (5, 6, 7, 8), (9, 10, 11, 12)))
# MAGIC   }
# MAGIC   val output = benchmarkCount(testFunction)
# MAGIC   
# MAGIC   // Setup tests
# MAGIC   import scala.collection.mutable.ArrayBuffer
# MAGIC   val testsPassed = ArrayBuffer.empty[Boolean]
# MAGIC   
# MAGIC   def passedTest(result: Boolean, message: String = null) = {
# MAGIC     if (result) {
# MAGIC       testsPassed += true
# MAGIC     } else {
# MAGIC       testsPassed += false
# MAGIC       println(s"Failed Test: $message")
# MAGIC     } 
# MAGIC   }
# MAGIC   
# MAGIC   
# MAGIC   // Test that correct structure is returned
# MAGIC   try {
# MAGIC     assert(output.getClass.getName.startsWith("scala.Tuple"))
# MAGIC     assert(output.productArity == 3)
# MAGIC     assert(output._1.isInstanceOf[org.apache.spark.sql.DataFrame])
# MAGIC     assert(output._2.isInstanceOf[Long])
# MAGIC     assert(output._3.isInstanceOf[Long])
# MAGIC     passedTest(true)
# MAGIC   } catch {
# MAGIC     case a: AssertionError => {
# MAGIC       passedTest(false, "Correct structure not returned for benchmarkCount")
# MAGIC     }
# MAGIC     case _: Exception => {
# MAGIC       passedTest(false, "non-descript error for correct structure being returned for benchmarkCount")
# MAGIC     }
# MAGIC   }
# MAGIC   
# MAGIC   // Test that correct result is returned
# MAGIC   try {
# MAGIC     assert(output._1.rdd.collect().deep == testFunction().rdd.collect().deep)
# MAGIC     assert(output._2 == testFunction().count())
# MAGIC     assert(output._3 > 0)
# MAGIC     assert(output._3 < 10000)
# MAGIC     passedTest(true)
# MAGIC   } catch {
# MAGIC     case a: AssertionError => {
# MAGIC       passedTest(false, "Uncached table was not cached for cacheAs")
# MAGIC     }
# MAGIC     case _: Exception => {
# MAGIC       passedTest(false, "non-descript error for uncached table being cached for cacheAs")
# MAGIC     }
# MAGIC   }
# MAGIC 
# MAGIC   val numTestsPassed = testsPassed.groupBy(identity).mapValues(_.size)(true)
# MAGIC   if (numTestsPassed == testsPassed.length) {
# MAGIC     println(s"All $numTestsPassed tests for benchmarkCount passed")
# MAGIC     true
# MAGIC   } else {
# MAGIC     println(s"$numTestsPassed of ${testsPassed.length} tests for benchmarkCount passed")
# MAGIC     false
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC functionPassed(testBenchmarkCount())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def testBenchmarkCount():
# MAGIC   
# MAGIC     from pyspark.sql import DataFrame
# MAGIC     def testFunction():
# MAGIC       return spark.createDataFrame([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]])
# MAGIC     output = benchmarkCount(testFunction)
# MAGIC  
# MAGIC     # Setup tests
# MAGIC     testsPassed = []
# MAGIC     
# MAGIC     def passedTest(result, message = None):
# MAGIC         if result:
# MAGIC             testsPassed[len(testsPassed) - 1] = True
# MAGIC         else:
# MAGIC             testsPassed[len(testsPassed) - 1] = False
# MAGIC             print('Failed Test: {}'.format(message))
# MAGIC     
# MAGIC     # Test that correct structure is returned
# MAGIC     testsPassed.append(None)
# MAGIC     try:
# MAGIC         assert isinstance(output, tuple)
# MAGIC         assert len(output) == 3
# MAGIC         assert isinstance(output[0], DataFrame)
# MAGIC         assert isinstance(output[1], int)
# MAGIC         assert isinstance(output[2], float)
# MAGIC         passedTest(True)
# MAGIC     except:
# MAGIC         passedTest(False, "Correct structure not returned for benchmarkCount")
# MAGIC         
# MAGIC     # Test that correct result is returned
# MAGIC     testsPassed.append(None)
# MAGIC     try:
# MAGIC         assert output[0].rdd.collect() == testFunction().rdd.collect()
# MAGIC         assert output[1] == testFunction().count()
# MAGIC         assert output[2] > 0 and output[2] < 10000
# MAGIC         passedTest(True)
# MAGIC     except:
# MAGIC         passedTest(False, "Correct structure not returned for benchmarkCount")    
# MAGIC      
# MAGIC     # Print final info and return
# MAGIC     if all(testsPassed):
# MAGIC         print('All {} tests for benchmarkCount passed'.format(len(testsPassed)))
# MAGIC         return True
# MAGIC     else:
# MAGIC         print('{} of {} tests for benchmarkCount passed'.format(testsPassed.count(True), len(testsPassed)))
# MAGIC         return False
# MAGIC 
# MAGIC functionPassed(testBenchmarkCount()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test **`untilStreamIsReady()`**

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val dataPath = "dbfs:/mnt/training/definitive-guide/data/activity-data-stream.json"
# MAGIC val dataSchema = "Recorded_At timestamp, Device string, Index long, Model string, User string, _corrupt_record String, gt string, x double, y double, z double"
# MAGIC 
# MAGIC val initialDF = spark
# MAGIC   .readStream                             // Returns DataStreamReader
# MAGIC   .option("maxFilesPerTrigger", 1)        // Force processing of only 1 file per trigger 
# MAGIC   .schema(dataSchema)                     // Required for all streaming DataFrames
# MAGIC   .json(dataPath)                         // The stream's source directory and file type
# MAGIC 
# MAGIC val name = "Testing_123"
# MAGIC 
# MAGIC display(initialDF, streamName = name)
# MAGIC untilStreamIsReady(name)
# MAGIC assert(spark.streams.active.length == 1, "Expected 1 active stream, found " + spark.streams.active.length)

# COMMAND ----------

# MAGIC %scala
# MAGIC for (stream <- spark.streams.active) {
# MAGIC   stream.stop()
# MAGIC   var queries = spark.streams.active.filter(_.name == stream.name)
# MAGIC   while (queries.length > 0) {
# MAGIC     Thread.sleep(5*1000) // Give it a couple of seconds
# MAGIC     queries = spark.streams.active.filter(_.name == stream.name)
# MAGIC   }
# MAGIC   println("""The stream "%s" has beend terminated.""".format(stream.name))
# MAGIC }

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC dataPath = "dbfs:/mnt/training/definitive-guide/data/activity-data-stream.json"
# MAGIC dataSchema = "Recorded_At timestamp, Device string, Index long, Model string, User string, _corrupt_record String, gt string, x double, y double, z double"
# MAGIC 
# MAGIC initialDF = (spark
# MAGIC   .readStream                            # Returns DataStreamReader
# MAGIC   .option("maxFilesPerTrigger", 1)       # Force processing of only 1 file per trigger 
# MAGIC   .schema(dataSchema)                    # Required for all streaming DataFrames
# MAGIC   .json(dataPath)                        # The stream's source directory and file type
# MAGIC )
# MAGIC 
# MAGIC name = "Testing_123"
# MAGIC 
# MAGIC display(initialDF, streamName = name)
# MAGIC untilStreamIsReady(name)
# MAGIC assert len(spark.streams.active) == 1, "Expected 1 active stream, found " + str(len(spark.streams.active))

# COMMAND ----------

# MAGIC %python
# MAGIC for stream in spark.streams.active:
# MAGIC   stream.stop()
# MAGIC   queries = list(filter(lambda query: query.name == stream.name, spark.streams.active))
# MAGIC   while (len(queries) > 0):
# MAGIC     time.sleep(5) # Give it a couple of seconds
# MAGIC     queries = list(filter(lambda query: query.name == stream.name, spark.streams.active))
# MAGIC   print("""The stream "{}" has been terminated.""".format(stream.name))

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val numTestsPassed = scalaTests.groupBy(identity).mapValues(_.size)(true)
# MAGIC if (numTestsPassed == scalaTests.length) {
# MAGIC   println(s"All $numTestsPassed tests for Scala passed")
# MAGIC } else {
# MAGIC   println(s"$numTestsPassed of ${scalaTests.length} tests for Scala passed")
# MAGIC   throw new Exception(s"$numTestsPassed of ${scalaTests.length} tests for Scala passed")
# MAGIC }

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC if all(pythonTests):
# MAGIC     print('All {} tests for Python passed'.format(len(pythonTests)))
# MAGIC else:
# MAGIC     print('{} of {} tests for Python passed'.format(pythonTests.count(True), len(pythonTests)))
# MAGIC     raise Exception('{} of {} tests for Python passed'.format(pythonTests.count(True), len(pythonTests)))
