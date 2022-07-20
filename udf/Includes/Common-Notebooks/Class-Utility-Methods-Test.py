# Databricks notebook source
# MAGIC %md
# MAGIC # Class-Utility-Methods-Test
# MAGIC The purpose of this notebook is to faciliate testing of courseware-specific utility methos.

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set("com.databricks.training.module-name", "common-notebooks")

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.set("com.databricks.training.module-name", "common-notebooks")

# COMMAND ----------

# MAGIC %md a lot of these tests evolve around the current DBR version.
# MAGIC 
# MAGIC It shall be assumed that the cluster is configured properly and that these tests are updated with each publishing of courseware against a new DBR

# COMMAND ----------

# MAGIC %run ./Class-Utility-Methods

# COMMAND ----------

# MAGIC %scala
# MAGIC def functionPassed(result: Boolean) = {
# MAGIC   if (!result) {
# MAGIC     assert(false, "Test failed")
# MAGIC   } 
# MAGIC }

# COMMAND ----------

# MAGIC %python
# MAGIC def functionPassed(result):
# MAGIC   if not result:
# MAGIC     raise AssertionError("Test failed")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `getTags`

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def testGetTags(): Boolean = {
# MAGIC   
# MAGIC   val testTags = getTags()
# MAGIC   
# MAGIC   import scala.collection.mutable.ArrayBuffer
# MAGIC 
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
# MAGIC   // Test that tags result is correct type
# MAGIC   try {
# MAGIC     assert(testTags.isInstanceOf[Map[com.databricks.logging.TagDefinition,String]])
# MAGIC     passedTest(true)
# MAGIC   } catch {
# MAGIC     case a: AssertionError => {
# MAGIC       passedTest(false, "tags is not an instance of Map[com.databricks.logging.TagDefinition,String]")
# MAGIC     }
# MAGIC     case _: Exception => {
# MAGIC       passedTest(false, "non-descript error for tags being an instance of Map[com.databricks.logging.TagDefinition,String]")
# MAGIC     }
# MAGIC   }
# MAGIC 
# MAGIC   val numTestsPassed = testsPassed.groupBy(identity).mapValues(_.size)(true)
# MAGIC   if (numTestsPassed == testsPassed.length) {
# MAGIC     println(s"All $numTestsPassed tests for getTags passed")
# MAGIC     true
# MAGIC   } else {
# MAGIC     throw new Exception(s"$numTestsPassed of ${testsPassed.length} tests for getTags passed")
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC functionPassed(testGetTags())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def testGetTags():
# MAGIC   
# MAGIC     testTags = getTags()
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
# MAGIC     # Test that getTags returns correct type
# MAGIC     testsPassed.append(None)
# MAGIC     try:
# MAGIC         from py4j.java_collections import JavaMap
# MAGIC         assert isinstance(getTags(), JavaMap)
# MAGIC         passedTest(True)
# MAGIC     except:
# MAGIC         passedTest(False, "The correct type is not returned by getTags")
# MAGIC         
# MAGIC     # Test that getTags does not return an empty dict
# MAGIC     testsPassed.append(None)
# MAGIC     try:
# MAGIC         assert len(testTags) > 0
# MAGIC         passedTest(True)
# MAGIC     except:
# MAGIC         passedTest(False, "A non-empty dict is returned by getTags")
# MAGIC     
# MAGIC     # Print final info and return
# MAGIC     if all(testsPassed):
# MAGIC         print('All {} tests for getTags passed'.format(len(testsPassed)))
# MAGIC         return True
# MAGIC     else:
# MAGIC         raise Exception('{} of {} tests for getTags passed'.format(testsPassed.count(True), len(testsPassed)))
# MAGIC 
# MAGIC functionPassed(testGetTags()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `getTag()`

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def testGetTag(): Boolean = {
# MAGIC   
# MAGIC   import scala.collection.mutable.ArrayBuffer
# MAGIC   
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
# MAGIC   // Test that returns null when not present
# MAGIC   try {
# MAGIC     assert(getTag("thiswillneverbeincluded") == null)
# MAGIC     passedTest(true)
# MAGIC   } catch {
# MAGIC     case e: Exception => passedTest(false, "tag value for 'thiswillneverbeincluded' is not null")
# MAGIC   }
# MAGIC   
# MAGIC   // Test that default value is returned when not present
# MAGIC   try {
# MAGIC     assert(getTag("thiswillneverbeincluded", "default-test").contentEquals("default-test"))
# MAGIC     passedTest(true)
# MAGIC   } catch {
# MAGIC     case e: Exception => passedTest(false, "tag value for 'thiswillneverbeincluded' is not the default value")
# MAGIC   }
# MAGIC   
# MAGIC   // Test that correct result is returned when default value is not set
# MAGIC   try {
# MAGIC     val orgId = getTags().collect({ case (t, v) if t.name == "orgId" => v }).toSeq(0)
# MAGIC     assert(orgId.isInstanceOf[String])
# MAGIC     assert(orgId.size > 0)
# MAGIC     assert(orgId.contentEquals(getTag("orgId")))
# MAGIC     passedTest(true)
# MAGIC   } catch {
# MAGIC     case e: Exception => passedTest(false, "Unexpected tag value returned for getTag")
# MAGIC   }
# MAGIC 
# MAGIC   // Print final info and return
# MAGIC   val numTestsPassed = testsPassed.groupBy(identity).mapValues(_.size)(true)
# MAGIC   if (numTestsPassed == testsPassed.length) {
# MAGIC     println(s"All $numTestsPassed tests for getTag passed")
# MAGIC     true
# MAGIC   } else {
# MAGIC     throw new Exception(s"$numTestsPassed of ${testsPassed.length} tests for getTag passed")
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC functionPassed(testGetTag())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def testGetTag():
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
# MAGIC     # Test that getTag returns null when defaultValue is not set and tag is not present
# MAGIC     testsPassed.append(None)
# MAGIC     try:
# MAGIC         assert getTag("thiswillneverbeincluded") == None
# MAGIC         passedTest(True)
# MAGIC     except:
# MAGIC         passedTest(False, "NoneType is not returned when defaultValue is not set and tag is not present for getTag")
# MAGIC         
# MAGIC     # Test that getTag returns defaultValue when it is set and tag is not present
# MAGIC     testsPassed.append(None)
# MAGIC     try:
# MAGIC         assert getTag("thiswillneverbeincluded", "default-value") == "default-value"
# MAGIC         passedTest(True)
# MAGIC     except:
# MAGIC         passedTest(False, "defaultValue is not returned when defaultValue is set and tag is not present for getTag")
# MAGIC         
# MAGIC     # Test that getTag returns correct value when default value is not set and tag is present
# MAGIC     testsPassed.append(None)
# MAGIC     try:
# MAGIC         orgId = getTags()["orgId"]
# MAGIC         assert isinstance(orgId, str)
# MAGIC         assert len(orgId) > 0
# MAGIC         assert orgId == getTag("orgId")
# MAGIC         passedTest(True)
# MAGIC     except:
# MAGIC         passedTest(False, "A non-empty dict is returned by getTags")
# MAGIC     
# MAGIC     # Print final info and return
# MAGIC     if all(testsPassed):
# MAGIC         print('All {} tests for getTag passed'.format(len(testsPassed)))
# MAGIC         return True
# MAGIC     else:
# MAGIC         raise Exception('{} of {} tests for getTag passed'.format(testsPassed.count(True), len(testsPassed)))
# MAGIC 
# MAGIC functionPassed(testGetTag()) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test `getDbrMajorAndMinorVersions()`

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def testGetDbrMajorAndMinorVersions(): Boolean = {
# MAGIC   // We cannot rely on the assumption that all courses are
# MAGIC   // running latestDbrMajor.latestDbrMinor. The best we
# MAGIC   // can do here is make sure it matches the environment
# MAGIC   // variable from which it came.
# MAGIC   val dbrVersion = System.getenv().get("DATABRICKS_RUNTIME_VERSION")
# MAGIC   
# MAGIC   val (major,minor) = getDbrMajorAndMinorVersions()
# MAGIC   assert(dbrVersion.startsWith(f"${major}.${minor}"), f"Found ${major}.${minor} for ${dbrVersion}")
# MAGIC   
# MAGIC   return true
# MAGIC }
# MAGIC 
# MAGIC functionPassed(testGetDbrMajorAndMinorVersions())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def testGetDbrMajorAndMinorVersions():
# MAGIC   import os
# MAGIC   # We cannot rely on the assumption that all courses are
# MAGIC   # running latestDbrMajor.latestDbrMinor. The best we
# MAGIC   # can do here is make sure it matches the environment
# MAGIC   # variable from which it came.
# MAGIC   dbrVersion = os.environ["DATABRICKS_RUNTIME_VERSION"]
# MAGIC   
# MAGIC   (major,minor) = getDbrMajorAndMinorVersions()
# MAGIC   assert dbrVersion.startswith(f"{major}.{minor}"), f"Found {major}.{minor} for {dbrVersion}"
# MAGIC   
# MAGIC   return True
# MAGIC       
# MAGIC functionPassed(testGetDbrMajorAndMinorVersions()) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test `getPythonVersion()`

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def testGetPythonVersion():
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
# MAGIC     # Test output for structure
# MAGIC     testsPassed.append(None)
# MAGIC     try:
# MAGIC         pythonVersion = getPythonVersion()
# MAGIC         assert isinstance(pythonVersion, str)
# MAGIC         assert len(pythonVersion.split(".")) >= 2
# MAGIC         passedTest(True)
# MAGIC     except:
# MAGIC         passedTest(False, "pythonVersion does not match expected structure")
# MAGIC         
# MAGIC     # Test output for correctness
# MAGIC     testsPassed.append(None)
# MAGIC     try:
# MAGIC         pythonVersion = getPythonVersion()
# MAGIC         assert pythonVersion[0] == "2" or pythonVersion[0] == "3"
# MAGIC         passedTest(True)
# MAGIC     except:
# MAGIC         passedTest(False, "pythonVersion does not match expected value")
# MAGIC         
# MAGIC 
# MAGIC     # Print final info and return
# MAGIC     if all(testsPassed):
# MAGIC         print('All {} tests for getPythonVersion passed'.format(len(testsPassed)))
# MAGIC         return True
# MAGIC     else:
# MAGIC         raise Exception('{} of {} tests for getPythonVersion passed'.format(testsPassed.count(True), len(testsPassed)))
# MAGIC 
# MAGIC functionPassed(testGetPythonVersion()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `getUsername()`

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def testGetUsername(): Boolean = {
# MAGIC   val username = getUsername()
# MAGIC   assert(username.isInstanceOf[String])
# MAGIC   assert(!username.contentEquals(""))
# MAGIC   
# MAGIC   return true
# MAGIC }
# MAGIC functionPassed(testGetUsername())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def testGetUsername():
# MAGIC   username = getUsername()
# MAGIC   assert isinstance(username, str)
# MAGIC   assert username != ""
# MAGIC   
# MAGIC   return True
# MAGIC     
# MAGIC functionPassed(testGetUsername()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `getUserhome`

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def testGetUserhome(): Boolean = {
# MAGIC   val userhome = getUserhome()
# MAGIC   assert(userhome.isInstanceOf[String])
# MAGIC   assert(!userhome.contentEquals(""))
# MAGIC   assert(userhome == "dbfs:/user/" + getUsername())
# MAGIC     
# MAGIC   return true
# MAGIC }
# MAGIC 
# MAGIC functionPassed(testGetUserhome())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def testGetUserhome():
# MAGIC   userhome = getUserhome()
# MAGIC   assert isinstance(userhome, str)
# MAGIC   assert userhome != ""
# MAGIC   assert userhome == "dbfs:/user/" + getUsername()
# MAGIC     
# MAGIC   return True
# MAGIC 
# MAGIC functionPassed(testGetUserhome()) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `assertDbrVersion`

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def testAssertDbrVersion(): Boolean = {
# MAGIC   
# MAGIC   val (majorVersion, minorVersion) = getDbrMajorAndMinorVersions()
# MAGIC   val major = majorVersion.toInt
# MAGIC   val minor = minorVersion.toInt
# MAGIC 
# MAGIC   val goodVersions = Seq(
# MAGIC     ("SG1", major-1, minor-1),
# MAGIC     ("SG2", major-1, minor),
# MAGIC     ("SG3", major, minor-1),
# MAGIC     ("SG4", major, minor)
# MAGIC   )
# MAGIC   
# MAGIC   for ( (name, testMajor, testMinor) <- goodVersions) {
# MAGIC     println(f"-- ${name} ${testMajor}.${testMinor}")
# MAGIC     assertDbrVersion(null, testMajor, testMinor, false)
# MAGIC     println(f"-"*80)
# MAGIC   }
# MAGIC 
# MAGIC   val badVersions = Seq(
# MAGIC     ("SB1", major+1, minor+1),
# MAGIC     ("SB2", major+1, minor),
# MAGIC     ("SB3", major, minor+1)
# MAGIC   )
# MAGIC 
# MAGIC   for ( (name, testMajor, testMinor) <- badVersions) {
# MAGIC     try {
# MAGIC       println(f"-- ${name} ${testMajor}.${testMinor}")
# MAGIC       assertDbrVersion(null, testMajor, testMinor, false)
# MAGIC       throw new Exception("Expected AssertionError")
# MAGIC     } catch {
# MAGIC       case e: AssertionError => println(e)
# MAGIC     }
# MAGIC     println(f"-"*80)
# MAGIC   }
# MAGIC   return true
# MAGIC }
# MAGIC functionPassed(testAssertDbrVersion())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def testAssertDbrVersion():
# MAGIC   
# MAGIC   (majorVersion, minorVersion) = getDbrMajorAndMinorVersions()
# MAGIC   major = int(majorVersion)
# MAGIC   minor = int(minorVersion)
# MAGIC 
# MAGIC   goodVersions = [
# MAGIC     ("PG1", major-1, minor-1),
# MAGIC     ("PG2", major-1, minor),
# MAGIC     ("PG3", major, minor-1),
# MAGIC     ("PG4", major, minor)
# MAGIC   ]
# MAGIC   
# MAGIC   for (name, testMajor, testMinor) in goodVersions:
# MAGIC     print(f"-- {name} {testMajor}.{testMinor}")
# MAGIC     assertDbrVersion(None, testMajor, testMinor, False)
# MAGIC     print(f"-"*80)
# MAGIC 
# MAGIC   badVersions = [
# MAGIC     ("PB1", major+1, minor+1),
# MAGIC     ("PB2", major+1, minor),
# MAGIC     ("PB3", major, minor+1)
# MAGIC   ]
# MAGIC 
# MAGIC   for (name, testMajor, testMinor) in badVersions:
# MAGIC     try:
# MAGIC       print(f"-- {name} {testMajor}.{testMinor}")
# MAGIC       assertDbrVersion(None, testMajor, testMinor, False)
# MAGIC       raise Exception("Expected AssertionError")
# MAGIC       
# MAGIC     except AssertionError as e:
# MAGIC       print(e)
# MAGIC     
# MAGIC     print(f"-"*80)
# MAGIC 
# MAGIC   return True
# MAGIC         
# MAGIC functionPassed(testAssertDbrVersion())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `assertIsMlRuntime`

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // def testAssertIsMlRuntime(): Boolean = {
# MAGIC     
# MAGIC //   assertIsMlRuntime("5.5.x-ml-scala2.11")
# MAGIC //   assertIsMlRuntime("5.5.x-cpu-ml-scala2.11")
# MAGIC 
# MAGIC //   try {
# MAGIC //     assertIsMlRuntime("5.5.x-scala2.11")
# MAGIC //     assert(false, s"Expected to throw an IllegalArgumentException")
# MAGIC //   } catch {
# MAGIC //     case _: AssertionError => ()
# MAGIC //   }
# MAGIC 
# MAGIC //   try {
# MAGIC //     assertIsMlRuntime("5.5.xml-scala2.11")
# MAGIC //     assert(false, s"Expected to throw an IllegalArgumentException")
# MAGIC //   } catch {
# MAGIC //     case _: AssertionError => ()
# MAGIC //   }
# MAGIC 
# MAGIC //   return true
# MAGIC // }
# MAGIC // functionPassed(testAssertIsMlRuntime())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # def testAssertIsMlRuntime():
# MAGIC 
# MAGIC #   assertIsMlRuntime("6.3.x-ml-scala2.11")
# MAGIC #   assertIsMlRuntime("6.3.x-cpu-ml-scala2.11")
# MAGIC 
# MAGIC #   try:
# MAGIC #     assertIsMlRuntime("5.5.x-scala2.11")
# MAGIC #     assert False, "Expected to throw an ValueError"
# MAGIC #   except AssertionError:
# MAGIC #     pass
# MAGIC 
# MAGIC #   try:
# MAGIC #     assertIsMlRuntime("5.5.xml-scala2.11")
# MAGIC #     assert False, "Expected to throw an ValueError"
# MAGIC #   except AssertionError:
# MAGIC #     pass
# MAGIC 
# MAGIC #   return True
# MAGIC 
# MAGIC # functionPassed(testAssertIsMlRuntime())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Legacy Functions
# MAGIC 
# MAGIC Note: Legacy functions will not be tested. Use at your own risk.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test `createUserDatabase`

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def testCreateUserDatabase(): Boolean = {
# MAGIC 
# MAGIC   val courseType = "wa"
# MAGIC   val username = "mickey.mouse@disney.com"
# MAGIC   val moduleName = "Testing-Stuff 101"
# MAGIC   val lessonName = "TS 03 - Underwater Basket Weaving"
# MAGIC   
# MAGIC   // Test that correct database name is returned
# MAGIC   val expectedDatabaseName = "mickey_mouse_disney_com" + "_" + "testing_stuff_101" + "_" + "ts_03_underwater_basket_weaving" + "_" + "s" + "wa"
# MAGIC   
# MAGIC   val databaseName = getDatabaseName(courseType, username, moduleName, lessonName)
# MAGIC   assert(databaseName == expectedDatabaseName)
# MAGIC   
# MAGIC   val actualDatabaseName = createUserDatabase(courseType, username, moduleName, lessonName)
# MAGIC   assert(actualDatabaseName == expectedDatabaseName)
# MAGIC 
# MAGIC   assert(spark.sql(s"SHOW DATABASES LIKE '$expectedDatabaseName'").first.getAs[String]("databaseName") == expectedDatabaseName)
# MAGIC   assert(spark.sql("SELECT current_database()").first.getAs[String]("current_database()") == expectedDatabaseName)
# MAGIC   
# MAGIC   return true
# MAGIC }
# MAGIC functionPassed(testCreateUserDatabase())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def testCreateUserDatabase(): 
# MAGIC 
# MAGIC   courseType = "wa"
# MAGIC   username = "mickey.mouse@disney.com"
# MAGIC   moduleName = "Testing-Stuff 101"
# MAGIC   lessonName = "TS 03 - Underwater Basket Weaving"
# MAGIC   
# MAGIC   # Test that correct database name is returned
# MAGIC   expectedDatabaseName = "mickey_mouse_disney_com" + "_" + "testing_stuff_101" + "_" + "ts_03_underwater_basket_weaving" + "_" + "p" + "wa"
# MAGIC   
# MAGIC   databaseName = getDatabaseName(courseType, username, moduleName, lessonName)
# MAGIC   assert databaseName == expectedDatabaseName, "Expected {}, found {}".format(expectedDatabaseName, databaseName)
# MAGIC   
# MAGIC   actualDatabaseName = createUserDatabase(courseType, username, moduleName, lessonName)
# MAGIC   assert actualDatabaseName == expectedDatabaseName, "Expected {}, found {}".format(expectedDatabaseName, databaseName)
# MAGIC 
# MAGIC   assert spark.sql(f"SHOW DATABASES LIKE '{expectedDatabaseName}'").first()["databaseName"] == expectedDatabaseName
# MAGIC   assert spark.sql("SELECT current_database()").first()["current_database()"] == expectedDatabaseName
# MAGIC   
# MAGIC   return True
# MAGIC 
# MAGIC functionPassed(testCreateUserDatabase())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test `getExperimentId()`

# COMMAND ----------

# MAGIC %scala
# MAGIC def testGetExperimentId(): Boolean = {
# MAGIC   
# MAGIC   import scala.collection.mutable.ArrayBuffer
# MAGIC 
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
# MAGIC   // Test that result is correct type
# MAGIC   try {
# MAGIC     assert(getExperimentId().isInstanceOf[Long])
# MAGIC     passedTest(true)
# MAGIC   } catch {
# MAGIC     case e: Exception => passedTest(false, "result of getExperimentId is not of Long type")
# MAGIC   }
# MAGIC   
# MAGIC   // Note that the tags are an immutable map so we can't test other values
# MAGIC   // Another option is add parameters to getExperimentId, but that could break existing uses
# MAGIC   
# MAGIC   // Test that result comes out as expected
# MAGIC   try {
# MAGIC     val notebookId = try {
# MAGIC       com.databricks.logging.AttributionContext.current.tags(com.databricks.logging.TagDefinition("notebookId", ""))
# MAGIC     } catch {
# MAGIC       case e: Exception => null
# MAGIC     }
# MAGIC     val jobId = try {
# MAGIC       com.databricks.logging.AttributionContext.current.tags(com.databricks.logging.TagDefinition("jobId", ""))
# MAGIC     } catch {
# MAGIC       case e: Exception => null
# MAGIC     }
# MAGIC     val expectedResult = if (notebookId != null){
# MAGIC       notebookId.toLong
# MAGIC     } else {
# MAGIC       if (jobId != null) {
# MAGIC         jobId.toLong
# MAGIC       } else {
# MAGIC         0
# MAGIC       }
# MAGIC     }
# MAGIC     assert(expectedResult == getExperimentId())
# MAGIC     passedTest(true)
# MAGIC   } catch {
# MAGIC     case e: Exception => passedTest(false, "unexpected result for getExperimentId")
# MAGIC   }
# MAGIC   
# MAGIC   val numTestsPassed = testsPassed.groupBy(identity).mapValues(_.size)(true)
# MAGIC   if (numTestsPassed == testsPassed.length) {
# MAGIC     println(s"All $numTestsPassed tests for getExperimentId passed")
# MAGIC     true
# MAGIC   } else {
# MAGIC     throw new Exception(s"$numTestsPassed of ${testsPassed.length} tests for getExperimentId passed")
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC functionPassed(testGetExperimentId())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test `classroomCleanup()`

# COMMAND ----------

# MAGIC %scala
# MAGIC classroomCleanup(daLogger, "sp", getUsername(), getModuleName(), getLessonName(), false)

# COMMAND ----------

# MAGIC %scala
# MAGIC classroomCleanup(daLogger, "il", getUsername(), getModuleName(), getLessonName(), true)

# COMMAND ----------

# MAGIC %scala
# MAGIC classroomCleanup(daLogger, "sp", getUsername(), getModuleName(), getLessonName(), false)

# COMMAND ----------

# MAGIC %scala
# MAGIC classroomCleanup(daLogger, "il", getUsername(), getModuleName(), getLessonName(), true)

# COMMAND ----------

# MAGIC %python
# MAGIC classroomCleanup(daLogger, "sp", getUsername(), getModuleName(), getLessonName(), False)

# COMMAND ----------

# MAGIC %python
# MAGIC classroomCleanup(daLogger, "il", getUsername(), getModuleName(), getLessonName(), True)

# COMMAND ----------

# MAGIC %python
# MAGIC classroomCleanup(daLogger, "sp", getUsername(), getModuleName(), getLessonName(), False)

# COMMAND ----------

# MAGIC %python 
# MAGIC classroomCleanup(daLogger, "il", getUsername(), getModuleName(), getLessonName(), True)

# COMMAND ----------

# MAGIC %md ## Test FILL_IN

# COMMAND ----------

# MAGIC %scala
# MAGIC println(FILL_IN)
# MAGIC println(FILL_IN.VALUE)
# MAGIC println(FILL_IN.ARRAY)
# MAGIC println(FILL_IN.SCHEMA)
# MAGIC println(FILL_IN.ROW)
# MAGIC println(FILL_IN.LONG)
# MAGIC println(FILL_IN.INT)
# MAGIC println(FILL_IN.DATAFRAME)
# MAGIC println(FILL_IN.DATASET)

# COMMAND ----------

# MAGIC %python
# MAGIC print(FILL_IN)
# MAGIC print(FILL_IN.VALUE)
# MAGIC print(FILL_IN.LIST)
# MAGIC print(FILL_IN.SCHEMA)
# MAGIC print(FILL_IN.ROW)
# MAGIC print(FILL_IN.INT)
# MAGIC print(FILL_IN.DATAFRAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test `showStudentSurvey()`

# COMMAND ----------

# MAGIC %scala
# MAGIC val html = renderStudentSurvey()

# COMMAND ----------

# MAGIC %python
# MAGIC html = renderStudentSurvey()
# MAGIC print(html)

# COMMAND ----------

# MAGIC %python
# MAGIC showStudentSurvey()

# COMMAND ----------

# MAGIC %scala
# MAGIC showStudentSurvey()

# COMMAND ----------


