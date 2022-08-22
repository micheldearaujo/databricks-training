# Databricks notebook source
# MAGIC %scala
# MAGIC 
# MAGIC //*******************************************
# MAGIC // TAG API FUNCTIONS
# MAGIC //*******************************************
# MAGIC 
# MAGIC // Get all tags
# MAGIC def getTags(): Map[com.databricks.logging.TagDefinition,String] = {
# MAGIC   com.databricks.logging.AttributionContext.current.tags
# MAGIC } 
# MAGIC 
# MAGIC // Get a single tag's value
# MAGIC def getTag(tagName: String, defaultValue: String = null): String = {
# MAGIC   val values = getTags().collect({ case (t, v) if t.name == tagName => v }).toSeq
# MAGIC   values.size match {
# MAGIC     case 0 => defaultValue
# MAGIC     case _ => values.head.toString
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC //*******************************************
# MAGIC // Get Databricks runtime major and minor versions
# MAGIC //*******************************************
# MAGIC 
# MAGIC def getDbrMajorAndMinorVersions(): (Int, Int) = {
# MAGIC   val dbrVersion = System.getenv().get("DATABRICKS_RUNTIME_VERSION")
# MAGIC   val Array(dbrMajorVersion, dbrMinorVersion, _*) = dbrVersion.split("""\.""")
# MAGIC   return (dbrMajorVersion.toInt, dbrMinorVersion.toInt)
# MAGIC }
# MAGIC 
# MAGIC 
# MAGIC //*******************************************
# MAGIC // USER, USERNAME, AND USERHOME FUNCTIONS
# MAGIC //*******************************************
# MAGIC 
# MAGIC // Get the user's username
# MAGIC def getUsername(): String = {
# MAGIC   return try {
# MAGIC     dbutils.widgets.get("databricksUsername")
# MAGIC   } catch {
# MAGIC     case _: Exception => getTag("user", java.util.UUID.randomUUID.toString.replace("-", ""))
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC // Get the user's userhome
# MAGIC def getUserhome(): String = {
# MAGIC   val username = getUsername()
# MAGIC   return s"dbfs:/user/$username"
# MAGIC }
# MAGIC 
# MAGIC def getModuleName(): String = {
# MAGIC   // This will/should fail if module-name is not defined in the Classroom-Setup notebook
# MAGIC   return spark.conf.get("com.databricks.training.module-name")
# MAGIC }
# MAGIC 
# MAGIC def getLessonName(): String = {
# MAGIC   // If not specified, use the notebook's name.
# MAGIC   return dbutils.notebook.getContext.notebookPath.get.split("/").last
# MAGIC }
# MAGIC 
# MAGIC def getWorkingDir(courseType:String): String = {
# MAGIC   val langType = "s" // for scala
# MAGIC   val moduleName = getModuleName().replaceAll("[^a-zA-Z0-9]", "_").toLowerCase()
# MAGIC   val lessonName = getLessonName().replaceAll("[^a-zA-Z0-9]", "_").toLowerCase()
# MAGIC   val workingDir = "%s/%s/%s_%s%s".format(getUserhome(), moduleName, lessonName, langType, courseType)
# MAGIC   return workingDir.replace("__", "_").replace("__", "_").replace("__", "_").replace("__", "_")
# MAGIC }
# MAGIC 
# MAGIC //**********************************
# MAGIC // VERSION ASSERTION FUNCTIONS
# MAGIC //**********************************
# MAGIC 
# MAGIC // When migrating DBR versions this should be one
# MAGIC // of the only two places that needs to be updated
# MAGIC 
# MAGIC val latestDbrMajor = 6
# MAGIC val latestDbrMinor = 2
# MAGIC 
# MAGIC // Assert an appropriate Databricks Runtime version
# MAGIC def assertDbrVersion(expected:String, latestMajor:Int = latestDbrMajor, latestMinor:Int = latestDbrMinor, display:Boolean=true):String = {
# MAGIC   
# MAGIC   var expMajor = latestMajor
# MAGIC   var expMinor = latestMinor
# MAGIC   
# MAGIC   if (expected != null && expected.trim() != "" && expected != "{{dbr}}") {
# MAGIC     expMajor = expected.split('.')(0).toInt
# MAGIC     expMinor = expected.split('.')(1).toInt
# MAGIC   }
# MAGIC   
# MAGIC   val (major, minor) = getDbrMajorAndMinorVersions()
# MAGIC 
# MAGIC   if ((major < expMajor) || (major == expMajor && minor < expMinor)) {
# MAGIC     throw new AssertionError(s"This notebook must be run on DBR $expMajor.$expMinor or newer. Your cluster is using $major.$minor. You must update your cluster configuration before proceeding.")
# MAGIC   }
# MAGIC   
# MAGIC   val html = if (major != expMajor || minor != expMinor) {
# MAGIC     s"""
# MAGIC       <div style="color:red; font-weight:bold">WARNING: This notebook was tested on DBR $expMajor.$expMinor but we found DBR $major.$minor.</div>
# MAGIC       <div style="font-weight:bold">Using an untested DBR may yield unexpected results and/or various errors</div>
# MAGIC       <div style="font-weight:bold">Please update your cluster configuration and/or <a href="https://academy.databricks.com/" target="_blank">download a newer version of this course</a> before proceeding.</div>
# MAGIC     """
# MAGIC   } else {
# MAGIC     s"Running on <b>DBR $major.$minor</b>"
# MAGIC   }
# MAGIC 
# MAGIC   if (display) 
# MAGIC     displayHTML(html) 
# MAGIC   else 
# MAGIC     println(html)
# MAGIC   
# MAGIC   return s"$major.$minor"
# MAGIC }
# MAGIC 
# MAGIC // Assert that the Databricks Runtime is an ML Runtime
# MAGIC // def assertIsMlRuntime(testValue:String = null):Unit = {
# MAGIC   
# MAGIC //   val sourceValue = if (testValue != null) testValue
# MAGIC //   else getRuntimeVersion()
# MAGIC 
# MAGIC //   if (sourceValue.contains("-ml-") == false) {
# MAGIC //     throw new AssertionError(s"This notebook must be ran on a Databricks ML Runtime, found $sourceValue.")
# MAGIC //   }
# MAGIC // }
# MAGIC 
# MAGIC 
# MAGIC //**********************************
# MAGIC // LEGACY TESTING FUNCTIONS
# MAGIC //********************************getDbrMajorAndMinorVersions**
# MAGIC 
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC 
# MAGIC // Test results map to store results
# MAGIC val testResults = scala.collection.mutable.Map[String, (Boolean, String)]()
# MAGIC 
# MAGIC // Hash a string value
# MAGIC def toHash(value:String):Int = {
# MAGIC   import org.apache.spark.sql.functions.hash
# MAGIC   import org.apache.spark.sql.functions.abs
# MAGIC   spark.createDataset(List(value)).select(abs(hash($"value")).cast("int")).as[Int].first()
# MAGIC }
# MAGIC 
# MAGIC // Clear the testResults map
# MAGIC def clearYourResults(passedOnly:Boolean = true):Unit = {
# MAGIC   val whats = testResults.keySet.toSeq.sorted
# MAGIC   for (what <- whats) {
# MAGIC     val passed = testResults(what)._1
# MAGIC     if (passed || passedOnly == false) testResults.remove(what)
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC // Validate DataFrame schema
# MAGIC def validateYourSchema(what:String, df:DataFrame, expColumnName:String, expColumnType:String = null):Unit = {
# MAGIC   val label = s"$expColumnName:$expColumnType"
# MAGIC   val key = s"$what contains $label"
# MAGIC   
# MAGIC   try{
# MAGIC     val actualTypeTemp = df.schema(expColumnName).dataType.typeName
# MAGIC     val actualType = if (actualTypeTemp.startsWith("decimal")) "decimal" else actualTypeTemp
# MAGIC     
# MAGIC     if (expColumnType == null) {
# MAGIC       testResults.put(key,(true, "validated"))
# MAGIC       println(s"""$key: validated""")
# MAGIC       
# MAGIC     } else if (actualType == expColumnType) {
# MAGIC       val answerStr = "%s:%s".format(expColumnName, actualType)
# MAGIC       testResults.put(key,(true, "validated"))
# MAGIC       println(s"""$key: validated""")
# MAGIC       
# MAGIC     } else {
# MAGIC       val answerStr = "%s:%s".format(expColumnName, actualType)
# MAGIC       testResults.put(key,(false, answerStr))
# MAGIC       println(s"""$key: NOT matching ($answerStr)""")
# MAGIC     }
# MAGIC   } catch {
# MAGIC     case e:java.lang.IllegalArgumentException => {
# MAGIC       testResults.put(key,(false, "-not found-"))
# MAGIC       println(s"$key: NOT found")
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC // Validate an answer
# MAGIC def validateYourAnswer(what:String, expectedHash:Int, answer:Any):Unit = {
# MAGIC   // Convert the value to string, remove new lines and carriage returns and then escape quotes
# MAGIC   val answerStr = if (answer == null) "null" 
# MAGIC   else answer.toString
# MAGIC 
# MAGIC   val hashValue = toHash(answerStr)
# MAGIC 
# MAGIC   if (hashValue == expectedHash) {
# MAGIC     testResults.put(what,(true, answerStr))
# MAGIC     println(s"""$what was correct, your answer: ${answerStr}""")
# MAGIC   } else{
# MAGIC     testResults.put(what,(false, answerStr))
# MAGIC     println(s"""$what was NOT correct, your answer: ${answerStr}""")
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC // Summarize results in the testResults function
# MAGIC def summarizeYourResults():Unit = {
# MAGIC   var html = """<html><body><div style="font-weight:bold; font-size:larger; border-bottom: 1px solid #f0f0f0">Your Answers</div><table style='margin:0'>"""
# MAGIC 
# MAGIC   val whats = testResults.keySet.toSeq.sorted
# MAGIC   for (what <- whats) {
# MAGIC     val passed = testResults(what)._1
# MAGIC     val answer = testResults(what)._2
# MAGIC     val color = if (passed) "green" else "red" 
# MAGIC     val passFail = if (passed) "passed" else "FAILED" 
# MAGIC     html += s"""<tr style='font-size:larger; white-space:pre'>
# MAGIC                   <td>${what}:&nbsp;&nbsp;</td>
# MAGIC                   <td style="color:${color}; text-align:center; font-weight:bold">${passFail}</td>
# MAGIC                   <td style="white-space:pre; font-family: monospace">&nbsp;&nbsp;${answer}</td>
# MAGIC                 </tr>"""
# MAGIC   }
# MAGIC   html += "</table></body></html>"
# MAGIC   displayHTML(html)
# MAGIC }
# MAGIC 
# MAGIC // Log test results to a file
# MAGIC def logYourTest(path:String, name:String, value:Double):Unit = {
# MAGIC   if (path.contains("\"")) throw new AssertionError("The name cannot contain quotes.")
# MAGIC   
# MAGIC   dbutils.fs.mkdirs(path)
# MAGIC 
# MAGIC   val csv = """ "%s","%s" """.format(name, value).trim()
# MAGIC   val file = "%s/%s.csv".format(path, name).replace(" ", "-").toLowerCase
# MAGIC   dbutils.fs.put(file, csv, true)
# MAGIC }
# MAGIC 
# MAGIC // Load the test results log file
# MAGIC def loadYourTestResults(path:String):org.apache.spark.sql.DataFrame = {
# MAGIC   return spark.read.schema("name string, value double").csv(path)
# MAGIC }
# MAGIC 
# MAGIC // Load the test result log file into map
# MAGIC def loadYourTestMap(path:String):scala.collection.mutable.Map[String,Double] = {
# MAGIC   case class TestResult(name:String, value:Double)
# MAGIC   val rows = loadYourTestResults(path).collect()
# MAGIC   
# MAGIC   val map = scala.collection.mutable.Map[String,Double]()
# MAGIC   for (row <- rows) map.put(row.getString(0), row.getDouble(1))
# MAGIC   
# MAGIC   return map
# MAGIC }
# MAGIC 
# MAGIC //**********************************
# MAGIC // USER DATABASE FUNCTIONS
# MAGIC //**********************************
# MAGIC 
# MAGIC def getDatabaseName(courseType:String, username:String, moduleName:String, lessonName:String):String = {
# MAGIC   val langType = "s" // for scala
# MAGIC   var databaseName = username + "_" + moduleName + "_" + lessonName + "_" + langType + courseType
# MAGIC   databaseName = databaseName.toLowerCase
# MAGIC   databaseName = databaseName.replaceAll("[^a-zA-Z0-9]", "_")
# MAGIC   return databaseName.replace("__", "_").replace("__", "_").replace("__", "_").replace("__", "_")
# MAGIC }
# MAGIC 
# MAGIC // Create a user-specific database
# MAGIC def createUserDatabase(courseType:String, username:String, moduleName:String, lessonName:String):String = {
# MAGIC   val databaseName = getDatabaseName(courseType, username, moduleName, lessonName)
# MAGIC 
# MAGIC   spark.sql("CREATE DATABASE IF NOT EXISTS %s".format(databaseName))
# MAGIC   spark.sql("USE %s".format(databaseName))
# MAGIC 
# MAGIC   return databaseName
# MAGIC }
# MAGIC 
# MAGIC //**********************************
# MAGIC // ML FLOW UTILITY FUNCTIONS
# MAGIC //**********************************
# MAGIC 
# MAGIC // Create the experiment ID from available notebook information
# MAGIC def getExperimentId(): Long = {
# MAGIC   val notebookId = getTag("notebookId", null)
# MAGIC   val jobId = getTag("jobId", null)
# MAGIC   
# MAGIC   
# MAGIC   val retval = (notebookId != null) match { 
# MAGIC       case true => notebookId.toLong
# MAGIC       case false => (jobId != null) match { 
# MAGIC         case true => jobId.toLong
# MAGIC         case false => 0
# MAGIC       }
# MAGIC   }
# MAGIC   
# MAGIC   spark.conf.set("com.databricks.training.experimentId", retval)
# MAGIC   retval
# MAGIC }
# MAGIC 
# MAGIC // ****************************************************************************
# MAGIC // Utility method to determine whether a path exists
# MAGIC // ****************************************************************************
# MAGIC 
# MAGIC def pathExists(path:String):Boolean = {
# MAGIC   try {
# MAGIC     dbutils.fs.ls(path)
# MAGIC     return true
# MAGIC   } catch{
# MAGIC     case e: Exception => return false
# MAGIC   } 
# MAGIC }
# MAGIC 
# MAGIC // ****************************************************************************
# MAGIC // Utility method for recursive deletes
# MAGIC // Note: dbutils.fs.rm() does not appear to be truely recursive
# MAGIC // ****************************************************************************
# MAGIC 
# MAGIC def deletePath(path:String):Unit = {
# MAGIC   val files = dbutils.fs.ls(path)
# MAGIC 
# MAGIC   for (file <- files) {
# MAGIC     val deleted = dbutils.fs.rm(file.path, true)
# MAGIC     
# MAGIC     if (deleted == false) {
# MAGIC       if (file.isDir) {
# MAGIC         deletePath(file.path)
# MAGIC       } else {
# MAGIC         throw new java.io.IOException("Unable to delete file: " + file.path)
# MAGIC       }
# MAGIC     }
# MAGIC   }
# MAGIC   
# MAGIC   if (dbutils.fs.rm(path, true) == false) {
# MAGIC     throw new java.io.IOException("Unable to delete directory: " + path)
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC // ****************************************************************************
# MAGIC // Utility method to clean up the workspace at the end of a lesson
# MAGIC // ****************************************************************************
# MAGIC 
# MAGIC def classroomCleanup(daLogger:DatabricksAcademyLogger, courseType:String, username:String, moduleName:String, lessonName:String, dropDatabase: Boolean):Unit = {
# MAGIC   
# MAGIC   var actions = ""
# MAGIC 
# MAGIC   // Stop any active streams
# MAGIC   for (stream <- spark.streams.active) {
# MAGIC     stream.stop()
# MAGIC     
# MAGIC     // Wait for the stream to stop
# MAGIC     var queries = spark.streams.active.filter(_.name == stream.name)
# MAGIC 
# MAGIC     while (queries.length > 0) {
# MAGIC       Thread.sleep(5*1000) // Give it a couple of seconds
# MAGIC       queries = spark.streams.active.filter(_.name == stream.name)
# MAGIC     }
# MAGIC 
# MAGIC     actions += s"""<li>Terminated stream: <b>${stream.name}</b></li>"""
# MAGIC   }
# MAGIC   
# MAGIC   // Drop the tables only from specified database
# MAGIC   val database = getDatabaseName(courseType, username, moduleName, lessonName)
# MAGIC   try {
# MAGIC     val tables = spark.sql(s"show tables from $database").select("tableName").collect()
# MAGIC     for (row <- tables){
# MAGIC       var tableName = row.getAs[String]("tableName")
# MAGIC       spark.sql("drop table if exists %s.%s".format(database, tableName))
# MAGIC 
# MAGIC       // In some rare cases the files don't actually get removed.
# MAGIC       Thread.sleep(1000) // Give it just a second...
# MAGIC       val hivePath = "dbfs:/user/hive/warehouse/%s.db/%s".format(database, tableName)
# MAGIC       dbutils.fs.rm(hivePath, true) // Ignoring the delete's success or failure
# MAGIC       
# MAGIC       actions += s"""<li>Dropped table: <b>${tableName}</b></li>"""
# MAGIC     } 
# MAGIC   } catch {
# MAGIC     case _: Exception => () // ignored
# MAGIC   }
# MAGIC 
# MAGIC   // Drop the database if instructed to
# MAGIC   if (dropDatabase){
# MAGIC     spark.sql(s"DROP DATABASE IF EXISTS $database CASCADE")
# MAGIC 
# MAGIC     // In some rare cases the files don't actually get removed.
# MAGIC     Thread.sleep(1000) // Give it just a second...
# MAGIC     val hivePath = "dbfs:/user/hive/warehouse/%s.db".format(database)
# MAGIC     dbutils.fs.rm(hivePath, true) // Ignoring the delete's success or failure
# MAGIC     
# MAGIC     actions += s"""<li>Dropped database: <b>${database}</b></li>"""
# MAGIC   }
# MAGIC   
# MAGIC   // Remove files created from previous runs
# MAGIC   val path = getWorkingDir(courseType)
# MAGIC   if (pathExists(path)) {
# MAGIC     deletePath(path)
# MAGIC 
# MAGIC     actions += s"""<li>Removed working directory: <b>$path</b></li>"""
# MAGIC   }
# MAGIC   
# MAGIC   var htmlMsg = "Cleaning up the learning environment..."
# MAGIC   if (actions.length == 0) htmlMsg += "no actions taken."
# MAGIC   else htmlMsg += s"<ul>$actions</ul>"
# MAGIC   displayHTML(htmlMsg)
# MAGIC   
# MAGIC   if (dropDatabase) daLogger.logEvent("Classroom-Cleanup-Final")
# MAGIC   else daLogger.logEvent("Classroom-Cleanup-Preliminary")
# MAGIC }
# MAGIC 
# MAGIC // ****************************************************************************
# MAGIC // Utility method to delete a database
# MAGIC // ****************************************************************************
# MAGIC 
# MAGIC def deleteTables(database:String):Unit = {
# MAGIC   spark.sql("DROP DATABASE IF EXISTS %s CASCADE".format(database))
# MAGIC }
# MAGIC 
# MAGIC // ****************************************************************************
# MAGIC // DatabricksAcademyLogger and Student Feedback
# MAGIC // ****************************************************************************
# MAGIC 
# MAGIC class DatabricksAcademyLogger() extends org.apache.spark.scheduler.SparkListener {
# MAGIC   import org.apache.spark.scheduler._
# MAGIC 
# MAGIC   val hostname = "https://rqbr3jqop0.execute-api.us-west-2.amazonaws.com/prod"
# MAGIC 
# MAGIC   def logEvent(eventId: String, message: String = null):Unit = {
# MAGIC     import org.apache.http.entity._
# MAGIC     import org.apache.http.impl.client.{HttpClients, BasicResponseHandler}
# MAGIC     import org.apache.http.client.methods.HttpPost
# MAGIC     import java.net.URLEncoder.encode
# MAGIC     import org.json4s.jackson.Serialization
# MAGIC     implicit val formats = org.json4s.DefaultFormats
# MAGIC 
# MAGIC     val start = System.currentTimeMillis // Start the clock
# MAGIC 
# MAGIC     var client:org.apache.http.impl.client.CloseableHttpClient = null
# MAGIC 
# MAGIC     try {
# MAGIC       val utf8 = java.nio.charset.StandardCharsets.UTF_8.toString;
# MAGIC       val username = encode(getUsername(), utf8)
# MAGIC       val moduleName = encode(getModuleName(), utf8)
# MAGIC       val lessonName = encode(getLessonName(), utf8)
# MAGIC       val event = encode(eventId, utf8)
# MAGIC       val url = s"$hostname/logger"
# MAGIC     
# MAGIC       val content = Map(
# MAGIC         "tags" ->       getTags().map(x => (x._1.name, s"${x._2}")),
# MAGIC         "moduleName" -> getModuleName(),
# MAGIC         "lessonName" -> getLessonName(),
# MAGIC         "orgId" ->      getTag("orgId", "unknown"),
# MAGIC         "username" ->   getUsername(),
# MAGIC         "eventId" ->    eventId,
# MAGIC         "eventTime" ->  s"${System.currentTimeMillis}",
# MAGIC         "language" ->   getTag("notebookLanguage", "unknown"),
# MAGIC         "notebookId" -> getTag("notebookId", "unknown"),
# MAGIC         "sessionId" ->  getTag("sessionId", "unknown"),
# MAGIC         "message" ->    message
# MAGIC       )
# MAGIC       
# MAGIC       val client = HttpClients.createDefault()
# MAGIC       val httpPost = new HttpPost(url)
# MAGIC       val entity = new StringEntity(Serialization.write(content))      
# MAGIC 
# MAGIC       httpPost.setEntity(entity)
# MAGIC       httpPost.setHeader("Accept", "application/json")
# MAGIC       httpPost.setHeader("Content-type", "application/json")
# MAGIC 
# MAGIC       val response = client.execute(httpPost)
# MAGIC       
# MAGIC       val duration = System.currentTimeMillis - start // Stop the clock
# MAGIC       org.apache.log4j.Logger.getLogger(getClass).info(s"DAL-$eventId-SUCCESS: Event completed in $duration ms")
# MAGIC       
# MAGIC     } catch {
# MAGIC       case e:Exception => {
# MAGIC         val duration = System.currentTimeMillis - start // Stop the clock
# MAGIC         val msg = s"DAL-$eventId-FAILURE: Event completed in $duration ms"
# MAGIC         org.apache.log4j.Logger.getLogger(getClass).error(msg, e)
# MAGIC       }
# MAGIC     } finally {
# MAGIC       if (client != null) {
# MAGIC         try { client.close() } 
# MAGIC         catch { case _:Exception => () }
# MAGIC       }
# MAGIC     }
# MAGIC   }
# MAGIC   override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = logEvent("JobEnd: " + jobEnd.jobId)
# MAGIC   override def onJobStart(jobStart: SparkListenerJobStart): Unit = logEvent("JobStart: " + jobStart.jobId)
# MAGIC }
# MAGIC 
# MAGIC def showStudentSurvey():Unit = {
# MAGIC   val html = renderStudentSurvey()
# MAGIC   displayHTML(html);
# MAGIC }
# MAGIC 
# MAGIC def renderStudentSurvey():String = {
# MAGIC   import java.net.URLEncoder.encode
# MAGIC   val utf8 = java.nio.charset.StandardCharsets.UTF_8.toString;
# MAGIC   val username = encode(getUsername(), utf8)
# MAGIC   val userhome = encode(getUserhome(), utf8)
# MAGIC 
# MAGIC   val moduleName = encode(getModuleName(), utf8)
# MAGIC   val lessonName = encode(getLessonName(), utf8)
# MAGIC   val lessonNameUnencoded = getLessonName()
# MAGIC   
# MAGIC   val apiEndpoint = "https://rqbr3jqop0.execute-api.us-west-2.amazonaws.com/prod"
# MAGIC 
# MAGIC   import scala.collection.Map
# MAGIC   import org.json4s.DefaultFormats
# MAGIC   import org.json4s.jackson.JsonMethods._
# MAGIC   import org.json4s.jackson.Serialization.write
# MAGIC   import org.json4s.JsonDSL._
# MAGIC 
# MAGIC   implicit val formats: DefaultFormats = DefaultFormats
# MAGIC 
# MAGIC   val feedbackUrl = s"$apiEndpoint/feedback";
# MAGIC   
# MAGIC   val html = """
# MAGIC   <html>
# MAGIC   <head>
# MAGIC     <script src="https://files.training.databricks.com/static/js/classroom-support.min.js"></script>
# MAGIC     <script>
# MAGIC <!--    
# MAGIC       window.setTimeout( // Defer until bootstrap has enough time to async load
# MAGIC         () => { 
# MAGIC           $("#divComment").css("display", "visible");
# MAGIC 
# MAGIC           // Emulate radio-button like feature for multiple_choice
# MAGIC           $(".multiple_choicex").on("click", (evt) => {
# MAGIC                 const container = $(evt.target).parent();
# MAGIC                 $(".multiple_choicex").removeClass("checked"); 
# MAGIC                 $(".multiple_choicex").removeClass("checkedRed"); 
# MAGIC                 $(".multiple_choicex").removeClass("checkedGreen"); 
# MAGIC                 container.addClass("checked"); 
# MAGIC                 if (container.hasClass("thumbsDown")) { 
# MAGIC                     container.addClass("checkedRed"); 
# MAGIC                 } else { 
# MAGIC                     container.addClass("checkedGreen"); 
# MAGIC                 };
# MAGIC                 
# MAGIC                 // Send the like/dislike before the comment is shown so we at least capture that.
# MAGIC                 // In analysis, always take the latest feedback for a module (if they give a comment, it will resend the like/dislike)
# MAGIC                 var json = {
# MAGIC                   moduleName:  "GET_MODULE_NAME", 
# MAGIC                   lessonName:  "GET_LESSON_NAME", 
# MAGIC                   orgId:       "GET_ORG_ID",
# MAGIC                   username:    "GET_USERNAME",
# MAGIC                   language:    "scala",
# MAGIC                   notebookId:  "GET_NOTEBOOK_ID",
# MAGIC                   sessionId:   "GET_SESSION_ID",
# MAGIC                   survey: $(".multiple_choicex.checked").attr("value"), 
# MAGIC                   comment: $("#taComment").val() 
# MAGIC                 };
# MAGIC                 
# MAGIC                 $("#vote-response").html("Recording your vote...");
# MAGIC 
# MAGIC                 $.ajax({
# MAGIC                   type: "PUT", 
# MAGIC                   url: "FEEDBACK_URL", 
# MAGIC                   data: JSON.stringify(json),
# MAGIC                   dataType: "json",
# MAGIC                   processData: false
# MAGIC                 }).done(function() {
# MAGIC                   $("#vote-response").html("Thank you for your vote!<br/>Please feel free to share more if you would like to...");
# MAGIC                   $("#divComment").show("fast");
# MAGIC                 }).fail(function() {
# MAGIC                   $("#vote-response").html("There was an error submitting your vote");
# MAGIC                 }); // End of .ajax chain
# MAGIC           });
# MAGIC 
# MAGIC 
# MAGIC            // Set click handler to do a PUT
# MAGIC           $("#btnSubmit").on("click", (evt) => {
# MAGIC               // Use .attr("value") instead of .val() - this is not a proper input box
# MAGIC               var json = {
# MAGIC                 moduleName:  "GET_MODULE_NAME", 
# MAGIC                 lessonName:  "GET_LESSON_NAME", 
# MAGIC                 orgId:       "GET_ORG_ID",
# MAGIC                 username:    "GET_USERNAME",
# MAGIC                 language:    "scala",
# MAGIC                 notebookId:  "GET_NOTEBOOK_ID",
# MAGIC                 sessionId:   "GET_SESSION_ID",
# MAGIC                 survey: $(".multiple_choicex.checked").attr("value"), 
# MAGIC                 comment: $("#taComment").val() 
# MAGIC               };
# MAGIC 
# MAGIC               $("#feedback-response").html("Sending feedback...");
# MAGIC 
# MAGIC               $.ajax({
# MAGIC                 type: "PUT", 
# MAGIC                 url: "FEEDBACK_URL", 
# MAGIC                 data: JSON.stringify(json),
# MAGIC                 dataType: "json",
# MAGIC                 processData: false
# MAGIC               }).done(function() {
# MAGIC                   $("#feedback-response").html("Thank you for your feedback!");
# MAGIC               }).fail(function() {
# MAGIC                   $("#feedback-response").html("There was an error submitting your feedback");
# MAGIC               }); // End of .ajax chain
# MAGIC           });
# MAGIC         }, 2000
# MAGIC       );
# MAGIC -->
# MAGIC     </script>    
# MAGIC     <style>
# MAGIC .multiple_choicex > img {    
# MAGIC     border: 5px solid white;
# MAGIC     border-radius: 5px;
# MAGIC }
# MAGIC .multiple_choicex.choice1 > img:hover {    
# MAGIC     border-color: green;
# MAGIC     background-color: green;
# MAGIC }
# MAGIC .multiple_choicex.choice2 > img:hover {    
# MAGIC     border-color: red;
# MAGIC     background-color: red;
# MAGIC }
# MAGIC .multiple_choicex {
# MAGIC     border: 0.5em solid white;
# MAGIC     background-color: white;
# MAGIC     border-radius: 5px;
# MAGIC }
# MAGIC .multiple_choicex.checkedGreen {
# MAGIC     border-color: green;
# MAGIC     background-color: green;
# MAGIC }
# MAGIC .multiple_choicex.checkedRed {
# MAGIC     border-color: red;
# MAGIC     background-color: red;
# MAGIC }
# MAGIC     </style>
# MAGIC   </head>
# MAGIC   <body>
# MAGIC     <h2 style="font-size:28px; line-height:34.3px"><img style="vertical-align:middle" src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"/>What did you think?</h2>
# MAGIC     <p>Please let us know if you liked this notebook, <b>LESSON_NAME_UNENCODED</b></p>
# MAGIC     <div id="feedback" style="clear:both;display:table;">
# MAGIC       <span class="multiple_choicex choice1 thumbsUp" value="positive"><img style="width:100px" src="https://files.training.databricks.com/images/feedback/thumbs-up.png"/></span>
# MAGIC       <span class="multiple_choicex choice2 thumbsDown" value="negative"><img style="width:100px" src="https://files.training.databricks.com/images/feedback/thumbs-down.png"/></span>
# MAGIC       <div id="vote-response" style="color:green; margin:1em 0; font-weight:bold">&nbsp;</div>
# MAGIC       <table id="divComment" style="display:none; border-collapse:collapse;">
# MAGIC         <tr>
# MAGIC           <td style="padding:0"><textarea id="taComment" placeholder="How can we make this lesson better? (optional)" style="height:4em;width:30em;display:block"></textarea></td>
# MAGIC           <td style="padding:0"><button id="btnSubmit" style="margin-left:1em">Send</button></td>
# MAGIC         </tr>
# MAGIC       </table>
# MAGIC     </div>
# MAGIC     <div id="feedback-response" style="color:green; margin-top:1em; font-weight:bold">&nbsp;</div>
# MAGIC   </body>
# MAGIC   </html>
# MAGIC   """
# MAGIC 
# MAGIC   return html.replaceAll("GET_MODULE_NAME", getModuleName())
# MAGIC              .replaceAll("GET_LESSON_NAME", getLessonName())
# MAGIC              .replaceAll("GET_ORG_ID", getTag("orgId", "unknown"))
# MAGIC              .replaceAll("GET_USERNAME", getUsername())
# MAGIC              .replaceAll("GET_NOTEBOOK_ID", getTag("notebookId", "unknown"))
# MAGIC              .replaceAll("GET_SESSION_ID", getTag("sessionId", "unknown"))
# MAGIC              .replaceAll("LESSON_NAME_UNENCODED", lessonNameUnencoded)
# MAGIC              .replaceAll("FEEDBACK_URL", feedbackUrl)
# MAGIC 
# MAGIC }
# MAGIC 
# MAGIC // ****************************************************************************
# MAGIC // Facility for advertising functions, variables and databases to the student
# MAGIC // ****************************************************************************
# MAGIC def allDone(advertisements: scala.collection.mutable.Map[String,(String,String,String)]):Unit = {
# MAGIC   
# MAGIC   var functions = scala.collection.mutable.Map[String,(String,String,String)]()
# MAGIC   var variables = scala.collection.mutable.Map[String,(String,String,String)]()
# MAGIC   var databases = scala.collection.mutable.Map[String,(String,String,String)]()
# MAGIC   
# MAGIC   for ( (key,value) <- advertisements) {
# MAGIC     if (value._1 == "f" && spark.conf.get(s"com.databricks.training.suppress.${key}", null) != "true") {
# MAGIC       functions += (key -> value)
# MAGIC     }
# MAGIC   }
# MAGIC   
# MAGIC   for ( (key,value) <- advertisements) {
# MAGIC     if (value._1 == "v" && spark.conf.get(s"com.databricks.training.suppress.${key}", null) != "true") {
# MAGIC       variables += (key -> value)
# MAGIC     }
# MAGIC   }
# MAGIC   
# MAGIC   for ( (key,value) <- advertisements) {
# MAGIC     if (value._1 == "d" && spark.conf.get(s"com.databricks.training.suppress.${key}", null) != "true") {
# MAGIC       databases += (key -> value)
# MAGIC     }
# MAGIC   }
# MAGIC   
# MAGIC   var html = ""
# MAGIC   if (functions.size > 0) {
# MAGIC     html += "The following functions were defined for you:<ul style='margin-top:0'>"
# MAGIC     for ( (key,value) <- functions) {
# MAGIC       html += s"""<li style="cursor:help" onclick="document.getElementById('${key}').style.display='block'">
# MAGIC         <span style="color: green; font-weight:bold">${key}</span>
# MAGIC         <span style="font-weight:bold">(</span>
# MAGIC         <span style="color: green; font-weight:bold; font-style:italic">${value._2}</span>
# MAGIC         <span style="font-weight:bold">)</span>
# MAGIC         <div id="${key}" style="display:none; margin:0.5em 0; border-left: 3px solid grey; padding-left: 0.5em">${value._3}</div>
# MAGIC         </li>"""
# MAGIC     }
# MAGIC     html += "</ul>"
# MAGIC   }
# MAGIC   if (variables.size > 0) {
# MAGIC     html += "The following variables were defined for you:<ul style='margin-top:0'>"
# MAGIC     for ( (key,value) <- variables) {
# MAGIC       html += s"""<li style="cursor:help" onclick="document.getElementById('${key}').style.display='block'">
# MAGIC         <span style="color: green; font-weight:bold">${key}</span>: <span style="font-style:italic; font-weight:bold">${value._2} </span>
# MAGIC         <div id="${key}" style="display:none; margin:0.5em 0; border-left: 3px solid grey; padding-left: 0.5em">${value._3}</div>
# MAGIC         </li>"""
# MAGIC     }
# MAGIC     html += "</ul>"
# MAGIC   }
# MAGIC   if (databases.size > 0) {
# MAGIC     html += "The following database were created for you:<ul style='margin-top:0'>"
# MAGIC     for ( (key,value) <- databases) {
# MAGIC       html += s"""<li style="cursor:help" onclick="document.getElementById('${key}').style.display='block'">
# MAGIC         Now using the database identified by <span style="color: green; font-weight:bold">${key}</span>: 
# MAGIC         <div style="font-style:italic; font-weight:bold">${value._2}</div>
# MAGIC         <div id="${key}" style="display:none; margin:0.5em 0; border-left: 3px solid grey; padding-left: 0.5em">${value._3}</div>
# MAGIC         </li>"""
# MAGIC     }
# MAGIC     html += "</ul>"
# MAGIC   }
# MAGIC   html += "All done!"
# MAGIC   displayHTML(html)
# MAGIC }
# MAGIC 
# MAGIC // ****************************************************************************
# MAGIC // Placeholder variables for coding challenge type specification
# MAGIC // ****************************************************************************
# MAGIC object FILL_IN {
# MAGIC   val VALUE = null
# MAGIC   val ARRAY = Array(Row())
# MAGIC   val SCHEMA = org.apache.spark.sql.types.StructType(List())
# MAGIC   val ROW = Row()
# MAGIC   val LONG: Long = 0
# MAGIC   val INT: Int = 0
# MAGIC   def DATAFRAME = spark.emptyDataFrame
# MAGIC   def DATASET = spark.createDataset(Seq(""))
# MAGIC }
# MAGIC 
# MAGIC // ****************************************************************************
# MAGIC // Initialize the logger so that it can be used down-stream
# MAGIC // ****************************************************************************
# MAGIC val daLogger = new DatabricksAcademyLogger()
# MAGIC // if (spark.conf.get("com.databricks.training.studentStatsService.registered", null) != "registered") {
# MAGIC //   sc.addSparkListener(daLogger)
# MAGIC //   spark.conf.set("com.databricks.training.studentStatsService", "registered")
# MAGIC // }
# MAGIC daLogger.logEvent("Initialized", "Initialized the Scala DatabricksAcademyLogger")
# MAGIC 
# MAGIC displayHTML("Defining courseware-specific utility methods...")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC #############################################
# MAGIC # TAG API FUNCTIONS
# MAGIC #############################################
# MAGIC 
# MAGIC # Get all tags
# MAGIC def getTags() -> dict: 
# MAGIC   return sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
# MAGIC     dbutils.entry_point.getDbutils().notebook().getContext().tags()
# MAGIC   )
# MAGIC 
# MAGIC # Get a single tag's value
# MAGIC def getTag(tagName: str, defaultValue: str = None) -> str:
# MAGIC   values = getTags()[tagName]
# MAGIC   try:
# MAGIC     if len(values) > 0:
# MAGIC       return values
# MAGIC   except:
# MAGIC     return defaultValue
# MAGIC 
# MAGIC #############################################
# MAGIC # Get Databricks runtime major and minor versions
# MAGIC #############################################
# MAGIC 
# MAGIC def getDbrMajorAndMinorVersions() -> (int, int):
# MAGIC   import os
# MAGIC   dbrVersion = os.environ["DATABRICKS_RUNTIME_VERSION"]
# MAGIC   dbrVersion = dbrVersion.split(".")
# MAGIC   return (int(dbrVersion[0]), int(dbrVersion[1]))
# MAGIC 
# MAGIC # Get Python version
# MAGIC def getPythonVersion() -> str:
# MAGIC   import sys
# MAGIC   pythonVersion = sys.version[0:sys.version.index(" ")]
# MAGIC   spark.conf.set("com.databricks.training.python-version", pythonVersion)
# MAGIC   return pythonVersion
# MAGIC 
# MAGIC #############################################
# MAGIC # USER, USERNAME, AND USERHOME FUNCTIONS
# MAGIC #############################################
# MAGIC 
# MAGIC # Get the user's username
# MAGIC def getUsername() -> str:
# MAGIC   import uuid
# MAGIC   try:
# MAGIC     return dbutils.widgets.get("databricksUsername")
# MAGIC   except:
# MAGIC     return getTag("user", str(uuid.uuid1()).replace("-", ""))
# MAGIC 
# MAGIC # Get the user's userhome
# MAGIC def getUserhome() -> str:
# MAGIC   username = getUsername()
# MAGIC   return "dbfs:/user/{}".format(username)
# MAGIC 
# MAGIC def getModuleName() -> str: 
# MAGIC   # This will/should fail if module-name is not defined in the Classroom-Setup notebook
# MAGIC   return spark.conf.get("com.databricks.training.module-name")
# MAGIC 
# MAGIC def getLessonName() -> str:
# MAGIC   # If not specified, use the notebook's name.
# MAGIC   return dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None).split("/")[-1]
# MAGIC 
# MAGIC def getWorkingDir(courseType:str) -> str:
# MAGIC   import re
# MAGIC   langType = "p" # for python
# MAGIC   moduleName = re.sub(r"[^a-zA-Z0-9]", "_", getModuleName()).lower()
# MAGIC   lessonName = re.sub(r"[^a-zA-Z0-9]", "_", getLessonName()).lower()
# MAGIC   workingDir = "{}/{}/{}_{}{}".format(getUserhome(), moduleName, lessonName, langType, courseType)
# MAGIC   return workingDir.replace("__", "_").replace("__", "_").replace("__", "_").replace("__", "_")
# MAGIC 
# MAGIC 
# MAGIC #############################################
# MAGIC # VERSION ASSERTION FUNCTIONS
# MAGIC #############################################
# MAGIC 
# MAGIC # When migrating DBR versions this should be one
# MAGIC # of the only two places that needs to be updated
# MAGIC latestDbrMajor = 6
# MAGIC latestDbrMinor = 2
# MAGIC 
# MAGIC   # Assert an appropriate Databricks Runtime version
# MAGIC def assertDbrVersion(expected:str, latestMajor:int=latestDbrMajor, latestMinor:int=latestDbrMinor, display:bool = True):
# MAGIC   
# MAGIC   expMajor = latestMajor
# MAGIC   expMinor = latestMinor
# MAGIC   
# MAGIC   if expected and expected != "{{dbr}}":
# MAGIC     expMajor = int(expected.split(".")[0])
# MAGIC     expMinor = int(expected.split(".")[1])
# MAGIC 
# MAGIC   (major, minor) = getDbrMajorAndMinorVersions()
# MAGIC 
# MAGIC   if (major < expMajor) or (major == expMajor and minor < expMinor):
# MAGIC     msg = f"This notebook must be run on DBR {expMajor}.{expMinor} or newer. Your cluster is using {major}.{minor}. You must update your cluster configuration before proceeding."
# MAGIC 
# MAGIC     raise AssertionError(msg)
# MAGIC     
# MAGIC   if major != expMajor or minor != expMinor:
# MAGIC     html = f"""
# MAGIC       <div style="color:red; font-weight:bold">WARNING: This notebook was tested on DBR {expMajor}.{expMinor}, but we found DBR {major}.{minor}.</div>
# MAGIC       <div style="font-weight:bold">Using an untested DBR may yield unexpected results and/or various errors</div>
# MAGIC       <div style="font-weight:bold">Please update your cluster configuration and/or <a href="https://academy.databricks.com/" target="_blank">download a newer version of this course</a> before proceeding.</div>
# MAGIC     """
# MAGIC 
# MAGIC   else:
# MAGIC     html = f"Running on <b>DBR {major}.{minor}</b>"
# MAGIC   
# MAGIC   if display:
# MAGIC     displayHTML(html)
# MAGIC   else:
# MAGIC     print(html)
# MAGIC   
# MAGIC   return f"{major}.{minor}"
# MAGIC 
# MAGIC # Assert that the Databricks Runtime is ML version
# MAGIC # def assertIsMlRuntime(testValue: str = None):
# MAGIC 
# MAGIC #   if testValue is not None: sourceValue = testValue
# MAGIC #   else: sourceValue = getRuntimeVersion()
# MAGIC 
# MAGIC #   if "-ml-" not in sourceValue:
# MAGIC #     raise AssertionError(f"This notebook must be ran on a Databricks ML Runtime, found {sourceValue}.")
# MAGIC 
# MAGIC     
# MAGIC ############################################
# MAGIC # USER DATABASE FUNCTIONS
# MAGIC ############################################
# MAGIC 
# MAGIC def getDatabaseName(courseType:str, username:str, moduleName:str, lessonName:str) -> str:
# MAGIC   import re
# MAGIC   langType = "p" # for python
# MAGIC   databaseName = username + "_" + moduleName + "_" + lessonName + "_" + langType + courseType
# MAGIC   databaseName = databaseName.lower()
# MAGIC   databaseName = re.sub("[^a-zA-Z0-9]", "_", databaseName)
# MAGIC   return databaseName.replace("__", "_").replace("__", "_").replace("__", "_").replace("__", "_")
# MAGIC 
# MAGIC 
# MAGIC # Create a user-specific database
# MAGIC def createUserDatabase(courseType:str, username:str, moduleName:str, lessonName:str) -> str:
# MAGIC   databaseName = getDatabaseName(courseType, username, moduleName, lessonName)
# MAGIC 
# MAGIC   spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(databaseName))
# MAGIC   spark.sql("USE {}".format(databaseName))
# MAGIC 
# MAGIC   return databaseName
# MAGIC 
# MAGIC     
# MAGIC #############################################
# MAGIC # LEGACY TESTING FUNCTIONS
# MAGIC #############################################
# MAGIC 
# MAGIC # Test results dict to store results
# MAGIC testResults = dict()
# MAGIC 
# MAGIC # Hash a string value
# MAGIC def toHash(value):
# MAGIC   from pyspark.sql.functions import hash
# MAGIC   from pyspark.sql.functions import abs
# MAGIC   values = [(value,)]
# MAGIC   return spark.createDataFrame(values, ["value"]).select(abs(hash("value")).cast("int")).first()[0]
# MAGIC 
# MAGIC # Clear the testResults map
# MAGIC def clearYourResults(passedOnly = True):
# MAGIC   whats = list(testResults.keys())
# MAGIC   for what in whats:
# MAGIC     passed = testResults[what][0]
# MAGIC     if passed or passedOnly == False : del testResults[what]
# MAGIC 
# MAGIC # Validate DataFrame schema
# MAGIC def validateYourSchema(what, df, expColumnName, expColumnType = None):
# MAGIC   label = "{}:{}".format(expColumnName, expColumnType)
# MAGIC   key = "{} contains {}".format(what, label)
# MAGIC 
# MAGIC   try:
# MAGIC     actualType = df.schema[expColumnName].dataType.typeName()
# MAGIC     
# MAGIC     if expColumnType == None: 
# MAGIC       testResults[key] = (True, "validated")
# MAGIC       print("""{}: validated""".format(key))
# MAGIC     elif actualType == expColumnType:
# MAGIC       testResults[key] = (True, "validated")
# MAGIC       print("""{}: validated""".format(key))
# MAGIC     else:
# MAGIC       answerStr = "{}:{}".format(expColumnName, actualType)
# MAGIC       testResults[key] = (False, answerStr)
# MAGIC       print("""{}: NOT matching ({})""".format(key, answerStr))
# MAGIC   except:
# MAGIC       testResults[what] = (False, "-not found-")
# MAGIC       print("{}: NOT found".format(key))
# MAGIC 
# MAGIC # Validate an answer
# MAGIC def validateYourAnswer(what, expectedHash, answer):
# MAGIC   # Convert the value to string, remove new lines and carriage returns and then escape quotes
# MAGIC   if (answer == None): answerStr = "null"
# MAGIC   elif (answer is True): answerStr = "true"
# MAGIC   elif (answer is False): answerStr = "false"
# MAGIC   else: answerStr = str(answer)
# MAGIC 
# MAGIC   hashValue = toHash(answerStr)
# MAGIC   
# MAGIC   if (hashValue == expectedHash):
# MAGIC     testResults[what] = (True, answerStr)
# MAGIC     print("""{} was correct, your answer: {}""".format(what, answerStr))
# MAGIC   else:
# MAGIC     testResults[what] = (False, answerStr)
# MAGIC     print("""{} was NOT correct, your answer: {}""".format(what, answerStr))
# MAGIC 
# MAGIC # Summarize results in the testResults dict
# MAGIC def summarizeYourResults():
# MAGIC   html = """<html><body><div style="font-weight:bold; font-size:larger; border-bottom: 1px solid #f0f0f0">Your Answers</div><table style='margin:0'>"""
# MAGIC 
# MAGIC   whats = list(testResults.keys())
# MAGIC   whats.sort()
# MAGIC   for what in whats:
# MAGIC     passed = testResults[what][0]
# MAGIC     answer = testResults[what][1]
# MAGIC     color = "green" if (passed) else "red" 
# MAGIC     passFail = "passed" if (passed) else "FAILED" 
# MAGIC     html += """<tr style='font-size:larger; white-space:pre'>
# MAGIC                   <td>{}:&nbsp;&nbsp;</td>
# MAGIC                   <td style="color:{}; text-align:center; font-weight:bold">{}</td>
# MAGIC                   <td style="white-space:pre; font-family: monospace">&nbsp;&nbsp;{}</td>
# MAGIC                 </tr>""".format(what, color, passFail, answer)
# MAGIC   html += "</table></body></html>"
# MAGIC   displayHTML(html)
# MAGIC 
# MAGIC # Log test results to a file
# MAGIC def logYourTest(path, name, value):
# MAGIC   value = float(value)
# MAGIC   if "\"" in path: raise AssertionError("The name cannot contain quotes.")
# MAGIC   
# MAGIC   dbutils.fs.mkdirs(path)
# MAGIC 
# MAGIC   csv = """ "{}","{}" """.format(name, value).strip()
# MAGIC   file = "{}/{}.csv".format(path, name).replace(" ", "-").lower()
# MAGIC   dbutils.fs.put(file, csv, True)
# MAGIC 
# MAGIC # Load test results from log file
# MAGIC def loadYourTestResults(path):
# MAGIC   from pyspark.sql.functions import col
# MAGIC   return spark.read.schema("name string, value double").csv(path)
# MAGIC 
# MAGIC # Load test results from log file into a dict
# MAGIC def loadYourTestMap(path):
# MAGIC   rows = loadYourTestResults(path).collect()
# MAGIC   
# MAGIC   map = dict()
# MAGIC   for row in rows:
# MAGIC     map[row["name"]] = row["value"]
# MAGIC   
# MAGIC   return map
# MAGIC 
# MAGIC # ****************************************************************************
# MAGIC # Utility method to determine whether a path exists
# MAGIC # ****************************************************************************
# MAGIC 
# MAGIC def pathExists(path):
# MAGIC   try:
# MAGIC     dbutils.fs.ls(path)
# MAGIC     return True
# MAGIC   except:
# MAGIC     return False
# MAGIC   
# MAGIC # ****************************************************************************
# MAGIC # Utility method for recursive deletes
# MAGIC # Note: dbutils.fs.rm() does not appear to be truely recursive
# MAGIC # ****************************************************************************
# MAGIC 
# MAGIC def deletePath(path):
# MAGIC   files = dbutils.fs.ls(path)
# MAGIC 
# MAGIC   for file in files:
# MAGIC     deleted = dbutils.fs.rm(file.path, True)
# MAGIC     
# MAGIC     if deleted == False:
# MAGIC       if file.is_dir:
# MAGIC         deletePath(file.path)
# MAGIC       else:
# MAGIC         raise IOError("Unable to delete file: " + file.path)
# MAGIC   
# MAGIC   if dbutils.fs.rm(path, True) == False:
# MAGIC     raise IOError("Unable to delete directory: " + path)
# MAGIC 
# MAGIC # ****************************************************************************
# MAGIC # Utility method to clean up the workspace at the end of a lesson
# MAGIC # ****************************************************************************
# MAGIC 
# MAGIC def classroomCleanup(daLogger:object, courseType:str, username:str, moduleName:str, lessonName:str, dropDatabase:str): 
# MAGIC   import time
# MAGIC 
# MAGIC   actions = ""
# MAGIC   
# MAGIC   # Stop any active streams
# MAGIC   for stream in spark.streams.active:
# MAGIC     stream.stop()
# MAGIC     
# MAGIC     # Wait for the stream to stop
# MAGIC     queries = list(filter(lambda query: query.name == stream.name, spark.streams.active))
# MAGIC     
# MAGIC     while (len(queries) > 0):
# MAGIC       time.sleep(5) # Give it a couple of seconds
# MAGIC       queries = list(filter(lambda query: query.name == stream.name, spark.streams.active))
# MAGIC 
# MAGIC     actions += f"""<li>Terminated stream: <b>{stream.name}</b></li>"""
# MAGIC   
# MAGIC   # Drop all tables from the specified database
# MAGIC   database = getDatabaseName(courseType, username, moduleName, lessonName)
# MAGIC   try:
# MAGIC     tables = spark.sql("show tables from {}".format(database)).select("tableName").collect()
# MAGIC     for row in tables:
# MAGIC       tableName = row["tableName"]
# MAGIC       spark.sql("drop table if exists {}.{}".format(database, tableName))
# MAGIC 
# MAGIC       # In some rare cases the files don't actually get removed.
# MAGIC       time.sleep(1) # Give it just a second...
# MAGIC       hivePath = "dbfs:/user/hive/warehouse/{}.db/{}".format(database, tableName)
# MAGIC       dbutils.fs.rm(hivePath, True) # Ignoring the delete's success or failure
# MAGIC       
# MAGIC       actions += f"""<li>Dropped table: <b>{tableName}</b></li>"""
# MAGIC 
# MAGIC   except:
# MAGIC     pass # ignored
# MAGIC 
# MAGIC   # The database should only be dropped in a "cleanup" notebook, not "setup"
# MAGIC   if dropDatabase: 
# MAGIC     spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(database))
# MAGIC     
# MAGIC     # In some rare cases the files don't actually get removed.
# MAGIC     time.sleep(1) # Give it just a second...
# MAGIC     hivePath = "dbfs:/user/hive/warehouse/{}.db".format(database)
# MAGIC     dbutils.fs.rm(hivePath, True) # Ignoring the delete's success or failure
# MAGIC     
# MAGIC     actions += f"""<li>Dropped database: <b>{database}</b></li>"""
# MAGIC 
# MAGIC   # Remove any files that may have been created from previous runs
# MAGIC   path = getWorkingDir(courseType)
# MAGIC   if pathExists(path):
# MAGIC     deletePath(path)
# MAGIC 
# MAGIC     actions += f"""<li>Removed working directory: <b>{path}</b></li>"""
# MAGIC     
# MAGIC   htmlMsg = "Cleaning up the learning environment..."
# MAGIC   if len(actions) == 0: htmlMsg += "no actions taken."
# MAGIC   else:  htmlMsg += f"<ul>{actions}</ul>"
# MAGIC   displayHTML(htmlMsg)
# MAGIC   
# MAGIC   if dropDatabase: daLogger.logEvent("Classroom-Cleanup-Final")
# MAGIC   else: daLogger.logEvent("Classroom-Cleanup-Preliminary")
# MAGIC 
# MAGIC   
# MAGIC # Utility method to delete a database  
# MAGIC def deleteTables(database):
# MAGIC   spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(database))
# MAGIC   
# MAGIC     
# MAGIC # ****************************************************************************
# MAGIC # DatabricksAcademyLogger and Student Feedback
# MAGIC # ****************************************************************************
# MAGIC 
# MAGIC class DatabricksAcademyLogger:
# MAGIC   
# MAGIC   def logEvent(self, eventId: str, message: str = None):
# MAGIC     import time
# MAGIC     import json
# MAGIC     import requests
# MAGIC 
# MAGIC     hostname = "https://rqbr3jqop0.execute-api.us-west-2.amazonaws.com/prod"
# MAGIC     
# MAGIC     try:
# MAGIC       username = getUsername().encode("utf-8")
# MAGIC       moduleName = getModuleName().encode("utf-8")
# MAGIC       lessonName = getLessonName().encode("utf-8")
# MAGIC       event = eventId.encode("utf-8")
# MAGIC     
# MAGIC       content = {
# MAGIC         "tags":       dict(map(lambda x: (x[0], str(x[1])), getTags().items())),
# MAGIC         "moduleName": getModuleName(),
# MAGIC         "lessonName": getLessonName(),
# MAGIC         "orgId":      getTag("orgId", "unknown"),
# MAGIC         "username":   getUsername(),
# MAGIC         "eventId":    eventId,
# MAGIC         "eventTime":  f"{int(round(time.time() * 1000))}",
# MAGIC         "language":   getTag("notebookLanguage", "unknown"),
# MAGIC         "notebookId": getTag("notebookId", "unknown"),
# MAGIC         "sessionId":  getTag("sessionId", "unknown"),
# MAGIC         "message":    message
# MAGIC       }
# MAGIC       
# MAGIC       response = requests.post( 
# MAGIC           url=f"{hostname}/logger", 
# MAGIC           json=content,
# MAGIC           headers={
# MAGIC             "Accept": "application/json; charset=utf-8",
# MAGIC             "Content-Type": "application/json; charset=utf-8"
# MAGIC           })
# MAGIC       
# MAGIC     except Exception as e:
# MAGIC       pass
# MAGIC 
# MAGIC     
# MAGIC def showStudentSurvey():
# MAGIC   html = renderStudentSurvey()
# MAGIC   displayHTML(html);
# MAGIC 
# MAGIC def renderStudentSurvey():
# MAGIC   username = getUsername().encode("utf-8")
# MAGIC   userhome = getUserhome().encode("utf-8")
# MAGIC 
# MAGIC   moduleName = getModuleName().encode("utf-8")
# MAGIC   lessonName = getLessonName().encode("utf-8")
# MAGIC   lessonNameUnencoded = getLessonName()
# MAGIC   
# MAGIC   apiEndpoint = "https://rqbr3jqop0.execute-api.us-west-2.amazonaws.com/prod"
# MAGIC 
# MAGIC   feedbackUrl = f"{apiEndpoint}/feedback";
# MAGIC   
# MAGIC   html = """
# MAGIC   <html>
# MAGIC   <head>
# MAGIC     <script src="https://files.training.databricks.com/static/js/classroom-support.min.js"></script>
# MAGIC     <script>
# MAGIC <!--    
# MAGIC       window.setTimeout( // Defer until bootstrap has enough time to async load
# MAGIC         () => { 
# MAGIC           $("#divComment").css("display", "visible");
# MAGIC 
# MAGIC           // Emulate radio-button like feature for multiple_choice
# MAGIC           $(".multiple_choicex").on("click", (evt) => {
# MAGIC                 const container = $(evt.target).parent();
# MAGIC                 $(".multiple_choicex").removeClass("checked"); 
# MAGIC                 $(".multiple_choicex").removeClass("checkedRed"); 
# MAGIC                 $(".multiple_choicex").removeClass("checkedGreen"); 
# MAGIC                 container.addClass("checked"); 
# MAGIC                 if (container.hasClass("thumbsDown")) { 
# MAGIC                     container.addClass("checkedRed"); 
# MAGIC                 } else { 
# MAGIC                     container.addClass("checkedGreen"); 
# MAGIC                 };
# MAGIC                 
# MAGIC                 // Send the like/dislike before the comment is shown so we at least capture that.
# MAGIC                 // In analysis, always take the latest feedback for a module (if they give a comment, it will resend the like/dislike)
# MAGIC                 var json = {
# MAGIC                   moduleName: "GET_MODULE_NAME", 
# MAGIC                   lessonName: "GET_LESSON_NAME", 
# MAGIC                   orgId:       "GET_ORG_ID",
# MAGIC                   username:    "GET_USERNAME",
# MAGIC                   language:    "python",
# MAGIC                   notebookId:  "GET_NOTEBOOK_ID",
# MAGIC                   sessionId:   "GET_SESSION_ID",
# MAGIC                   survey: $(".multiple_choicex.checked").attr("value"), 
# MAGIC                   comment: $("#taComment").val() 
# MAGIC                 };
# MAGIC                 
# MAGIC                 $("#vote-response").html("Recording your vote...");
# MAGIC 
# MAGIC                 $.ajax({
# MAGIC                   type: "PUT", 
# MAGIC                   url: "FEEDBACK_URL", 
# MAGIC                   data: JSON.stringify(json),
# MAGIC                   dataType: "json",
# MAGIC                   processData: false
# MAGIC                 }).done(function() {
# MAGIC                   $("#vote-response").html("Thank you for your vote!<br/>Please feel free to share more if you would like to...");
# MAGIC                   $("#divComment").show("fast");
# MAGIC                 }).fail(function() {
# MAGIC                   $("#vote-response").html("There was an error submitting your vote");
# MAGIC                 }); // End of .ajax chain
# MAGIC           });
# MAGIC 
# MAGIC 
# MAGIC            // Set click handler to do a PUT
# MAGIC           $("#btnSubmit").on("click", (evt) => {
# MAGIC               // Use .attr("value") instead of .val() - this is not a proper input box
# MAGIC               var json = {
# MAGIC                 moduleName: "GET_MODULE_NAME", 
# MAGIC                 lessonName: "GET_LESSON_NAME", 
# MAGIC                 orgId:       "GET_ORG_ID",
# MAGIC                 username:    "GET_USERNAME",
# MAGIC                 language:    "python",
# MAGIC                 notebookId:  "GET_NOTEBOOK_ID",
# MAGIC                 sessionId:   "GET_SESSION_ID",
# MAGIC                 survey: $(".multiple_choicex.checked").attr("value"), 
# MAGIC                 comment: $("#taComment").val() 
# MAGIC               };
# MAGIC 
# MAGIC               $("#feedback-response").html("Sending feedback...");
# MAGIC 
# MAGIC               $.ajax({
# MAGIC                 type: "PUT", 
# MAGIC                 url: "FEEDBACK_URL", 
# MAGIC                 data: JSON.stringify(json),
# MAGIC                 dataType: "json",
# MAGIC                 processData: false
# MAGIC               }).done(function() {
# MAGIC                   $("#feedback-response").html("Thank you for your feedback!");
# MAGIC               }).fail(function() {
# MAGIC                   $("#feedback-response").html("There was an error submitting your feedback");
# MAGIC               }); // End of .ajax chain
# MAGIC           });
# MAGIC         }, 2000
# MAGIC       );
# MAGIC -->
# MAGIC     </script>    
# MAGIC     <style>
# MAGIC .multiple_choicex > img {    
# MAGIC     border: 5px solid white;
# MAGIC     border-radius: 5px;
# MAGIC }
# MAGIC .multiple_choicex.choice1 > img:hover {    
# MAGIC     border-color: green;
# MAGIC     background-color: green;
# MAGIC }
# MAGIC .multiple_choicex.choice2 > img:hover {    
# MAGIC     border-color: red;
# MAGIC     background-color: red;
# MAGIC }
# MAGIC .multiple_choicex {
# MAGIC     border: 0.5em solid white;
# MAGIC     background-color: white;
# MAGIC     border-radius: 5px;
# MAGIC }
# MAGIC .multiple_choicex.checkedGreen {
# MAGIC     border-color: green;
# MAGIC     background-color: green;
# MAGIC }
# MAGIC .multiple_choicex.checkedRed {
# MAGIC     border-color: red;
# MAGIC     background-color: red;
# MAGIC }
# MAGIC     </style>
# MAGIC   </head>
# MAGIC   <body>
# MAGIC     <h2 style="font-size:28px; line-height:34.3px"><img style="vertical-align:middle" src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"/>What did you think?</h2>
# MAGIC     <p>Please let us know if you liked this notebook, <b>LESSON_NAME_UNENCODED</b></p>
# MAGIC     <div id="feedback" style="clear:both;display:table;">
# MAGIC       <span class="multiple_choicex choice1 thumbsUp" value="positive"><img style="width:100px" src="https://files.training.databricks.com/images/feedback/thumbs-up.png"/></span>
# MAGIC       <span class="multiple_choicex choice2 thumbsDown" value="negative"><img style="width:100px" src="https://files.training.databricks.com/images/feedback/thumbs-down.png"/></span>
# MAGIC       <div id="vote-response" style="color:green; margin:1em 0; font-weight:bold">&nbsp;</div>
# MAGIC       <table id="divComment" style="display:none; border-collapse:collapse;">
# MAGIC         <tr>
# MAGIC           <td style="padding:0"><textarea id="taComment" placeholder="How can we make this lesson better? (optional)" style="height:4em;width:30em;display:block"></textarea></td>
# MAGIC           <td style="padding:0"><button id="btnSubmit" style="margin-left:1em">Send</button></td>
# MAGIC         </tr>
# MAGIC       </table>
# MAGIC     </div>
# MAGIC     <div id="feedback-response" style="color:green; margin-top:1em; font-weight:bold">&nbsp;</div>
# MAGIC   </body>
# MAGIC   </html>
# MAGIC   """
# MAGIC 
# MAGIC   return (html.replace("GET_MODULE_NAME", getModuleName())
# MAGIC               .replace("GET_LESSON_NAME", getLessonName())
# MAGIC               .replace("GET_ORG_ID", getTag("orgId", "unknown"))
# MAGIC               .replace("GET_USERNAME", getUsername())
# MAGIC               .replace("GET_NOTEBOOK_ID", getTag("notebookId", "unknown"))
# MAGIC               .replace("GET_SESSION_ID", getTag("sessionId", "unknown"))
# MAGIC               .replace("LESSON_NAME_UNENCODED", lessonNameUnencoded)
# MAGIC               .replace("FEEDBACK_URL", feedbackUrl)
# MAGIC          )
# MAGIC 
# MAGIC # ****************************************************************************
# MAGIC # Facility for advertising functions, variables and databases to the student
# MAGIC # ****************************************************************************
# MAGIC def allDone(advertisements):
# MAGIC   
# MAGIC   functions = dict()
# MAGIC   variables = dict()
# MAGIC   databases = dict()
# MAGIC   
# MAGIC   for key in advertisements:
# MAGIC     if advertisements[key][0] == "f" and spark.conf.get(f"com.databricks.training.suppress.{key}", None) != "true":
# MAGIC       functions[key] = advertisements[key]
# MAGIC   
# MAGIC   for key in advertisements:
# MAGIC     if advertisements[key][0] == "v" and spark.conf.get(f"com.databricks.training.suppress.{key}", None) != "true":
# MAGIC       variables[key] = advertisements[key]
# MAGIC   
# MAGIC   for key in advertisements:
# MAGIC     if advertisements[key][0] == "d" and spark.conf.get(f"com.databricks.training.suppress.{key}", None) != "true":
# MAGIC       databases[key] = advertisements[key]
# MAGIC   
# MAGIC   html = ""
# MAGIC   if len(functions) > 0:
# MAGIC     html += "The following functions were defined for you:<ul style='margin-top:0'>"
# MAGIC     for key in functions:
# MAGIC       value = functions[key]
# MAGIC       html += f"""<li style="cursor:help" onclick="document.getElementById('{key}').style.display='block'">
# MAGIC         <span style="color: green; font-weight:bold">{key}</span>
# MAGIC         <span style="font-weight:bold">(</span>
# MAGIC         <span style="color: green; font-weight:bold; font-style:italic">{value[1]}</span>
# MAGIC         <span style="font-weight:bold">)</span>
# MAGIC         <div id="{key}" style="display:none; margin:0.5em 0; border-left: 3px solid grey; padding-left: 0.5em">{value[2]}</div>
# MAGIC         </li>"""
# MAGIC     html += "</ul>"
# MAGIC 
# MAGIC   if len(variables) > 0:
# MAGIC     html += "The following variables were defined for you:<ul style='margin-top:0'>"
# MAGIC     for key in variables:
# MAGIC       value = variables[key]
# MAGIC       html += f"""<li style="cursor:help" onclick="document.getElementById('{key}').style.display='block'">
# MAGIC         <span style="color: green; font-weight:bold">{key}</span>: <span style="font-style:italic; font-weight:bold">{value[1]} </span>
# MAGIC         <div id="{key}" style="display:none; margin:0.5em 0; border-left: 3px solid grey; padding-left: 0.5em">{value[2]}</div>
# MAGIC         </li>"""
# MAGIC     html += "</ul>"
# MAGIC 
# MAGIC   if len(databases) > 0:
# MAGIC     html += "The following database were created for you:<ul style='margin-top:0'>"
# MAGIC     for key in databases:
# MAGIC       value = databases[key]
# MAGIC       html += f"""<li style="cursor:help" onclick="document.getElementById('{key}').style.display='block'">
# MAGIC         Now using the database identified by <span style="color: green; font-weight:bold">{key}</span>: 
# MAGIC         <div style="font-style:italic; font-weight:bold">{value[1]}</div>
# MAGIC         <div id="{key}" style="display:none; margin:0.5em 0; border-left: 3px solid grey; padding-left: 0.5em">{value[2]}</div>
# MAGIC         </li>"""
# MAGIC     html += "</ul>"
# MAGIC 
# MAGIC   html += "All done!"
# MAGIC   displayHTML(html)
# MAGIC 
# MAGIC # ****************************************************************************
# MAGIC # Placeholder variables for coding challenge type specification
# MAGIC # ****************************************************************************
# MAGIC class FILL_IN:
# MAGIC   from pyspark.sql.types import Row, StructType
# MAGIC   VALUE = None
# MAGIC   LIST = []
# MAGIC   SCHEMA = StructType([])
# MAGIC   ROW = Row()
# MAGIC   INT = 0
# MAGIC   DATAFRAME = sqlContext.createDataFrame(sc.emptyRDD(), StructType([]))
# MAGIC 
# MAGIC # ****************************************************************************
# MAGIC # Initialize the logger so that it can be used down-stream
# MAGIC # ****************************************************************************
# MAGIC 
# MAGIC daLogger = DatabricksAcademyLogger()
# MAGIC daLogger.logEvent("Initialized", "Initialized the Python DatabricksAcademyLogger")
# MAGIC 
# MAGIC displayHTML("Defining courseware-specific utility methods...")
