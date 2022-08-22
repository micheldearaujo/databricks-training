// Databricks notebook source
// MAGIC %python
// MAGIC # ALL_NOTEBOOKS
// MAGIC 
// MAGIC # ****************************************************************************
// MAGIC # Utility method to count & print the number of records in each partition.
// MAGIC # ****************************************************************************
// MAGIC 
// MAGIC def printRecordsPerPartition(df):
// MAGIC   def countInPartition(iterator): yield __builtin__.sum(1 for _ in iterator)
// MAGIC   results = (df.rdd                   # Convert to an RDD
// MAGIC     .mapPartitions(countInPartition)  # For each partition, count
// MAGIC     .collect()                        # Return the counts to the driver
// MAGIC   )
// MAGIC   
// MAGIC   print("Per-Partition Counts")
// MAGIC   i = 0
// MAGIC   for result in results: 
// MAGIC     i = i + 1
// MAGIC     print("#{}: {:,}".format(i, result))
// MAGIC   
// MAGIC # ****************************************************************************
// MAGIC # Utility to count the number of files in and size of a directory
// MAGIC # ****************************************************************************
// MAGIC 
// MAGIC def computeFileStats(path):
// MAGIC   bytes = 0
// MAGIC   count = 0
// MAGIC 
// MAGIC   files = dbutils.fs.ls(path)
// MAGIC   
// MAGIC   while (len(files) > 0):
// MAGIC     fileInfo = files.pop(0)
// MAGIC     if (fileInfo.isDir() == False):               # isDir() is a method on the fileInfo object
// MAGIC       count += 1
// MAGIC       bytes += fileInfo.size                      # size is a parameter on the fileInfo object
// MAGIC     else:
// MAGIC       files.extend(dbutils.fs.ls(fileInfo.path))  # append multiple object to files
// MAGIC       
// MAGIC   return (count, bytes)
// MAGIC 
// MAGIC # ****************************************************************************
// MAGIC # Utility method to cache a table with a specific name
// MAGIC # ****************************************************************************
// MAGIC 
// MAGIC def cacheAs(df, name, level = "MEMORY-ONLY"):
// MAGIC   from pyspark.sql.utils import AnalysisException
// MAGIC   if level != "MEMORY-ONLY":
// MAGIC     print("WARNING: The PySpark API currently does not allow specification of the storage level - using MEMORY-ONLY")  
// MAGIC     
// MAGIC   try: spark.catalog.uncacheTable(name)
// MAGIC   except AnalysisException: None
// MAGIC   
// MAGIC   df.createOrReplaceTempView(name)
// MAGIC   spark.catalog.cacheTable(name)
// MAGIC   
// MAGIC   return df
// MAGIC 
// MAGIC 
// MAGIC # ****************************************************************************
// MAGIC # Simplified benchmark of count()
// MAGIC # ****************************************************************************
// MAGIC 
// MAGIC def benchmarkCount(func):
// MAGIC   import time
// MAGIC   start = float(time.time() * 1000)                    # Start the clock
// MAGIC   df = func()
// MAGIC   total = df.count()                                   # Count the records
// MAGIC   duration = float(time.time() * 1000) - start         # Stop the clock
// MAGIC   return (df, total, duration)
// MAGIC 
// MAGIC # ****************************************************************************
// MAGIC # Utility methods to terminate streams
// MAGIC # ****************************************************************************
// MAGIC 
// MAGIC def getActiveStreams():
// MAGIC   try:
// MAGIC     return spark.streams.active
// MAGIC   except:
// MAGIC     # In extream cases, this funtion may throw an ignorable error.
// MAGIC     print("Unable to iterate over all active streams - using an empty set instead.")
// MAGIC     return []
// MAGIC 
// MAGIC def stopStream(s):
// MAGIC   try:
// MAGIC     print("Stopping the stream {}.".format(s.name))
// MAGIC     s.stop()
// MAGIC     print("The stream {} was stopped.".format(s.name))
// MAGIC   except:
// MAGIC     # In extream cases, this funtion may throw an ignorable error.
// MAGIC     print("An [ignorable] error has occured while stoping the stream.")
// MAGIC 
// MAGIC def stopAllStreams():
// MAGIC   streams = getActiveStreams()
// MAGIC   while len(streams) > 0:
// MAGIC     stopStream(streams[0])
// MAGIC     streams = getActiveStreams()
// MAGIC     
// MAGIC # ****************************************************************************
// MAGIC # Utility method to wait until the stream is read
// MAGIC # ****************************************************************************
// MAGIC 
// MAGIC def untilStreamIsReady(name, progressions=3):
// MAGIC   import time
// MAGIC   queries = list(filter(lambda query: query.name == name, getActiveStreams()))
// MAGIC 
// MAGIC   while (len(queries) == 0 or len(queries[0].recentProgress) < progressions):
// MAGIC     time.sleep(5) # Give it a couple of seconds
// MAGIC     queries = list(filter(lambda query: query.name == name, getActiveStreams()))
// MAGIC 
// MAGIC   print("The stream {} is active and ready.".format(name))
// MAGIC 
// MAGIC None

// COMMAND ----------

// MAGIC %scala
// MAGIC // ALL_NOTEBOOKS
// MAGIC 
// MAGIC // ****************************************************************************
// MAGIC // Utility method to count & print the number of records in each partition.
// MAGIC // ****************************************************************************
// MAGIC 
// MAGIC def printRecordsPerPartition(df:org.apache.spark.sql.Dataset[Row]):Unit = {
// MAGIC   // import org.apache.spark.sql.functions._
// MAGIC   val results = df.rdd                                   // Convert to an RDD
// MAGIC     .mapPartitions(it => Array(it.size).iterator, true)  // For each partition, count
// MAGIC     .collect()                                           // Return the counts to the driver
// MAGIC 
// MAGIC   println("Per-Partition Counts")
// MAGIC   var i = 0
// MAGIC   for (r <- results) {
// MAGIC     i = i +1
// MAGIC     println("#%s: %,d".format(i,r))
// MAGIC   }
// MAGIC }
// MAGIC 
// MAGIC // ****************************************************************************
// MAGIC // Utility to count the number of files in and size of a directory
// MAGIC // ****************************************************************************
// MAGIC 
// MAGIC def computeFileStats(path:String):(Long,Long) = {
// MAGIC   var bytes = 0L
// MAGIC   var count = 0L
// MAGIC 
// MAGIC   import scala.collection.mutable.ArrayBuffer
// MAGIC   var files=ArrayBuffer(dbutils.fs.ls(path):_ *)
// MAGIC 
// MAGIC   while (files.isEmpty == false) {
// MAGIC     val fileInfo = files.remove(0)
// MAGIC     if (fileInfo.isDir == false) {
// MAGIC       count += 1
// MAGIC       bytes += fileInfo.size
// MAGIC     } else {
// MAGIC       files.append(dbutils.fs.ls(fileInfo.path):_ *)
// MAGIC     }
// MAGIC   }
// MAGIC   (count, bytes)
// MAGIC }
// MAGIC 
// MAGIC // ****************************************************************************
// MAGIC // Utility method to cache a table with a specific name
// MAGIC // ****************************************************************************
// MAGIC 
// MAGIC def cacheAs(df:org.apache.spark.sql.DataFrame, name:String, level:org.apache.spark.storage.StorageLevel):org.apache.spark.sql.DataFrame = {
// MAGIC   try spark.catalog.uncacheTable(name)
// MAGIC   catch { case _: org.apache.spark.sql.AnalysisException => () }
// MAGIC   
// MAGIC   df.createOrReplaceTempView(name)
// MAGIC   spark.catalog.cacheTable(name, level)
// MAGIC   return df
// MAGIC }
// MAGIC 
// MAGIC // ****************************************************************************
// MAGIC // Simplified benchmark of count()
// MAGIC // ****************************************************************************
// MAGIC 
// MAGIC def benchmarkCount(func:() => org.apache.spark.sql.DataFrame):(org.apache.spark.sql.DataFrame, Long, Long) = {
// MAGIC   val start = System.currentTimeMillis            // Start the clock
// MAGIC   val df = func()                                 // Get our lambda
// MAGIC   val total = df.count()                          // Count the records
// MAGIC   val duration = System.currentTimeMillis - start // Stop the clock
// MAGIC   (df, total, duration)
// MAGIC }
// MAGIC 
// MAGIC // ****************************************************************************
// MAGIC // Benchmarking and cache tracking tool
// MAGIC // ****************************************************************************
// MAGIC 
// MAGIC case class JobResults[T](runtime:Long, duration:Long, cacheSize:Long, maxCacheBefore:Long, remCacheBefore:Long, maxCacheAfter:Long, remCacheAfter:Long, result:T) {
// MAGIC   def printTime():Unit = {
// MAGIC     if (runtime < 1000)                 println(f"Runtime:  ${runtime}%,d ms")
// MAGIC     else if (runtime < 60 * 1000)       println(f"Runtime:  ${runtime/1000.0}%,.2f sec")
// MAGIC     else if (runtime < 60 * 60 * 1000)  println(f"Runtime:  ${runtime/1000.0/60.0}%,.2f min")
// MAGIC     else                                println(f"Runtime:  ${runtime/1000.0/60.0/60.0}%,.2f hr")
// MAGIC     
// MAGIC     if (duration < 1000)                println(f"All Jobs: ${duration}%,d ms")
// MAGIC     else if (duration < 60 * 1000)      println(f"All Jobs: ${duration/1000.0}%,.2f sec")
// MAGIC     else if (duration < 60 * 60 * 1000) println(f"All Jobs: ${duration/1000.0/60.0}%,.2f min")
// MAGIC     else                                println(f"Job Dur: ${duration/1000.0/60.0/60.0}%,.2f hr")
// MAGIC   }
// MAGIC   def printCache():Unit = {
// MAGIC     if (Math.abs(cacheSize) < 1024)                    println(f"Cached:   ${cacheSize}%,d bytes")
// MAGIC     else if (Math.abs(cacheSize) < 1024 * 1024)        println(f"Cached:   ${cacheSize/1024.0}%,.3f KB")
// MAGIC     else if (Math.abs(cacheSize) < 1024 * 1024 * 1024) println(f"Cached:   ${cacheSize/1024.0/1024.0}%,.3f MB")
// MAGIC     else                                               println(f"Cached:   ${cacheSize/1024.0/1024.0/1024.0}%,.3f GB")
// MAGIC     
// MAGIC     println(f"Before:   ${remCacheBefore / 1024.0 / 1024.0}%,.3f / ${maxCacheBefore / 1024.0 / 1024.0}%,.3f MB / ${100.0*remCacheBefore/maxCacheBefore}%.2f%%")
// MAGIC     println(f"After:    ${remCacheAfter / 1024.0 / 1024.0}%,.3f / ${maxCacheAfter / 1024.0 / 1024.0}%,.3f MB / ${100.0*remCacheAfter/maxCacheAfter}%.2f%%")
// MAGIC   }
// MAGIC   def print():Unit = {
// MAGIC     printTime()
// MAGIC     printCache()
// MAGIC   }
// MAGIC }
// MAGIC 
// MAGIC case class Node(driver:Boolean, executor:Boolean, address:String, maximum:Long, available:Long) {
// MAGIC   def this(address:String, maximum:Long, available:Long) = this(address.contains("-"), !address.contains("-"), address, maximum, available)
// MAGIC }
// MAGIC 
// MAGIC class Tracker() extends org.apache.spark.scheduler.SparkListener() {
// MAGIC   
// MAGIC   sc.addSparkListener(this)
// MAGIC   
// MAGIC   val jobStarts = scala.collection.mutable.Map[Int,Long]()
// MAGIC   val jobEnds = scala.collection.mutable.Map[Int,Long]()
// MAGIC   
// MAGIC   def track[T](func:() => T):JobResults[T] = {
// MAGIC     jobEnds.clear()
// MAGIC     jobStarts.clear()
// MAGIC 
// MAGIC     val executorsBefore = sc.getExecutorMemoryStatus.map(x => new Node(x._1, x._2._1, x._2._2)).filter(_.executor)
// MAGIC     val maxCacheBefore = executorsBefore.map(_.maximum).sum
// MAGIC     val remCacheBefore = executorsBefore.map(_.available).sum
// MAGIC     
// MAGIC     val start = System.currentTimeMillis()
// MAGIC     val result = func()
// MAGIC     val runtime = System.currentTimeMillis() - start
// MAGIC     
// MAGIC     Thread.sleep(1000) // give it a second to catch up
// MAGIC 
// MAGIC     val executorsAfter = sc.getExecutorMemoryStatus.map(x => new Node(x._1, x._2._1, x._2._2)).filter(_.executor)
// MAGIC     val maxCacheAfter = executorsAfter.map(_.maximum).sum
// MAGIC     val remCacheAfter = executorsAfter.map(_.available).sum
// MAGIC 
// MAGIC     var duration = 0L
// MAGIC     
// MAGIC     for ((jobId, startAt) <- jobStarts) {
// MAGIC       assert(jobEnds.keySet.exists(_ == jobId), s"A conclusion for Job ID $jobId was not found.") 
// MAGIC       duration += jobEnds(jobId) - startAt
// MAGIC     }
// MAGIC     JobResults(runtime, duration, remCacheBefore-remCacheAfter, maxCacheBefore, remCacheBefore, maxCacheAfter, remCacheAfter, result)
// MAGIC   }
// MAGIC   override def onJobStart(jobStart: org.apache.spark.scheduler.SparkListenerJobStart):Unit = jobStarts.put(jobStart.jobId, jobStart.time)
// MAGIC   override def onJobEnd(jobEnd: org.apache.spark.scheduler.SparkListenerJobEnd): Unit = jobEnds.put(jobEnd.jobId, jobEnd.time)
// MAGIC }
// MAGIC 
// MAGIC val tracker = new Tracker()
// MAGIC 
// MAGIC // ****************************************************************************
// MAGIC // Utility methods to terminate streams
// MAGIC // ****************************************************************************
// MAGIC 
// MAGIC def getActiveStreams():Seq[org.apache.spark.sql.streaming.StreamingQuery] = {
// MAGIC   return try {
// MAGIC     spark.streams.active
// MAGIC   } catch {
// MAGIC     case e:Throwable => {
// MAGIC       // In extream cases, this funtion may throw an ignorable error.
// MAGIC       println("Unable to iterate over all active streams - using an empty set instead.")
// MAGIC       Seq[org.apache.spark.sql.streaming.StreamingQuery]()
// MAGIC     }
// MAGIC   }
// MAGIC }
// MAGIC 
// MAGIC def stopStream(s:org.apache.spark.sql.streaming.StreamingQuery):Unit = {
// MAGIC   try {
// MAGIC     s.stop()
// MAGIC   } catch {
// MAGIC     case e:Throwable => {
// MAGIC       // In extream cases, this funtion may throw an ignorable error.
// MAGIC       println(s"An [ignorable] error has occured while stoping the stream.")
// MAGIC     }
// MAGIC   }
// MAGIC }
// MAGIC 
// MAGIC def stopAllStreams():Unit = {
// MAGIC   var streams = getActiveStreams()
// MAGIC   while (streams.length > 0) {
// MAGIC     stopStream(streams(0))
// MAGIC     streams = getActiveStreams()
// MAGIC   }
// MAGIC }
// MAGIC 
// MAGIC // ****************************************************************************
// MAGIC // Utility method to wait until the stream is read
// MAGIC // ****************************************************************************
// MAGIC 
// MAGIC def untilStreamIsReady(name:String, progressions:Int = 3):Unit = {
// MAGIC   var queries = getActiveStreams().filter(_.name == name)
// MAGIC   
// MAGIC   while (queries.length == 0 || queries(0).recentProgress.length < progressions) {
// MAGIC     Thread.sleep(5*1000) // Give it a couple of seconds
// MAGIC     queries = getActiveStreams().filter(_.name == name)
// MAGIC   }
// MAGIC   println("The stream %s is active and ready.".format(name))
// MAGIC }
// MAGIC 
// MAGIC displayHTML("Defining user-facing utility methods...")
