// Databricks notebook source
// MAGIC %python
// MAGIC 
// MAGIC class DummyData:
// MAGIC   from datetime import datetime
// MAGIC   from pyspark.sql import DataFrame
// MAGIC   from pyspark.sql import functions
// MAGIC   from pyspark.sql.window import Window
// MAGIC   from pyspark.sql.types import IntegerType, StringType, TimestampType, NullType
// MAGIC   from math import ceil
// MAGIC   from string import ascii_letters, digits
// MAGIC   import pyspark.sql.functions as F
// MAGIC   import re, random
// MAGIC 
// MAGIC   
// MAGIC   def __init__(self, tableName, defaultDatabaseName=databaseName, seed=None, numRows=300):
// MAGIC     
// MAGIC     self.__tableName = tableName
// MAGIC     self.__numRows = numRows
// MAGIC     
// MAGIC     # create database for user
// MAGIC     username = getUsername()
// MAGIC     userhome = getUserhome()
// MAGIC 
// MAGIC     self.__dbName = defaultDatabaseName
// MAGIC     self.__dbName = self.re.sub("[^a-zA-Z]", "", self.__dbName)
// MAGIC 
// MAGIC     spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(self.__dbName))
// MAGIC 
// MAGIC     # set initial seed number
// MAGIC     seed = userhome if seed is None else seed
// MAGIC     self.__seedNum = hash(seed)
// MAGIC       
// MAGIC     # initialize dataframe
// MAGIC     self.__id = "id"
// MAGIC     self.__df = spark.range(self.__numRows)
// MAGIC     
// MAGIC     # words reference
// MAGIC     self.__loremIpsum = "amet luctus venenatis lectus magna fringilla urna porttitor rhoncus dolor purus non enim praesent elementum facilisis leo vel fringilla est ullamcorper eget nulla facilisi etiam dignissim diam quis enim lobortis scelerisque fermentum dui faucibus in ornare quam viverra orci sagittis eu volutpat odio facilisis mauris sit amet massa vitae tortor condimentum lacinia quis vel eros donec ac odio tempor orci dapibus ultrices in iaculis nunc sed augue lacus viverra vitae congue eu consequat ac felis donec et odio pellentesque diam volutpat commodo sed egestas egestas fringilla phasellus faucibus scelerisque eleifend donec pretium vulputate sapien nec sagittis aliquam malesuada bibendum arcu vitae elementum curabitur vitae nunc sed velit dignissim sodales ut eu sem integer vitae justo eget magna fermentum iaculis eu non diam phasellus vestibulum lorem sed risus ultricies tristique nulla aliquet enim tortor at auctor urna nunc id cursus metus aliquam eleifend mi in nulla posuere sollicitudin aliquam ultrices sagittis orci a scelerisque purus semper eget duis at tellus at urna condimentum mattis pellentesque id nibh tortor id aliquet lectus proin nibh nisl condimentum id venenatis a condimentum vitae sapien pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas sed tempus urna et pharetra pharetra massa"
// MAGIC     
// MAGIC     # states reference
// MAGIC     self.__states = ["Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","Florida","Georgia","Hawaii","Idaho",
// MAGIC                      "Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota","Mississippi",
// MAGIC                      "Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York","North Carolina","North Dakota","Ohio",
// MAGIC                      "Oklahoma","Oregon","Pennsylvania","Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont","Virginia",
// MAGIC                      "Washington","West Virginia","Wisconsin","Wyoming"]
// MAGIC     
// MAGIC     # chars reference
// MAGIC     self.__chars = self.ascii_letters + self.digits
// MAGIC     
// MAGIC   def __getSeed(self):
// MAGIC     self.__seedNum += 1
// MAGIC     return self.__seedNum  
// MAGIC     
// MAGIC   def toDF(self):
// MAGIC     fullTableName = self.__dbName + "." + self.__tableName + "_p"
// MAGIC     self.__df.write.format("delta").mode("overwrite").saveAsTable(fullTableName)
// MAGIC     return spark.read.table(fullTableName).orderBy(self.__id)
// MAGIC   
// MAGIC   def renameId(self, name):
// MAGIC     self.__df = self.__df.withColumnRenamed(self.__id, name)
// MAGIC     self.__id = name
// MAGIC     return self
// MAGIC   
// MAGIC   def makeNull(self, name, proportion = 0.2): 
// MAGIC     self.__df = (self.addBooleans("_isNull", proportion).__df
// MAGIC            .withColumn(name, self.F.when(self.F.col("_isNull") == True, None)
// MAGIC            .otherwise(self.F.col(name)))
// MAGIC            .drop("_isNull"))
// MAGIC     return self
// MAGIC   
// MAGIC   def addIntegers(self, name: str, low: float = 0, high: float = 5000):
// MAGIC     self.__df = self.__df.withColumn(name, (self.F.rand(self.__getSeed()) * (high - low) + low).cast(self.IntegerType()))
// MAGIC     return self
// MAGIC   
// MAGIC   def addDoubles(self, name, low = 0, high = 5000, roundNum = 6):
// MAGIC     self.__df = self.__df.withColumn(name, self.F.round(self.F.rand(self.__getSeed()) * (high - low) + low, roundNum))
// MAGIC     return self
// MAGIC 
// MAGIC   def addProportions(self, name, roundNum = 6):
// MAGIC     self.__df = self.__df.withColumn(name, self.F.round(self.F.rand(self.__getSeed()), roundNum))
// MAGIC     return self
// MAGIC    
// MAGIC   def addBooleans(self, name, proportionTrue = 0.5):
// MAGIC     self.__df = self.__df.withColumn(name, self.F.rand(self.__getSeed()) < proportionTrue)
// MAGIC     return self
// MAGIC     
// MAGIC   def addPriceDoubles(self, name, low = 100, high = 5000):
// MAGIC     self.__df = self.__df.withColumn(name, self.F.round(self.F.rand(self.__getSeed()) * (high - low) + low, 2))
// MAGIC     return self
// MAGIC     
// MAGIC   def addPriceStrings(self, name, low = 100, high = 5000):
// MAGIC     self.__df = self.__df.withColumn(name, self.F.format_number(self.F.round(self.F.rand(self.__getSeed()) * (high - low) + low, 2), 2))
// MAGIC     self.__df = self.__df.withColumn(name, self.F.concat(self.F.lit("$"), name))
// MAGIC     return self
// MAGIC   
// MAGIC   def addCategories(self, name, categories = ["first", "second", "third", "fourth"]):
// MAGIC     self.__df = self.__df.withColumn(name, (self.F.rand(self.__getSeed()) * len(categories)).cast(self.IntegerType()))
// MAGIC     tempDF = sqlContext.createDataFrame(zip(range(len(categories)), categories), schema=[name, name + "Text"])
// MAGIC     self.__df = self.__df.join(self.F.broadcast(tempDF), name)
// MAGIC     self.__df = self.__df.drop(name)
// MAGIC     self.__df = self.__df.withColumnRenamed(name + "Text", name)
// MAGIC     return self
// MAGIC   
// MAGIC   def addPasswords(self, name: str = "password"):
// MAGIC     w = self.Window().orderBy(self.F.lit('A'))
// MAGIC     passwordDF = (spark
// MAGIC                     .createDataFrame(
// MAGIC                       [''.join(self.__chars[self.random.randint(0, len(self.__chars) - 1)] for i in range(8,20)) for x in range(self.__numRows)], 
// MAGIC                       schema = StringType()
// MAGIC                     )
// MAGIC                     .withColumnRenamed("value", name)
// MAGIC                     .withColumn("row_num", self.F.row_number().over(w)))
// MAGIC     
// MAGIC     self.__df = self.__df.withColumn("row_num", self.F.row_number().over(w))
// MAGIC     self.__df = self.__df.join(passwordDF, "row_num").drop("row_num")
// MAGIC     return self
// MAGIC   
// MAGIC   def addWords(self, name, num = 5):
// MAGIC     loremCount = len(self.__loremIpsum.split(" "))
// MAGIC     words = (self.__loremIpsum + " ") * int(self.ceil(self.__numRows / float(loremCount)))
// MAGIC     word_list = words.split(" ")
// MAGIC     
// MAGIC     self.__df = self.__df.withColumn(name, self.F.lit(""))
// MAGIC     self.random.seed(self.__getSeed())
// MAGIC     
// MAGIC     for i in range(num):
// MAGIC       self.random.shuffle(word_list)
// MAGIC       word_data = list(zip(word_list, range(0, len(word_list))))
// MAGIC       
// MAGIC       wordsDF = (spark.createDataFrame(word_data, ["word" + str(i), self.__id])
// MAGIC                       .sort(self.__id).limit(self.__numRows))
// MAGIC       
// MAGIC       self.__df = (self.__df.join(wordsDF, self.__id)
// MAGIC                        .withColumn(name, self.F.concat(self.F.col(name), self.F.lit(" "), self.F.col("word" + str(i))))
// MAGIC                        .drop("word" + str(i)))
// MAGIC       
// MAGIC     self.__df = self.__df.withColumn(name, self.F.ltrim(self.F.col(name)))
// MAGIC     return self
// MAGIC     
// MAGIC   def addNames(self, name, num = 2):
// MAGIC     self.__df = self.addWords(name, num).__df.withColumn(name, self.F.initcap(self.F.col(name)))
// MAGIC     return self
// MAGIC     
// MAGIC   def addWordArrays(self, name, num = 5):
// MAGIC     self.__df = self.addWords(name, num).__df.withColumn(name, self.F.split(self.F.col(name), " "))
// MAGIC     return self
// MAGIC 
// MAGIC   def addTimestamps(self, name, start_date_expr = "2015-08-05 12:00:00", end_date_expr = "2019-08-05 12:00:00", format = "%Y-%m-%d %H:%M:%S"):
// MAGIC     start_timestamp = self.datetime.strptime(start_date_expr, format).timestamp()
// MAGIC     end_timestamp = self.datetime.strptime(end_date_expr, format).timestamp()
// MAGIC     self.__df = self.addIntegers(name, start_timestamp, end_timestamp).__df
// MAGIC     return self
// MAGIC   
// MAGIC   def addDateStrings(self, name, start_date_expr = "2015-08-05 12:00:00", end_date_expr = "2019-08-05 12:00:00", format = "yyyy-MM-dd HH:mm:ss"):
// MAGIC     self.__df = (self.addTimestamps(name, start_date_expr, end_date_expr).__df
// MAGIC                      .withColumn(name, self.F.date_format(self.F.col(name).cast(self.TimestampType()), format)))
// MAGIC     return self
// MAGIC   
// MAGIC   def addStates(self, name):
// MAGIC     self.__df = self.addCategories(name, self.__states).__df
// MAGIC     return self
// MAGIC   
// MAGIC   # needs to be done: add probabilities to categories
// MAGIC   # needs to be done: add arrays of all types
// MAGIC   # needs to be done: add normal distribution, skewed data
// MAGIC   # needs to be done: add data dependent on another column
// MAGIC 
// MAGIC displayHTML("Initializing Databricks Academy's services for generating dynamic data...")

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC class DummyData(val tableName: String, val defaultDatabaseName:String = databaseName, val seed: String = null, val numRows: Int = 300) {
// MAGIC   import java.sql.Timestamp
// MAGIC   import java.time.format.DateTimeFormatter
// MAGIC   import java.time.LocalDateTime
// MAGIC   import org.apache.spark.sql.DataFrame
// MAGIC   import org.apache.spark.sql.functions.{broadcast, col, concat, date_format, explode, format_number, initcap, ltrim, monotonically_increasing_id, 
// MAGIC                                          lit, rand, randn, round, shuffle, split, udf, when, row_number}
// MAGIC   import org.apache.spark.sql.expressions.Window
// MAGIC   import org.apache.spark.sql.types.{IntegerType, TimestampType}
// MAGIC   import scala.math.ceil
// MAGIC   import scala.util.Random
// MAGIC 
// MAGIC   // create database for user
// MAGIC   private val username = getUsername()
// MAGIC   private val userhome = getUserhome()
// MAGIC   
// MAGIC   private var dbName = defaultDatabaseName
// MAGIC   dbName = dbName.replaceAll("[^a-zA-Z]","")
// MAGIC   
// MAGIC   spark.sql("CREATE DATABASE IF NOT EXISTS %s".format(dbName))
// MAGIC   
// MAGIC   // set initial seed number
// MAGIC   private var seedNum = if (seed != null) seed.hashCode else userhome.hashCode
// MAGIC   
// MAGIC   // initalize dataframe
// MAGIC   private var id = "id"
// MAGIC   private var df = spark.range(numRows).toDF
// MAGIC   
// MAGIC   // words reference
// MAGIC   private val loremIpsum = "amet luctus venenatis lectus magna fringilla urna porttitor rhoncus dolor purus non enim praesent elementum facilisis leo vel fringilla est ullamcorper eget nulla facilisi etiam dignissim diam quis enim lobortis scelerisque fermentum dui faucibus in ornare quam viverra orci sagittis eu volutpat odio facilisis mauris sit amet massa vitae tortor condimentum lacinia quis vel eros donec ac odio tempor orci dapibus ultrices in iaculis nunc sed augue lacus viverra vitae congue eu consequat ac felis donec et odio pellentesque diam volutpat commodo sed egestas egestas fringilla phasellus faucibus scelerisque eleifend donec pretium vulputate sapien nec sagittis aliquam malesuada bibendum arcu vitae elementum curabitur vitae nunc sed velit dignissim sodales ut eu sem integer vitae justo eget magna fermentum iaculis eu non diam phasellus vestibulum lorem sed risus ultricies tristique nulla aliquet enim tortor at auctor urna nunc id cursus metus aliquam eleifend mi in nulla posuere sollicitudin aliquam ultrices sagittis orci a scelerisque purus semper eget duis at tellus at urna condimentum mattis pellentesque id nibh tortor id aliquet lectus proin nibh nisl condimentum id venenatis a condimentum vitae sapien pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas sed tempus urna et pharetra pharetra massa"
// MAGIC   
// MAGIC   // states reference
// MAGIC   private val states = List("Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","Florida","Georgia","Hawaii","Idaho",
// MAGIC                     "Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota","Mississippi",
// MAGIC                     "Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York","North Carolina","North Dakota","Ohio",
// MAGIC                     "Oklahoma","Oregon","Pennsylvania","Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont","Virginia",
// MAGIC                     "Washington","West Virginia","Wisconsin","Wyoming")
// MAGIC 
// MAGIC   private def getSeed(): Int = {
// MAGIC     seedNum += 1
// MAGIC     seedNum
// MAGIC   }  
// MAGIC   
// MAGIC   def toDF(): DataFrame = {
// MAGIC     val fullTableName = dbName + "." + tableName + "_s"
// MAGIC     df.write.format("delta").mode("overwrite").saveAsTable(fullTableName)
// MAGIC     spark.read.table(fullTableName).orderBy(id)
// MAGIC   }  
// MAGIC   
// MAGIC   def renameId(name: String): this.type = {
// MAGIC     df = df.withColumnRenamed(id, name)
// MAGIC     id = name
// MAGIC     this
// MAGIC   }
// MAGIC   
// MAGIC   // TODO: add option to drop an exact number of values - e.g. 6 nulls
// MAGIC   def makeNull(name: String, proportion: Double = 0.2): this.type = {
// MAGIC     df = addBooleans("_isNull", proportion).df
// MAGIC            .withColumn(name, when($"_isNull" === true, null)
// MAGIC              .otherwise(col(name)))
// MAGIC            .drop("_isNull")
// MAGIC     this
// MAGIC   }
// MAGIC   
// MAGIC   def makeDuplicates(proportion: Double = 0.2): this.type = {
// MAGIC     df = df.union(df.sample(proportion))
// MAGIC     this
// MAGIC   }
// MAGIC   
// MAGIC   def addIntegers(name: String, low: Double = 0, high: Double = 5000): this.type = {
// MAGIC     df = df.withColumn(name, (rand(getSeed()) * (high - low) + low).cast(IntegerType))
// MAGIC     this
// MAGIC   }
// MAGIC   
// MAGIC   def addDoubles(name: String, low: Double = 0, high: Double = 5000, roundNum: Int = 6): this.type = {
// MAGIC     df = df.withColumn(name, round(rand(getSeed()) * (high - low) + low, roundNum))
// MAGIC     this
// MAGIC   }
// MAGIC 
// MAGIC   def addProportions(name: String, roundNum: Int = 6): this.type = {
// MAGIC     df = df.withColumn(name, round(rand(getSeed()), roundNum))
// MAGIC     this
// MAGIC   }
// MAGIC    
// MAGIC   def addBooleans(name: String, proportionTrue: Double = 0.5): this.type = {
// MAGIC     df = df.withColumn(name, rand(getSeed()) < proportionTrue)
// MAGIC     this
// MAGIC   }
// MAGIC     
// MAGIC   def addPriceDoubles(name: String, low: Double = 100, high: Double = 5000): this.type = {
// MAGIC     df = df.withColumn(name, round(rand(getSeed()) * (high - low) + low, 2))
// MAGIC     this
// MAGIC   }
// MAGIC     
// MAGIC   def addPriceStrings(name: String, low: Double = 100, high: Double = 5000): this.type = {
// MAGIC     df = df.withColumn(name, concat(lit("$"), format_number(round(rand(getSeed()) * (high - low) + low, 2), 2)))
// MAGIC     this
// MAGIC   }
// MAGIC     
// MAGIC   def addCategories(name: String, categories: Seq[String] = Seq("first", "second", "third", "fourth")): this.type = {
// MAGIC 
// MAGIC     df = df.withColumn(name, (rand(getSeed()) * categories.size).cast(IntegerType))
// MAGIC     val tempDF = categories.zipWithIndex.toDF(name + "Text", name)
// MAGIC     df = df.join(broadcast(tempDF), name).drop(name).withColumnRenamed(name + "Text", name)
// MAGIC        
// MAGIC     this
// MAGIC   }
// MAGIC   
// MAGIC   def addPasswords(name: String = "password"): this.type = {
// MAGIC     val w = Window.orderBy(lit("A"))
// MAGIC     val passwords = for (row <- df.collect()) 
// MAGIC       yield new scala.util.Random().alphanumeric.take(8 + new scala.util.Random().nextInt(20 - 8) + 1).mkString("")
// MAGIC     val passwordDF = passwords.toList
// MAGIC                               .toDF()
// MAGIC                               .withColumnRenamed("value", name)
// MAGIC                               .withColumn("row_num", row_number().over(w))
// MAGIC     
// MAGIC     df = df.withColumn("row_num", row_number().over(w))
// MAGIC     df = df.join(passwordDF, "row_num").drop("row_num")
// MAGIC     this
// MAGIC   }
// MAGIC     
// MAGIC   def addWords(name: String, num: Int = 5): this.type = {
// MAGIC     val loremCount = loremIpsum.split(" ").length
// MAGIC     val words = (loremIpsum + " ") * ceil(numRows / loremCount.toDouble).toInt
// MAGIC     val word_list = words.split(" ").toList
// MAGIC     
// MAGIC     // needs to be done: implement worsDF creation using spark/DF to scale
// MAGIC     
// MAGIC     df = df.withColumn(name, lit(""))
// MAGIC     
// MAGIC     val randomGen = new Random(getSeed())
// MAGIC     for (i <- 1 to num) {
// MAGIC       val wordsDF = spark.createDataset(randomGen.shuffle(word_list).zipWithIndex)
// MAGIC                          .toDF("word" + i.toString, id)
// MAGIC                          .sort(id)
// MAGIC                          .limit(numRows)
// MAGIC       
// MAGIC       df = df.join(wordsDF, id)
// MAGIC              .withColumn(name, concat(col(name), lit(" "), col("word" + i.toString)))
// MAGIC              .drop("word" + i.toString)
// MAGIC     }
// MAGIC 
// MAGIC     df = df.withColumn(name, ltrim(col(name)))
// MAGIC     this
// MAGIC   }
// MAGIC     
// MAGIC   def addNames(name: String, num: Int = 2): this.type = {
// MAGIC     df = addWords(name, num).df.withColumn(name, initcap(col(name)))
// MAGIC     this
// MAGIC   }
// MAGIC     
// MAGIC   def addWordArrays(name: String, num: Int = 5): this.type = {
// MAGIC     df = addWords(name, num).df.withColumn(name, split(col(name), " "))
// MAGIC     this
// MAGIC   }
// MAGIC 
// MAGIC   def addTimestamps(name: String, start_date_expr: String = "2015-08-05 12:00:00", end_date_expr: String = "2019-08-05 12:00:00", format: String = "yyyy-MM-dd HH:mm:ss"): this.type = {
// MAGIC     val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
// MAGIC     val start_timestamp = Timestamp.valueOf(LocalDateTime.parse(start_date_expr, formatter)).getTime() / 1000
// MAGIC     val end_timestamp = Timestamp.valueOf(LocalDateTime.parse(end_date_expr, formatter)).getTime() / 1000
// MAGIC     df = addIntegers(name, start_timestamp, end_timestamp).df
// MAGIC     this
// MAGIC   }
// MAGIC   
// MAGIC   def addDateStrings(name: String, start_date_expr: String = "2015-08-05 12:00:00", end_date_expr: String = "2019-08-05 12:00:00", format: String = "yyyy-MM-dd HH:mm:ss"): this.type = {
// MAGIC     df = addTimestamps(name, start_date_expr, end_date_expr).df
// MAGIC            .withColumn(name, date_format(col(name).cast(TimestampType), format))
// MAGIC     this
// MAGIC   }
// MAGIC   
// MAGIC   def addStates(name: String): this.type = {
// MAGIC     df = addCategories(name, this.states).df
// MAGIC     this
// MAGIC   }
// MAGIC 
// MAGIC   // needs to be done: add probabilities to categories
// MAGIC   // needs to be done: add arrays of all types
// MAGIC   // needs to be done: add normal distribution, skewed data
// MAGIC   // needs to be done: add data dependent on another column
// MAGIC   
// MAGIC }
// MAGIC 
// MAGIC displayHTML("Initializing Databricks Academy's services for generating dynamic data...")
