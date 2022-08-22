# Databricks notebook source
# MAGIC %scala
# MAGIC // SELF_PACED_ONLY
# MAGIC val courseType = "sp"
# MAGIC val courseAdvertisements = scala.collection.mutable.Map[String,(String,String,String)]()
# MAGIC 
# MAGIC displayHTML("Preparing the Scala environment...")

# COMMAND ----------

# MAGIC %scala
# MAGIC // ILT_ONLY
# MAGIC val courseType = "il"
# MAGIC val courseAdvertisements = scala.collection.mutable.Map[String,(String,String,String)]()
# MAGIC 
# MAGIC displayHTML("Preparing the Scala environment...")

# COMMAND ----------

# MAGIC %python
# MAGIC # SELF_PACED_ONLY
# MAGIC courseType = "sp"
# MAGIC courseAdvertisements = dict()
# MAGIC 
# MAGIC displayHTML("Preparing the Python environment...")

# COMMAND ----------

# MAGIC %python
# MAGIC # ILT_ONLY
# MAGIC courseType = "il"
# MAGIC courseAdvertisements = dict()
# MAGIC 
# MAGIC displayHTML("Preparing the Python environment...")

# COMMAND ----------

# MAGIC %run ./Class-Utility-Methods

# COMMAND ----------

# MAGIC %run ./Utility-Methods

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val moduleName = getModuleName()
# MAGIC val lessonName = getLessonName()
# MAGIC val username = getUsername()
# MAGIC val userhome = getUserhome()
# MAGIC val workingDir = getWorkingDir(courseType)
# MAGIC val databaseName = createUserDatabase(courseType, username, moduleName, lessonName)
# MAGIC 
# MAGIC // ****************************************************************************
# MAGIC // Advertise variables we declared for the user - there are more, but these are the common ones
# MAGIC // ****************************************************************************
# MAGIC courseAdvertisements += ("moduleName" ->   ("v", moduleName,   "No additional information was provided."))
# MAGIC courseAdvertisements += ("lessonName" ->   ("v", lessonName,   "No additional information was provided."))
# MAGIC courseAdvertisements += ("username" ->     ("v", username,     "No additional information was provided."))
# MAGIC courseAdvertisements += ("userhome" ->     ("v", userhome,     "No additional information was provided."))
# MAGIC courseAdvertisements += ("workingDir" ->   ("v", workingDir,   "No additional information was provided."))
# MAGIC courseAdvertisements += ("databaseName" -> ("d", databaseName, "This is a private, per-notebook, database used to provide isolation from other users and exercises."))
# MAGIC 
# MAGIC // ****************************************************************************
# MAGIC // Advertise functions we declared for the user - there are more, but these are the common ones
# MAGIC // ****************************************************************************
# MAGIC courseAdvertisements += ("untilStreamIsReady" -> ("f", "name, progressions=3", """
# MAGIC   <div>Introduced in the course <b>Structured Streaming</b>, this method blocks until the stream is actually ready for processing.</div>
# MAGIC   <div>By default, it waits for 3 progressions of the stream to ensure sufficent data has been processed.</div>"""))
# MAGIC courseAdvertisements += ("stopAllStreams" -> ("f", "", """
# MAGIC   <div>Introduced in the course <b>Structured Streaming</b>, this method stops all active streams while providing extra exception handling.</div>
# MAGIC   <div>It is functionally equivilent to:<div>
# MAGIC   <div><code>for (s <- spark.streams.active) s.stop()</code></div>"""))
# MAGIC 
# MAGIC displayHTML("Defining custom variables for this lesson...")
# MAGIC 
# MAGIC // ****************************************************************************
# MAGIC // Advertise are done here 100% of the time so that the are consistently 
# MAGIC // formatted. If left to each lesson, the meta data associated to each has to 
# MAGIC // be maintained in dozens of different places. This means that if a course
# MAGIC // developer doesn't use a variable/function, then can simply suppress it later
# MAGIC // by setting com.databricks.training.suppress.{key} to "true"
# MAGIC // ****************************************************************************

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC moduleName = getModuleName()
# MAGIC lessonName = getLessonName()
# MAGIC username = getUsername()
# MAGIC userhome = getUserhome()
# MAGIC workingDir = getWorkingDir(courseType)
# MAGIC databaseName = createUserDatabase(courseType, username, moduleName, lessonName)
# MAGIC 
# MAGIC # ****************************************************************************
# MAGIC # Advertise variables we declared for the user - there are more, but these are the common ones
# MAGIC # ****************************************************************************
# MAGIC courseAdvertisements["moduleName"] =   ("v", moduleName,   "No additional information was provided.")
# MAGIC courseAdvertisements["lessonName"] =   ("v", lessonName,   "No additional information was provided.")
# MAGIC courseAdvertisements["username"] =     ("v", username,     "No additional information was provided.")
# MAGIC courseAdvertisements["userhome"] =     ("v", userhome,     "No additional information was provided.")
# MAGIC courseAdvertisements["workingDir"] =   ("v", workingDir,   "No additional information was provided.")
# MAGIC courseAdvertisements["databaseName"] = ("d", databaseName, "This is a private, per-notebook, database used to provide isolation from other users and exercises.")
# MAGIC 
# MAGIC # ****************************************************************************
# MAGIC # Advertise functions we declared for the user - there are more, but these are the common ones
# MAGIC # ****************************************************************************
# MAGIC courseAdvertisements["untilStreamIsReady"] = ("f", "name, progressions=3", """
# MAGIC   <div>Introduced in the course <b>Structured Streaming</b>, this method blocks until the stream is actually ready for processing.</div>
# MAGIC   <div>By default, it waits for 3 progressions of the stream to ensure sufficent data has been processed.</div>""")
# MAGIC courseAdvertisements["stopAllStreams"] = ("f", "", """
# MAGIC   <div>Introduced in the course <b>Structured Streaming</b>, this method stops all active streams while providing extra exception handling.</div>
# MAGIC   <div>It is functionally equivilent to:<div>
# MAGIC   <div><code>for (s <- spark.streams.active) s.stop()</code></div>""")
# MAGIC 
# MAGIC displayHTML("Defining custom variables for this lesson...")
# MAGIC 
# MAGIC # ****************************************************************************
# MAGIC # Advertise are done here 100% of the time so that the are consistently 
# MAGIC # formatted. If left to each lesson, the meta data associated to each has to 
# MAGIC # be maintained in dozens of different places. This means that if a course
# MAGIC # developer doesn't use a variable/function, then can simply suppress it later
# MAGIC # by setting com.databricks.training.suppress.{key} to "true"
# MAGIC # ****************************************************************************

# COMMAND ----------

# MAGIC %run ./Assertion-Utils

# COMMAND ----------

# MAGIC %run ./Dummy-Data-Generator

# COMMAND ----------

# MAGIC %run ./Dataset-Mounts

# COMMAND ----------

# MAGIC %python
# MAGIC # This script sets up MLflow and handles the case that 
# MAGIC # it is executed by Databricks' automated testing server
# MAGIC 
# MAGIC def mlflowAttached():
# MAGIC   try:
# MAGIC     import mlflow
# MAGIC     return True
# MAGIC   except ImportError:
# MAGIC     return False
# MAGIC 
# MAGIC if mlflowAttached():
# MAGIC   import os
# MAGIC   import mlflow
# MAGIC   from mlflow.tracking import MlflowClient
# MAGIC   from databricks_cli.configure.provider import get_config
# MAGIC   from mlflow.exceptions import RestException
# MAGIC   
# MAGIC   notebookId = getTag("notebookId")
# MAGIC   os.environ['DATABRICKS_HOST'] = get_config().host
# MAGIC   os.environ['DATABRICKS_TOKEN'] = get_config().token
# MAGIC   
# MAGIC   if notebookId:
# MAGIC     os.environ["MLFLOW_AUTODETECT_EXPERIMENT_ID"] = 'true'
# MAGIC                     
# MAGIC   else:
# MAGIC     # Handles notebooks run by test server (executed as run)
# MAGIC     # Convention is to use the notebook's name in the users' home directory which our testing framework abides by
# MAGIC     experiment_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
# MAGIC     client = MlflowClient()
# MAGIC     
# MAGIC     try: experiment = client.get_experiment_by_name(experiment_name)
# MAGIC     except Exception as e: pass # experiment doesn't exists
# MAGIC 
# MAGIC     if experiment:
# MAGIC       # Delete past runs if possible
# MAGIC       try: client.delete_experiment(experiment.experiment_id) 
# MAGIC       except Exception as e: pass # ignored
# MAGIC 
# MAGIC     try: mlflow.create_experiment(experiment_name)
# MAGIC     except Exception as e: pass # ignored
# MAGIC     
# MAGIC     os.environ['MLFLOW_EXPERIMENT_NAME'] = experiment_name
# MAGIC   
# MAGIC   # Silence YAML deprecation issue https://github.com/yaml/pyyaml/wiki/PyYAML-yaml.load(input)-Deprecation
# MAGIC   os.environ["PYTHONWARNINGS"] = 'ignore::yaml.YAMLLoadWarning' 
# MAGIC   
# MAGIC None # suppress output

# COMMAND ----------

# MAGIC %scala
# MAGIC classroomCleanup(daLogger, courseType, username, moduleName, lessonName, false)

# COMMAND ----------

# MAGIC %python
# MAGIC classroomCleanup(daLogger, courseType, username, moduleName, lessonName, False)
# MAGIC None # Suppress output

# COMMAND ----------

# MAGIC %scala
# MAGIC assertDbrVersion(spark.conf.get("com.databricks.training.expected-dbr", null))

# COMMAND ----------

# MAGIC %python
# MAGIC assertDbrVersion(spark.conf.get("com.databricks.training.expected-dbr", None))
# MAGIC None # Suppress output
