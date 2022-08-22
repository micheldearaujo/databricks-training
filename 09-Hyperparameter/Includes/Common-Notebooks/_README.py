# Databricks notebook source
# MAGIC %md
# MAGIC # Common-Notebooks README
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This is a collection of common notebooks to be used across different modules. Its intention is to consolidate classroom setup, student feedback, and utility functions into a single, easily managed space.
# MAGIC 
# MAGIC ## Usage
# MAGIC 
# MAGIC The following should be done to utilize these notebooks:
# MAGIC 
# MAGIC 1. Clone the entire `Common-Notebooks` folder into the `./Includes` folder of your module
# MAGIC 2. Create a generic `Classroom-Setup` notebook in your `./Includes` folder of your module
# MAGIC 3. Add the following to your generic `./Includes/Classroom-Setup` notebook (note: they must be in this order and the first two must be designated for `# ALL_NOTEBOOKS`)
# MAGIC   * `module_name = <YOUR_MODULE_NAME>`
# MAGIC   * `spark.conf.set("com.databricks.training.module-name", module_name)`
# MAGIC   * `%run ./Common-Notebooks/Common`
# MAGIC 4. Create a generic `Classroom-Cleanup` notebook in your `./Includes` folder of your module
# MAGIC 5. Add the following to your generic `./Includes/Classroom-Cleanup` notebook (note: they must be in this order)
# MAGIC   * `classroomCleanup()` in a `%python` cell and a `%scala` cell &mdash; note that `classroomCleanup()` has a `path` parameter that is set to `getWorkingDir()` by default
# MAGIC   * `showStudentSurvey()` in a `%scala` cell with `// ALL_NOTEBOOKS`
# MAGIC 6. Add `./Includes/Classroom-Setup` to the beginning of each lesson's notebook
# MAGIC 7. Add `./Includes/Classroom-Cleanup` to the end of each lesson's notebook
# MAGIC 
# MAGIC ## Notebooks
# MAGIC 
# MAGIC Below is a description of each of the notebooks:
# MAGIC 
# MAGIC * **`Assertion-Utils`** - this is a collection of assessment-focused utilities. This includes our certification test classes, assertion functions for comparisons, legacy testing functions, and empty placeholder types.
# MAGIC * **`Class-Utility-Methods`** - this is a collection of classroom-focused utilities. This includes our student progress tracking features, student survey features, user variable declarations and version assertions.
# MAGIC * **`Common`** - this is a generic notebook used to call all the other notebooks to define their logic as well as instantiate their logic when necessary
# MAGIC * **`Dataset-Mounts`** - this is a collection of dataset-mounting utilities. This allows datasets to be mounted to cloud connections.
# MAGIC * **`Dummy-Data-Generator`** - this is a data generation utility class used to create example or dummy data for lessons, labs, capstones, and certifications.
# MAGIC * **`Utility-Methods`** - this is a collection of productivity-focused utilities. This includes finding the number of records per partition, etc. 
# MAGIC 
# MAGIC ## Testing
# MAGIC 
# MAGIC Each of these notebooks can be tested individually by running their corresponding `<NOTEBOOK_NAME>_TEST` notebook. 
