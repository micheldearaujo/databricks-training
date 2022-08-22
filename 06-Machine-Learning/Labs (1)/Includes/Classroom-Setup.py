# Databricks notebook source
# MAGIC %python
# MAGIC 
# MAGIC spark.conf.set("com.databricks.training.module-name", "deep-learning")
# MAGIC spark.conf.set("com.databricks.training.expected-dbr", "6.4")
# MAGIC 
# MAGIC spark.conf.set("com.databricks.training.suppress.untilStreamIsReady", "true")
# MAGIC spark.conf.set("com.databricks.training.suppress.stopAllStreams", "true")
# MAGIC spark.conf.set("com.databricks.training.suppress.moduleName", "true")
# MAGIC spark.conf.set("com.databricks.training.suppress.lessonName", "true")
# MAGIC # spark.conf.set("com.databricks.training.suppress.username", "true")
# MAGIC spark.conf.set("com.databricks.training.suppress.userhome", "true")
# MAGIC # spark.conf.set("com.databricks.training.suppress.workingDir", "true")
# MAGIC spark.conf.set("com.databricks.training.suppress.databaseName", "true")
# MAGIC 
# MAGIC import warnings
# MAGIC warnings.filterwarnings("ignore")
# MAGIC 
# MAGIC import tensorflow
# MAGIC 
# MAGIC def display_run_uri(experiment_id, run_id):
# MAGIC     host_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName").get()
# MAGIC     uri = "https://{}/#mlflow/experiments/{}/runs/{}".format(host_name,experiment_id,run_id)
# MAGIC     displayHTML("""<b>Run URI:</b> <a href="{}">{}</a>""".format(uri,uri))
# MAGIC 
# MAGIC def waitForMLflow():
# MAGIC   try:
# MAGIC     import mlflow; 
# MAGIC     if int(mlflow.__version__.split(".")[1]) >= 2:
# MAGIC         print("""The module "mlflow" is attached and ready to go.""");
# MAGIC     else:
# MAGIC         print("""You need MLflow version 1.2.0+ installed.""")
# MAGIC   except ModuleNotFoundError:
# MAGIC     print("""The module "mlflow" is not yet attached to the cluster, waiting...""");
# MAGIC     while True:
# MAGIC       try: import mlflow; print("""The module "mlflow" is attached and ready to go."""); break;
# MAGIC       except ModuleNotFoundError: import time; time.sleep(1); print(".", end="");
# MAGIC 
# MAGIC 
# MAGIC from sklearn.metrics import confusion_matrix,f1_score,accuracy_score,fbeta_score,precision_score,recall_score
# MAGIC import matplotlib.pyplot as plt
# MAGIC import numpy as np
# MAGIC from sklearn.utils.multiclass import unique_labels
# MAGIC 
# MAGIC def plot_confusion_matrix(y_true, y_pred, classes,
# MAGIC                           title=None,
# MAGIC                           cmap=plt.cm.Blues):
# MAGIC     # Compute confusion matrix
# MAGIC     cm = confusion_matrix(y_true, y_pred)
# MAGIC     fig, ax = plt.subplots()
# MAGIC     im = ax.imshow(cm, interpolation='nearest', cmap=cmap)
# MAGIC     ax.figure.colorbar(im, ax=ax)
# MAGIC     ax.set(xticks=np.arange(cm.shape[1]),
# MAGIC            yticks=np.arange(cm.shape[0]),
# MAGIC            xticklabels=classes, yticklabels=classes,
# MAGIC            title=title,
# MAGIC            ylabel='True label',
# MAGIC            xlabel='Predicted label')
# MAGIC 
# MAGIC     plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
# MAGIC              rotation_mode="anchor")
# MAGIC 
# MAGIC     fmt = 'd'
# MAGIC     thresh = cm.max() / 2.
# MAGIC     for i in range(cm.shape[0]):
# MAGIC         for j in range(cm.shape[1]):
# MAGIC             ax.text(j, i, format(cm[i, j], fmt),
# MAGIC                     ha="center", va="center",
# MAGIC                     color="white" if cm[i, j] > thresh else "black")
# MAGIC     fig.tight_layout()
# MAGIC     return fig
# MAGIC 
# MAGIC np.set_printoptions(precision=2)
# MAGIC 
# MAGIC displayHTML("Preparing the learning environment...")

# COMMAND ----------

# MAGIC %run ./Common-Notebooks/Common

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC courseAdvertisements["waitForMLflow"] =         ("f", "", "MLflow is an open source platform to manage the ML lifecycle, including experimentation, reproducibility and deployment")
# MAGIC courseAdvertisements["display_run_uri"] =       ("f", "experiment_id, run_id", "Experiment and run ids")
# MAGIC courseAdvertisements["plot_confusion_matrix"] = ("f", "y_true, y_pred, classes, title=None, cmap=plt.cm.Blues", "Confusion matrix")
# MAGIC 
# MAGIC working_path = workingDir.replace("dbfs:/", "/dbfs/")
# MAGIC courseAdvertisements["working_path"] = ("v", working_path, "This is working directory.")
# MAGIC # Make sure workingDir exists before continuing
# MAGIC dbutils.fs.mkdirs(working_path.replace("/dbfs/", "dbfs:/"))
# MAGIC 
# MAGIC # Optimized fuse mount
# MAGIC ml_working_path = f"/dbfs/ml/{username.replace('+', '')}"
# MAGIC courseAdvertisements["ml_working_path"] = ("v", ml_working_path, "This is ML working directory.")
# MAGIC # Make sure workingDir exists before continuing
# MAGIC dbutils.fs.mkdirs(ml_working_path.replace("/dbfs/", "dbfs:/"))
# MAGIC 
# MAGIC dbutils.fs.mkdirs(f"{workingDir}/temp")
# MAGIC 
# MAGIC allDone(courseAdvertisements)
