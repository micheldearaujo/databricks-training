# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Exercise: Horovod with Petastorm for training a deep learning model
# MAGIC 
# MAGIC In this exercise we are going to build a model on the Boston housing dataset and distribute the deep learning training process using both HorovodRunner and Petastorm.
# MAGIC 
# MAGIC **Required Libraries**: 
# MAGIC * `petastorm==0.8.2` via PyPI

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to set up our environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md ## 1. Load and process data
# MAGIC 
# MAGIC We again load the Boston housing data. However, as we saw in the demo, for Horovod we want to shard the data before passing into HorovodRunner. 
# MAGIC 
# MAGIC For the `get_dataset` function below, load the data, split into 80/20 train-test, standardize the features and return train and test sets.

# COMMAND ----------

from sklearn.datasets import load_boston
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

def get_dataset(rank=0, size=1):
  scaler = StandardScaler()
  
  boston_housing = load_boston()

  # split 80/20 train-test
  X_train, X_test, y_train, y_test = train_test_split(boston_housing.data,
                                                          boston_housing.target,
                                                          test_size=0.2,
                                                          random_state=1)
  
  scaler.fit(X_train)
  X_train = scaler.transform(X_train[rank::size])
  y_train = y_train[rank::size]
  X_test = scaler.transform(X_test[rank::size])
  y_test = y_test[rank::size]
  
  return (X_train, y_train), (X_test, y_test)

# COMMAND ----------

# MAGIC %md ##2. Build Model
# MAGIC 
# MAGIC Using the same model from earlier, let's define our model architecture

# COMMAND ----------

import numpy as np
np.random.seed(0)
import tensorflow as tf
tf.set_random_seed(42) # For reproducibility
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense

def build_model():
  return Sequential([Dense(50, input_dim=13, activation='relu'),
                    Dense(20, activation='relu'),
                    Dense(1, activation='linear')])

# COMMAND ----------

# MAGIC %md ## 3. Horovod
# MAGIC 
# MAGIC In order to distribute the training of our Keras model with Horovod, we must define our `run_training_horovod` training function

# COMMAND ----------

# TODO
import horovod.tensorflow.keras as hvd
from tensorflow.keras import optimizers
from tensorflow.keras.callbacks import *

def run_training_horovod():
  # Horovod: initialize Horovod.
  hvd.init()
  print(f"Rank is: {hvd.rank()}")
  print(f"Size is: {hvd.size()}")
  
  FILL_IN # LOAD DATA
  
  model = FILL_IN
  from tensorflow.keras import optimizers
  optimizer = FILL_IN
  optimizer = FILL_IN
  
  model.compile(optimizer=optimizer, loss="mse", metrics=["mse"])
  checkpoint_dir = f"{ml_working_path}/horovod_checkpoint_weights_lab.ckpt"
  
  callbacks = FILL_IN
  
  # Horovod: save checkpoints only on worker 0 to prevent other workers from corrupting them.
  if hvd.rank() == 0:
    callbacks.append(ModelCheckpoint(checkpoint_dir, save_weights_only=True))
  
  # (make sure you use batch_size of 16 for the learning rate warmup callback, or else you might get a division by 0 error with this small dataset)
  history = model.fit(X_train, y_train, batch_size=16, FILL_IN)

# COMMAND ----------

# MAGIC %md Let's now run our model on all workers.

# COMMAND ----------

# TODO
from sparkdl import HorovodRunner

hr = FILL_IN

# COMMAND ----------

# MAGIC %md ## 4. Horovod with Petastorm
# MAGIC 
# MAGIC We're now going to build a distributed deep learning model capable of handling data in Apache Parquet format. To do so, we can use Horovod along with Petastorm. 
# MAGIC 
# MAGIC First let's load the Boston housing data, and create a Spark DataFrame from the training data.

# COMMAND ----------

import pandas as pd

boston_housing = load_boston()

# split 80/20 train-test
X_train, X_test, y_train, y_test = train_test_split(boston_housing.data,
                                                        boston_housing.target,
                                                        test_size=0.2,
                                                        random_state=1)

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# concatenate our features and label, then create a Spark DataFrame from our Pandas DataFrame.
data = pd.concat([pd.DataFrame(X_train, columns=boston_housing.feature_names), 
                  pd.DataFrame(y_train, columns=["label"])], axis=1)
trainDF = spark.createDataFrame(data)
display(trainDF)

# COMMAND ----------

# MAGIC %md ### Create Vectors
# MAGIC 
# MAGIC Use the VectorAssembler to combine all the features (not including the label) into a single column called `features`.

# COMMAND ----------

# TODO
from pyspark.ml.feature import VectorAssembler

vecAssembler = FILL_IN
vecTrainDF = FILL_IN

# COMMAND ----------

# MAGIC %md Let's now create a UDF to convert our Vector into an Array.

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.linalg.Vector
# MAGIC val toArray = udf { v: Vector => v.toArray }
# MAGIC spark.udf.register("toArray", toArray)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Save the DataFrame out as a parquet file to DBFS. 
# MAGIC 
# MAGIC Let's remember to remove the committed and started metadata files in the Parquet folder! Horovod with Petastorm will not work otherwise.

# COMMAND ----------

file_path = f"{workingDir}/petastorm.parquet"
vecTrainDF.selectExpr("toArray(features) AS features", "label").repartition(8).write.mode("overwrite").parquet(file_path)
[dbutils.fs.rm(i.path) for i in dbutils.fs.ls(file_path) if ("_committed_" in i.name) | ("_started_" in i.name)]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's now define our `run_training_horovod` to format our data using Petastorm and distribute the training of our Keras model using Horovod.

# COMMAND ----------

# TODO
from petastorm import make_batch_reader
from petastorm.tf_utils import make_petastorm_dataset
import horovod.tensorflow.keras as hvd

abs_file_path = file_path.replace("dbfs:/", "/dbfs/")

def run_training_horovod():
  # Horovod: initialize Horovod.
  hvd.init()
  with make_batch_reader("file://" + abs_file_path, 
                         num_epochs=100, 
                         cur_shard=hvd.rank(), 
                         shard_count= hvd.size()) as reader:
    
    dataset = FILL_IN
    model = FILL_IN
    from tensorflow.keras import optimizers
    optimizer = FILL_IN
    optimizer = FILL_IN
    
    model.compile(optimizer=optimizer, loss='mse')
    
    checkpoint_dir = f"{ml_working_path}/petastorm_checkpoint_weights_lab.ckpt"
    
    callbacks = [
      hvd.callbacks.BroadcastGlobalVariablesCallback(0),
      hvd.callbacks.MetricAverageCallback(),
      hvd.callbacks.LearningRateWarmupCallback(warmup_epochs=5, verbose=1),
      ReduceLROnPlateau(monitor="loss", patience=10, verbose=1)
    ]

    # Horovod: save checkpoints only on worker 0 to prevent other workers from corrupting them.
    if hvd.rank() == 0:
      callbacks.append(ModelCheckpoint(checkpoint_dir, save_weights_only=True))

    history = FILL_IN # (use steps_per_epoch=10)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Finally, let's run our newly define Horovod training function with Petastorm to run across all workers.

# COMMAND ----------

# TODO
from sparkdl import HorovodRunner

hr = FILL_IN
