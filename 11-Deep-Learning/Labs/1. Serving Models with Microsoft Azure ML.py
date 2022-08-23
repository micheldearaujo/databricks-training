# Databricks notebook source
# MAGIC %md
# MAGIC # Serving Models with Microsoft Azure ML
# MAGIC In this lesson we will use MLflow and Azure ML to deploy and quey model to different environments.
# MAGIC 
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC * Create or load an Azure ML Workspace
# MAGIC * Build an Azure Container Image for model deployment
# MAGIC * Deploy the model to "dev" using Azure Container Instances (ACI)
# MAGIC * Query the deployed model in "dev"
# MAGIC * Deploy the model to production using Azure Kubernetes Service (AKS)
# MAGIC * Query the deployed model in production
# MAGIC * Update the production deployment
# MAGIC * Clean up the deployments
# MAGIC 
# MAGIC **Required Libraries**:
# MAGIC * `mlflow==1.7.0` via PyPI
# MAGIC * `azureml-sdk==1.2.0` via PyPI

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to set up our environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md ### 1. Create or load an Azure ML Workspace
# MAGIC Before models can be deployed to Azure ML, you must create or obtain an Azure ML Workspace. The `azureml.core.Workspace.create()` function will load a workspace of a specified name or create one if it does not already exist. For more information about creating an Azure ML Workspace, see the [Azure ML Workspace management documentation](https://docs.microsoft.com/en-us/azure/machine-learning/service/how-to-manage-workspace).

# COMMAND ----------

import azureml
from azureml.core import Workspace

workspace_name = "<workspace-name>"
workspace_location="<workspace-location>"
resource_group = "<resource-group>"
subscription_id = "<subscription-id>"

workspace = Workspace.create(name = workspace_name,
                             location = workspace_location,
                             resource_group = resource_group,
                             subscription_id = subscription_id,
                             exist_ok=True)

# COMMAND ----------

# MAGIC %md ### 2. Train the Diabetes Model and build a Container Image for the trained model

# COMMAND ----------

# MAGIC %md #### Train the Diabetes Model
# MAGIC 
# MAGIC We will uses the `diabetes` dataset in scikit-learn and predicts the progression metric (a quantitative measure of disease progression after one year after) based on BMI, blood pressure, etc. We will uses the scikit-learn ElasticNet linear regression model. We will use MLflow to log  metrics, parameters, artifacts and model.

# COMMAND ----------

import os
import warnings
import sys
from random import random
import pandas as pd
import numpy as np
from itertools import cycle
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import ElasticNet
from sklearn.linear_model import lasso_path, enet_path
from sklearn import datasets
# Import mlflow
import mlflow
import mlflow.sklearn

# Load Diabetes datasets
diabetes = datasets.load_diabetes()
X = diabetes.data
y = diabetes.target

# Create pandas DataFrame for sklearn ElasticNet linear_model
Y = np.array([y]).transpose()
d = np.concatenate((X, Y), axis=1)
cols = ['age', 'sex', 'bmi', 'bp', 's1', 's2', 's3', 's4', 's5', 's6', 'progression']
data = pd.DataFrame(d, columns=cols)

def train_diabetes(data, in_alpha, in_l1_ratio):
  # Evaluate metrics
  def eval_metrics(actual, pred):
      rmse = np.sqrt(mean_squared_error(actual, pred))
      mae = mean_absolute_error(actual, pred)
      r2 = r2_score(actual, pred)
      return rmse, mae, r2

  warnings.filterwarnings("ignore")
  np.random.seed(40)

  # Split the data into training and test sets. (0.75, 0.25) split.
  train, test = train_test_split(data)

  # The predicted column is "progression" which is a quantitative measure of disease progression one year after baseline
  train_x = train.drop(["progression"], axis=1)
  test_x = test.drop(["progression"], axis=1)
  train_y = train[["progression"]]
  test_y = test[["progression"]]

  if float(in_alpha) is None:
    alpha = 0.05
  else:
    alpha = float(in_alpha)
    
  if float(in_l1_ratio) is None:
    l1_ratio = 0.05
  else:
    l1_ratio = float(in_l1_ratio)
  
  # Start an MLflow run; the "with" keyword ensures we'll close the run even if this cell crashes
  with mlflow.start_run() as run:
    lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
    lr.fit(train_x, train_y)

    predicted_qualities = lr.predict(test_x)

    (rmse, mae, r2) = eval_metrics(test_y, predicted_qualities)

    # Print out ElasticNet model metrics
    print("Elasticnet model (alpha=%f, l1_ratio=%f):" % (alpha, l1_ratio))
    print("  RMSE: %s" % rmse)
    print("  MAE: %s" % mae)
    print("  R2: %s" % r2)

    # Log mlflow attributes for mlflow UI
    mlflow.log_param("alpha", alpha)
    mlflow.log_param("l1_ratio", l1_ratio)
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", r2)
    mlflow.log_metric("mae", mae)
    mlflow.sklearn.log_model(lr, "model")
    rand_int = int(random()*10000)
    modelpath = "/dbfs/mlflow/test_diabetes/model-%f-%f-%f" % (alpha, l1_ratio, rand_int)
    mlflow.sklearn.save_model(lr, modelpath)
    
    run_id = run.info.run_id
    print('Run ID: ', run_id)
    model_uri = "runs:/" + run_id + "/model"
    print('model_uri: ', model_uri)
    
    return run_id, model_uri
    
run_id, model_uri = train_diabetes(data, 0.01, 1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use MLflow to build a Container Image for the trained model
# MAGIC 
# MAGIC Use the `mlflow.azuereml.build_image` function to build an Azure Container Image for the trained MLflow model. This function also registers the MLflow model with a specified Azure ML workspace. The resulting image can be deployed to Azure Container Instances (ACI) or Azure Kubernetes Service (AKS) for real-time serving.

# COMMAND ----------

import mlflow.azureml

model_image, azure_model = mlflow.azureml.build_image(model_uri=model_uri, 
                                                      workspace=workspace,
                                                      model_name="model",
                                                      image_name="model",
                                                      description="Sklearn ElasticNet image for predicting diabetes progression",
                                                      synchronous=False)
model_image.wait_for_creation(show_output=True)

# COMMAND ----------

# MAGIC %md ### 3. Create an ACI webservice deployment
# MAGIC 
# MAGIC The [ACI platform](https://docs.microsoft.com/en-us/azure/container-instances/) is the recommended environment for staging and developmental model deployments. Using the Azure ML SDK, deploy the Container Image for the trained MLflow model to ACI.

# COMMAND ----------

from azureml.core.webservice import AciWebservice, Webservice

dev_webservice_name = "diabetes-model"
dev_webservice_deployment_config = AciWebservice.deploy_configuration()
dev_webservice = Webservice.deploy_from_image(name=dev_webservice_name, image=model_image, deployment_config=dev_webservice_deployment_config, workspace=workspace)
dev_webservice.wait_for_deployment()

# COMMAND ----------

# Create a sample data 
from sklearn import datasets
import pandas as pd
import numpy as np
import requests
import json

diabetes = datasets.load_diabetes()
X = diabetes.data
y = diabetes.target
Y = np.array([y]).transpose()
d = np.concatenate((X, Y), axis=1)
cols = ['age', 'sex', 'bmi', 'bp', 's1', 's2', 's3', 's4', 's5', 's6', 'progression']
data = pd.DataFrame(d, columns=cols)
sample = data.drop(["progression"], axis=1).iloc[[0]]
                                                 
query_input = sample.to_json(orient='split')
query_input = eval(query_input)
query_input.pop('index', None)


# sending an HTTP request
def query_endpoint_example(scoring_uri, inputs, service_key=None):
  headers = {
    "Content-Type": "application/json",
  }
  if service_key is not None:
    headers["Authorization"] = "Bearer {service_key}".format(service_key=service_key)
    
  print("Sending batch prediction request with inputs: {}".format(inputs))
  response = requests.post(scoring_uri, data=json.dumps(inputs), headers=headers)
  preds = json.loads(response.text)
  print("Received response: {}".format(preds))
  return preds

# COMMAND ----------

dev_webservice.scoring_uri
dev_prediction = query_endpoint_example(scoring_uri=dev_webservice.scoring_uri, inputs=query_input)

# COMMAND ----------

# MAGIC %md ### 4. Deploy the model to production using [Azure Kubernetes Service (AKS)](https://azure.microsoft.com/en-us/services/kubernetes-service/)

# COMMAND ----------

# MAGIC %md #### Option 1: Create a new AKS cluster
# MAGIC 
# MAGIC If you do not have an active AKS cluster for model deployment, create one using the Azure ML SDK.

# COMMAND ----------

from azureml.core.compute import AksCompute, ComputeTarget

# Use the default configuration (you can also provide parameters to customize this)
prov_config = AksCompute.provisioning_configuration()

aks_cluster_name = "diabetes-cluster" 
# Create the cluster
aks_target = ComputeTarget.create(workspace = workspace, 
                                  name = aks_cluster_name, 
                                  provisioning_configuration = prov_config)

# Wait for the create process to complete
aks_target.wait_for_completion(show_output = True)
print(aks_target.provisioning_state)
print(aks_target.provisioning_errors)

# COMMAND ----------

# MAGIC %md #### Option 2: Connect to an existing AKS cluster in your workspace

# COMMAND ----------

from azureml.core.compute import AksCompute, ComputeTarget

# Give the cluster a local name
aks_cluster_name = "diabetes-cluster"

aks_target = ComputeTarget(workspace=workspace, name=aks_cluster_name)
print(aks_target.provisioning_state)
print(aks_target.provisioning_errors)

# COMMAND ----------

# MAGIC %md ### 5. Deploy to the model's image to the specified AKS cluster

# COMMAND ----------

from azureml.core.webservice import Webservice, AksWebservice

# Set configuration and service name
prod_webservice_name = "diabetes-model-prod"
prod_webservice_deployment_config = AksWebservice.deploy_configuration()

# Deploy from image
prod_webservice = Webservice.deploy_from_image(workspace = workspace, 
                                               name = prod_webservice_name,
                                               image = model_image,
                                               deployment_config = prod_webservice_deployment_config,
                                               deployment_target = aks_target)

# COMMAND ----------

# Wait for the deployment to complete
prod_webservice.wait_for_deployment(show_output = True)

# COMMAND ----------

# MAGIC %md
# MAGIC We can evaluate the sample data by sending an HTTP request. Query the AKS webservice's scoring endpoint by sending an HTTP POST request that includes the input vector. The production AKS deployment may require an authorization token (service key) for queries. Include this key in the HTTP request header.

# COMMAND ----------

import requests
import json

def query_endpoint_example(scoring_uri, inputs, service_key=None):
  headers = {
    "Content-Type": "application/json",
  }
  if service_key is not None:
    headers["Authorization"] = "Bearer {service_key}".format(service_key=service_key)
    
  print("Sending batch prediction request with inputs: {}".format(inputs))
  response = requests.post(scoring_uri, data=json.dumps(inputs), headers=headers)
  preds = json.loads(response.text)
  print("Received response: {}".format(preds))
  return preds

prod_scoring_uri = prod_webservice.scoring_uri
prod_service_key = prod_webservice.get_keys()[0] if len(prod_webservice.get_keys()) > 0 else None
prod_prediction1 = query_endpoint_example(scoring_uri=prod_scoring_uri, service_key=prod_service_key, inputs=query_input)

# COMMAND ----------

# MAGIC %md ### 6. Update the production deployment
# MAGIC 
# MAGIC Train a new model with different hyperparameters and deploy the new model to production.

# COMMAND ----------

import mlflow.azureml

# Train a new model with different hyperparameters
run_id_new, model_uri = train_diabetes(data, 0.01, 0.9)

# Build a container image for the new trained model
model_image_updated, azure_model_updated = mlflow.azureml.build_image(model_uri=model_uri, 
                                                                      workspace=workspace,
                                                                      model_name="model-updated",
                                                                      image_name="model-updated",
                                                                      description="Sklearn ElasticNet image for predicting diabetes progression",
                                                                      synchronous=False)
model_image_updated.wait_for_creation(show_output=True)

# COMMAND ----------

# MAGIC %md ### 7. Deploy the new model's image to the AKS cluster
# MAGIC 
# MAGIC Using the [`azureml.core.webservice.AksWebservice.update()`](https://docs.microsoft.com/en-us/python/api/azureml-core/azureml.core.webservice.akswebservice?view=azure-ml-py#update) function, replace the deployment's existing model image with the new model image.

# COMMAND ----------

prod_webservice.update(image=model_image_updated)
prod_webservice.wait_for_deployment(show_output = True)

# COMMAND ----------

# MAGIC %md
# MAGIC We can now query the updated model and compare the results.

# COMMAND ----------

prod_prediction2 = query_endpoint_example(scoring_uri=prod_scoring_uri, service_key=prod_service_key, inputs=query_input)
print("Run ID: {} Prediction: {}".format(run_id, prod_prediction1)) 
print("Run ID: {} Prediction: {}".format(run_id_new, prod_prediction2))

# COMMAND ----------

# MAGIC %md ### 8. Clean up the deployments

# COMMAND ----------

# MAGIC %md 
# MAGIC We can now terminate the "dev" ACI webservice. Because ACI manages compute resources on your behalf, deleting the "dev" ACI webservice will remove all resources associated with the "dev" model deployment

# COMMAND ----------

dev_webservice.delete()
prod_webservice.delete()
aks_target.delete()
