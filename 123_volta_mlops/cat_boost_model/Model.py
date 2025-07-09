# Databricks notebook source
# MAGIC %run ../PipInstalls

# COMMAND ----------

# MAGIC %pip install numpy==1.24.1
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./CrossValidation

# COMMAND ----------

import pandas as pd
from catboost import Pool, CatBoostRegressor, EFstrType, cv
from sqlalchemy.types import *
import sys
import os
from mlflow.types import Schema, ColSpec
import mlflow
import mlflow.sklearn
import matplotlib.pyplot as plt
import mlflow.models.signature
from sklearn.model_selection import train_test_split
from mlflow.models.signature import infer_signature
from mlflow.tracking import MlflowClient
from databricks.sdk import WorkspaceClient
import time
import requests
import shutil
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# COMMAND ----------

class Model:
    def __init__(self, params, patterns, category_features, logger, verbosity=False):
        self.params = params
        self.patterns = patterns
        self.category_features = category_features
        self.logger = logger
        self.verbosity = verbosity
        self.model = None

        self.MLFLOW_MODEL_NAME = "123_volta_etl_mlops"
        self.STORAGE_ACCOUNT_NAME = "stdatadagpbieprowe"
        self.BLOB_CONTAINER_NAME = "mlflow-exported-models"
        self.AZURE_SAS_TOKEN = dbutils.secrets.get(scope="dag_secrets_scope_pro", key="blob-storage-sas-token")
        self.BLOB_SERVICE_URL = f"https://{self.STORAGE_ACCOUNT_NAME}.blob.core.windows.net/"
        self.LOCAL_TEMP_PATH = "/tmp/exported_mlflow_model"

    def train(self):
        """ Entrena el model, realiza validación cruzada y guarda resultados en MLflow """
        self.logger.info("Starting training process...")
        os.environ["TMPDIR"] = "/tmp/catboost/"
        os.makedirs(os.environ["TMPDIR"], exist_ok=True)

        try:
            train_tmp_dir = "/tmp/catboost/"
            os.makedirs(train_tmp_dir, exist_ok=True)

            targetColumns = self.patterns.Price
            X = self.patterns.drop(['Price', 'Model'], axis=1)  
            Y = targetColumns

            
            for col in X.select_dtypes(include=['int']).columns:
                X[col] = X[col].astype('float64')

            
            for col in self.category_features:
                X[col] = X[col].astype(str)

            
            cat_features_idx = [X.columns.get_loc(col) for col in self.category_features]
            
            mlflow.set_experiment(experiment_id="902609704652107")

            with mlflow.start_run(run_name=f"123_volta_etl_mlops{self.params['depth']}_{self.params['learning_rate']}"):
                mlflow.log_params(self.params)

                
                train_pool = Pool(data=X, label=Y, cat_features=cat_features_idx)

                
                self.model = CatBoostRegressor(**{k: v for k, v in self.params.items() if k != "category_features"},
                                               train_dir=train_tmp_dir)
                self.model.fit(train_pool, verbose=self.verbosity)
                cross_val = CrossValidation("all", self.model, self.patterns, self.category_features, self.logger, self.verbosity)
                cross_val.validate()

                # === Análisis SHAP ===
                import shap
                import matplotlib.pyplot as plt

                explainer = shap.Explainer(self.model)
                shap_values = explainer(X)

                # Visualización y guardado del resumen SHAP
                shap.summary_plot(shap_values, X, show=False)
                plt.savefig("/shap_summary.png")
                mlflow.log_artifact("/shap_summary.png")
                plt.close()

                self.model.save_model("/tmp/exp_123_mlops.cbm")
                
                X_sample = X[:5]  
                y_sample = self.model.predict(X_sample)  

                
                y_sample = np.array(y_sample, dtype=np.float64)
                signature = infer_signature(X_sample, y_sample)
                mlflow.catboost.log_model(self.model, "123_volta_etl_mlops", signature=signature)

                
                model_uri = "runs:/"+mlflow.active_run().info.run_id+"/123_volta_etl_mlops"
                registered_model = mlflow.register_model(model_uri=model_uri, name="123_volta_etl_mlops")

                self.log_feature_importance(cross_val.feature_importance_df)

                # Asignar alias `production` a la nueva versión**
                client = MlflowClient()
                client.set_registered_model_alias("123_volta_etl_mlops", "production", registered_model.version)
                self.logger.info(f"Alias ​​'production' assigned to the version {registered_model.version}.")

                self.export_model_to_blob(registered_model.version)
                
                self.logger.info("Model training, validation, and logging completed successfully!")

        except Exception as err:
            self.logger.error("An error has happened during the training process. Error description: " + str(err))
            raise


    def log_feature_importance(self, feature_importance_df):
        """ Registra un gráfico de importancia de características en MLflow """
        if feature_importance_df is not None:
            plt.figure(figsize=(10, 6))
            feature_importance_df.sort_values(by="Importances", ascending=True).plot(kind="barh", x="FeatureName", y="Importances")
            plt.xlabel("Importancia")
            plt.ylabel("Feature")
            plt.title("Importancia de Características - CatBoost")
            
            # Guardar gráfico en MLflow
            plt.savefig("/feature_importance.png")
            mlflow.log_artifact("/feature_importance.png")
            plt.close()

    def export_model_to_blob(self, model_version):
        """
        Exporta el modelo registrado en MLflow a Azure Blob Storage
        """
        self.logger.info(f"Exporting model version {model_version} to Blob Storage...")

        try:
            model_uri = f"models:/{self.MLFLOW_MODEL_NAME}/{model_version}"

            # Limpiar directorio local
            if os.path.exists(self.LOCAL_TEMP_PATH):
                shutil.rmtree(self.LOCAL_TEMP_PATH)
            os.makedirs(self.LOCAL_TEMP_PATH)

            # Descargar artefactos localmente
            mlflow.artifacts.download_artifacts(
                artifact_uri=model_uri,
                dst_path=self.LOCAL_TEMP_PATH
            )
            self.logger.info(f"Artifacts downloaded locally to {self.LOCAL_TEMP_PATH}")

            # Subir a Blob Storage
            blob_service_client = BlobServiceClient(account_url=self.BLOB_SERVICE_URL, credential=self.AZURE_SAS_TOKEN)
            container_client = blob_service_client.get_container_client(self.BLOB_CONTAINER_NAME)

            blob_destination_folder = f"{self.MLFLOW_MODEL_NAME}"

            for root, _, files in os.walk(self.LOCAL_TEMP_PATH):
                for file_name in files:
                    local_file_path = os.path.join(root, file_name)
                    relative_path = os.path.relpath(local_file_path, self.LOCAL_TEMP_PATH)
                    blob_path = os.path.join(blob_destination_folder, relative_path).replace("\\", "/")

                    self.logger.info(f"Uploading {local_file_path} to blob://{self.BLOB_CONTAINER_NAME}/{blob_path}")

                    blob_client = container_client.get_blob_client(blob=blob_path)
                    with open(local_file_path, "rb") as data:
                        blob_client.upload_blob(data, overwrite=True)

            self.logger.info(f"Model exported successfully to blob://{self.BLOB_CONTAINER_NAME}/{blob_destination_folder}")

        except Exception as e:
            self.logger.error(f"Error during model export: {e}")
        finally:
            if os.path.exists(self.LOCAL_TEMP_PATH):
                shutil.rmtree(self.LOCAL_TEMP_PATH)
                self.logger.info(f"Temporary directory {self.LOCAL_TEMP_PATH} cleaned.")


# COMMAND ----------

class Endpoint():
    def __init__(self, logger):
        self.logger = logger
        self.instance = spark.conf.get('spark.databricks.workspaceUrl')

        self.logger.info(f"self.Instance: {self.instance}")

        TOKEN = dbutils.widgets.get("token").strip()

        self.headers = {
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json"
        }

        client = MlflowClient()
        self.model_name = "catalog_dag.default.123_volta_etl_mlops"
        version = client.get_model_version_by_alias(self.model_name, "production")
        latest_version = version.version

        self.model_serving_endpoint_name = "123_volta_etl_mlops"

        
        self.my_json = {
            "name": self.model_serving_endpoint_name,
            "config": {
                "served_models": [{
                    "model_name": self.model_name,
                    "model_version": latest_version,
                    "workload_size": "Small",
                    "workload_type": "CPU",
                    "scale_to_zero_enabled": True
                }]
            }
        }
    
    def func_create_endpoint(self):
        endpoint_url = f"https://{self.instance}/api/2.0/serving-endpoints"
        url = f"{endpoint_url}/{self.model_serving_endpoint_name}"

        
        r = requests.get(url, headers=self.headers)
        
        if "RESOURCE_DOES_NOT_EXIST" in r.text:
            self.logger.info(f"Creating new endpoint: {self.model_serving_endpoint_name}")
        else:
            self.logger.info(f"The endpoint '{self.model_serving_endpoint_name}' already exists. By deleting it and recreating it...")
            
            self.func_delete_model_serving_endpoint()

        
        re = requests.post(endpoint_url, headers=self.headers, json=self.my_json)
        
        assert re.status_code == 200, f"API Error: HTTP {re.status_code}"
        self.logger.info(f"Endpoint '{self.model_serving_endpoint_name}' created successfully.")

    
    def func_delete_model_serving_endpoint(self):
        endpoint_url = f"https://{self.instance}/api/2.0/serving-endpoints"
        url =  f"{endpoint_url}/{self.model_serving_endpoint_name}" 

        self.response = requests.delete(url, headers=self.headers)
        
        if self.response.status_code == 200:
            self.logger.info(f"Endpoint '{self.model_serving_endpoint_name}' deleted successfully.")
        else:
            raise Exception(f"Error deleting endpoint: {self.response.status_code}, {self.response.text}")


    def wait_for_endpoint(self):
        endpoint_url = f"https://{self.instance}/api/2.0/serving-endpoints"
        while True:
            url = f"{endpoint_url}/{self.model_serving_endpoint_name}"
            self.response = requests.get(url, headers=self.headers)

            if self.response.status_code == 403:
                raise Exception("You don't have sufficient permissions to access the endpoint. Check your token or permissions.")

            assert self.response.status_code == 200, f"Error HTTP {self.response.status_code}\n{self.response.text}"

            status = self.response.json().get("state", {}).get("ready", {})

            if status == "READY":
                self.logger.info(f"Endpoint '{self.model_serving_endpoint_name}' is ready.")
                self.logger.info("-" * 80)
                return
            else:
                self.logger.info(f"Endpoint '{self.model_serving_endpoint_name}' is NOT ready ({status}), waiting 120 seconds...")
                time.sleep(120)
