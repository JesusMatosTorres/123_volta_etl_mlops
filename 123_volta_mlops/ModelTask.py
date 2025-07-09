# Databricks notebook source
# MAGIC %run ./PipInstalls

# COMMAND ----------

# MAGIC %run ./cat_boost_model/Model

# COMMAND ----------

# MAGIC %run ../123_volta_etl/Utils/LoggerHelper

# COMMAND ----------

# MAGIC %run ./constants

# COMMAND ----------

import pandas as pd
import sys
import os
import json

# COMMAND ----------

def run_main(marker_table=None, external_logger=None):

    catalog_name = dbutils.widgets.get("catalog_name")
    engine_gz = f"{catalog_name}.golden"

    # Set the logger path

    df_spark = spark.read.table(f"{engine_gz}.ds_123_volta_etl_mlops_001_trainingpatterns")
    df_spark = df_spark.toPandas()
    table_info = {"ds_123_volta_etl_mlops_001_trainingpatterns": df_spark}


    if (external_logger is None):
        logger = LoggerHelper("Model_training").create_logger()
    else:
        logger = external_logger
    
    # Crear instancia de Model y entrenar
    patterns = df_spark.copy()
    category_features = params['category_features']
    model_instance = Model(params, patterns, category_features, logger)
    model_instance.train()
    service = Endpoint(logger)
    service.func_create_endpoint()

    api_url = mlflow.utils.databricks_utils.get_webapp_url()
    print(api_url)

    service.wait_for_endpoint()

    if marker_table is not None:
        marker_table.touch()


if __name__ == "__main__":
    run_main()
