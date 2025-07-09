# Databricks notebook source
# MAGIC %run ./cat_boost_model/Model

# COMMAND ----------

# MAGIC %run ../123_volta_etl/Utils/LoggerHelper

# COMMAND ----------

# MAGIC %run ./cat_boost_model/ModelPersistence

# COMMAND ----------

# MAGIC %run ./constants

# COMMAND ----------

import pandas as pd
import numpy as np
import sys
import os
import json
import random
from datetime import datetime



def run_main(marker_table=None, external_logger=None, n_bootstrap=50):

    engine_gz = "ws_dag_dev.golden"

    initial_patterns = spark.read.table(f"{engine_gz}.ds_123_volta_etl_mlops_001_TrainingPatterns")
    initial_patterns = initial_patterns.toPandas()
    initial_patterns.reset_index(inplace=True, drop=True)


    if (external_logger is None):
        logger = LoggerHelper("Model_boosting").create_logger()
    else:
        logger = external_logger

    for i in range(n_bootstrap):
        random.seed(datetime.now().timestamp())

        l = initial_patterns.shape[0]

        bootstrapIndex = np.random.choice(range(l), size=l, replace=True)

        table_info = {"ds_123_volta_etl_mlops_001_TrainingPatterns": initial_patterns.loc[bootstrapIndex, :]}

        cat_boost_model = Model(logger=logger,
                                engine=engine_gz,
                                params=params)

        cat_boost_model.train_model(table_info=table_info)

        model_to_persist = ModelPersistence(model_to_persist=cat_boost_model,
                                            logger=logger)
        model_to_persist.save_model_to_file(file_path='./saved_models/',
                                            file_number=str(i))

    if marker_table is not None:
        marker_table.touch()


if __name__ == "__main__":
    run_main()
