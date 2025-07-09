# Databricks notebook source
# MAGIC %run ../123_volta_etl/Utils/LoggerHelper

# COMMAND ----------

# MAGIC %run ./PipInstalls

# COMMAND ----------

# MAGIC %run ./new_vehicle_price_level/LevelPriceManager

# COMMAND ----------

import pandas as pd
import datetime
import os
from sqlalchemy.types import *
import re
import unidecode
import sys
import json
import pickle
from pathlib import Path
import re
from pyspark.dbutils import DBUtils

# COMMAND ----------

class GetDictionaryTask:
    def __init__(self,
                 logger=None,
                 marker_table = None):
        # Create the engine to DB
        catalog_name = dbutils.widgets.get("catalog_name")
        sz_engine = f"{catalog_name}.silver"
        bz_engine = f"{catalog_name}.bronze"

        self.marker_table = marker_table
        self.logger = logger

        self.engine = {"BronzeZone": bz_engine, "SilverZone": sz_engine}


        #Poner en false una vez entrenado
        self.list_price_df = LevelPriceTask(engine=self.engine,
                                       logger= self.logger,
                                       is_training=True).get_median_price(None)
        self.dictionary_marca_modelo_version = LevelPriceTask(engine=self.engine,
                                                              logger = self.logger).get_dictionary_vehicles_version()
        self.dictionary_marca_modelo_power = LevelPriceTask(engine=self.engine,
                                                            logger = self.logger).get_dictionary_vehicles_power()

def run_main(marker_table = None, external_logger = None):

    task = GetDictionaryTask(logger = external_logger, marker_table=marker_table)

    task.logger.info("Creating list of bags with words Ok")

    task.logger.info("Creating dictionaries of power and version Ok")

    with open("/power.pickle", "wb") as handle:
        pickle.dump(task.dictionary_marca_modelo_power, handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open("/bags.pickle", 'wb') as handle:
        pickle.dump(task.list_price_df.to_dict(orient='records'), handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open("/version.pickle", 'wb') as handle:
        pickle.dump(task.dictionary_marca_modelo_version, handle, protocol=pickle.HIGHEST_PROTOCOL)

    task.logger.info("Saving dictionaries of power and version on pickle Ok")

    if marker_table != None:
        task.marker_table.touch()

if __name__ == "__main__":
    logger = LoggerHelper("GetDictionaryTask").create_logger()
    run_main(external_logger = logger)


