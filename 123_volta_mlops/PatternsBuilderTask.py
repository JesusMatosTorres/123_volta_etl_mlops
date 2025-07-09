# Databricks notebook source
# MAGIC %run ../123_volta_etl/Utils/LoggerHelper

# COMMAND ----------

# MAGIC %run ./PipInstalls

# COMMAND ----------

# MAGIC %run ./new_vehicle_price_level/LevelPriceManager

# COMMAND ----------

import os
import sys
import json
import pandas as pd
from datetime import datetime
from sqlalchemy.types import *
from pandas.api.types import CategoricalDtype

# COMMAND ----------

spark = spark.builder \
    .appName("Model_Patterns_Builder") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .getOrCreate()

# COMMAND ----------

class PatternsBuilder:

    def __init__(self,
                 logger,
                 training=True):
        """
        Constructor
        :param logger: The ETL logger
        """
        self.logger = logger
        self.training = training
        self.feature_vector = ['Brand',
                               'Model',
                               'Shortage',
                               'Fuel',
                               'Gearbox',
                               'CV',
                               'Segment',
                               'Kms',
                               'Price',
                               'Age',
                               'Sale',
                               'PriceMed']


        self.datatypes = {"Brand": NVARCHAR(50),
                          "Model": NVARCHAR(50),
                          "Shortage": BigInteger(),
                          "Fuel": NVARCHAR(60),
                          "Gearbox": NVARCHAR(60),
                          "CV": BigInteger(),
                          "Segment": NVARCHAR(60),
                          "Kms": BigInteger(),
                          "Price": BigInteger(),
                          "Age": BigInteger(),
                          "Sale": NVARCHAR(30),
                          "PriceMed": Float()
                          }

        self.datatypes_mappings = {"Brand": NVARCHAR(50),
                                   "Model": NVARCHAR(50),
                                   "Segment": NVARCHAR(60)
                                   }

    @staticmethod
    def drop_rare_categories(df, category_column, threshold=0.1):
        lines_counts = 100 * df[category_column].value_counts() / len(df)
        list_vehicles = list(lines_counts[lines_counts > threshold].index)
        df = df[df[category_column].isin(list_vehicles)]
        df[category_column] = df[category_column].astype(CategoricalDtype(categories=list_vehicles))

        return df

    @staticmethod
    def group_rare_categories(df, category_column, group_name='Otros', threshold=0.1):
        lines_counts = 100 * df[category_column].value_counts() / len(df)
        list_vehicles = list(lines_counts[lines_counts < threshold].index)
        df.loc[df[category_column].isin(list_vehicles), category_column] = group_name
        categories_list = list(df[category_column].unique())
        df[category_column] = df[category_column].astype(CategoricalDtype(categories=categories_list))

        return df

    def create_model_segment_mappings(self, table_info):
        df_vo = table_info["tb_123_volta_etl_mlops_markets_canary"]
        table_info['volta_model_segment_mappings'] = df_vo.groupby(['Brand', 'Model'])[
            'Segment'].first().reset_index().dropna()

        return table_info

    def clean_data(self, df):
        df = df[df.CV.notnull() & df.Segment.notnull()]

        df = df[(df.Web != 'COMPRAMOSTUCOCHE.ES') & (df.Web != 'CICAR')]

        dt_today = datetime.now()

        # One and a half year historical prices
        df = df[(dt_today - df.Publication).dt.days <= 365]

        ix = (df['Fuel'] != 'GASOLINA') & (df['Fuel'] != 'DIESEL')
        df.loc[ix, 'Fuel'] = 'OtroFuel'

        df['Brand'] = df.Brand.str.replace('-', '')
        df['Brand'] = df.Brand.str.replace(' ', '')
        df['Model'] = df.Model.str.replace('-', '')
        df['Model'] = df.Model.str.replace(' ', '')

        if (self.training == True):
            df = PatternsBuilder.group_rare_categories(df=df,
                                                       category_column='Segment',
                                                       group_name='OtroSegmento',
                                                       threshold=0.1)

            df = PatternsBuilder.group_rare_categories(df=df,
                                                       category_column='Brand',
                                                       group_name='OTROS',
                                                       threshold=0.1)

        df = df[df.Age >= 0]

        df.drop_duplicates(subset=self.feature_vector, inplace=True)

        df = df[(df.Age < 20) & (df.Kms < 300000)]

        df = df[df.CV <= 650]

        return df

    def create_prediction_patterns(self,
                                   table_info,
                                   engine):

        """
        Creates the patterns used to predict the incoming rentals.
       :param table_info: If the incoming data is just a data frame, it is a dictionary that contains the name and the
       data frame by using the keys 'name' and 'data' respectively. In case it contains more than one data frame, it is
       a dictionary that contains as key the name of the table and as value the data frame.
       :param parameters: Dictionary that, in case of existing more than one data frame in the table_info, contains as
       key the value 'table_to_process' and as value the name of the table to process. This is useful when multiples tables
       are coming and the process just has to use one, or some of them.
       :return: Dictionary with the same structure than the table_info parameter.
        """

        # Load the tables
        try:
            df_vo = table_info["tb_123_volta_etl_mlops_markets_canary"]
        except KeyError as err:
            self.logger.error(
                "'create_prediction_patterns' transformation process could not be done. Error description: " + str(
                    err) + "\n")
            return {"data": None, "name": None}
        try:
            #Escasez
            df_vo['Brand_Model'] = df_vo.Brand + "_" + df_vo.Model
            frecuencia = df_vo['Brand_Model'].value_counts(normalize=True)
            escasez = 1 / frecuencia
            df_vo['Shortage'] = df_vo['Brand_Model'].map(escasez)
            df_vo=df_vo.drop(['Brand_Model'], axis=1)

            df_vo = df_vo[df_vo['Features'].str.len() > 0]
            df_vo = LevelPriceTask(engine=engine, logger=self.logger).get_median_price(df_vo)
            df_vo.dropna(subset=['PriceMed', 'Sale'], inplace=True)

            df_vo = self.clean_data(df_vo)


            patterns = df_vo[self.feature_vector]

            # Return
            self.logger.info("'create_prediction_patterns' transformation process done.")
            table_info["training_patterns"] = patterns
            return table_info

        except Exception as err:
            self.logger.error(
                "'create_prediction_patterns' transformation process could not be done. Error description: " + str(
                    err) + "\n")
            return {"data": None, "name": None}


def run_main(marker_table=None, external_logger=None):

    #catalog_name = dbutils.widgets.get("catalog_name")
    catalog_name = "catalog_dag"
    silver = f"{catalog_name}.silver"
    bronze = f"{catalog_name}.bronze"
    golden = f"{catalog_name}.golden"
    engine = {"BronzeZone": bronze, "SilverZone": silver}

    table_info = {}

    if (external_logger is None):
        logger = LoggerHelper("PatternsBuilder").create_logger()
    else:
        logger = external_logger

    # Create the patterns
    builder = PatternsBuilder(logger=logger, training=True)

    logger.info("Building patterns for training ...")

    # Load the data
    logger.info("Loading data ...")

    try:
        df_spark = spark.read.table(f"{silver}.tb_123_volta_etl_mlops_markets_canary")
        table_info["tb_123_volta_etl_mlops_markets_canary"] = df_spark.toPandas()
        logger.info(" - tb_123_volta_etl_mlops_markets_canary: OK")
    except Exception as err:
        logger.error(
            "An error has happened during the loading process for table 'tb_123_volta_etl_mlops_markets_canary'. Error description: " + str(
                err))
    try:
        df_spark = spark.read.table(f"{bronze}.tb_123_volta_etl_mlops_lista_precios")
        table_info["tb_123_volta_etl_mlops_lista_precios"] = df_spark.toPandas()
        logger.info(" - tb_123_volta_etl_mlops_lista_precios: OK")
    except Exception as err:
        logger.error(
            "An error has happened during the loading process for table 'tb_123_volta_etl_mlops_lista_precios'. Error description: " + str(
                err))

    # Build up the patterns
    table_info = builder.create_prediction_patterns(table_info=table_info, engine=engine)

    # Send to the database
    try:
        logger.info("Running storing process.")
        df_spark = spark.createDataFrame(table_info["training_patterns"])
        df_spark.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{golden}.ds_123_volta_etl_mlops_001_TrainingPatterns")

        logger.info("Table ds_123_volta_etl_mlops_001_TrainingPatterns stored properly.")

        # Update Luigi Target marker_table as task completed succesfully
        if marker_table is not None:
            marker_table.touch()
    except Exception as err:
        logger.error("Table ds_123_volta_etl_mlops_001_TrainingPatterns could not be stored. Error description: " + str(err) + "\n")


if __name__ == '__main__':
    run_main()
