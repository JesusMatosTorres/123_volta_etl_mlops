# Databricks notebook source
# MAGIC %run ./Utils/SetUpScraper

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /tmp/firefox
# MAGIC
# MAGIC wget -O /tmp/firefox.tar.bz2 "https://ftp.mozilla.org/pub/firefox/releases/91.0esr/linux-x86_64/en-US/firefox-91.0esr.tar.bz2"
# MAGIC
# MAGIC tar xjf /tmp/firefox.tar.bz2 -C /tmp/firefox --strip-components=1
# MAGIC
# MAGIC chmod +x /tmp/firefox/firefox
# MAGIC
# MAGIC sudo apt-get update
# MAGIC sudo apt-get install -y libgtk-3-0 libdbus-glib-1-2 libx11-xcb1 libxtst6 libxcomposite1 libasound2 libxi6

# COMMAND ----------

# MAGIC %run ./Utils/LoggerHelper

# COMMAND ----------

import os
import sys
import json
import selenium
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from sqlalchemy.types import *
from selenium import webdriver
import time
import datetime
from collections import OrderedDict
from sqlalchemy.types import *
from sqlalchemy import create_engine
from urllib import parse
from typing_extensions import dataclass_transform
from pyspark.sql.functions import col, max
from pyspark.sql.functions import initcap, lit, split, when
import random
import subprocess

# COMMAND ----------

spark = spark.builder \
    .appName("ETL_Scraper_MLeon") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .getOrCreate()

# COMMAND ----------

class MLeonTask():

    def __init__(self):

        user = os.environ["USER"]
        self.geckodriver = f"/local_disk0/tmp/{user}/geckodriver/geckodriver"
        FIREFOX_BINARY = "/tmp/firefox/firefox"

        self.log_file_path = f"/tmp/geckodriver_{random.randint(1000, 9999)}.log"
        if os.path.exists(self.log_file_path):
            os.remove(self.log_file_path)

        options = webdriver.FirefoxOptions()
        options.add_argument('-headless')
        options.set_preference("dom.disable_open_during_load", False)
        options.set_preference('dom.popup_maximum', -1)
        options.binary_location = FIREFOX_BINARY

        self.browser = webdriver.Firefox(executable_path=self.geckodriver, service_log_path=self.log_file_path, options=options)

    def get_features(self, anuncio):
        self.browser.execute_script("arguments[0].scrollIntoView();", anuncio)
        time.sleep(2)
        anuncio.click()
        time.sleep(2)
        features = self.browser.find_elements_by_xpath('//div[@class="caract"]')

        return features


    @staticmethod
    def features_to_dict(features):
        feature_dict = dict()
        for feature in features:
            if feature.text.split(":")[0].strip() == "Marca":
                feature_dict["Marca"] = feature.text.split(":")[1].strip()
                continue
            if feature.text.split(":")[0].strip() == "Modelo":
                feature_dict["Modelo"] = feature.text.split(":")[1].strip()
                continue
            if feature.text.split(":")[0].strip() == "Versión":
                feature_dict["Características"] = feature.text.split(":")[1].strip()
                continue
            if feature.text.split(":")[0].strip() in ("Precio", "Precio contado"):
                feature_dict["Precio"] = feature.text.split(":")[1].strip()
                continue
            if feature.text.split(":")[0].strip() in ("Año", "Año Desde"):
                feature_dict["Anyo"] = feature.text.split(":")[1].strip()
                continue
            if feature.text.split(":")[0].strip() == "Combustible":
                feature_dict["Combustible"] = feature.text.split(":")[1].strip()
                continue
            if feature.text.split(":")[0].strip() == "Cambio":
                feature_dict["Cambio"] = feature.text.split(":")[1].strip()
                continue
            if feature.text.split(":")[0].strip() == "Potencia(CV)":
                feature_dict["Cilindrada_CV"] = feature.text.split(":")[1].strip()
                continue
            if feature.text.split(":")[0].strip() == "Kilómetros":
                feature_dict["Kilómetros"] = feature.text.split(":")[1].strip()
                continue

        return feature_dict


def run_main(logger, table_name, marker_table=None):
    MAX_RETRIES = 5
    retry_count = 0
    while retry_count < MAX_RETRIES:
        try:
            task = MLeonTask()
            break
        except:
            retry_count += 1
            time.sleep(3)

    logger.info("\n\n###############  MLEON  ###############\n")
    logger.info("MLEON: running exctraction algorithm")

    try:
        df_original = spark.read.table(table_name)
        datos_originales = df_original.count()
        logger.info(f"MLEON: reading original dataset with {datos_originales} rows")
    except:
        datos_originales = 0
        df_original = spark.createDataFrame()
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                Marca STRING,
                Modelo STRING
            )
            USING DELTA
            TBLPROPERTIES (
                'delta.columnMapping.mode' = 'name',
                'delta.minReaderVersion' = '2',
                'delta.minWriterVersion' = '5'
            )
        """)


    url="http://miguelleonautomoviles.inventario.pro/coches?"

    task.browser.get(url)

    time.sleep(5)

    try:
        task.browser.find_element_by_xpath('//a[@class="btn btn-default"]').click()
    except:
        pass

    time.sleep(5)

    task.browser.maximize_window()

    time.sleep(5)
    page = 0

    df_pages = pd.DataFrame()

    while(page < 5):

        df_first = pd.DataFrame()

        try:
            window_before = task.browser.window_handles[0]
            task.browser.switch_to.window(window_before)
        except Exception as e:
            logger.info(f"Error al cambiar de ventana: {e}")
            break

        try:
            anuncios_list = task.browser.find_elements_by_xpath('//div[@id="listado1"]//div[@class="row"]//div[@id="card1"]')
            anuncios_list = list(dict.fromkeys(anuncios_list))
        except Exception as e:
            logger.info(f"Error al obtener la lista de anuncios: {e}")
            continue

        for i in range(len(anuncios_list)):
            try:
                anuncio = anuncios_list[i]
                element = anuncio.find_element_by_xpath('.//a[@href]')
                enlace = element.get_attribute('href')

                features = task.get_features(anuncio)
                if len(features) == 0:
                    time.sleep(2)
                    features = task.get_features(anuncio)

                features_dict = task.features_to_dict(features)

                venta = "Profesional"
                location = "LAS PALMAS"

                try:
                    precio_financiado = features_dict["PrecioFinanciado"]
                    financiacion = "FINANCIADO"
                except:
                    financiacion = "SIN FINANCIACIÓN"

                try:
                    pd_anuncio = pd.DataFrame({'Marca': features_dict["Marca"],
                                        'Modelo': features_dict["Modelo"],
                                        'Caracteristicas': features_dict["Características"].upper(),
                                        'Precio': features_dict["Precio"],
                                        'Venta': venta,
                                        'AnyoMatriculacion': features_dict["Anyo"],
                                        'Kms': features_dict["Kilómetros"],
                                        'Combustible': features_dict["Combustible"],
                                        'Cilindrada': features_dict["Cilindrada_CV"],
                                        'Caja': features_dict["Cambio"],
                                        'Links': enlace,
                                        'Provincia': location,
                                        'Financiado': financiacion
                                        }, index=[0])
                    df_first = pd.concat([df_first, pd_anuncio], axis=0).reset_index(drop=True)
                except:
                    pass

            except Exception as e:
                logger.info(f"Error procesando anuncio {i}: {e}")
                continue

            try:
                task.browser.back()
                time.sleep(5)
            except Exception as e:
                logger.info(f"Error al volver a la página anterior: {e}")
            
            
            try:
                anuncios_list = task.browser.find_elements_by_xpath(
                    '//div[@id="listado1"]//div[@class="row"]//div[@id="card1"]')
                anuncios_list = list(dict.fromkeys(anuncios_list))
            except Exception as e:
                logger.info(f"Error al actualizar la lista de anuncios: {e}")

            time.sleep(5)

        df_pages = pd.concat([df_pages,df_first], axis=0)
        page += 1
        try:
            task.browser.find_elements_by_xpath('//ul[@class="paginacion"]//li')[-1].click()
            time.sleep(5)
        except:
            break




    df_pages = df_pages.drop_duplicates()

    task.browser.quit()

    try:
        df_pages["Web"] = "Mleon"

        hoy = datetime.datetime.now()
        ayer = datetime.datetime.now() - datetime.timedelta(days=1)

        if (len(str(hoy.day)) == 1) & (len(str(hoy.month)) == 1):
            hoy_day = str("0") + str(hoy.day) + str("/0") + str(hoy.month)
        if (len(str(hoy.day)) == 2) & (len(str(hoy.month)) == 1):
            hoy_day = str(hoy.day) + str("/0") + str(hoy.month)
        if (len(str(hoy.day)) == 1) & (len(str(hoy.month)) == 2):
            hoy_day = str("0") + str(hoy.day) + str("/") + str(hoy.month)
        if (len(str(hoy.day)) == 2) & (len(str(hoy.month)) == 2):
            hoy_day = str(hoy.day) + str("/") + str(hoy.month)

        df_pages["Publicacion"] = str(hoy_day) + " " + str(hoy.hour) + ":" + str(hoy.minute)
        df_pages["ExtractedDate"] = datetime.datetime.now().date()
        df_pages["Precio"] = df_pages["Precio"].str.replace("€", "", regex=False).str.replace(".", "", regex=False).str.strip()

        if df_pages.shape[0] == 0:
            raise Exception("DataFrame empty")

        logger.info("MLEON: exctracted data from web, with " + str(df_pages.shape[0]) + " rows")

        #df_pages = pd.concat([df_original, df_pages], axis=0).reset_index(drop=True)
    except:
        #df_pages = df_original.copy()

        #if df_pages.shape[0] <= 2:
            #raise Exception("DataFrame empty")
        pass

    df_pages = df_pages[["Marca",
                         "Modelo",
                         "Caracteristicas",
                         "Kms",
                         "Precio",
                         "Combustible",
                         "Caja",
                         "Cilindrada",
                         "AnyoMatriculacion",
                         "Provincia",
                         "Publicacion",
                         "ExtractedDate",
                         "Financiado",
                         "Venta",
                         "Web",
                         "Links"]]

    df_total_cleaned = spark.createDataFrame(df_pages)
    try:
        df_total_cleaned = df_original.unionByName(df_total_cleaned, allowMissingColumns=True)
        logger.info("MLEON: appended to original dataset OK, now total rows of " + str(df_total_cleaned.count()))
    except:
        pass


    #datatypes, df_total_cleaned = task.get_datatypes_dataframe(df_total_cleaned)
    #logger.info("MLEON: getting datatypes of dataset")
    df_total_cleaned = df_total_cleaned.dropDuplicates(
        ["Marca", "Modelo", "Precio", "Financiado", "Provincia", "AnyoMatriculacion", "Kms"])
    logger.info("MLEON: removing duplicates OK, now we have " + str(df_total_cleaned.count()) + " rows")


    df_total_cleaned = df_total_cleaned.withColumn("PublicacionAnyo", lit(datetime.datetime.now().year))

    logger.info(
        "MLEON: appended data to original, with " + str(df_total_cleaned.count()) + " rows, afterward remove duplicates")

    datatypes = {
        "Marca": NVARCHAR(60),
        "Modelo": NVARCHAR(150),
        "Caracteristicas": NVARCHAR(250),
        "Precio": NVARCHAR(20),
        "Financiado": NVARCHAR(40),
        "Publicacion": NVARCHAR(20),
        "Provincia": NVARCHAR(40),
        "Combustible": NVARCHAR(40),
        "AnyoMatriculacion": NVARCHAR(15),
        "Kms": NVARCHAR(25),
        "ExtractedDate": DATETIME(),
        "Cilindrada": NVARCHAR(20),
        "Caja": NVARCHAR(50),
        "Web": NVARCHAR(20),
        "Links": NVARCHAR(350),
        "Venta": NVARCHAR(100)
    }


    #datatypes, df_pages = DatabaseManager.convert_automated_datatypes_to_sqlalchemy_datatypes(df_pages)

    #df_total_cleaned = df_first #igualar al dataframe limpiado


    try:
        delta_table = DeltaTable.forName(spark, table_name)
        df_anterior = spark.table(table_name)
        datos_originales = df_anterior.count()
    except:
        datos_originales = 0


    df_total_cleaned.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .saveAsTable(table_name)


    datos_nuevos = df_total_cleaned.count()


    ultima_fecha = df_total_cleaned.select(max("ExtractedDate")).collect()[0][0]


    logger.info(f"MLEON: loading into Delta Table OK, with {datos_nuevos} total rows, last extracted date {ultima_fecha}")


    logger.info(f"MLEON: {datos_nuevos - datos_originales} new vehicles OK, {datos_nuevos} total rows, last extracted date {ultima_fecha}")


    if ultima_fecha and ultima_fecha < (datetime.datetime.now() - datetime.timedelta(days=7)).date():
        logger.error(f"MLEON: ExtractedDate not updated, some is wrong, last extracted date {ultima_fecha}")
    if marker_table is not None:
        marker_table.touch()


if __name__ == "__main__":
    logger = LoggerHelper("MLeonTask").create_logger()
    #catalog_name = dbutils.widgets.get("catalog_name")
    catalog_name = "catalog_dag"
    name = f"{catalog_name}.bronze.tb_123_mleontask"
    run_main(logger=logger, table_name=name)
