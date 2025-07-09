# Databricks notebook source
import time
time.sleep(35)

# COMMAND ----------

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

import selenium
import pandas as pd
import json
import sys
import os
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
import datetime
from sqlalchemy.types import *
from urllib import parse
from collections import OrderedDict
from sqlalchemy import create_engine
from typing_extensions import dataclass_transform
from pyspark.sql.functions import col, max
from pyspark.sql.functions import lit
import time
import random
import subprocess

# COMMAND ----------

spark = spark.builder \
    .appName("ETL_Scraper_Autoscout") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .getOrCreate()

# COMMAND ----------

class AutoscoutTask():

    def __init__(self):
        user = os.environ["USER"]
        self.geckodriver = f"/local_disk0/tmp/{user}/geckodriver/geckodriver"
        FIREFOX_BINARY = "/tmp/firefox/firefox"

        self.log_file_path = f"/tmp/geckodriver_{random.randint(1000, 9999)}.log"
        if os.path.exists(self.log_file_path):
            os.remove(self.log_file_path)

        self.options = webdriver.FirefoxOptions()
        self.options.add_argument('-headless')
        self.options.binary_location = FIREFOX_BINARY

    def initialize_browser(self):
        self.browser = webdriver.Firefox(executable_path=self.geckodriver, service_log_path=self.log_file_path, options=self.options)


def run_main(logger, table_name, marker_table=None):
    MAX_RETRIES = 5
    retry_count = 0
    while retry_count < MAX_RETRIES:
        try:
            task = AutoscoutTask()
            task.initialize_browser()
            break
        except:
            retry_count += 1
            time.sleep(3)

    logger.info("\n\n###############  AUTOSCOUT24  ###############\n")
    logger.info("AUTOSCOUT24: running exctraction algorithm")
    
    for i in range(2):
        df_first = pd.DataFrame()
        if i != 0:
            time.sleep(10)

        try:
            df_original = spark.read.table(table_name)
            datos_originales = df_original.count()
            logger.info(f"AUTOSCOUT24: reading original dataset with {datos_originales} rows")
        except:
            datos_originales = 0
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

        if i == 0:
            provincia = "Las Palmas"
            url = "https://www.autoscout24.es/lst?sort=standard&desc=0&ustate=N%2CU&lon=-15.430021&lat=28.124827&zip=Las%20Palmas%20de%20Gran%20Canaria&zipr=200&cy=E&atype=C"

        if i == 1:

            provincia = "S/C Tenerife"
            url = "https://www.autoscout24.es/lst?sort=standard&desc=0&cy=E&zipr=50&zip=Santa%20Cruz%20de%20Tenerife&lon=-16.25484&lat=28.4698&atype=C"


        try:
            task.browser.get(url)
        except:
            time.sleep(10)
            try:
                task.browser.get(url)
            except:
                continue


        time.sleep(10)

        task.browser.maximize_window()

        time.sleep(5)

        try:
            iframe = task.browser.find_element_by_xpath('//iframe[@id="gdpr-consent-notice"]')
            task.browser.switch_to.frame(iframe)
            task.browser.find_element_by_xpath('//button[@id="save"]//div[@class="action-wrapper"]/span').click()
            task.browser.refresh()
        except:
            pass

        anuncios_list = task.browser.find_elements_by_xpath('//article')

        for anuncio in anuncios_list:
            task.browser.execute_script("arguments[0].scrollIntoView();", anuncio)
            title = anuncio.find_element_by_xpath('.//a/h2').text
            enlace = anuncio.find_element_by_xpath('.//a').get_attribute('href')
            precio_str = anuncio.find_element_by_xpath('.//div/div[3]/div[1]').text
            precio = precio_str.split(',')[0].replace(".", "").replace('€', "").strip()
            try:
                kms_str = anuncio.find_element_by_xpath('.//div/div[3]/div[2]/span[1]').text
                caja = anuncio.find_element_by_xpath('.//div/div[3]/div[2]/span[2]').text
                anyo_str = anuncio.find_element_by_xpath('.//div/div[3]/div[2]/span[3]').text
                combustible = anuncio.find_element_by_xpath('.//div/div[3]/div[2]/span[4]').text
                potencia_str = anuncio.find_element_by_xpath('.//div/div[3]/div[2]/span[5]').text
                potencia = potencia_str.upper().split("(")[1].replace(")", "").replace('CV', "").strip()
            except:
                try:
                    features_str = anuncio.find_element_by_xpath('.//div/div[3]/div[3]').text
                    features_list = features_str.split("\n")
                    kms_str = features_list[0]
                    anyo_str = features_list[1]
                    potencia = features_list[2].split("(")[1].split("CV")[0].strip()
                    caja = features_list[5]
                    combustible = features_list[6]
                    venta_str = anuncio.text.upper()
                except:
                    try:
                        features_str = anuncio.find_element_by_xpath('.//div/div[3]/div[3]').text
                        features_list = features_str.split("\n")
                        kms_str = features_list[0]
                        anyo_str = features_list[2]
                        potencia = features_list[4].split("(")[1].split("CV")[0].strip()
                        caja = features_list[1]
                        combustible = features_list[3]
                        venta_str = anuncio.text.upper()
                    except:
                        continue


            kms = kms_str.replace(".", "").replace('km', "").replace('kms', "").strip()
            try:
                venta_str = anuncio.find_element_by_xpath('.//div/div[3]/div[2]/span[5]').text
            except:
                venta_str = 'Profesional'
            try:
                anyo = anyo_str.split('/')[1].strip()
            except:
                continue

            if "PARTICULAR" in venta_str:
                venta = 'Particular'
            else:
                venta = 'Profesional'

            title = title.replace('\n', " ")
            marca = title.split(" ")[0].strip()
            modelo = title.split(" ")[1].split(" ")[0]

            if (modelo == 'CLASE') | (modelo == 'SERIE'):
                modelo = modelo + ' ' + title.split(" - ")[1].split(" ")[1]

            pd_anuncio = pd.DataFrame({'Marca': marca,
                                       'Modelo': modelo,
                                       'Caracteristicas': title,
                                       'Precio': precio,
                                       'Venta': venta,
                                       'AnyoMatriculacion': anyo,
                                       'Kms': kms.strip().split(" ")[0].replace(".", ""),
                                       'Combustible': combustible,
                                       'Cilindrada': potencia.strip().split(" ")[0],
                                       'Caja': caja.strip(),
                                       'Links': enlace,
                                       'Financiado': 'SIN FINANCIACIÓN',
                                       'Provincia': provincia

                                       }, index=[0])
            df_first = pd.concat([df_first, pd_anuncio], ignore_index=True).reset_index(drop=True)


        hoy = datetime.datetime.now()
        if (len(str(hoy.day)) == 1) & (len(str(hoy.month)) == 1):
            hoy_day = str("0") + str(hoy.day) + str("/0") + str(hoy.month)
        if (len(str(hoy.day)) == 2) & (len(str(hoy.month)) == 1):
            hoy_day = str(hoy.day) + str("/0") + str(hoy.month)
        if (len(str(hoy.day)) == 1) & (len(str(hoy.month)) == 2):
            hoy_day = str("0") + str(hoy.day) + str("/") + str(hoy.month)
        if (len(str(hoy.day)) == 2) & (len(str(hoy.month)) == 2):
            hoy_day = str(hoy.day) + str("/") + str(hoy.month)

        df_first["Publicacion"] = str(hoy_day) + " " + str(hoy.hour) + ":" + str(hoy.minute)
        df_first["ExtractedDate"] = datetime.datetime.now().date()
        df_first['Web'] = "Autoscout24.es"

        df_first.Combustible = df_first.Combustible.replace("Eléctrico", "Eléctrico/Híbrido")

        if df_first.shape[0] <= 2:
            raise Exception("DataFrame empty")

        logger.info("AUTOSCOUT24: exctracted data from web, region " + str(provincia) + ", with " + str(df_first.shape[0]) + " rows")

        
        df_first = spark.createDataFrame(df_first)
        try:
            df_first = df_original.unionByName(df_first, allowMissingColumns=True)
            logger.info("AUTOSCOUT24: appended to original dataset OK, now total rows of " + str(df_first.count()))
        except:
            pass

        df_first = df_first.dropDuplicates(
            ["Marca", "Modelo", "Caracteristicas", "Precio", "Financiado", "Provincia", "AnyoMatriculacion", "Kms"])

        df_first = df_first[["Marca",
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
                             "Web",
                             "Venta",
                             "Links"]]

        df_first = df_first.withColumn("PublicacionAnyo", lit(datetime.datetime.now().year))

        logger.info("AUTOSCOUT24: appended data to original, region " + str(provincia) + ", with " + str(df_first.count()) + " rows, afterward remove duplicates")

        datatypes = {
            "Marca": NVARCHAR(20),
            "Modelo": NVARCHAR(30),
            "Caracteristicas": NVARCHAR(200),
            "Precio": NVARCHAR(15),
            "Financiado": NVARCHAR(20),
            "Publicacion": NVARCHAR(15),
            "Provincia": NVARCHAR(20),
            "Combustible": NVARCHAR(30),
            "AnyoMatriculacion": NVARCHAR(20),
            "Kms": NVARCHAR(15),
            "ExtractedDate": DATETIME(),
            "Cilindrada": NVARCHAR(15),
            "Caja": NVARCHAR(25),
            "Web": NVARCHAR(20),
            "PublicacionAnyo": NVARCHAR(4),
            "Venta":NVARCHAR(100),
            "Links": NVARCHAR(300)
        }

        df_total_cleaned = df_first


        try:
            delta_table = DeltaTable.forName(spark, table_name)
            df_anterior = spark.table(table_name)
            datos_originales = df_anterior.count()
        except:
            datos_originales = 0

        df_total_cleaned = df_total_cleaned.withColumn("PublicacionAnyo", col("PublicacionAnyo").cast("string"))

        df_total_cleaned.write.format("delta") \
            .option("mergeSchema", "true") \
            .mode("overwrite") \
            .saveAsTable(table_name)


        datos_nuevos = df_total_cleaned.count()


        ultima_fecha = df_total_cleaned.select(max("ExtractedDate")).collect()[0][0]


        logger.info(f"AUTOSCOUT24: loading into Delta Table OK, with {datos_nuevos} total rows, last extracted date {ultima_fecha}")


        logger.info(f"AUTOSCOUT24: {datos_nuevos - datos_originales} new vehicles OK, {datos_nuevos} total rows, last extracted date {ultima_fecha}")

        time.sleep(5)

        if ultima_fecha and ultima_fecha < (datetime.datetime.now() - datetime.timedelta(days=7)).date():
            logger.error(f"AUTOSCOUT24: ExtractedDate not updated, some is wrong, last extracted date {ultima_fecha}")
            raise Exception("At least one week ExtractedDate not updated")            

    task.browser.quit()

    if marker_table is not None:
        marker_table.touch()


if __name__ == '__main__':
    logger = LoggerHelper("AutoscoutTask").create_logger()
    #catalog_name = dbutils.widgets.get("catalog_name")
    catalog_name = "catalog_dag"
    name = f"{catalog_name}.bronze.tb_123_autoscouttask"
    run_main(logger=logger, table_name=name)
