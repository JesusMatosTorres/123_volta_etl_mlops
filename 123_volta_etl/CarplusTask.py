# Databricks notebook source
import time
time.sleep(40)

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

import os
import random
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
from pyspark.sql.functions import initcap, col, max
from pyspark.sql.functions import lit
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import subprocess

# COMMAND ----------

spark = spark.builder \
    .appName("ETL_Scraper_Carplus") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .getOrCreate()


# COMMAND ----------

class CarplusTask():

    def __init__(self):

        user = os.environ["USER"]
        self.geckodriver = f"/local_disk0/tmp/{user}/geckodriver/geckodriver"
        FIREFOX_BINARY = "/tmp/firefox/firefox"
        

        options = webdriver.FirefoxOptions()
        options.add_argument('-headless')
        options.set_preference("dom.disable_open_during_load", False)
        options.set_preference('dom.popup_maximum', -1)
        options.binary_location = FIREFOX_BINARY

        self.log_file_path = f"/tmp/geckodriver_{random.randint(1000, 9999)}.log"
        if os.path.exists(self.log_file_path):
            os.remove(self.log_file_path)

        self.browser = webdriver.Firefox(executable_path=self.geckodriver, service_log_path=self.log_file_path, options=options)

    def get_features(self, anuncio):
        self.browser.execute_script("arguments[0].scrollIntoView();", anuncio)
        time.sleep(2)
        anuncio.click()
        time.sleep(2)
        features = self.browser.find_elements_by_xpath('//div[@class="caract"]')

        return features


def run_main(logger, table_name, marker_table=None):
    MAX_RETRIES = 5
    retry_count = 0
    while retry_count < MAX_RETRIES:
        try:
            task = CarplusTask()
            break
        except:
            retry_count += 1
            time.sleep(3)
    
    logger.info("\n\n###############  CARPLUS  ###############\n")
    logger.info("CARPLUS: running exctraction algorithm")

    try:
        df_original = spark.read.table(table_name)
        datos_originales = df_original.count()
        logger.info(f"CARPLUS: reading original dataset with {datos_originales} rows")
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

    url="https://carpluscanarias.net/coches/"

    task.browser.get(url)

    time.sleep(5)

    try:
        task.browser.find_element_by_xpath('//button[@id="didomi-notice-agree-button"]').click()
    except:
        pass
    time.sleep(2)

    task.browser.maximize_window()

    for i in range(10):
        try:
            task.browser.find_element_by_xpath("//*[contains(text(),'Ver mas')]").click()
            time.sleep(1)
        except:
            break

    time.sleep(5)

    window_before = task.browser.window_handles[0]
    task.browser.switch_to.window(window_before)

    anuncios_list = task.browser.find_elements_by_xpath('//div[@id="container-model-list"]//li')

    df_anuncios_list = pd.DataFrame()

    for anuncio in anuncios_list:
        try:
            title = anuncio.find_element_by_xpath('.//span').text.upper()
            if ("SILENCE" in title) or ("MOTO" in title) or ("DUCATI" in title):
                continue
            marca = anuncio.find_element_by_xpath('.//span').text.split(" ")[0].strip()
            modelo = anuncio.find_element_by_xpath('.//span').text.split(" ")[1].strip()

            if ("NUEVO" in modelo.upper()):
                modelo = anuncio.find_element_by_xpath('.//span').text.split(" ")[2].strip()

            caracteristicas = anuncio.find_element_by_xpath('.//strong').text
            anyo = anuncio.find_elements_by_xpath('.//span[1]')[1].text
            kms = anuncio.find_element_by_xpath('.//span[2]').text.replace("km", "").strip()
            combustible = anuncio.find_element_by_xpath('.//span[3]').text.strip()
            caja = anuncio.find_element_by_xpath('.//span[4]').text.strip()
            precio = anuncio.find_elements_by_xpath('.//strong')[1].text.replace(".", "").replace("€", "")
            if anuncio.find_element_by_xpath(".//small").text.upper().strip() == "FINANCIADO":
                financiacion = "FINANCIADO"
            else:
                financiacion = "SIN FINANCIACIÓN"


            link = anuncio.find_element_by_xpath('.//a').get_attribute("href")

            link_element = anuncio.find_element_by_xpath('.//h3[@class="ignore_toc"]/a')

            task.browser.execute_script("arguments[0].scrollIntoView();", link_element)
            time.sleep(1)
            
            if link_element.is_displayed() and link_element.is_enabled():
                try:
                    link_element.click()
                except:
                    task.browser.execute_script("arguments[0].click();", link_element)
            else:
                task.browser.execute_script("arguments[0].click();", link_element)
            
            time.sleep(2)

            window_after = task.browser.window_handles[1]
            task.browser.switch_to.window(window_after)

            time.sleep(2)

            list_details = task.browser.find_elements_by_xpath('.//ul[@class="flex"]/li')[0].text.split("\n")
            cv = [elem for elem in list_details if " CV" in elem]
            potencia = cv[-1].replace("CV", "").strip()

            time.sleep(2)

            task.browser.switch_to.window(window_before)

            pd_anuncio = pd.DataFrame({'Marca': marca,
                                   'Modelo': modelo,
                                   'Caracteristicas': caracteristicas.upper(),
                                   'Precio': precio,
                                   'Venta': "PROFESIONAL",
                                   'AnyoMatriculacion': anyo,
                                   'Kms': kms,
                                   'Combustible': combustible,
                                   'Cilindrada': potencia,
                                   'Caja': caja,
                                   'Links': link,
                                   'Provincia': "LAS PALMAS",
                                   'Financiado': financiacion
                                   }, index=[0])
            df_anuncios_list = df_anuncios_list.append(pd_anuncio).reset_index(drop=True)
        except:
            continue


    df_anuncios_list = df_anuncios_list.drop_duplicates()

    task.browser.quit()

    try:
        df_anuncios_list["Web"] = "Carplus"

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

        ix = df_anuncios_list['Caja'].str.contains("Manual")
        df_anuncios_list.loc[ix, 'Caja'] = "Manual"
        ix = df_anuncios_list['Caja'].str.upper().str.contains("TRONIC")
        df_anuncios_list.loc[ix, 'Caja'] = "Automática"
        ix = df_anuncios_list['Caja'].str.upper().str.contains("DSG")
        df_anuncios_list.loc[ix, 'Caja'] = "Automática"
        df_anuncios_list["Publicacion"] = str(hoy_day) + " " + str(hoy.hour) + ":" + str(hoy.minute)
        df_anuncios_list["ExtractedDate"] = datetime.datetime.now().date()
        df_anuncios_list["Precio"] = df_anuncios_list["Precio"].str.replace("€", "", regex=True).str.replace(".", "", regex=True).str.strip()
        

        if df_anuncios_list.shape[0] == 0:
            raise Exception("DataFrame empty")

        logger.info("CARPLUS: exctracted data from web, with " + str(df_anuncios_list.shape[0]) + " rows")

        df_pages = spark.createDataFrame(df_anuncios_list)
        df_pages = df_original.unionByName(df_pages, allowMissingColumns=True)

    except:
        df_pages = df_original

        if df_pages.count() <= 2:
            raise Exception("DataFrame empty")

    #datatypes, df_pages = task.get_datatypes_dataframe(df_pages)
    logger.info("CARPLUS: getting datatypes of dataset")
    df_pages = df_pages.dropDuplicates(
        ["Marca", "Modelo", "Precio", "Financiado", "Provincia", "AnyoMatriculacion", "Kms"])
    logger.info("CARPLUS: removing duplicates OK, now we have " + str(df_pages.count()) + " rows")


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

    df_pages = df_pages.withColumn("PublicacionAnyo", lit(datetime.datetime.now().year))
    df_pages = df_pages.withColumn("Venta", lit("Profesional"))
    df_pages = df_pages.withColumn("Caja", initcap(col("Caja")))

    logger.info(
        "CARPLUS: appended data to original, with " + str(df_pages.count()) + " rows, afterward remove duplicates")

    datatypes = {

        "Marca": NVARCHAR(100),
        "Modelo": NVARCHAR(200),
        "Caracteristicas": NVARCHAR(300),
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
        "Links": NVARCHAR(400),
        "Venta": NVARCHAR(100)
    }


    #datatypes, df_pages = DatabaseManager.convert_automated_datatypes_to_sqlalchemy_datatypes(df_pages)

    df_total_cleaned = df_pages


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


    logger.info(f"CARPLUS: loading into Delta Table OK, with {datos_nuevos} total rows, last extracted date {ultima_fecha}")


    logger.info(f"CARPLUS: {datos_nuevos - datos_originales} new vehicles OK, {datos_nuevos} total rows, last extracted date {ultima_fecha}")


    if ultima_fecha and ultima_fecha < (datetime.datetime.now() - datetime.timedelta(days=7)).date():
        logger.error(f"CARPLUS: ExtractedDate not updated, some is wrong, last extracted date {ultima_fecha}")

    if marker_table is not None:
        marker_table.touch()


if __name__ == "__main__":
    logger = LoggerHelper("CarplusTask").create_logger()
    #catalog_name = dbutils.widgets.get("catalog_name")
    catalog_name = "catalog_dag"
    name = f"{catalog_name}.bronze.tb_123_carplustask"
    run_main(logger=logger, table_name=name)
