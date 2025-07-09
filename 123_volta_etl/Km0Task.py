# Databricks notebook source
import time
time.sleep(10)

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

import selenium, pandas as pd, numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
import time, datetime
from typing_extensions import dataclass_transform
from sqlalchemy.types import *
from urllib import parse
from collections import OrderedDict
from sqlalchemy import create_engine
from pyspark.sql.functions import col, max
import random
import subprocess
import os

# COMMAND ----------

spark = spark.builder \
    .appName("ETL_Scraper") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .getOrCreate()

# COMMAND ----------

class Km0Task:

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
        self.initialize_browser()

    def initialize_browser(self):
        self.browser = webdriver.Firefox(executable_path=self.geckodriver, service_log_path=self.log_file_path, options=self.options)

    def get_datatypes_dataframe(self, df, changes=None):
        """
        Function to get datatypes dictionary to load data into databases

        function to get the DataFrame and build up the dictionary with the datatypes,
        it recognize the datatypes of pandas dataframe and change for SQL datatypes,
        if a change is directly necessary we have changes parameter ({"colum_to_change": datatypes_to_change})
        Also all columns with "date" in their names automatically will change to datetime type

        :param df: dataframe with the data
        :param changes: (optional) if a directly change is needed

        :return: dataframe with the datatypes properly selected
        :return: dictionary with the datatypes

        """
        datatypes = {}
        types_defined = [
         'int', 
         'float', 
         'obj', 
         'str', 
         'bool', 
         'date']
        datatypes_defined = {'int':Integer(), 
         'float':Float(), 
         'bool':Boolean()}
        df = df.where(pd.notnull(df), None)
        df = df.replace({"nan": None})
        df = df.replace({"None": None})
        if changes is not None:
            for key, value in changes.items():
                df[key] = df[key].astype(value)

        types = df.dtypes.to_dict()
        for key, value in types.items():
            type_selected = [type_defined for type_defined in types_defined if type_defined in str(value)][0]
            if ((type_selected == "obj") | (type_selected == "str")) & ("date" not in key.lower()):
                df[key] = df[key].astype(str)
                max_lenght = df[key].map(len).max() + 5
                type_to_get = NVARCHAR(max_lenght)
                datatypes[key] = type_to_get
            elif "date" in key.lower():
                df[key] = pd.to_datetime(df[key])
                datatypes[key] = DateTime()
            else:
                datatypes[key] = datatypes_defined[type_selected]

        df = df.where(pd.notnull(df), None)
        df = df.replace({"nan": None})
        df = df.replace({"None": None})
        return (
         datatypes, df)

    def clean_data(self, row):
        row["Financiado"] = "SIN FINANCIACIÓN"
        row["Precio"] = row["Precio"].split("€")[0].replace(".", "").strip()
        row["Kms"] = row["Kms"].replace(".", "").strip()
        row["Caracteristicas"] = row["Caracteristicas"].strip()
        row["Marca"] = row["Marca"].strip()
        row["Modelo"] = row["Modelo"].strip()

        try:
            if "(" in row["Combustible"] or "Eléctrico" in row["Combustible"]:
                row["Combustible"] = "Electrico/hibrido"
        except:
            pass

        try:
            if "Serie" in row["Modelo"]:
                row["Modelo"] = row["Caracteristicas"].split(" ")[0]
        except:
            pass

        return row


def run_main(logger, table_name, marker_table=None):
    MAX_RETRIES = 5
    retry_count = 0
    while retry_count < MAX_RETRIES:
        try:
            task = Km0Task()
            break
        except:
            retry_count += 1
            time.sleep(3)

    logger.info("\n\n###############  KM0  ###############\n")
    logger.info("KM0: running exctraction algorithm")
    
    try:
        df_original = spark.read.table(table_name)
        datos_originales = df_original.count()
        logger.info(f"KM0: reading original dataset with {datos_originales} rows")
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

    url = "https://www.tukm0.com/coches"
    task.browser.get(url)
    task.browser.maximize_window()
    time.sleep(2)
    logger.info("KM0: openning web browser Ok")
    try:
        task.browser.find_element_by_xpath(".//*[contains(text(),'Entendido')]").click()
    except:
        pass

    try:
        task.browser.find_element_by_xpath('//div[@id="cookiescript_accept"]').click()
    except:
        pass

    logger.info("KM0: getting vehicles from web Ok")
    df_total_full = pd.DataFrame()
    for i in range(5):
        df_total = pd.DataFrame()
        anuncios_total = task.browser.find_elements_by_xpath('//div[@id="card16"]')
        logger.info("KM0: extracting cars, from " + str(i) + " page")
        
        try:
            if len(anuncios_total) == 0:
                logger.info("KM0: no cars found in " + str(i) + " page")
            
            for j in range(len(anuncios_total)):

                try:
                    anuncio = anuncios_total[j].find_element_by_xpath(".//img")
                    task.browser.execute_script("arguments[0].scrollIntoView()", anuncio)
                    time.sleep(2)
                    anuncio.click()
                    time.sleep(1)
                    
                    modelo_not_cleaned = task.browser.find_element_by_xpath('//div[@class="container-caracteristicas"]').text.split("\n")
                    
                    try:
                        precio = modelo_not_cleaned[modelo_not_cleaned.index("Precio:") + 1]
                        marca = modelo_not_cleaned[modelo_not_cleaned.index("Marca:") + 1]
                        caracteristicas = modelo_not_cleaned[modelo_not_cleaned.index("Versión:") + 1]
                        modelo = modelo_not_cleaned[modelo_not_cleaned.index("Modelo:") + 1]
                        kms = modelo_not_cleaned[modelo_not_cleaned.index("Kilómetros:") + 1]
                        potencia = modelo_not_cleaned[modelo_not_cleaned.index("Potencia(CV):") + 1]
                        caja = modelo_not_cleaned[modelo_not_cleaned.index("Cambio:") + 1]
                        combustible = modelo_not_cleaned[modelo_not_cleaned.index("Combustible:") + 1]
                        
                        try:
                            matriculacion = modelo_not_cleaned[modelo_not_cleaned.index("Año:") + 1]
                        except:
                            matriculacion = modelo_not_cleaned[modelo_not_cleaned.index("Año Desde:") + 1]

                        link = task.browser.current_url
                        time.sleep(1)
                        df_initial = pd.DataFrame({'Marca':marca.strip(), 
                            'Modelo':modelo.strip(), 
                            'Precio':precio.strip(), 
                            'Caracteristicas':caracteristicas.strip(), 
                            'Cilindrada':potencia.strip(), 
                            'AnyoMatriculacion':matriculacion.strip(), 
                            'Venta':"Profesional", 
                            'Kms':kms.strip(), 
                            'Web':"TuKm0", 
                            'Caja':caja.strip(), 
                            'Combustible':combustible.strip(), 
                            'Links':link.strip()},
                        index=[0])

                    except Exception as e:
                        logger.info("ERROR in ANUNCIO " + str(j+1) + " " + str(i) + " page: " + str(e))
                        continue

                    task.browser.back()
                    time.sleep(2)
                    df_total = pd.concat([df_total, df_initial], ignore_index=True).reset_index(drop=True)
                    anuncios_total = task.browser.find_elements_by_xpath('//div[@id="card16"]')

                except Exception as e:
                    logger.info("ERROR en la interaccion con el anuncio " + str(j+1) + " " + str(i) + " page: " + str(e))
                    continue

        except Exception as e:
            logger.info("ERROR in PAGE " + str(i) + " " + str(e))
            task.browser.back()
            time.sleep(2)
            try:
                task.browser.find_element_by_xpath("//*[contains(text(), 'Siguiente')]").click()
            except:
                break

            time.sleep(2)
            continue
        
        try:
            task.browser.find_element_by_xpath("//*[contains(text(), 'Siguiente')]").click()
        except Exception as e:
            logger.info("KM0: error in next page " + str(e))

        time.sleep(2)
        df_total_full = pd.concat([df_total_full, df_total], ignore_index=True).reset_index(drop=True)

    logger.info("KM0: extraction vehicles Ok, with " + str(df_total_full.shape[0]) + " rows")
    task.browser.close()
    logger.info("KM0: cleaning data of vehicles Ok, with " + str(df_total_full.shape[0]) + " rows")
    df_total_cleaned = df_total_full.apply((lambda x: task.clean_data(x)), axis=1)
    df_total_cleaned["Provincia"] = "Las Palmas"
    df_total_cleaned["Publicacion"] = datetime.datetime.now().date()
    df_total_cleaned["ExtractedDate"] = datetime.datetime.now().date()

    
    df_total_cleaned = spark.createDataFrame(df_total_cleaned)
    try:
        df_total_cleaned = df_original.unionByName(df_total_cleaned, allowMissingColumns=True)
        logger.info("KM0: appended to original dataset OK, now total rows of " + str(df_total_cleaned.count()))
    except:
        pass

    #datatypes, df_total_cleaned = task.get_datatypes_dataframe(df_total_cleaned)
    logger.info("KM0: getting datatypes of dataset")
    df_total_cleaned = df_total_cleaned.dropDuplicates(
        ["Marca", "Modelo", "Precio", "Financiado", "Provincia", "AnyoMatriculacion", "Kms"])
    logger.info("KM0: removing duplicates OK, now we have " + str(df_total_cleaned.count()) + " rows")

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

    logger.info(f"KM0: loading into Delta Table OK, with {datos_nuevos} total rows, last extracted date {ultima_fecha}")

    logger.info(f"KM0: {datos_nuevos - datos_originales} new vehicles OK, {datos_nuevos} total rows, last extracted date {ultima_fecha}")

    if ultima_fecha and ultima_fecha.date() < (datetime.datetime.now() - datetime.timedelta(days=7)).date():
        logger.error(f"KM0: ExtractedDate not updated, some is wrong, last extracted date {ultima_fecha}")
        raise Exception("At least one week ExtractedDate not updated")
    if marker_table is not None:
        marker_table.touch()


if __name__ == "__main__":
    logger = LoggerHelper("Km0Task").create_logger()
    #catalog_name = dbutils.widgets.get("catalog_name")
    catalog_name = "catalog_dag"
    name = f"{catalog_name}.bronze.tb_123_km0task"
    run_main(logger=logger, table_name=name)
