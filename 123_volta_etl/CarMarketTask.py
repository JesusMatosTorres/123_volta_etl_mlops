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

import selenium
import random
import pandas as pd
import os
import sys
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
import time
import datetime
from sqlalchemy.types import *
from urllib import parse
import json
from collections import OrderedDict
from sqlalchemy import create_engine
from pyspark.sql.functions import max
from pyspark.sql.functions import initcap, col, split, when
import subprocess

# COMMAND ----------

spark = spark.builder \
    .appName("ETL_Scraper_CarMarketTask") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .getOrCreate()


# COMMAND ----------

class CarMarket():
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

        self.iteration = 0

    def initialize_browser(self):
        self.browser = webdriver.Firefox(executable_path=self.geckodriver, service_log_path=self.log_file_path, options=self.options)

    def get_datatypes_dataframe(self,
                                df,
                                changes=None):
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
            "int",
            "float",
            "obj",
            "str",
            "bool",
            "date"
        ]

        datatypes_defined = {
            "int": Integer(),
            "float": Float(),
            "bool": Boolean()
        }

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

            elif ("date" in key.lower()):
                df[key] = pd.to_datetime(df[key])
                datatypes[key] = DateTime()
            else:
                datatypes[key] = datatypes_defined[type_selected]

        return datatypes, df


    def clean_data(self,
                   row):
        row["Modelo"] = " ".join(row["Marca"].split(" ")[1:-1])
        row[0] = row[0].replace("Nuevo", "").replace("", "").strip()
        row["Marca"] = row[0].split(" ")[0]

        row["Caracteristicas"] = " ".join(row["Caracteristicas"].split(" ")[1:])
        try:
            row["Precio"] = row["Precio"].replace("€", "").replace(".", "")
        except:
            pass
        if row["Modelo"] == "":
            try:
                row["Modelo"] = row["Caracteristicas"].split(" ")[0]
            except:
                row["Modelo"] = None
        row["Financiado"] = "FINANCIADO"
        row["AnyoMatriculacion"] = row["Caracteristicas"].split("|")[1].strip()
        try:
            row["Kms"] = row["Kms"].replace("km", "")
        except:
            pass

        try:
            row["Cilindrada"] = row["Cilindrada"].replace(" CV", "")
        except:
            pass
        try:
            if "." in row["Caracteristicas"]:
                position = row["Caracteristicas"].find(".")
        except:
            row["Capacidad"] = None
        try:
            row["Caja"] = row["Caja"].split(" ")[0]
        except:
            row["Caja"] = row["Caja"]
        return row

def run_main(logger, table_name, marker_table=None):
    MAX_RETRIES = 5
    retry_count = 0
    while retry_count < MAX_RETRIES:
        try:
            task = CarMarket()
            task.initialize_browser()
            break
        except:
            retry_count += 1
            time.sleep(3)

    logger.info("\n\n###############  CAR - MARKET  ###############\n")
    logger.info("CARMARKET: running exctraction algorithm")
    
    try:
        df_original = spark.read.table(table_name)
        datos_originales = df_original.count()
        logger.info(f"CARMARKET: reading original dataset with {datos_originales} rows")
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

    url = "https://lookar.es/coches/"
    
    task.browser.get(url)

    task.browser.maximize_window()
    time.sleep(4)

    try:
        task.browser.find_element_by_xpath("//div[@id='didomi-notice']//button[2]").click()
    except:
        pass
    try:
        task.browser.find_element_by_xpath("//div[@id='didomi-popup']").click()
    except:
        pass
    try:
        task.browser.find_element_by_xpath("//button[@id='didomi-notice-agree-button']").click()
    except:
        pass

    logger.info("CARMARKET: opened web properly, beginning extraction of data Ok")

    no_pages = 10
    for _ in range(no_pages):
        try:
            car_list = task.browser.find_element_by_id("container-model-list")
            cars = car_list.find_elements_by_tag_name("li")
            task.browser.execute_script("arguments[0].scrollIntoView()", cars[-1])
            time.sleep(5)
            task.browser.find_element_by_xpath("//div[8]/button").click()
            time.sleep(2)
        except:
            continue

    car_list = task.browser.find_element_by_id("container-model-list")
    cars = car_list.find_elements_by_tag_name("li")
    task.browser.execute_script("arguments[0].scrollIntoView()", cars[0])
    df_all_cars = pd.DataFrame()

    for car in cars:
        try:
            task.browser.execute_script("arguments[0].scrollIntoView()", car)
            car.find_element_by_xpath(".//span[@class='text-sm block']").click()
        except:
            continue

        time.sleep(1)

        current_window = task.browser.current_window_handle
        new_window = [window for window in task.browser.window_handles if window != current_window][0]
        task.browser.switch_to.window(new_window)
        link = task.browser.current_url
        if link == "about:blank":
            time.sleep(1)
            link = task.browser.current_url
        else:
            pass
        try:
            carros = task.browser.find_elements_by_class_name("flex-grow")
            details = carros[1].text.split("\n")
            df_getting_cars = pd.DataFrame({"Marca": details[0],
                                            "Caracteristicas": details[1],
                                            "Kms": details[2],
                                            "Cilindrada": details[3],
                                            "Caja": details[4],
                                            "Combustible": task.browser.find_elements_by_class_name("my-3")[0].text.split("\n")[-1],
                                            "Precio": task.browser.find_elements_by_class_name("py-6")[1].text.split("\n")[1],
                                            "Links": link},
                                           index=[0])
            
            df_all_cars = pd.concat([df_all_cars, df_getting_cars], ignore_index=True).reset_index(drop=True)
        except:
            pass
        task.browser.close()
        task.browser.switch_to.window(current_window)
        time.sleep(1)
        if int(df_all_cars.shape[0])%no_pages == 0:
            logger.info("CARMARKET: getting partials vehicles Ok, extraction with " + str(df_all_cars.shape[0]) + " rows")
        task.browser.back()

    task.browser.close()

    logger.info("CARMARKET: getting total vehicles Ok, extraction with " + str(df_all_cars.shape[0]) + " rows")

    try:
        task.browser.quit()
    except:
        pass

    logger.info("CARMARKET: extraction of data from web Ok")

    df_vehicles_cleaned = df_all_cars.apply(lambda x: task.clean_data(x), axis=1)

    logger.info("CARMARKET: task of cleaning Ok, cleaning with " + str(df_vehicles_cleaned.shape[0]) + " rows")

    df_vehicles_cleaned = df_vehicles_cleaned[df_vehicles_cleaned.Marca != "DUCATI"]
    df_vehicles_cleaned = df_vehicles_cleaned[df_vehicles_cleaned.Marca != "HONDA"]

    df_vehicles_cleaned = df_vehicles_cleaned[(pd.isnull(df_vehicles_cleaned["AnyoMatriculacion"]) == False) &
                                              (pd.isnull(df_vehicles_cleaned["Modelo"]) == False) &
                                              (pd.isnull(df_vehicles_cleaned["Caja"]) == False)]

    mapper = {"S": "Automático", "Manual": "Manual", "Automática": "Automático"}
    df_vehicles_cleaned["Caja"] = df_vehicles_cleaned["Caja"].str.strip().str.capitalize().map(mapper)

    df_vehicles_cleaned.Precio = pd.to_numeric(df_vehicles_cleaned.Precio,errors='coerce')
    df_vehicles_cleaned.Kms = pd.to_numeric(df_vehicles_cleaned.Kms, errors='coerce')
    df_vehicles_cleaned["Cilindrada"] = pd.to_numeric(df_vehicles_cleaned["Cilindrada"], errors='coerce')

    df_vehicles_cleaned["Provincia"] = "Las Palmas"
    df_vehicles_cleaned["Publicacion"] = datetime.datetime.now().date()
    df_vehicles_cleaned["ExtractedDate"] = datetime.datetime.now().date()

    df_vehicles_cleaned["Web"] = "CarMarket"
    df_vehicles_cleaned["Venta"] = "Profesional"

    logger.info("CARMARKET: cleaning dataset Ok")

    
    df_vehicles_cleaned = spark.createDataFrame(df_vehicles_cleaned)
    df_vehicles_cleaned = df_vehicles_cleaned \
            .withColumn("Kms", split(col("Kms").cast("string"), "\\.").getItem(0)) \
            .withColumn("Precio", split(col("Precio").cast("string"), "\\.").getItem(0)) \
            .withColumn("Cilindrada", when(col("Cilindrada").isNotNull(), split(col("Cilindrada").cast("string"), "\\.").getItem(0)))
    
    df_vehicles_cleaned = df_vehicles_cleaned.withColumn("Combustible", initcap(col("Combustible")))

    try:
        df_vehicles_cleaned = df_original.unionByName(df_vehicles_cleaned, allowMissingColumns=True)
        logger.info("CARMARKET: appended to original dataset OK, now total rows of " +str(df_vehicles_links.count()))
    except:
        df_vehicles_cleaned = df_vehicles_cleaned


    df_total_cleaned = df_vehicles_cleaned.dropDuplicates(
        ["Marca", "Modelo", "Precio", "Financiado", "Provincia", "AnyoMatriculacion", "Kms"])

    #datatypes, df_vehicles_links = task.get_datatypes_dataframe(df_vehicles_links)

    logger.info("CARMARKET: removing duplicates OK, now we have " + str(df_total_cleaned.count()) + " rows")


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


    logger.info(f"CARMARKET: loading into Delta Table OK, with {datos_nuevos} total rows, last extracted date {ultima_fecha}")


    logger.info(f"CARMARKET: {datos_nuevos - datos_originales} new vehicles OK, {datos_nuevos} total rows, last extracted date {ultima_fecha}")


    if ultima_fecha and ultima_fecha < (datetime.datetime.now() - datetime.timedelta(days=7)).date():
        logger.error(f"CARMARKET: ExtractedDate not updated, some is wrong, last extracted date {ultima_fecha}")
        raise Exception("At least one week ExtractedDate not updated")
    if marker_table is not None:
        marker_table.touch()

if __name__ == "__main__":
    logger = LoggerHelper("CarMarketTask").create_logger()
    #catalog_name = dbutils.widgets.get("catalog_name")
    catalog_name = "catalog_dag"
    name = f"{catalog_name}.bronze.tb_123_carmarkettask"
    run_main(logger=logger, table_name=name)
