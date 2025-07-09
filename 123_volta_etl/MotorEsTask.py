# Databricks notebook source
import time
time.sleep(5)

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
    .appName("ETL_Scraper_Motores") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .getOrCreate()

# COMMAND ----------

class MotorESTask():

    def __init__(self):

        user = os.environ["USER"]
        self.geckodriver = f"/local_disk0/tmp/{user}/geckodriver/geckodriver"
        FIREFOX_BINARY = "/tmp/firefox/firefox"

        options = webdriver.FirefoxOptions()
        options.add_argument('-headless')
        options.set_preference("dom.disable_open_during_load", True)
        options.set_preference('dom.popup_maximum', -1)
        options.binary_location = FIREFOX_BINARY


        self.log_file_path = f"/tmp/geckodriver_{random.randint(1000, 9999)}.log"
        if os.path.exists(self.log_file_path):
            os.remove(self.log_file_path)

        self.browser = webdriver.Firefox(executable_path=self.geckodriver, service_log_path=self.log_file_path, options=options)


def run_main(logger, table_name, marker_table=None):
    MAX_RETRIES = 5
    retry_count = 0
    while retry_count < MAX_RETRIES:
        try:
            task = MotorESTask()
            task.browser.execute_script("""
                var ads = document.querySelectorAll("iframe[id^='google_ads_iframe']");
                ads.forEach(el => el.remove());
            """)
            break
        except:
            retry_count += 1
            time.sleep(3)
    
    logger.info("\n\n###############  MOTORES  ###############\n")
    logger.info("MOTORES: running exctraction algorithm")

    try:
        df_original = spark.read.table(table_name)
        datos_originales = df_original.count()
        logger.info(f"MOTORES: reading original dataset with {datos_originales} rows")
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

    url="https://www.motor.es/coches-segunda-mano/coches/?localizacion=1-4-5211&radio=300"

    task.browser.get(url)

    time.sleep(5)

    try:
        task.browser.find_element_by_xpath('//a[@id = "acceptAllMain"]').click()
    except:
        pass

    try:
        task.browser.find_element_by_xpath('//button[@id="btn-gdpr-accept"]').click()
    except:
        pass

    try:
        iframe = task.browser.find_element_by_xpath('//iframe[@title="Iframe title"]')
        task.browser.switch_to.frame(iframe)
        try:
            task.browser.find_element_by_xpath('//div[@id = "notice"]//button[2]').click()
        except:
            pass
        task.browser.switch_to.default_content()
    except:
        pass


    time.sleep(5)

    total = list()

    task.browser.maximize_window()

    time.sleep(5)

    df_first = pd.DataFrame()

    try:
        task.browser.find_element_by_xpath('//span[@class="accion lower"]').click()
    except:
        pass

    for _ in range(2):

        try:
            task.browser.find_element_by_xpath('//div[@id="guarda_busqueda_modal"]//a[@title="Cerrar"]').click()
        except Exception as e:
            logger.info(f"MOTORES: Error guarda_busqueda_modal: {e}")


        #anuncios_list = task.browser.find_elements_by_xpath('//span[@class = "cuerpo-resultado"]//article[@class="elemento-segunda-mano"]')
        anuncios_list = task.browser.find_elements_by_xpath(
            '//span[@class = "cuerpo-resultado"]//article[contains(@class, "elemento-segunda-mano")]')
        enlaces_anuncios = []
        for anuncio in anuncios_list:
            try:
                link = anuncio.find_element_by_xpath('.//a[1]')
                enlaces_anuncios.append(link.get_attribute('href'))
            except Exception as e:
                logger.info(f"MOTORES: Error obteniendo enlaces_anuncios: {e}")
        logger.info(f"MOTORES: Total enlaces_anuncios recolectados: {len(enlaces_anuncios)}")
        
        for i in range(len(enlaces_anuncios)):

            try:
                try:
                    task.browser.get(enlaces_anuncios[i])
                    time.sleep(2)
                except Exception as e:
                    logger.info(f"MOTORES: Error procesando enlace: {e}")
                    continue

                precio = task.browser.find_element_by_xpath('//dd[@class="ddprecio"]/strong').text
                precio = precio.replace(".", "").replace('€', "").replace('\n',"").strip()
                precios = precio.split(" ")
                if (len(precios) > 1) and (len(precios[1]) > 1):
                    financiacion = "FINANCIADO"
                else:
                    financiacion = "SIN FINANCIACIÓN"
                precio = precios[0]

                title = task.browser.find_element_by_xpath('//h1[@class="titulo-pagina"]').text
                anyo = task.browser.find_element_by_xpath('//section[contains(@class, "ficha ancho-principal")]//dt[contains(text(), "Matriculación")]/following-sibling::dd[1]').text.strip()
                anyo = anyo.split("/")[-1].strip()
                combustible = task.browser.find_element_by_xpath('//section[contains(@class, "ficha ancho-principal")]//dt[contains(text(), "Combustible")]/following-sibling::dd[1]').text
                kms = task.browser.find_element_by_xpath('//section[contains(@class, "ficha ancho-principal")]//dt[contains(text(), "Kilómetros")]/following-sibling::dd[1]').text
                kms = kms.split("Km")[0].strip().replace(".", "")
                potencia = task.browser.find_element_by_xpath('//section[contains(@class, "ficha ancho-principal")]//dt[contains(text(), "Potencia")]/following-sibling::dd[1]').text
                potencia = potencia.split("CV")[0].strip().replace(".", "")
                try:
                    location_str = task.browser.find_element_by_xpath('.//span[@class="lugar"]').text
                except:
                    location_str = "DESCONOCIDA"

                if 'PALMAS' in location_str.upper():
                    location = 'LAS PALMAS'
                else:
                    location = 'S/C TENERIFE'

                marca = title.split(" ")[0].strip()
                modelo = title.split(" ")[1].split(" ")[0]

                marca = marca.upper()
                modelo = modelo.upper()

                if ("CLASE" in modelo) or ("SERIE" in modelo):
                    modelo_list = title.split(" ")[1:3]
                    modelo = ' '.join(modelo_list)
                    modelo = modelo.upper()

                version = task.browser.find_element_by_xpath('//dt[text()="Versión"]/following-sibling::dd').text
                caracteristicas = f"{marca} {modelo} {version}".strip()
                try:
                    try:
                        ficha = task.browser.find_element_by_xpath('//section[@class="zona-contenido ficha ancho-principal"]').text
                    except:
                        ficha = ""

                    if 'Manual' in ficha:
                        caja = 'Manual'
                    else:
                        caja = 'Automático'

                    try:
                        vendedor = task.browser.find_element_by_xpath('//section[contains(@class, "datos-vendedor")]').text
                    except:
                        vendedor = ""

                    if "PARTICULAR" in vendedor.upper():
                        venta = "Particular"
                    else:
                        venta = "Profesional"

                except Exception as e:
                    logger.info(f"MOTORES: Error procesando ficha: {e}")

                try:
                    pd_anuncio = pd.DataFrame({'Marca': marca,
                                            'Modelo': modelo,
                                            'Caracteristicas': caracteristicas,
                                            'Precio': precio,
                                            'Venta': venta,
                                            'AnyoMatriculacion': anyo,
                                            'Kms': kms,
                                            'Combustible': combustible,
                                            'Cilindrada': potencia,
                                            'Caja': caja.strip(),
                                            'Links': enlaces_anuncios[i],
                                            'Provincia': location,
                                            'Financiado': financiacion
                                            }, index=[0])
                    df_first = pd.concat([df_first, pd_anuncio], axis= 0).reset_index(drop=True)

                except Exception as e:
                    logger.info(f"MOTORES: Error procesando anuncio: {e}")
            
            except Exception as e:
                logger.info(f"MOTORES: Error con anuncio {enlaces_anuncios[i]}: {e}")
                time.sleep(5)
                continue
            time.sleep(5)

            try:
                task.browser.find_element_by_xpath('//a[@id = "acceptAllMain"]').click()
            except Exception as e:
                pass

            time.sleep(2)

            try:
                task.browser.find_element_by_xpath('//a[@id="listado_btn"]').click()
            except Exception as e:
                logger.info(f"MOTORES: Error con listado_btn: {e}")

            time.sleep(2)

        try:
            task.browser.find_element_by_xpath('//a[@title="Siguiente"]').click()
        except Exception as e:
            logger.info(f"MOTORES: Error clickando en boton Siguiente: {e}")

        time.sleep(5)


    df_first = df_first.drop_duplicates()

    task.browser.quit()

    try:
        df_first["Web"] = "MotorES"

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

        df_first["Publicacion"] = str(hoy_day) + " " + str(hoy.hour) + ":" + str(hoy.minute)
        df_first["ExtractedDate"] = datetime.datetime.now().date()

        df_first = df_first[df_first["Combustible"].str.contains("puertas") == False]

        caja = {"manual": "Manual",
                "automatico": "Automático",
                "automat": "Automático",
                "Manual": "Manual",
                "Automático": "Automático"
                }
        combustibles = {"gasolina": "Gasolina",
                        "diesel": "Diésel",
                        "Gasolina": "Gasolina",
                        "Diesel": "Diesel",
                        'híbrido': 'Híbrido',
                        'hibrido': 'Híbrido',
                        'Híbrido': 'Híbrido',
                        'electrico': 'Eléctrico',
                        'eléctrico': 'Eléctrico',
                        'Eléctrico': 'Eléctrico'}

        df_first.Caja = df_first.Caja.map(caja)
        df_first.Combustible = df_first.Combustible.map(combustibles)

        df_first.loc[(df_first.Modelo == "LAND") & (df_first.Marca == "TOYOTA"), 'Modelo'] = 'LAND CRUISER'
        df_first.loc[(df_first.Marca == "LAND"), 'Marca'] = 'LAND ROVER'
        ix = df_first.Marca == "MERCEDES"
        df_first.loc[ix, 'Marca'] = 'MERCEDES BENZ'

        if df_first.shape[0] == 0:
            raise Exception("DataFrame empty")

        logger.info("MOTORES: exctracted data from web, with " + str(df_first.shape[0]) + " rows")

        #df_first = pd.concat([df_first, df_original], axis=0).reset_index(drop=True)

    except:
        #df_first = df_original.copy()

        #if df_first.shape[0] <= 2:
            #raise Exception("DataFrame empty")
        pass

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
                         "Venta",
                         "Web",
                         "Links"]]

    df_first = spark.createDataFrame(df_first)
    try:
        df_first = df_original.unionByName(df_first, allowMissingColumns=True)
        logger.info("MOTORES: appended to original dataset OK, now total rows of " + str(df_first.count()))
    except:
        pass


    #datatypes, df_total_cleaned = task.get_datatypes_dataframe(df_total_cleaned)
    #logger.info("MOTORES: getting datatypes of dataset")
    df_first = df_first.dropDuplicates(
        ["Marca", "Modelo", "Precio", "Financiado", "Provincia", "AnyoMatriculacion", "Kms"])
    logger.info("MOTORES: removing duplicates OK, now we have " + str(df_first.count()) + " rows")


    df_first = df_first.withColumn("PublicacionAnyo", lit(datetime.datetime.now().year))

    logger.info(
        "MOTORES: appended data to original, with " + str(df_first.count()) + " rows, afterward remove duplicates")

    datatypes = {
        "Marca": NVARCHAR(20),
        "Modelo": NVARCHAR(20),
        "Caracteristicas": NVARCHAR(100),
        "Precio": NVARCHAR(20),
        "Financiado": NVARCHAR(20),
        "Publicacion": NVARCHAR(20),
        "Provincia": NVARCHAR(20),
        "Combustible": NVARCHAR(10),
        "AnyoMatriculacion": NVARCHAR(15),
        "Kms": NVARCHAR(15),
        "ExtractedDate": DATETIME(),
        "Cilindrada": NVARCHAR(10),
        "Caja": NVARCHAR(15),
        "Web": NVARCHAR(15),
        "PublicacionAnyo": BigInteger(),
        "Links": NVARCHAR(350),
        "Venta": NVARCHAR(100)
    }
    
    df_total_cleaned = df_first #igualar al dataframe limpiado

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


    logger.info(f"MOTORES: loading into Delta Table OK, with {datos_nuevos} total rows, last extracted date {ultima_fecha}")


    logger.info(f"MOTORES: {datos_nuevos - datos_originales} new vehicles OK, {datos_nuevos} total rows, last extracted date {ultima_fecha}")


    if ultima_fecha and ultima_fecha < (datetime.datetime.now() - datetime.timedelta(days=7)).date():
        logger.error(f"MOTORES: ExtractedDate not updated, some is wrong, last extracted date {ultima_fecha}")

    if marker_table is not None:
        marker_table.touch()


if __name__ == "__main__":
    logger = LoggerHelper("MotorEsTask").create_logger()
    #catalog_name = dbutils.widgets.get("catalog_name")
    catalog_name = "catalog_dag"
    name = f"{catalog_name}.bronze.tb_123_motorestask"
    run_main(logger=logger, table_name=name)
