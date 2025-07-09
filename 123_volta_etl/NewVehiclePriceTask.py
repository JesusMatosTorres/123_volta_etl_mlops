# Databricks notebook source
# MAGIC %run ./Utils/LoggerHelper

# COMMAND ----------

# MAGIC %run ../123_volta_mlops/PipInstalls

# COMMAND ----------

import datetime
import io
import json
import os
import re
import sys
import time

import pandas as pd
import unidecode
from googleapiclient import discovery
from apiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from httplib2 import Http
from google.oauth2 import service_account
from sqlalchemy.types import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

spark = spark.builder \
    .appName("ETL_Scraper_Newvehicleprice") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .getOrCreate()

# COMMAND ----------

class NewVehiclePrice:

    def __init__(self,
                 logger=None,
                 path_to_driver=None,
                 marker_table=None,
                 google_drive_source = False):
        self.marker_table = marker_table
        self.logger = logger


        if google_drive_source:
            # define path variables

            credentials_file_path = './credentials_drive/credentials.json'
            clientsecret_file_path = './credentials_drive/client_secret.json'

            # define API scope
            SCOPE = 'https://www.googleapis.com/auth/drive'

            # define store
            store = file.Storage(credentials_file_path)
            credentials = store.get()
            # get access token
            if not credentials or credentials.invalid:
                flow = client.flow_from_clientsecrets(clientsecret_file_path, SCOPE)
                credentials = tools.run_flow(flow, store)

            # define API service
            http = credentials.authorize(Http())
            self.drive = discovery.build('drive', 'v3', http=http)

        self.df_segmentation  = pd.read_excel("./xls/modelos_segmentos.xls")

        self.df_segmentation = self.df_segmentation.groupby(by=["Marca", "Modelo_Cube", "Segmento"], as_index=False)[
            "Marca", "Modelo_Cube", "Segmento"].tail(1)

        self.df_segmentation.Marca = self.df_segmentation.Marca.astype(str).str.strip()
        self.df_segmentation.Modelo_Cube = self.df_segmentation.Modelo_Cube.astype(str).str.strip()

        self.df_segmentation.Marca = self.df_segmentation.Marca.str.upper()
        self.df_segmentation.Modelo_Cube = self.df_segmentation.Modelo_Cube.str.upper()

        self.df_segmentation.Modelo_Cube = self.df_segmentation.Modelo_Cube.map(lambda x: unidecode.unidecode(x))
        self.df_segmentation.Marca = self.df_segmentation.Marca.map(lambda x: unidecode.unidecode(x))

        self.df_segmentation.loc[
            (self.df_segmentation[self.df_segmentation.Marca == "ALFAROMEO"]).index, "Marca"] = "ALFA ROMEO"

        self.df_segmentation.Marca = self.df_segmentation.Marca.apply(lambda x: x.replace("-", "").replace(" ", ""))
        self.df_segmentation.Modelo_Cube = self.df_segmentation.Modelo_Cube.apply(
            lambda x: x.replace("-", "").replace(" ", ""))

        self.df_segmentation.Marca = self.df_segmentation.Marca.apply(lambda x: re.sub(r"[^A-Za-z0-9]", "", x))
        self.df_segmentation.Modelo_Cube = self.df_segmentation.Modelo_Cube.apply(
            lambda x: re.sub(r"[^A-Za-z0-9]", "", x))

        self.df_segmentation.Modelo_Cube = self.df_segmentation.Modelo_Cube.str.replace("CLASE", "")

    def retrieve_all_files(self,
                           filename_to_search):

        api_service = self.drive
        results = []
        page_token = None

        while True:
            try:
                param = {}

                if page_token:
                    param['pageToken'] = page_token

                files = api_service.files().list(
                    q="'1XZYFP4a8eJ45f41PyzpVaD1wlZmG6Pzh' in parents and trashed=false").execute()
                # append the files from the current result page to our list
                results.extend(files.get('files'))
                # Google Drive API shows our files in multiple pages when the number of files exceed 100
                page_token = files.get('nextPageToken')

                if not page_token:
                    break

            except HttpError as error:
                print(f'An error has occurred: {error}')
                break
        # output the file metadata to console
        for file in results:
            if filename_to_search in file.get('name'):
                break

        return results, file

    def clean_dataframe(self,
                        df):

        df_historical_segments = pd.read_csv("./xls/segmentos_vo.csv")

        self.df_segmentation = self.df_segmentation.append(df_historical_segments,
                                                           ignore_index=True)

        self.df_segmentation.drop_duplicates(subset=["Marca", "Modelo_Cube"],
                                             keep="last",
                                             inplace=True)

        self.df_definitive = df.copy()

        self.df_definitive.Modelo = self.df_definitive.Modelo.astype(str).str.strip()
        self.df_definitive.Marca = self.df_definitive.Marca.astype(str).str.strip()

        self.df_definitive.Modelo = self.df_definitive.Modelo.map(lambda x: unidecode.unidecode(x))
        self.df_definitive.Marca = self.df_definitive.Marca.map(lambda x: unidecode.unidecode(x))

        self.df_definitive.Marca = self.df_definitive.Marca.str.replace("-", "")
        self.df_definitive.Modelo = self.df_definitive.Modelo.str.replace("-", "")

        self.df_definitive.Precios = pd.to_numeric(self.df_definitive.Precios)

        self.df_definitive.Cilindrada_CV = pd.to_numeric(self.df_definitive.Cilindrada_CV)

        self.df_definitive.EndDate = pd.to_numeric(self.df_definitive.EndDate, errors='coerce')
        self.df_definitive.StarDate = pd.to_numeric(self.df_definitive.StarDate, errors='coerce')

        self.df_definitive.EndDate = self.df_definitive.EndDate.fillna(datetime.datetime.now().year)
        self.df_definitive.StarDate = self.df_definitive.StarDate.fillna(datetime.datetime.now().year)

        self.df_definitive = self.df_definitive[self.df_definitive.EndDate >= 2005]




        self.df_definitive.Modelo = self.df_definitive.Modelo.str.upper()
        self.df_definitive.Marca = self.df_definitive.Marca.str.upper()

        self.df_definitive.reset_index(drop=True,
                                       inplace=True)

        self.df_definitive.Marca = self.df_definitive.Marca.apply(lambda x: re.sub(r"[^A-Za-z0-9]", "", x))
        self.df_definitive.Modelo = self.df_definitive.Modelo.apply(lambda x: re.sub(r"[^A-Za-z0-9]", "", x))

        self.df_definitive = self.df_definitive.merge(right=self.df_segmentation,
                                                      how="left",
                                                      left_on=["Marca", "Modelo"],
                                                      right_on=["Marca", "Modelo_Cube"],
                                                      suffixes=("", "_y"))

        del self.df_definitive["Modelo_Cube"]

        self.df_definitive = self.df_definitive[[column for column in self.df_definitive.columns if "_y" not in column]]
        self.df_definitive = self.df_definitive[["Marca", "Modelo", "Cilindrada_CV", "Combustible", "Precios", "Caracteristicas",
                                                 "Segmento", "StarDate", "EndDate"]]
        self.df_definitive.columns = ["Marca", "Modelo", "Cilindrada_CV", "Combustible", "Precios", "Caracteristicas",
                                      "Segmento", "StarDate", "EndDate"]

        self.logger.info("Dataframe definitive cleaned properly, OK")

    def get_clean_model(self,
                        row):
        try:
            row["Caracteristicas"] = " ".join(row["Modelo"].split(" ")[1:])
            row["Modelo"] = row["Modelo"].split(" ")[0]

            if (row["Modelo"] == "XC") | (row["Modelo"] == "MAZDA"):
                row["Modelo"] = row["Modelo"] + row["Caracteristicas"].split(" ")[0]
                row["Caracteristicas"] = " ".join(row["Caracteristicas"].split(" ")[1:])

            if (row["Marca"] == "BMW") & (len(row["Caracteristicas"].split(" ")[0]) == 1):
                row["Modelo"] = row["Modelo"] + row["Caracteristicas"].split(" ")[0]
                row["Caracteristicas"] = " ".join(row["Caracteristicas"].split(" ")[1:])

            if (row["Marca"] == "VOLVO") & (len(row["Modelo"]) == 1) & (row["Caracteristicas"][1].isdigit() == True):
                row["Modelo"] = row["Modelo"] + row["Caracteristicas"].split(" ")[0]
                row["Caracteristicas"] = " ".join(row["Caracteristicas"].split(" ")[1:])

            if (row["Modelo"] == "ALFA"):
                row["Modelo"] = row["Caracteristicas"].split(" ")[0]
                row["Caracteristicas"] = " ".join(row["Caracteristicas"].split(" ")[1:])

            if (row["Modelo"].strip() == "Range"):
                row["Modelo"] = row["Modelo"] + row["Caracteristicas"].split(" ")[0]
                row["Caracteristicas"] = " ".join(row["Caracteristicas"].split(" ")[1:])

                if (row["Caracteristicas"].split(" ")[0] == "Velar") | (
                        row["Caracteristicas"].split(" ")[0] == "Evoque"):
                    row["Modelo"] = row["Modelo"] + row["Caracteristicas"].split(" ")[0]
                    row["Caracteristicas"] = " ".join(row["Caracteristicas"].split(" ")[1:])
        except:
            row["Caracteristicas"] = "-"
            row["Modelo"] = row["Modelo"]
        try:
            row["StarDate"] = row["Matriculacion"].split("-")[0]
        except:
            row["StarDate"] = datetime.datetime.now().year
        try:
            if len(row.Matriculacion.split("-")[1]) < 3:
                row["EndDate"] = datetime.datetime.now().year
            else:
                row["EndDate"] = row["Matriculacion"].split("-")[1]
        except:
            row["EndDate"] = datetime.datetime.now().year
        return row

    def save_results(self, table_name):
        schema = StructType([
            StructField("Brand", StringType(), True),
            StructField("Model", StringType(), True),
            StructField("CV", IntegerType(), True),  # SmallInteger → IntegerType
            StructField("Fuel", StringType(), True),
            StructField("Price", DoubleType(), True),  # Float → DoubleType
            StructField("Features", StringType(), True),
            StructField("Segment", StringType(), True),
            StructField("StarDate", IntegerType(), True),  # SmallInteger → IntegerType
            StructField("EndDate", IntegerType(), True)  # SmallInteger → IntegerType
        ])

        if self.df_definitive.shape[0] == 0:
            raise Exception("Data frame is empty")
        else:
            
            try:
                column_translation = {
                    "Marca": "Brand",
                    "Modelo": "Model",
                    "Cilindrada_CV": "CV",
                    "Combustible": "Fuel",
                    "Precios": "Price",
                    "Caracteristicas": "Features",
                    "Segmento": "Segment",
                    "StarDate": "StarDate",
                    "EndDate": "EndDate",
                }

                self.df_definitive.rename(columns=column_translation, inplace=True)
            except:
                pass

            self.df_definitive["CV"] = self.df_definitive["CV"].fillna(0).astype(int)
            self.df_definitive["StarDate"] =self.df_definitive["StarDate"].fillna(0).astype(int)
            self.df_definitive["EndDate"] = self.df_definitive["EndDate"].fillna(0).astype(int)
            try:
                self.df_definitive["Price"] = self.df_definitive["Price"].astype(float)
            except:
                self.df_definitive["Precios"] = self.df_definitive["Precios"].astype(float)

            df_total_cleaned = spark.createDataFrame(self.df_definitive, schema=schema)

            df_total_cleaned.write.format("delta") \
                .option("overwriteSchema", "true") \
                .mode("overwrite") \
                .saveAsTable(table_name)


            if self.marker_table is not None:
                self.marker_table.touch()


def run_main(logger, marker_table=None):

    #catalog_name = dbutils.widgets.get("catalog_name")
    catalog_name = "catalog_dag"
    table_name = f"{catalog_name}.bronze.tb_123_volta_etl_mlops_lista_precios"

    google_drive_source = False
    nvp = NewVehiclePrice(logger=logger, marker_table=marker_table, google_drive_source=google_drive_source)

    nvp.logger.info("\nPROCESS LAUNCHED")

    if google_drive_source:
        filename_to_search = 'HACIENDA'
        all_files, search_file = nvp.retrieve_all_files(filename_to_search)

        request = nvp.drive.files().get_media(fileId=search_file["id"])
        fh = io.FileIO(search_file["name"], 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            time.sleep(10)

        df_lista = pd.read_excel(search_file["name"])
        df_lista.columns = df_lista.loc[9]

        df_lista = df_lista.loc[10:].iloc[:, :10]

        df_lista.columns = [str(columna).strip() for columna in df_lista.columns]
        df_lista = df_lista.rename({"nan": "Marca"}, axis=1)

        df_lista = df_lista[["Marca", "Modelo-Tipo", "Período comercial", "G/D", "Cilindrada_CV", "2020 Valor euros"]]

        df_lista.columns = ["Marca", "Modelo", "Matriculacion", "Combustible", "Cilindrada_CV", "Precios"]
    else:
        df_lista = pd.read_excel("./xls/MARCAS_MODELOS_HACIENDA_2024.xlsx", skiprows=0)

        df_lista["PERIODO COMERCIAL"] = df_lista['Periodo comercial – Inicio'].astype(str) + '-'+ df_lista['Periodo comercial – Fin'].astype(str)
        df_lista['MODELO'] = df_lista['MODELO'].apply(lambda x: str(x).replace(u'\xa0', u''))
        df_lista['MARCA'] = df_lista['MARCA'].apply(lambda x: str(x).replace(u'\xa0', u''))
        df_lista["PERIODO COMERCIAL"] = df_lista["PERIODO COMERCIAL"].apply(lambda x: str(x).replace(u'\xa0', u''))

        df_lista = df_lista[["MARCA", "MODELO", "PERIODO COMERCIAL", "G/D", "CV", "2023 VALOR EUROS"]]
        df_lista.columns = ["Marca", "Modelo", "Matriculacion", "Combustible", "Cilindrada_CV", "Precios"]
        df_lista["Modelo"] = df_lista["Modelo"].str.normalize('NFKD')

    mapper = {"G": "GASOLINA",
              "D": "DIESEL",
              "ELC": "ELECTRICO/HIBRIDO",
              "GLP": "GLP",
              "CNG": "GLP",
              "GyD": "ELECTRICO/HIBRIDO",
              "S": "GLP",
              "GyE": "ELECTRICO/HIBRIDO"}

    df_lista["Combustible"] = df_lista["Combustible"].map(mapper)
    df_lista = df_lista[pd.isnull(df_lista["Combustible"])==False]

    df_price_list = df_lista.apply(lambda x: nvp.get_clean_model(x), axis=1)
    del df_price_list["Matriculacion"]
    nvp.clean_dataframe(df_price_list)

    try:
        nvp.save_results(table_name=f"{catalog_name}.bronze.tb_123_consultoria_vehicleproduct")
    except:
        pass

    nvp.df_definitive = nvp.df_definitive[pd.isnull(nvp.df_definitive.Segment) == False]

    try:
        nvp.save_results(table_name=table_name)

    except Exception as err:
        nvp.logger.error("Table " + table_name + " could not be stored. Error description: " + str(err) + "\n")


if __name__ == "__main__":
    logger = LoggerHelper("NewVehiclePriceTask").create_logger()
    run_main(logger=logger)
