# Databricks notebook source
# MAGIC %run ./Utils/LoggerHelper

# COMMAND ----------

spark = spark.builder \
    .appName("ETL_Scraper_VoltaBZSZ") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .getOrCreate()

# COMMAND ----------

dbutils.widgets.text("target_catalog1", "catalog_dag.bronze", "Target Catalog Bronze")
catalog_bronze = dbutils.widgets.get("target_catalog1")
dbutils.widgets.text("target_catalog2", "catalog_dag.silver", "Target Catalog Silver")
catalog_silver = dbutils.widgets.get("target_catalog2")
dbutils.widgets.text("target_catalog3", "catalog_azure_sz_dto_catalog.dbo", "Target Catalog Azure")
catalog_azure = dbutils.widgets.get("target_catalog3")

# COMMAND ----------

import pandas as pd
import numpy as np
from sqlalchemy.types import *
import warnings
import os
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")
from sqlalchemy.types import *
import unidecode
import sys
import hashlib
from collections import OrderedDict
import json
from sqlalchemy import create_engine
from urllib import parse
from pyspark.sql.functions import col

# COMMAND ----------

sys.setrecursionlimit(10000)
class VO_BZ_SZ_Task():

    def get_id_first(self, row):

        row["Id"] = str(row["Brand"]) + str(row["Model"]) + str(row["Registration"]) + str(row["Finish"]) + str(
            row["Features"]) + str(row["Price"]) + str(row["Fuel"]) + str(row["Gearbox"]) + str(
            row["Province"]) + str(row["Kms"]) + str(row["Web"])
        row["Id"] = hashlib.sha224(row["Id"].encode("utf-8")).hexdigest()

        return row

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
            "float32": Float(),
            "float64": Float(),
            "bool": Boolean(),
            "date": DateTime,
            "int32": Integer(),
            "int64": Integer(),
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

        df = df.where(pd.notnull(df), None)
        df = df.replace({"nan": None})
        df = df.replace({"None": None})

        return datatypes, df

    def get_id(self, row):
        row["Id_vehicle"] = str(row["Brand"]) + str(row["Model"]) + str(row["Registration"]) + str(
            row["Fuel"]) + str(row["Gearbox"]) + str(row["Finish"]) + str(row["Province"]) + str(row["Kms"])
        row["Id_vehicle"] = hashlib.sha224(row["Id_vehicle"].encode("utf-8")).hexdigest()
        return row

    def get_id_unique(self, grouped_df):

        grouped_df["VehicleId"] = np.random.randint(low=0, high=9999999)
        return grouped_df

    def check_data(self, row):
        try:
            if row["Brand"][0].isdigit() == True:
                row["Brand"] = None
            else:
                row["Brand"] = row["Brand"]
        except:
            row["Brand"] = None

        try:
            row["Fuel"] = row["Fuel"].title()
        except:
            pass
        try:
            row["Fuel"] = unidecode.unidecode(row["Fuel"]).upper()
            if ("HIBRIDO" in row["Fuel"]) | ("ELECTRICO" in row["Fuel"]):
                row["Fuel"] = "ELECTRICO/HIBRIDO"
        except:
            row["Fuel"] = None
        if row["Fuel"] == "Gas":
            row["Fuel"] = "GLP"

        if row["Fuel"] in ['GASOLINA', 'DIESEL', 'ELECTRICO/HIBRIDO', 'GLP']:
            row["Fuel"] = row["Fuel"]
        else:
            row["Fuel"] = None
        try:
            row["Gearbox"] = unidecode.unidecode(row["Gearbox"])
        except:
            pass
        try:
            row["Gearbox"] = row["Gearbox"].strip()
        except:
            pass

        if row["Gearbox"] in ["Manual", "Automatico", "Semiautomatico", "Doble embrague", "Semi-automatico"]:
            row["Gearbox"] = row["Gearbox"]
        else:
            row["Gearbox"] = None

        if row["Gearbox"] in ["Automatico", "Semiautomatico", "Doble embrague", "Semi-automatico"]:
            row["Gearbox"] = "Automático"
        try:
            if (len(row["CV"]) < 4) & (len(row["CV"]) > 1):
                row["CV"] = row["CV"]
            else:
                row["CV"] = None
        except:
            row["CV"] = row["CV"]
        try:
            if ("w" in row["CV"]) | ("W" in row["CV"]) | (
                    row["CV"][0].isdigit() == False):
                row["CV"] = None
        except:
            row["CV"] = row["CV"]
        try:
            row["Price"] = row["Price"].strip()
        except:
            pass
        try:
            if "." in row["Price"]:
                position = row["Price"].find(".")
                zeros = row["Price"][position + 1:]

                if len(zeros) == 3:
                    row["Price"] = row["Price"]
                else:
                    row["Price"] = None
            else:
                row["Price"] = row["Price"]
        except:
            pass
        try:
            if "enchufable" in row["YearRegistration"]:
                row["YearRegistration"] = row["Kms"].split(" ")[0]
                row["Kms"] = row["Kms"].split(" ")[1]
        except:
            row["YearRegistration"] = row["YearRegistration"]

        try:
            if "/" in row["YearRegistration"]:
                row["YearRegistration"] = row["YearRegistration"].split("/")[1]
        except:
            row["YearRegistration"] = row["YearRegistration"]
        try:
            row["Publication"] = str(int(row["PublicationYear"])) + "/" + row["Publication"]
        except:
            try:
                row["Publication"] = str(int(datetime.now().year)) + "/" + row["Publication"]
            except:
                row["Publication"] = datetime.now().strftime("%Y/%d/%m %H:%M")
        try:
            row["CV"] = row["CV"].strip()
        except:
            pass
        try:
            row["CV"] = pd.to_numeric(row["CV"])
        except:
            row["CV"] = None
        try:
            if row["CV"] < 50:
                row["CV"] = row["CV"] + 100
        except:
            row["CV"] = None

        try:
            if (pd.isnull(row["Model"]) == True):
                row["Model"] = row["Features"].split(" ")[0]
            else:
                pass
        except:
            pass

        row["Model"] = unidecode.unidecode(row["Model"])
        try:
            row["Fuel"] = unidecode.unidecode(row["Fuel"])
        except:
            pass

        try:
            if row["Brand"].lower() == "alfa":
                row["Model"] = row["Model"].split(" ")[1]
            else:
                row["Model"] = row["Model"]
        except:
            row["Model"] = row["Model"]

        try:
            if ("clase" in row["Model"].lower()) | ("santa" in row["Model"].lower()) | (
                    "mini" in row["Model"].lower()) | ("grand" in row["Model"].lower()) | (
                    "serie" in row["Model"].lower()) | ("land" in row["Model"].lower()):

                row["Family_Model"] = row["Model"]
            else:
                row["Family_Model"] = row["Model"].split(" ")[0]
        except:
            row["Family_Model"] = row["Model"]

        try:
            if ((row["Family_Model"].lower()) == "land") & ("hiace" not in row["Features"].lower()):
                row["Model"] = "Land Cruiser"
                row["Family_Model"] = "Land Cruiser"
            else:
                if ("hiace" in row["Features"].lower()):
                    row["Model"] = "Hiace"
                    row["Family_Model"] = "Hiace"
                else:
                    row["Family_Model"] = row["Family_Model"]
        except:
            row["Family_Model"] = row["Family_Model"]

        try:
            if ((row["Family_Model"].lower()) == "space"):
                row["Model"] = "Space Star"
                row["Family_Model"] = "Space Star"
            else:
                pass
        except:
            pass

        try:
            if len(row["YearRegistration"]) != 4:
                row["YearRegistration"] = None
            else:
                pass
        except:
            pass
        try:
            if ((row["Family_Model"].lower()) == "range"):
                row["Family_Model"] = "Range Rover"
            else:
                pass
        except:
            pass

        try:
            if ((row["Family_Model"].lower()) == "serie"):
                row["Family_Model"] = str(row["Family_Model"]) + " " + row["Features"].split(" ")[0]
            else:
                pass
        except:
            if ((row["Family_Model"].lower()) == "serie"):
                row["Family_Model"] = str(row["Family_Model"]) + " " + str(row["Features"])
            else:
                pass

        try:
            if (row["Brand"] == "Mercedes") & ((len(row["Family_Model"]) == 1)):
                row["Family_Model"] = str("Clase") + " " + row["Family_Model"]
            else:
                pass
        except:
            pass

        try:
            if "nuevo" in (row["Family_Model"].lower()):
                row["Family_Model"] = str(row["Features"].split(" ")[0])
        except:
            pass

        try:
            if ((row["Brand"].lower() in row["Links"].lower())) | ((row["Model"].lower() in row["Links"].lower())) | (
                    (row["Family_Model"].lower() in row["Links"].lower())) | (
            (row["Web"].lower() in row["Links"].lower())):
                row["Links"] = row["Links"]
            else:
                row["Links"] = None
        except:
            row["Links"] = None

        try:
            detalles = row["Features"].split(" ")

            for i in range(len(detalles)):
                if len(detalles[i]) > 4:
                    row["Model"] = row["Model"] + " " + detalles[i]
        except:
            pass

        try:
            if (row["Family_Model"] == "Audi"):
                row["Family_Model"] = row["Features"].split(" ")[0]
            else:
                pass
        except:
            pass
        

        return row

    def get_kms_cicar(self,
                      row,
                      cicar):
        if row["Web"] == "CICAR":
            try:
                segment = row["Segment"]
                ratio = cicar[cicar.Segment == segment]["Ratio_Kms_Age"].values[0]
                row["Kms"] = row["Age"] * ratio
                return row
            except:
                pass
        else:
            return row


def run_main(logger, table_name, marker_table=None):
    task = VO_BZ_SZ_Task()
    logger.info("\n\n###############  CLEANING BZ -> SZ  ###############\n")
    logger.info("CLEANING: running cleaning algorithm")

    try:
        df_markets = spark.read.table(table_name)
        datos_originales = df_markets.count()
        logger.info(f"CLEANING: reading original dataset with {datos_originales} rows")
    except:
        datos_originales = 0
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                Brand STRING,
                Model STRING
            )
            USING DELTA
            TBLPROPERTIES (
                'delta.minReaderVersion' = '2',
                'delta.minWriterVersion' = '5'
            )
        """)
        df_markets = spark.read.table(table_name)

    df_segmentation = pd.read_excel("./xls/modelos_segmentos.xls")

    column_translation = {
                    "Id_Marca_Modelo": "Id_Brand_Model",
                    "Marca_Cube": "Brand_Cube",
                    "Modelo_Cube": "Model_Cube",
                    "Segmento_Cube": "Segment_Cube",
                    "Marca": "Brand",
                    "Modelo": "Model",
                    "Segmento": "Segment",
                    "Mercado": "Market",
                    "Premium": "Premium",
                    "SEG-1 (MSI)": "SEG-1_(MSI)",
                    "SEG-2 (MSI)": "SEG-2_(MSI)"
                }
    df_segmentation.rename(columns=column_translation, inplace=True)

    df_spark = spark.read.format("delta").table(f"{catalog_bronze}.tb_123_volta_etl_mlops_lista_precios")
    df_Prices = df_spark.toPandas()

    df_markets = df_markets.toPandas()
    column_translation = {
                    "VehicleId": "VehicleId",
                    "Id": "Id",
                    "Marca": "Brand",
                    "Modelo": "Model",
                    "Acabado": "Finish",
                    "Caracteristicas": "Features",
                    "Kms": "Kms",
                    "Precio": "Price",
                    "Combustible": "Fuel",
                    "Caja": "Gearbox",
                    "Cilindrada_CV": "CV", 
                    "Matriculacion": "Registration",
                    "Antiguedad": "Age",
                    "Segmento": "Segment",
                    "Provincia": "Province",
                    "Publicacion": "Publication",
                    "ExtractedDate": "ExtractedDate",
                    "Financiado": "Funded", 
                    "Web": "Web", 
                    "Venta": "Sale", 
                    "Links": "Links"
                }
    df_markets.rename(columns=column_translation, inplace=True)


    tables_df = spark.sql(f"SHOW TABLES IN {catalog_bronze}")

    pattern = r"^tb_123_.*task$"  # Expresión regular
    filtered_tables = tables_df.filter(col("tableName").rlike(pattern))

    table_list = [row.tableName for row in filtered_tables.collect()]

    for table in table_list:
        df_spark = spark.read.format("delta").table(f"{catalog_bronze}.{table}")
        df_spark = df_spark.filter(df_spark["ExtractedDate"] >= "2024-01-01")
        df_spark = df_spark.toPandas()
        df_total = pd.concat([df_spark],ignore_index=True)

    '''df_total = pd.concat(['df_vibbo,' df_scout, df_carmarket, 'df_flick,' df_cicar, 'df_automotor, df_mil, df_autocasion, df_hpromise, df_autosdibar, df_das, df_rosso,' df_km0, df_motores, df_mleon, df_carplus', df_gestican'],
                         ignore_index=True)'''

    logger.info("CLEANING: getting total market dataset extracted to clean with " + str(df_total.shape[0]) + " rows")

    #df_total = df_total.rename(columns={"CV": "cilindrada_cv"})

    column_translation = {
                    "Marca": "Brand",
                    "Modelo": "Model",
                    "Caracteristicas": "Features",
                    "Kms": "Kms",
                    "Precio": "Price",
                    "Combustible": "Fuel",
                    "Caja": "Gearbox",
                    "Cilindrada": "CV", 
                    "AnyoMatriculacion": "YearRegistration", 
                    "Provincia": "Province",
                    "Publicacion": "Publication",
                    "ExtractedDate": "ExtractedDate",
                    "Financiado": "Funded", 
                    "Venta": "Sale", 
                    "Web": "Web", 
                    "Links": "Links",
                    "PublicacionAnyo": "PublicationYear"
                }
    
    df_total.rename(columns=column_translation, inplace=True)
    
    df_total["Brand"] = df_total.Brand.str.capitalize()
    df_total["Brand"] = df_total["Brand"].str.replace("-", " ")
    df_total["CV"] = df_total["CV"].str.replace(" CV", "")
    df_total["Brand"] = df_total["Brand"].str.replace("Mercedes benz", "Mercedes")

    df_total = df_total.apply(lambda row: task.check_data(row), axis=1)

    df_total = df_total[(df_total.Brand != None)
                        | (pd.isnull(df_total.Brand) == False)]

    df_total = df_total[(df_total.Price != None)
                        | (pd.isnull(df_total.Price) == False)]

    df_total = df_total[pd.isnull(df_total["Price"]) != True]

    df_total = df_total[pd.isnull(df_total["YearRegistration"]) != True]

    df_total["Model"] = df_total.Model.str.capitalize()
    df_total["Features"] = df_total.Features.str.capitalize()

    df_total["Model"] = df_total.Model.str.capitalize()
    df_total["Features"] = df_total.Features.str.capitalize()

    df_total.Price = df_total.Price.str.strip()
    df_total.Brand = df_total.Brand.str.strip()
    df_total.Features = df_total.Features.str.strip()
    df_total.Model = df_total.Model.str.strip()
    df_total.Kms = df_total.Kms.str.strip()

    df_total.Fuel = df_total.Fuel.str.strip()
    df_total.Gearbox = df_total.Gearbox.str.strip()
    df_total.YearRegistration = df_total.YearRegistration.str.strip()
    df_total.Province = df_total.Province.str.strip()

    df_total.Funded = df_total.Funded.str.strip()

    df_total.ExtractedDate = pd.to_datetime(df_total.ExtractedDate)

    df_total.Publication = df_total.Publication.mask(df_total.Publication == None, datetime.now().date())
    df_total.Publication = pd.to_datetime(df_total.Publication, format='%Y/%d/%m %H:%M', errors='coerce')

    df_total.Kms = df_total.Kms.str.replace(".", "")
    df_total.Price = df_total.Price.str.replace(".", "")

    df_total["Brand"].loc[df_total.Brand == "Land"] = "Land rover"
    df_total["Brand"].loc[df_total.Brand == "Citroën"] = "Citroen"
    df_total["Brand"].loc[df_total.Brand == "Alfa"] = "Alfa romeo"

    df_total["Brand"].loc[df_total.Brand.str.lower().str.contains("hyundai") == True] = "Hyundai"
    df_total["Brand"].loc[df_total.Brand.str.lower().str.contains("opel") == True] = "Opel"

    df_total["Gearbox"].loc[df_total.Gearbox.str.lower() == "automatico"] = "Automático"

    del df_total["PublicationYear"]

    df_total["Kms"].loc[df_total.Kms == "N/D"] = 5
    df_total = df_total[df_total.Kms.str.isdigit() == True]

    df_total = df_total.where(pd.notnull(df_total), None)
    df_total = df_total.replace({"nan": None})
    df_total = df_total.replace({"None": None})

    df_total.Kms = pd.to_numeric(df_total.Kms)
    df_total.Price = pd.to_numeric(df_total.Price, errors='coerce')
    df_total.YearRegistration = pd.to_numeric(df_total.YearRegistration, errors='coerce')
    df_total["CV"] = pd.to_numeric(df_total["CV"], errors='coerce')

    df_total = df_total[pd.isnull(df_total["Province"]) == False]

    columnas = list(df_total.columns.values)
    columns = list()

    for columna in columnas:
        columna = columna.replace("(CV)", "_CV")
        columna = columna.replace("YearRegistration", "Registration")
        columns.append(unidecode.unidecode(columna))

    df_total.columns = columns

    df_total.Brand = df_total.Brand.str.upper()
    df_total.Model = df_total.Model.str.upper()
    df_total.Features = df_total.Features.str.upper()
    df_total.Fuel = df_total.Fuel.str.upper()
    df_total.Gearbox = df_total.Gearbox.str.upper()
    df_total.Province = df_total.Province.str.upper()
    df_total.Family_Model = df_total.Family_Model.str.upper()
    df_total.Web = df_total.Web.str.upper()

    df_total = df_total[df_total.Price <= 150000]
    df_total = df_total[df_total.Price >= 1000]
    df_total = df_total[df_total.Kms <= 1000000]
    df_total = df_total[df_total.Registration >= 1990]

    df_total = df_total[(pd.isnull(df_total.Fuel) == False) & (pd.isnull(df_total.Gearbox) == False)]

    df_total["Age"] = df_total["ExtractedDate"].dt.year - df_total["Registration"]

    df_total["Links"] = df_total["Links"].str.replace("#fotos", "")

    if df_total.shape[0] <= 2:
        raise Exception("DataFrame empty")

    df_total.rename({"Family_Model": "Model", "Model": "Finish"}, axis='columns', inplace=True)

    df_segmentation.drop_duplicates(subset=["Brand", "Model"], keep="first", inplace=True)

    df_segmentation.Brand = df_segmentation.Brand.astype(str)
    df_segmentation.Model_Cube = df_segmentation.Model_Cube.astype(str)

    df_segmentation.Brand = df_segmentation.Brand.str.upper().str.strip()
    df_segmentation.Model_Cube = df_segmentation.Model_Cube.str.upper().str.strip()

    df_total.loc[df_total.Features.str.contains("MITO", na=False), 'Model'] = "MITO"
    df_segmentation.Brand.loc[df_segmentation.Brand == "ALFAROMEO"] = "ALFA ROMEO"
    df_segmentation.Brand.loc[df_segmentation.Brand == "LANDROVER"] = "LAND ROVER"
    df_segmentation.Model_Cube.loc[df_segmentation.Model_Cube == "HI LUX"] = "HILUX"
    df_segmentation.Brand.loc[df_segmentation.Brand == "CITROËN"] = "CITROEN"
    df_segmentation.Model_Cube.loc[df_segmentation.Model_Cube == "C-ELYSEE"] = "CELYSEE"
    df_segmentation.Model_Cube.loc[df_segmentation.Model_Cube == "C-HR"] = "CHR"
    df_segmentation.Model_Cube.loc[df_segmentation.Model_Cube == "500L LIVING MPV"] = "500L"
    df_segmentation.Model_Cube.loc[df_segmentation.Model_Cube == "500X 5P"] = "500X"

    df_total.Model.loc[df_total.Model.str.lower().str.contains("rover")] = "RANGE ROVER"
    df_total.Model.loc[df_total.Model.str.lower().str.contains("santa")] = "SANTA FE"

    df_segmentation = df_segmentation[["Brand", "Model_Cube", "Segment"]]

    df_total = df_total.merge(how="left",
                              right=df_segmentation,
                              left_on=["Brand", "Model"],
                              right_on=["Brand", "Model_Cube"],
                              suffixes=("", "_x"))

    df_total = df_total[[column for column in df_total.columns if "_x" not in column]]

    df_total.Segment.loc[df_total.Model == "TUCSON"] = "SUVA"
    df_total.Segment.loc[df_total.Model == "SANTA FE"] = "SUVB"
    df_total.Segment.loc[df_total.Model.str.lower().str.contains("cooper")] = "A0"

    del df_total["Model_Cube"]

    df_total = df_total.apply(lambda row: task.get_id_first(row), axis=1)

    #df_cicar_segments["Ratio_Kms_Age"] = df_cicar_segments["Ratio_Kms_Age"] * 365

    #df_total = df_total.apply(lambda row: task.get_kms_cicar(row, df_cicar_segments), axis=1)

    df_total.drop_duplicates(subset="Id", keep="last", inplace=True)
    df_total = df_total[df_total.Model.str.find("P.") != True]
    df_total.Publication = df_total.Publication.mask(pd.isnull(df_total.Publication) == True,
                                                     datetime.now())
    df_total.reset_index(drop=True, inplace=True)

    df_total.drop_duplicates(subset=["Brand",
                                     "Model",
                                     "Finish",
                                     "Features",
                                     "Fuel",
                                     "Gearbox",
                                     "Province",
                                     "Registration",
                                     "Kms",
                                     "Price",
                                     "Web"],
                             keep="first", inplace=True)

    df_total = df_total.apply(lambda row: task.get_id(row), axis=1)

    df_total = df_total.groupby(by=["Id_vehicle"],
                                as_index=False).apply(
        lambda grouped_df: task.get_id_unique(grouped_df))  # We apply the function

    logger.info("CLEANING: New data afterward cleaning, with " + str(df_total.shape[0]) + " rows")

    df_total = pd.concat([df_markets, df_total], axis=0)
    df_total = df_total[(df_total.Province == "S/C TENERIFE") | (df_total.Province == "LAS PALMAS")]
    df_total = df_total[df_total.Brand != "NO SE SABE"]
    df_total["Gearbox"].loc[df_total.Gearbox.str.lower() == "automático"] = "AUTOMATICO"
    df_total = df_total[df_total.Province != "AÑO DE MATRICULACIÓN:"]

    df_total.reset_index(drop=True, inplace=True)
    df_total.loc[pd.isnull(df_total.Publication), 'Publication'] = df_total.ExtractedDate

    df_total.drop_duplicates(subset=["Brand",
                                     "Model",
                                     "Finish",
                                     "Features",
                                     "Fuel",
                                     "Gearbox",
                                     "Province",
                                     "Registration",
                                     "Kms",
                                     "Price",
                                     "Web"],
                             keep="last", inplace=True)

    df_total.reset_index(drop=True, inplace=True)

    logger.info(
        "CLEANING: New data afterward cleaning and appended to original, with " + str(df_total.shape[0]) + " rows")

    df_Prices.drop_duplicates(subset=["Brand", "Model"], keep="last", inplace=True)

    logger.info(
        "CLEANING: Cheking segments null before process, " + str(df_total.Segment.isnull().sum()))

    df_Prices = df_Prices[["Brand", "Model", "Segment"]]

    df_total.Model = df_total.Model.str.replace("KLASSE", "")
    df_total.Model = df_total.Model.str.replace("CLASE", "")

    df_Prices.Model = df_Prices.Model.str.replace("KLASSE", "")
    df_Prices.Model = df_Prices.Model.str.replace("CLASE", "")

    df_total.Model = df_total.Model.str.strip()
    df_total.Brand = df_total.Brand.str.strip()

    df_total = df_total.merge(how="left",
                              right=df_Prices,
                              left_on=["Brand", "Model"],
                              right_on=["Brand", "Model"],
                              suffixes=("", "_segment"))

    df_total.loc[pd.isnull(df_total.Segment), "Segment"] = df_total.Segment_segment
    df_total = df_total[[column for column in df_total.columns if "_segment" not in column]]

    logger.info(
        "CLEANING: Cheking segments with new vehicles, segments null " + str(df_total.Segment.isnull().sum()))

    logger.info(
        "CLEANING: Cheking % segments with new vehicles, segments null " + str(
            ((df_total.Segment.isnull().sum()) / df_total.shape[0]) * 100))

    df_total.Kms = pd.to_numeric(df_total.Kms, errors='coerce')
    df_total.Price = pd.to_numeric(df_total.Price, errors='coerce')
    df_total.Age = pd.to_numeric(df_total.Age, errors='coerce')
    df_total.Registration = pd.to_numeric(df_total.Registration, errors='ignore')
    df_total["CV"] = pd.to_numeric(df_total["CV"], errors='coerce')

    ix = df_total['Publication'] > df_total['ExtractedDate']
    df_total.loc[ix, 'Publication'] = df_total.loc[ix, 'ExtractedDate']

    ix = df_total['Sale'] == "PROFESIONAL"
    df_total.loc[ix, 'Sale'] = "Profesional"

    ix = df_total['Sale'] == "PARTICULAR"
    df_total.loc[ix, 'Sale'] = "Particular"


    def strip_Brand(x, position):
        out = x
        try:
            out = x.split(' ')[position].upper()
        except:
            pass
        return out

    df_total.Model_from_Features = df_total.Features.map(lambda x: strip_Brand(x, 0))
    ix1 = (df_total.Brand == df_total.Model) & (df_total.Brand == df_total.Model_from_Features)
    ix2 = (df_total.Brand == df_total.Model) & (df_total.Brand != df_total.Model_from_Features)

    df_total.loc[ix1, 'Model'] = df_total.loc[ix1, 'Features'].map(lambda x: strip_Brand(x, 1))
    df_total.loc[ix2, 'Model'] = df_total.loc[ix2, 'Features'].map(lambda x: strip_Brand(x, 0))

    df_total['Brand'] = df_total['Brand'].str.strip().str.replace(' ', '')
    df_total = df_total[~df_total.Brand.isnull()]
    df_total.loc[df_total['Brand']=='VW', 'Brand'] = 'VOLKSWAGEN'
    df_total.loc[df_total['Brand']=='ASTON', 'Brand'] = 'ASTONMARTIN'
    df_total.loc[df_total['Brand']=='DSAUTOMOBILES', 'Brand'] = 'DS'
    df_total.loc[df_total['Brand'].str.contains('RENAULT', regex=False), 'Brand'] = 'RENAULT'
    df_total =  df_total[df_total['Brand'].str.match('^[\w\s]*$')]
    df_total = df_total[df_total.Brand != 'NOSESABE']
    
    df_total = df_total[df_total.Model.str.strip() != '']
    df_total.loc[df_total['Model']=="CEE'D", 'Model'] = 'CEED'
    ix = df_total.Model.str.match('^SERIE\s[1-9]+$')
    df_total.loc[ix, 'Model'] = df_total.Model.str.strip().str.replace(' ', '') 
    ix = df_total.Model.str.match('^ID\.[1-9]+$')
    df_total.loc[ix, 'Model'] = df_total.Model.str.strip().str.replace('.', '') 
    df_total.loc[df_total['Model']=="UP!", 'Model'] = 'UP'  
    df_total[df_total['Model'].str.contains("NULL") == False] 
    df_total = df_total[df_total.Model != 'NUEVO']
    df_total = df_total[df_total.Model != '|']
    df_total['Model'] = df_total['Model'].str.replace('-', '')
    df_total['Model'] = df_total['Model'].str.replace(' ', '')


    df_total = df_total[["VehicleId",
                         "Id",
                         "Brand",
                         "Model",
                         "Finish",
                         "Features",
                         "Kms",
                         "Price",
                         "Fuel",
                         "Gearbox",
                         "CV",
                         "Registration",
                         "Age",
                         "Segment",
                         "Province",
                         "Publication",
                         "ExtractedDate",
                         "Funded",
                         "Web",
                         "Sale",
                         "Links"]]

    datatypes, df_total = task.get_datatypes_dataframe(df_total, changes={"Kms": int,
                                                                          "Registration": int,
                                                                          "Age": int})

    if df_total.empty:
        raise Exception('Dataframe df_total is empty')

    
    df_total_delta = spark.createDataFrame(df_total)

    df_total_delta.write.format("delta") \
        .option("overwriteSchema", "true") \
        .mode("overwrite") \
        .saveAsTable(table_name)    

    df_total.sort_values(by="ExtractedDate", inplace=True)

    datos_nuevos = df_total.shape[0]

    logger.info("CLEANING: loading into database OK, with " + str(
        df_total.shape[0]) + " total rows, last extracted date " + str(df_total["ExtractedDate"].iloc[-1]))
    logger.info("CLEANING: " + str(datos_nuevos - datos_originales) + " new vehicles OK, last extracted date " + str(
        df_total["ExtractedDate"].iloc[-1]))

    df_total.sort_values(by="ExtractedDate", inplace=True)
    if df_total["ExtractedDate"].iloc[-1] < (datetime.now() - timedelta(days=7)).date():
        logger.error("CLEANING: ExtractedDate not updated, some is wrong, last extracted date " + str(df_total["ExtractedDate"].iloc[-1]))
        raise Exception("At least one week ExtractedDate not updated")

    if marker_table is not None:
        marker_table.touch()


if __name__ == "__main__":
    logger = LoggerHelper("VO_BzSzTask").create_logger()
    name = f"{catalog_silver}.tb_123_volta_etl_mlops_markets_canary"
    run_main(logger=logger, table_name=name)
