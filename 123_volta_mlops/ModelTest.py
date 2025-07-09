# Databricks notebook source
# MAGIC %run ../123_volta_etl/Utils/LoggerHelper

# COMMAND ----------

# MAGIC %run ./PipInstalls

# COMMAND ----------

import os
import requests
import json
import pandas as pd

class ModelTest():
    def __init__(self, instance, service_name, token, logger):
        self.instance = instance
        self.service_name = service_name
        self.token = token
        self.logger = logger
        
    def run_test(self, data):
        df=data
        predictions = self.score_model(df)
        
        for idx, (index, row) in enumerate(data.iterrows()):
            fila_info = row.to_dict()
            prediction = predictions["predictions"][idx]
            
            self.logger.info(f"{json.dumps(fila_info, indent=2)} Prediction: {prediction}\n")
        return predictions   


    def prepare_data_for_scoring(self, data):
        
        if isinstance(data, pd.DataFrame):
            return json.dumps({"dataframe_split": data.to_dict(orient="split")}, allow_nan=True)
        elif isinstance(data, dict):
            return json.dumps({"inputs": {name: data[name].tolist() for name in data.keys()}}, allow_nan=True)
        else:
            raise ValueError("Los datos deben ser un DataFrame o un diccionario.")


    def score_model(self, dataset):
        
        url = f"https://{self.instance}/serving-endpoints/{self.service_name}/invocations"
        headers = {'Authorization': f"Bearer {self.token}", 'Content-Type': 'application/json'}

        data_json = self.prepare_data_for_scoring(dataset)

        response = requests.post(url, headers=headers, data=data_json)

        if response.status_code != 200:
            raise Exception(f"Error {response.status_code}: {response.text}")

        return response.json()


# COMMAND ----------

instance = "adb-602746394801203.3.azuredatabricks.net"
service_name = "123_volta_etl_mlops"
token = dbutils.widgets.get("token").strip()
#token = "dapif8efdda92b80f615350fa1e033153bcd-2"
logger = LoggerHelper("Model_test").create_logger()

model_tester = ModelTest(instance, service_name, token, logger)

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.model_selection import KFold

catalog_name = dbutils.widgets.get("catalog_name")
#catalog_name = "catalog_dag"
df1 = spark.read.table(f"{catalog_name}.golden.ds_123_volta_etl_mlops_001_trainingpatterns").toPandas()
df = df1.drop(['Price', 'Model'], axis=1)
y = df1["Price"]
df = df.reset_index(drop=True)

# === K-FOLD para scoring ===
kf = KFold(n_splits=30, shuffle=True, random_state=42)
df_resultados_kfold = []

for train_idx, val_idx in kf.split(df):
    df_val = df.iloc[val_idx].reset_index(drop=True)
    y_val = y.iloc[val_idx].reset_index(drop=True)

    predicciones = model_tester.run_test(df_val)["predictions"]

    df_parcial = pd.DataFrame({
        "Marca": df_val["Brand"],
        "y_real": y_val,
        "y_pred": predicciones,
        "error": np.abs(y_val - predicciones)
    })

    df_resultados_kfold.append(df_parcial)

df_resultados_kfold = pd.concat(df_resultados_kfold).reset_index(drop=True)

# === FUNCIÓN DE OPTIMIZACIÓN DE INTERVALOS ===
def optimizar_intervalo_marca(df_marca, cobertura_objetivo, usar_costo=True):
    y_real = df_marca["y_real"].values
    y_pred = df_marca["y_pred"].values
    pred_media = np.mean(y_pred)
    error_medio = df_marca["error"].mean()
    error_estimado = np.full_like(y_pred, error_medio)

    mejor_valor = None
    mejor_costo = float("inf")

    for z in np.arange(0.5, 5.0, 0.01):
        lower = y_pred - z * error_estimado
        upper = y_pred + z * error_estimado
        cubre = (y_real >= lower) & (y_real <= upper)
        cobertura = cubre.mean()
        ancho = np.mean(upper - lower)

        if cobertura < cobertura_objetivo:
            continue

        costo = (1 - cobertura)**2 + (ancho / pred_media)**2 if usar_costo else ancho

        if costo < mejor_costo:
            mejor_valor = {
                "z": z,
                "cobertura": cobertura,
                "ancho_promedio": ancho,
                "error_medio": error_medio,
                "costo": costo,
                "Marca": df_marca["Marca"].iloc[0]
            }
            mejor_costo = costo

    return mejor_valor

# === CÁLCULO DE INTERVALOS POR MARCA ===
tabla_segura, tabla_ajustada = [], []

for marca, grupo in df_resultados_kfold.groupby("Marca"):
    res_segura = optimizar_intervalo_marca(grupo, cobertura_objetivo=0.95)
    res_ajustada = optimizar_intervalo_marca(grupo, cobertura_objetivo=0.85)

    if res_segura: tabla_segura.append(res_segura)
    if res_ajustada: tabla_ajustada.append(res_ajustada)

df_tabla_segura = pd.DataFrame(tabla_segura)
df_tabla_ajustada = pd.DataFrame(tabla_ajustada)

media_pred_por_marca = df_resultados_kfold.groupby("Marca")["y_pred"].mean().to_dict()
for df_tabla in [df_tabla_segura, df_tabla_ajustada]:
    df_tabla["error_pct"] = df_tabla.apply(
        lambda row: f"{(row['error_medio'] / media_pred_por_marca.get(row['Marca'], 1)) * 100:.2f}%",
        axis=1
    )

# === COMPARACIÓN FINAL DE AMBAS OPCIONES ===
tabla_s = df_tabla_segura.rename(columns={
    "z": "z_segura", "cobertura": "cobertura_segura", "ancho_promedio": "ancho_segura",
    "error_medio": "error_segura", "costo": "costo_segura", "error_pct": "error_pct_segura"
})
tabla_a = df_tabla_ajustada.rename(columns={
    "z": "z_ajustada", "cobertura": "cobertura_ajustada", "ancho_promedio": "ancho_ajustada",
    "error_medio": "error_ajustada", "costo": "costo_ajustada", "error_pct": "error_pct_ajustada"
})
comparacion = pd.merge(tabla_s, tabla_a, on="Marca", how="inner")
comparacion["mejor_opcion"] = np.where(
    comparacion["costo_segura"] < comparacion["costo_ajustada"], "segura", "ajustada"
)
comparacion["diferencia_costo"] = comparacion["costo_ajustada"] - comparacion["costo_segura"]

comparacion["Z"] = np.where(comparacion["mejor_opcion"] == "ajustada", comparacion["z_ajustada"], comparacion["z_segura"])
comparacion["Coverage"] = np.where(comparacion["mejor_opcion"] == "ajustada", comparacion["cobertura_ajustada"], comparacion["cobertura_segura"])
comparacion["Width"] = np.where(comparacion["mejor_opcion"] == "ajustada", comparacion["ancho_ajustada"], comparacion["ancho_segura"])
comparacion["Mean_error"] = np.where(comparacion["mejor_opcion"] == "ajustada", comparacion["error_ajustada"], comparacion["error_segura"])
comparacion["Cost"] = np.where(comparacion["mejor_opcion"] == "ajustada", comparacion["costo_ajustada"], comparacion["costo_segura"])
comparacion["Error_pct"] = np.where(comparacion["mejor_opcion"] == "ajustada", comparacion["error_pct_ajustada"], comparacion["error_pct_segura"])

# Eliminar columnas auxiliares
comparacion = comparacion.drop(labels=[
    "z_segura", "cobertura_segura", "ancho_segura", "error_segura", "costo_segura", "error_pct_segura",
    "z_ajustada", "cobertura_ajustada", "ancho_ajustada", "error_ajustada", "costo_ajustada", "error_pct_ajustada",
    "mejor_opcion", "diferencia_costo"
], axis=1)
comparacion = comparacion.rename(columns={"Marca": "Brand"})

df = spark.createDataFrame(comparacion)
catalog_name = dbutils.widgets.get("catalog_name")
df.write.mode("overwrite").format("delta").saveAsTable(f"{catalog_name}.silver.tb_123_volta_etl_mlops_confidence_intervals")
logger.info(comparacion)

# COMMAND ----------

vol_schema_g = 'golden'
vol_schema_s = 'silver'
vol_schema_b = 'bronze'
vol_name = 'common_files'
table_ci = 'tb_123_volta_etl_mlops_confidence_intervals'
table_golden = 'ds_123_volta_etl_mlops_001_trainingpatterns'

config_ini = spark.read.json(f"/Volumes/{catalog_name}/{vol_schema_b}/{vol_name}/config_ini.json", multiLine=True)
config_ini_pd = config_ini.toPandas()
mode = "overwrite"
database = config_ini_pd['platinum_database'].values[0]
host = config_ini_pd['platinum_host'].values[0]
port = config_ini_pd['platinum_port'].values[0]
driver =  config_ini_pd['platinum_driver'].values[0]
user_platinum = config_ini_pd['platinum_user'].values[0] 
secret_user_platinum = config_ini_pd['secret_user_platinum'].values[0] 

pro_secrets_scope = config_ini_pd['pro_secrets_scope'].values[0]
dev_secrets_scope = config_ini_pd['dev_secrets_scope'].values[0]
scopes_list = dbutils.secrets.listScopes()

if pro_secrets_scope in [scope.name for scope in scopes_list]:
    # PRO environment
    password_platinum = dbutils.secrets.get(scope=pro_secrets_scope, key=secret_user_platinum)
elif dev_secrets_scope in [scope.name for scope in scopes_list]:
    # DEV environment
    password_platinum = dbutils.secrets.get(scope=dev_secrets_scope, key=secret_user_platinum)

properties = {"user": user_platinum,"password": password_platinum,"driver": driver}
url = f"jdbc:postgresql://{host}:{port}/{database}"

df_ci = spark.read.table(f"{catalog_name}.{vol_schema_s}.{table_ci}")
df_ci.write.jdbc(url=url, table=table_ci, mode=mode, properties=properties)

df_golden = spark.read.table(f"{catalog_name}.{vol_schema_g}.{table_golden}")
df_golden.write.jdbc(url=url, table=table_golden, mode=mode, properties=properties)

# COMMAND ----------

from pyspark.sql.functions import concat_ws
from pyspark.sql import Row
from pyspark.sql.functions import abs

class VehicleScorer:
    def __init__(self, catalog_name):
        self.df = spark.read.table(f"{catalog_name}.golden.ds_123_volta_etl_mlops_001_trainingpatterns")
        self.df = self.df.withColumn("Marca_Modelo", concat_ws("_", self.df["Brand"], self.df["Model"]))

    def calculate_shortage(self, cliente_input):
        marca_modelo = f"{cliente_input['Brand']}_{cliente_input['Model']}"
        fila = self.df.filter(self.df["Marca_Modelo"] == marca_modelo).collect()
        if fila:
            return fila[0]["Shortage"]
        shortage = self.df.approxQuantile("Shortage", [0.9], 0.05)[0]
        logger.info(f"No match para {marca_modelo}, se usa Shortage 90p: {shortage}")
        return shortage

    def calculate_pricemed(self, cliente_input):
        df_filtrado = self.df.filter(
            (self.df.Brand == cliente_input['Brand']) &
            (self.df.Model == cliente_input['Model']) &
            (self.df.Fuel == cliente_input['Fuel']) &
            (abs(self.df.CV - cliente_input['CV']) <= 5) &
            (abs(self.df.Age - cliente_input['Age']) <= 1)
        )
        if df_filtrado.count() > 0:
            return df_filtrado.approxQuantile("PriceMed", [0.5], 0.1)[0]
        logger.info("Sin referencias para PriceMed, se asigna None")
        return None
    
    def enrich_vehicle(self, cliente_input):
        shortage = self.calculate_shortage(cliente_input)
        pricemed = self.calculate_pricemed(cliente_input)

        enriched = {
            "Brand": cliente_input["Brand"],
            "Shortage": shortage,
            **{k: v for k, v in cliente_input.items() if k != "Brand"},
            "PriceMed": pricemed
        }
        return enriched


cliente_input1 = {
    "Brand": "BMW",
    "Model": "X1",
    "CV": 115,
    "Fuel": "DIESEL",
    "Gearbox": "MANUAL",
    "Segment": "SUV",
    "Kms": 85000,
    "Age": 5,
    "Sale": "Profesional"
}

cliente_input1 = {
    "Brand": "RENAULT",
    "Model": "CLIO",
    "CV": 90,
    "Fuel": "GASOLINA",
    "Gearbox": "MANUAL",
    "Segment": "UTILITARIO",
    "Kms": 65000,
    "Age": 4,
    "Sale": "Particular"
}

cliente_input = {
    "Brand": "LEXUS",
    "Model": "UX 250H",  # híbrido compacto poco común
    "CV": 184,
    "Fuel": "HIBRIDO",
    "Gearbox": "AUTOMATICO",
    "Segment": "SUV",
    "Kms": 22000,
    "Age": 2,
    "Sale": "Particular"
}

scorer = VehicleScorer(catalog_name)  # cambia a tu nombre real
vehiculo = scorer.enrich_vehicle(cliente_input)

vehiculo_df = pd.DataFrame([vehiculo])
predicciones = model_tester.run_test(vehiculo_df)["predictions"]
