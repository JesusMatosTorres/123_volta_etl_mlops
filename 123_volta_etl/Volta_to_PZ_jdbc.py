# Databricks notebook source
vol_catalog_name = dbutils.widgets.get("catalog_name")
vol_schema_b = 'bronze'
vol_schema_s = 'silver'
vol_name = 'common_files'
table_volta = "tb_123_volta_etl_mlops_markets_canary"
table_consultoria = "tb_123_volta_etl_mlops_lista_precios"

# COMMAND ----------

config_ini = spark.read.json(f"/Volumes/{vol_catalog_name}/{vol_schema_b}/{vol_name}/config_ini.json", multiLine=True)
config_ini_pd = config_ini.toPandas()

# COMMAND ----------

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

# COMMAND ----------

df_consultoria = spark.read.table(f"{vol_catalog_name}.{vol_schema_b}.{table_consultoria}")
df_markets = spark.read.table(f"{vol_catalog_name}.{vol_schema_s}.{table_volta}")

# COMMAND ----------

df_consultoria.write.jdbc(url=url, table=table_consultoria, mode=mode, properties=properties)
df_markets.write.jdbc(url=url, table=table_volta, mode=mode, properties=properties)
