# Databricks notebook source
# MAGIC %md
# MAGIC Catalog Name

# COMMAND ----------

dbutils.widgets.text("target_catalog", "ws_dag_dev", "Target Catalog")
catalog_name = dbutils.widgets.get("target_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC Actualización de paquetes configurados y solucionados

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /local_disk0/tmp/$USER/geckodriver

# COMMAND ----------

# MAGIC %sh
# MAGIC apt-get update --fix-missing

# COMMAND ----------

# MAGIC %md
# MAGIC Instalar Key

# COMMAND ----------

# MAGIC %sh
# MAGIC apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 119903C81FE2CAC2
# MAGIC
# MAGIC %sh
# MAGIC apt-key adv --keyserver keyserver.ubuntu.com --recv-keys A6DCF7707EBC211F
# MAGIC
# MAGIC %sh
# MAGIC apt-get update

# COMMAND ----------

# MAGIC %md
# MAGIC Instalar ubuntu (?)

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get update
# MAGIC sudo apt-get install -y libgtk-3-0 libdbus-glib-1-2 libxt6 libxrender1 libx11-xcb1

# COMMAND ----------

# MAGIC %md
# MAGIC Instalacion de Firefox

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

# MAGIC %md
# MAGIC Comprobacion de firefox

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lh /tmp/firefox | grep firefox

# COMMAND ----------

# MAGIC %md
# MAGIC NO NECESARIO SI ESTÁ EN CLUSTER
# MAGIC
# MAGIC Instalar selenium 3.141 y webdriver-manager

# COMMAND ----------

'''%pip install selenium==3.141.0  # Instala Selenium 3
%pip install webdriver-manager
%restart_python'''

# COMMAND ----------

# MAGIC %md
# MAGIC Instalar geckodriver en /tmp/

# COMMAND ----------

# MAGIC %sh
# MAGIC wget -O /local_disk0/tmp/$USER/geckodriver.tar.gz "https://github.com/mozilla/geckodriver/releases/download/v0.31.0/geckodriver-v0.31.0-linux64.tar.gz"
# MAGIC tar -xzf /local_disk0/tmp/$USER/geckodriver.tar.gz -C /local_disk0/tmp/$USER/geckodriver
# MAGIC chmod +x /local_disk0/tmp/$USER/geckodriver/geckodriver
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC /local_disk0/tmp/$USER/geckodriver/geckodriver --version

# COMMAND ----------

# MAGIC %md
# MAGIC Comprobación

# COMMAND ----------

import os
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import random
try:
    user = os.environ["USER"]
    geckodriver = f"/local_disk0/tmp/{user}/geckodriver/geckodriver"
    FIREFOX_BINARY = "/tmp/firefox/firefox"

    log_file_path = f"/tmp/geckodriver_{random.randint(1000, 9999)}.log"
    if os.path.exists(log_file_path):
        os.remove(log_file_path)

    options = webdriver.FirefoxOptions()
    options.add_argument('-headless')
    options.binary_location = FIREFOX_BINARY

    browser = webdriver.Firefox(executable_path=geckodriver, service_log_path=log_file_path, options=options)
    browser.get("https://www.google.com")
    print("✅ CONECTADO CON SUDO")
    browser.quit()
except:
    pass

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install selenium==3.141.0 protobuf==3.19.6 typing_extensions==4.9.0
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install --upgrade --no-cache-dir typing_extensions==4.9.0
# MAGIC

# COMMAND ----------

import pkg_resources
print(pkg_resources.get_distribution("typing_extensions").version)
