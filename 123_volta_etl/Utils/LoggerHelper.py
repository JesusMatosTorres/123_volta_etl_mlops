# Databricks notebook source
import logging
import os, json, sys

# COMMAND ----------

import logging
import sys

class SuppressSpecificLogs(logging.Filter):
    def filter(self, record):
        return "Received command c on object id" not in record.getMessage()
class LoggerHelper():
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        if not self.logger.hasHandlers():  # Asegurar que solo se añade un handler
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')
            handler.setFormatter(formatter)
            handler.addFilter(SuppressSpecificLogs())
            self.logger.addHandler(handler)

    def create_logger(self):
        return self.logger