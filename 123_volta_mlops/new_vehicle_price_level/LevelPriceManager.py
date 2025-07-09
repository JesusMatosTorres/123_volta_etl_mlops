# Databricks notebook source
# MAGIC %run ../PipInstalls

# COMMAND ----------

spark = spark.builder \
    .appName("Model_Level_Price_Manager") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .getOrCreate()

# COMMAND ----------

import pandas as pd
import numpy as np
import re
import json
import sys, os
import pickle

class LevelPriceTask:
    def __init__(self,
                 engine = None,
                 logger = None,
                 is_training = None):

        if logger != None:
            self.logger = logger
        else:
            self.logger = LoggerHelper('PatternsBuilder').create_logger()

        self.is_training = is_training

        if engine != None:
            bronze = engine["BronzeZone"]
            self.df_list_prices = spark.read.table(f"{bronze}.tb_123_volta_etl_mlops_lista_precios")
            self.df_list_prices = self.df_list_prices.toPandas()
            #self.df_list_prices = self.df_list_prices.rename(columns={"CV": "CV"})
            silver = engine["SilverZone"]
            self.df_markets = spark.read.table(f"{silver}.tb_123_volta_etl_mlops_markets_canary")
            self.df_markets = self.df_markets.toPandas()

    def get_median_price(self,
                         df_advertisements):
        #self.logger.info("Starting '__get_level_new_price_from_historical' function, to get vehicles prices")
        if self.is_training == None:

            with open('/bags.pickle', 'rb') as handle:
                dictionary_bags = pickle.load(handle)
                self.df_prices_with_bags = pd.DataFrame(dictionary_bags)

            return df_advertisements.apply(lambda x: self.__get_level_new_price_from_historical(x,
                                                                                                self.df_prices_with_bags), axis=1)
        if self.is_training == True:
            return self.__get_level_new_price_from_historical(None,
                                                            self.df_list_prices)

    def get_dictionary_vehicles_version(self):

        return self.__get_dict_vehicles(self.df_list_prices)


    def get_dictionary_vehicles_power(self):
        self.df_markets.Brand = self.df_markets.Brand.apply(lambda x: re.sub(r"[^A-Za-z0-9]", "", x))
        self.df_markets.Model = self.df_markets.Model.apply(lambda x: re.sub(r"[^A-Za-z0-9]", "", x))

        self.df_list_prices.Brand = self.df_list_prices.Brand.apply(lambda x: re.sub(r"[^A-Za-z0-9]", "", x))
        self.df_list_prices.Model = self.df_list_prices.Model.apply(lambda x: re.sub(r"[^A-Za-z0-9]", "", x))

        self.df_markets = self.df_markets[self.df_markets.Province == "LAS PALMAS"]
        self.df_markets = self.df_markets[["Brand", "Model", "CV"]]

        self.df_markets = self.df_markets[(self.df_markets.Model.isin(self.df_list_prices.Model.unique())) &
                                ((pd.isnull(self.df_markets.CV) == False))].drop_duplicates(subset=["Brand",
                                                                                                          "Model",
                                                                                                          "CV"])
        self.df_markets.sort_values(by=["Brand", "Model"], inplace=True)

        return self.__get_dict_vehicles_power(self.df_markets)


    def __get_level_new_price_from_historical(self,
                                              line,
                                              df_2):

        """
        Function to get the level new prices from historical data

        :param line: each row of our dataframe with VO vehicles.
        :param df_2: dataframe with the list of prices from scraper module.
        :return line:  return the dataframe row with the price of vehicle.
        """

        def find_nearest(array,
                         value):
            """
            Function to get the nearest value from CV
            :param array: array values where we find the neares "value" number
            :param value: the value to find the nearest number inside the array
            :return: array: nearest value give it
            """
            array = np.asarray(array)
            idx = (np.abs(array - value)).argmin()
            return array[idx]

        def quit_number(row):
            """
            Function to quit numbers of descriptions of vehicles and other unneccesary elements.

            :param row: each row where we create bags of words, with the most important words in the desciptions
            :return:
            """
            row = row.split(" ")
            row = list(filter(None, row))
            vector = row.copy()
            for word in vector:
                quit_word = word
                word = word.replace("(",
                                    "")
                try:
                    if (word.strip()[0].isdigit() == True) | (len(word.strip()) <= 1):
                        row.remove(quit_word)
                except:
                    pass
            row = " ".join(row).upper()
            return row

        df_news = df_2.copy()
        df_news = df_news.rename(columns={"CV": "CV"})

        if self.is_training == True:
            df_news["Features"] = df_news["Features"].fillna("-")
            df_news["Bags"] = df_news["Features"].map(quit_number)
            return df_news
        else:
            pass

        # we filter by attribute, each attribute it´s characterictis from vehicles
        for attribute in "Brand", \
                         "Model", \
                         "CV", \
                         "Fuel":

            if df_news[(df_news[attribute] == line[attribute])].shape[0] == 0:
                pass
            else:
                df_news = df_news[(df_news[attribute] == line[attribute])]
        
        if df_news[(df_news["EndDate"] >= line["Registration"]) & (df_news["StarDate"] <= line["Registration"])].shape[0] == 0:
            pass
        else:
            df_news = df_news[(df_news["EndDate"] >= line["Registration"]) & (df_news["StarDate"] <= line["Registration"])]

        line["Bags"] = quit_number(line["Features"])

        # We check if we can find the description of car by Bags of words
        for word in line["Bags"].split(' '):
            try:
                if df_news[df_news.Bags.str.upper().str.contains(word, regex=False)].shape[0] == 0:
                    pass
                else:
                    df_news = df_news[df_news.Bags.str.upper().str.contains(word, regex=False)]
            except:
                pass

        # At last opportunity we get the nearest value from power to get the closest car from list price.
        if df_news.shape[0] == 1:
            pass
        else:
            if pd.isnull(line["CV"]) == False:
                cilindrada = line["CV"]
                values = df_news["CV"].values
                nearest = find_nearest(values,
                                       cilindrada)
                df_news = df_news[df_news["CV"] == nearest]
            else:
                pass
        # We get the medium prices from the list of vehicles news
        df_news["Price"] = df_news["Price"]
        line["PriceMed"] = df_news["Price"].astype(float).sort_values().median()

        return line


    def __get_dict_vehicles(self,
                            df):
        """
        Function to get the dictionary with brand, model, and equipment detected

        :param df: dataframe with the list of vehicle new prices
        :return dictionary_model_brand: dictionary with brand, model and equipment for each vehicle level price
        """
        # Clean the data, without number and unnecessary words
        def hasNumbers(inputString):
            return any(char.isdigit() for char in inputString)

        # Clean the data, without number and unnecessary words
        def quit_number(row):
            row = row.split(" ")
            row = list(filter(None, row))
            vector = row.copy()
            for word in vector:
                quit_word = word
                word = word.replace("(", "")
                try:
                    if (word.strip()[0].isdigit() == True) | (len(word.strip()) <= 3):
                        row.remove(quit_word)
                except:
                    pass
            return row

        # We get the key words of each descriptions of cars, to create the dictionary
        def get_all_key_words(df):
            alled = {}
            for Brand in df.Brand.unique():
                df_test = df[df.Brand == Brand]
                alled[Brand] = {}
                for model in df_test.Model.unique():
                    df_test = df[df.Model == model]
                    conjunto = []
                    for lista in df_test["Bags"].values:
                        lista = [x.upper() for x in lista]
                        lista_clean = []
                        for word in lista:
                            try:
                                word = word.split("/")[1]
                            except:
                                pass
                            word = re.sub(r"[^A-Za-z0-9]", "", word)
                            if (len(word) >= 3) & (hasNumbers(word) == False):
                                lista_clean.append(word)
                            else:
                                pass
                        conjunto = conjunto + lista_clean
                        conjunto = list(dict.fromkeys(conjunto))
                    alled[df_test.Brand.iloc[0]][df_test.Model.iloc[0]] = sorted(set(conjunto))
            return alled

        # Create the bags of words and create the dictionary, as json
        df["Features"] = df["Features"].fillna("-")
        df["Bags"] = df["Features"].map(quit_number)

        df.sort_values(by=["Brand", "Model"], inplace=True)

        dictionary_model_brand = get_all_key_words(df)

        return dictionary_model_brand

    def __get_dict_vehicles_power(self,
                                df):
        def get_all_key_words(df):
            alled = {}
            for Brand in df.Brand.unique():
                df_test = df[df.Brand == Brand]
                alled[Brand] = {}
                for model in df_test.Model.unique():
                    df_test = df[df.Model == model]
                    alled[df_test.Brand.iloc[0]][df_test.Model.iloc[0]] = sorted(
                        list(set(df_test["CV"].to_list())))
            return alled

        dictionary_model_brand = get_all_key_words(df)

        return dictionary_model_brand
