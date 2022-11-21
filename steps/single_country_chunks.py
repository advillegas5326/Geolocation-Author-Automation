# Databricks notebook source
# MAGIC %md #Auto Geolocation by Chunks

# COMMAND ----------

# MAGIC %md ##Imports

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS geolocation_weekly;
# MAGIC CREATE DATABASE IF NOT EXISTS author_weekly;

# COMMAND ----------

# Imports
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
import numpy as np
import pandas as pd
import datetime
from delta.tables import *
import time
import requests

# COMMAND ----------

# MAGIC %md ##Widgets

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("splits", "")
splits = dbutils.widgets.get("splits")

dbutils.widgets.text("country_prediction", '')
country_prediction = dbutils.widgets.get("country_prediction")

dbutils.widgets.text("database", '')
database = dbutils.widgets.get("database")

dbutils.widgets.text("table", '')
table = dbutils.widgets.get("table")

dbutils.widgets.text("get_data", 'False')
get_data = dbutils.widgets.get("get_data")

dbutils.widgets.text("using_api", 'True')
using_api = dbutils.widgets.get("using_api")

# COMMAND ----------

# MAGIC %md ##Functions

# COMMAND ----------

# Functions


def dataframe_to_csv(data, path_to_save, save_name):
    dbutils.fs.mkdirs(path_to_save + "/" + save_name)
    path = "/dbfs" + path_to_save + "/" + \
        save_name + "/" + save_name + "_full" + ".csv"
    data.to_csv(path, index=False)
    return path


def csv_to_dataframe(csv_path):
    data = pd.read_csv(csv_path)
    return data


def create_execution_variables(database, table):
    save_database = "geolocation_weekly"
    path_to_save = "/FileStore/shared_uploads/nick_altgelt@bat.com/geolocation_chunks"

    today = datetime.datetime.now()

    initial_db = f"{database}.{table}"
    save_name = f"{country_prediction}_total_{today.year}{today.month}{today.day}"

    if (country_prediction == "brazil"):
        input_language = "pt"
        cities_table = "20220811_br_clustered_cities.csv"
    elif (country_prediction == "global"):
        input_language = "en"
        cities_table = "20220719_us_clustered_cities.csv"
    elif (country_prediction == "france"):
        input_language = "fr"
        cities_table = "20220719_us_clustered_cities.csv"
    elif (country_prediction == "mexico" or country_prediction == "chile"):
        input_language = "es"
        cities_table = "20220714_cl_clustered_cities.csv"
    elif (country_prediction == "russia"):
        input_language = "ru"
        cities_table = "20220719_us_clustered_cities.csv"
    else:
        raise Exception(f"Language {input_language} is not supported")

    table_results = f"geolocated_total_{country_prediction}"
    final_table_path = f"{save_database}.{table_results}"

    complete_countries = (initial_db, country_prediction, input_language,
                          table_results, final_table_path, cities_table, path_to_save, save_name)
    return complete_countries


def complement_or_complete_data(country, splits):
    temp_table = "geolocation_weekly"
    if not DeltaTable.isDeltaTable(spark, f'/user/hive/warehouse/{temp_table}.db/{country[3]}'):
        non_analyzed_dataframe = spark.sql(
            "select * from {}".format(country[0])).toPandas()
        print("Old size: ", non_analyzed_dataframe.shape)
        non_analyzed_dataframe = non_analyzed_dataframe.drop_duplicates(
            subset='SN_MSG_ID', keep="first")
        print("New size: ", non_analyzed_dataframe.shape)

        file_path = dataframe_to_csv(
            non_analyzed_dataframe, country[6], country[7])
        print("Path: ", file_path)
        size = non_analyzed_dataframe.shape[0] // int(splits)
        chunk_paths = csv_splitter(
            size, country, non_analyzed_dataframe, splits)
        frame = pd.DataFrame(chunk_paths)
        display(frame)
        return chunk_paths
    else:
        # MATCH TABLE WITH SAVED TABLE (HIDRATED AND GEOLOCATED)
        non_analyzed_dataframe = spark.sql(
            f"SELECT {country[0]}.* FROM {country[0]} LEFT JOIN {country[4]} ON ({country[0]}.SN_MSG_ID = {country[4]}.SN_MSG_ID) WHERE {country[4]}.SN_MSG_ID IS NULL").toPandas()
        print("Old size: ", non_analyzed_dataframe.shape)
        non_analyzed_dataframe = non_analyzed_dataframe.drop_duplicates(
            subset='SN_MSG_ID', keep="first")
        print("New non analized size: ", non_analyzed_dataframe.shape)
        file_path = dataframe_to_csv(
            non_analyzed_dataframe, country[6], country[7])
        print("Path: ", file_path)

    return (file_path, non_analyzed_dataframe)


def csv_splitter(size, country, frame, splits):
    directory = country[6] + '/' \
        + country[7]
    dbutils.fs.mkdirs(directory)
    counter = 0
    save_paths = []
    list_of_dfs = [frame.loc[i:i + size - 1, :]
                   for i in range(0, len(frame), size)]
    for i in list_of_dfs:
        file_name = f'/dbfs{directory}/{country[7]}_{i.shape[0]}_row_{counter}.csv'
        sparkDF = spark.createDataFrame(i)
        sparkDF.write.format("csv").mode("overwrite").save(file_name)
#     i.to_csv(file_name)
        save_paths.append({
            'country': country[1] + "-" + str(counter),
            'records': i.shape[0],
            'fpath': file_name,
            'experiment_name': country[7]
            + '_chunk_' + str(counter),
            'cities_table': country[5],
            'input_language': country[2],
            'table_results': country[3],
            'model': "geolocation",
        })
        counter += 1

    return save_paths


def getting_data(database, table, splits):

    print("\n---------------------Execution Variables---------------------\n")
    country = create_execution_variables(database, table)
    print(country)

    # Get data of all global countries + non analyzed
    print("\n------------------------Getting Data-------------------------\n")
    data = complement_or_complete_data(country, splits)
    return data


def send_to_api(data):
    url = "https://edp-middleware.herokuapp.com"
    path = "/create"
    response = requests.post(url=url + path, json=data)
    final = response.json()
    return final


def send_to_api_info(data):
    url = "https://edp-middleware.herokuapp.com"
    path = "/info"
    response = requests.post(url=url + path, json=data)
    final = response.json()
    return final

# COMMAND ----------

# MAGIC %md
# MAGIC ## Obtaining Execution variables

# COMMAND ----------


countries_array = getting_data(database, table, splits)

# COMMAND ----------

# data = pd.read_csv("/dbfs/FileStore/shared_uploads/nick_altgelt@bat.com/geolocation_chunks/chile_total_2022118/chile_total_2022118_10444_row_7.csv", lineterminator="\n")
# print(data["Age"].value_counts())# data = pd.read_csv("/dbfs/FileStore/shared_uploads/nick_altgelt@bat.com/geolocation_chunks/chile_total_2022118/chile_total_2022118_10444_row_7.csv", lineterminator="\n")
# print(data["Age"].value_counts())

# COMMAND ----------

data = []
if(get_data == "True"):
    for country in countries_array:
        data.append({
            'country': country[1],
            'records': f"Records sin geolocalizar: {country[9].shape[0]} - Columnas de la data : {country[9].shape[1]}",
        })
    print(data)
    send_to_api_info(data)
else:
    if(using_api == "True"):
        print(countries_array)
        send_to_api(countries_array)

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# class FileSettings(object):

#     def __init__(
#         self,
#         file_path,
#         row_size,
#         path_to_save,
#         save_name,
#         country,
#         experiment_name,
#         cities_table,
#         input_language,
#         table_results,
#         model,
#         ):
#         self.file_path = file_path
#         self.row_size = row_size
#         self.path_to_save = path_to_save
#         self.save_name = save_name
#         self.country = country
#         self.experiment_name = experiment_name
#         self.cities_table = cities_table
#         self.input_language = input_language
#         self.table_results = table_results
#         self.model = model


# class FileSplitter(object):

#     def __init__(self, file_settings):
#         self.file_settings = file_settings

#         if type(self.file_settings).__name__ != 'FileSettings':
#             raise Exception('Please pass correct instance ')

#         self.df = pd.read_csv(self.file_settings.file_path,
#                               chunksize=self.file_settings.row_size)

#     def run(self):
#         # path
#         directory = self.file_settings.path_to_save + '/' \
#             + self.file_settings.save_name

#         # creates a subfolder
#         dbutils.fs.mkdirs(directory)
#         save_file_paths = []
#         counter = 0
#         while True:
#             try:
#                 file_name = \
#                     '/dbfs{}/{}_{}_row_{}.csv'.format(directory,
#                         self.file_settings.save_name, counter,
#                         self.file_settings.row_size)
#                 self.df = self.df.drop["Unnamed: 0"]
#                 df = next(self.df).to_csv(file_name)
#                 save_file_paths.append({
#                     'country':self.file_settings.country+"-"+str(counter),
#                     'records':self.df.shape[0],
#                     'fpath': file_name,
#                     'experiment_name': self.file_settings.save_name
#                         + '_chunk_' + str(counter),
#                     'cities_table': self.file_settings.cities_table,
#                     'input_language': self.file_settings.input_language,
#                     'table_results': self.file_settings.table_results,
#                     'model': self.file_settings.model,
#                     })
#                 counter = counter + 1
#             except StopIteration:
#                 break
#             except Exception as e:
#                 print ('Error:', e)
#                 break
#         return save_file_paths
