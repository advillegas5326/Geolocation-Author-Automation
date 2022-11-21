# Databricks notebook source
# MAGIC %md # Notebook functions

# COMMAND ----------

# MAGIC %md ##Functions

# COMMAND ----------

# Imports
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import numpy as np
import pandas as pd
from delta.tables import *
import requests

# COMMAND ----------

dbutils.widgets.text("fpath", '')
fpath = dbutils.widgets.get("fpath")

dbutils.widgets.text("country", '')
country = dbutils.widgets.get("country")

dbutils.widgets.text("table_results", '')
table_results = dbutils.widgets.get("table_results")

dbutils.widgets.text("lang", '')
lang = dbutils.widgets.get("lang")

# COMMAND ----------

# Functions


def pipeline_driver(fpath, lang):
    return dbutils.notebook.run(path='/Users/nick_altgelt@bat.com/Author/v1.0/source/inference/Pipeline_driver', timeout_seconds=0, arguments={
        'author_model_dir': "author_model_v0.8.3_en_v0.7.5",
        'input_file_path': fpath,
        'output_file_path': fpath,
        'channel_column': "channel",
        'lang': lang,
        'tf_version': "2.5.0",
    })


def read_csv_files(files):
    data = []
    for file in files:
        data_file = pd.read_csv(file)
        data.append(data_file)
    return data


def dataframe_to_csv(data):
    path = path_to_save + save_name + ".csv"
    data.to_csv(path, index=False)
    return path


def csv_to_dataframe(csv_path):
    data = pd.read_csv(csv_path)
    return data


def search_for_non_analyzed(table_path):
    all_records = spark.sql("select * from {}".format(table_path)).toPandas()
    non_analyzed_dataframe = all_records.loc[all_records['is_analyzed'] is False]
    return non_analyzed_dataframe


def send_to_api(data):
    url = "https://edp-middleware.herokuapp.com"
    path = "/end_geolocation"
    response = requests.post(url=url + path, json=data)
    final = response.json()
    return final

# COMMAND ----------

# MAGIC %md ##Excecution

# COMMAND ----------


# author Saving Chunks
pipeline_result = pipeline_driver(fpath, lang)
print(pipeline_result)

# COMMAND ----------

# MAGIC %md ##Updating Database

# COMMAND ----------

#result_dataframe = csv_to_dataframe("/dbfs/FileStore/shared_uploads/nick_altgelt@bat.com/japan_october_2022112_full.csv")
result_dataframe = csv_to_dataframe(pipeline_result)
result_dataframe

# COMMAND ----------

# Get Result
for col in result_dataframe.columns:
    result_dataframe[col] = result_dataframe[col].astype(str)
if 'Unnamed: 0' in result_dataframe:
    result_dataframe = result_dataframe.drop('Unnamed: 0', axis=1)
# result_dataframe_ultra = result_dataframe[["Brand","Category","Permalink","Language","country_based_model_predictions","SN_MSG_ID","Created_Time","combined_city_prediction","Country","combined_country_prediction"]]
# print(result_dataframe.shape)

# COMMAND ----------

spark_df = spark.createDataFrame(result_dataframe)
spark_df.write.format("delta").mode("append").saveAsTable(
    f"author_weekly.{table_results}")

# COMMAND ----------

# response = send_to_api({"fpath":pipeline_result, "country":country})
# print(response)

# COMMAND ----------

dbutils.notebook.exit(pipeline_result)

# COMMAND ----------
