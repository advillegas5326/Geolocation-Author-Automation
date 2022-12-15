# Databricks notebook source
# MAGIC %md # Notebook functions

# COMMAND ----------

# MAGIC %md ##Functions

# COMMAND ----------

# Imports
import pandas as pd
from delta.tables import *
import requests
from datetime import date
import json

# COMMAND ----------

dbutils.widgets.text("author_fpath", '')
author_fpath = dbutils.widgets.get("author_fpath")

dbutils.widgets.text("country", '')
country = dbutils.widgets.get("country")

dbutils.widgets.text("table_results", '')
table_results = dbutils.widgets.get("table_results")

dbutils.widgets.text("column_names_dict_path", '')
column_names_dict_path = dbutils.widgets.get("column_names_dict_path")

dbutils.widgets.text("lang", '')
lang = dbutils.widgets.get("lang")

dbutils.widgets.text("experiment_name", '')
experiment_name = dbutils.widgets.get("experiment_name")

# COMMAND ----------

# Functions


def pipeline_driver(author_fpath, lang):

    today = date.today()
    d4 = today.strftime("%b_%d_%Y")

    author_models = {"jp": 'author_japanese', "de": 'author_germany', "en": 'author_english', "it": 'author_italian',
                     "ru": 'author_russian', "fr": 'author_french', "es": 'author_spanish', "sw": 'author_swedish', "kr": 'author_korean', "pt": 'author_spanish'}

    if(country != "japan"):

        return dbutils.notebook.run(path='/Users/nick_altgelt@bat.com/Author/v1.1/source/inference/Pipeline_driver', timeout_seconds=0, arguments={
            'model_name': author_models[lang],
            'input_file_path': author_fpath,
            'lang': lang,
            "column_names_dict_path": column_names_dict_path,
            "experiment_name": experiment_name
        })

    else:

        return dbutils.notebook.run(path='/Users/nick_altgelt@bat.com/Author/v1.1/source/inference/Pipeline_driver', timeout_seconds=0, arguments={
            'model_name': author_models[lang],
            'database': "default",
            'table': f"japan_temporal_results_{d4}",
            'lang': lang,
            'column_names_dict_path': column_names_dict_path,
            "experiment_name": experiment_name
        })


def csv_to_dataframe(csv_path):
    data = pd.read_csv(csv_path)
    return data


def send_to_api(data):
    url = "https://edp-middleware.herokuapp.com"
    path = "/end_author_pipeline"
    response = requests.post(url=url + path, json=data)
    final = response.json()
    return final


def send_telegram_error(text):
    dbutils.notebook.run(path='/Repos/nick_altgelt@bat.com/Geolocation-Author-Automation/steps/utils/telegram_live_notifications', timeout_seconds=0, arguments={
        'send_text': f"{text}",
    })
# COMMAND ----------

# MAGIC %md ##Excecution

# COMMAND ----------


# author Saving Chunks

try:
    pipeline_result = pipeline_driver(author_fpath, lang)
    print(pipeline_result)
except Exception as e:
    print(e)
    send_telegram_error(
        f"Sucedió un problema dentro de la ejecución de author pipeline con el siguiente error: {e}")

# COMMAND ----------

# MAGIC %md ##Updating Database

# COMMAND ----------

#result_dataframe = csv_to_dataframe("/dbfs/FileStore/shared_uploads/nick_altgelt@bat.com/japan_october_2022112_full.csv")
if(country != "japan"):
    result_dataframe = csv_to_dataframe(pipeline_result)
else:
    result_dataframe = spark.sql(f"SELECT * FROM {pipeline_result}").toPandas()
# COMMAND ----------

with open(column_names_dict_path, "r") as file:
    data = file.readline()
    _dict = json.loads(data)
    print(_dict)

new_dict = {}
for key in _dict:
    new_dict[_dict[key]] = key

# Get Result
for col in result_dataframe.columns:
    result_dataframe[col] = result_dataframe[col].astype(str)
if 'Unnamed: 0' in result_dataframe:
    result_dataframe = result_dataframe.drop('Unnamed: 0', axis=1)


result_dataframe_ultra = result_dataframe[["SN_MSG_ID", "Created_Time", "Month", "Year", "followers_count", "friends_count", "Brand", "Quarter", "Market", "Theme", "Category", "Funnel",
                                           "Sentiment", "Country", "Author_Predictions", "user_uid", "engagement_avg", "author_prediction_ori", "author_prediction", "author_prediction2", "influencer_prediction", "prediction", "prediction2"] + list(new_dict.values())
                                          ]
print(result_dataframe_ultra.shape)
display(result_dataframe_ultra.head(3))

# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, f'/user/hive/warehouse/author_weekly.db/{table_results}'):

    print("No existe")
    new_data = result_dataframe_ultra
    new_data = new_data.drop_duplicates(
        subset='SN_MSG_ID', keep="first")
    print(new_data.shape)

else:

    old_df = spark.sql(
        f"SELECT * from author_weekly.{table_results}").toPandas()
    old_df = old_df[["SN_MSG_ID", "Created_Time", "Month", "Year", "followers_count", "friends_count", "Brand", "Quarter", "Market", "Theme", "Category", "Funnel",
                     "Sentiment", "Country", "Author_Predictions", "user_uid", "engagement_avg", "author_prediction_ori", "author_prediction", "author_prediction2", "influencer_prediction", "prediction", "prediction2"] + column_names_dict_path.values()
                    ]
    print(old_df.shape)

    new_data = pd.concat([result_dataframe_ultra, old_df])
    new_data = new_data.drop_duplicates(
        subset='SN_MSG_ID', keep="first")
    print(new_data.shape)

# COMMAND ----------

spark_df = spark.createDataFrame(new_data)
spark_df.write.format("delta").mode("overwrite").saveAsTable(
    f"author_weekly.{table_results}")

# COMMAND ----------

response = send_to_api({"author_fpath": pipeline_result, "country": country})
send_telegram_error(f"Author pipeline terminado: {country}")
print(response)

# COMMAND ----------

dbutils.notebook.exit(pipeline_result)
