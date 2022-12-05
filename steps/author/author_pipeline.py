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

# COMMAND ----------

dbutils.widgets.text("author_fpath", '')
author_fpath = dbutils.widgets.get("author_fpath")

dbutils.widgets.text("country", '')
country = dbutils.widgets.get("country")

dbutils.widgets.text("table_results", '')
table_results = dbutils.widgets.get("table_results")

dbutils.widgets.text("lang", '')
lang = dbutils.widgets.get("lang")

# COMMAND ----------

# Functions


def pipeline_driver(author_fpath, lang):

    today = date.today()
    d4 = today.strftime("%b_%d_%Y")

    author_models = {"jp": 'author_model_v0.8.2_ja', "de": 'author_model_v0.8.3_de_v0.4.0', "en": 'author_model_v0.8.3_en_v0.7.5', "it": 'author_model_v0.8.3_it_v1.6',
                     "ru": 'author_model_v0.8.3_ru_v0.8.2', "fr": 'french_v0.4.0_v1.2', "es": 'spanish_v0.4.0_v1.4', "sw": 'swedish_v0_4_0_v2', "kr": 'korean_v0.4.0_v8', "pt": 'author_model_v0.8.3_en_v0.7.5'}

    if(country != "japan"):

        return dbutils.notebook.run(path='/Users/nick_altgelt@bat.com/Author/v1.0/source/inference/Pipeline_driver', timeout_seconds=0, arguments={
            'author_model_dir': author_models[lang],
            'input_file_path': author_fpath,
            'output_file_path': author_fpath,
            'channel_column': "channel",
            'lang': lang,
            'tf_version': "2.8.0",
        })

    else:

        return dbutils.notebook.run(path='/Users/nick_altgelt@bat.com/Author/v1.0/source/inference/Pipeline_driver', timeout_seconds=0, arguments={
            'author_model_dir': author_models[lang],
            'database': "default",
            'table': f"japan_temporal_results_{d4}",
            'channel_column': "channel",
            'lang': lang,
            'tf_version': "2.8.0",
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

# Get Result
for col in result_dataframe.columns:
    result_dataframe[col] = result_dataframe[col].astype(str)
if 'Unnamed: 0' in result_dataframe:
    result_dataframe = result_dataframe.drop('Unnamed: 0', axis=1)

result_dataframe_ultra = result_dataframe[["SN_MSG_ID", "channel", "Created_Time", "Month", "Year", "username", "followers_count", "friends_count", "Brand", "Quarter", "Market", "Theme", "Category", "Funnel",
                                           "Sentiment", "Country", "Author_Predictions", "user_uid", "engagement_avg", "author_prediction_ori", "author_prediction", "author_prediction2", "influencer_prediction", "prediction", "prediction2"]]
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
    old_df = old_df[["SN_MSG_ID", "channel", "Created_Time", "Month", "Year", "username", "followers_count", "friends_count", "Brand", "Quarter", "Market", "Theme", "Category", "Funnel", "Sentiment",
                    "Country", "Author_Predictions", "user_uid", "engagement_avg", "author_prediction_ori", "author_prediction", "author_prediction2", "influencer_prediction", "prediction", "prediction2"]]
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
