# Databricks notebook source
# MAGIC %md # Notebook functions

# COMMAND ----------

# MAGIC %md ##Functions

# COMMAND ----------

# Imports
import pandas as pd
from delta.tables import *
import requests

# COMMAND ----------

dbutils.widgets.text("experiment_name", '')
experiment_name = dbutils.widgets.get("experiment_name")

dbutils.widgets.text("geolocation_fpath", '')
geolocation_fpath = dbutils.widgets.get("geolocation_fpath")

dbutils.widgets.text("cities_table", '')
cities_table = dbutils.widgets.get("cities_table")

dbutils.widgets.text("input_language", '')
input_language = dbutils.widgets.get("input_language")

dbutils.widgets.text("table_results", '')
table_results = dbutils.widgets.get("table_results")

dbutils.widgets.text("country", '')
country = dbutils.widgets.get("country")

# COMMAND ----------

# Functions


def pipeline_driver(config_object):
    return dbutils.notebook.run(path='/Users/nick_altgelt@bat.com/Geolocation/v1.6.2/databricks/Pipeline_driver', timeout_seconds=0, arguments={
        'cities_table': f"/dbfs/mnt/dif_diamond/geolocation/csv/input_files/{config_object['cities_table']}",
        'creds_scope_name': 'geolocation',
        'creds_set_name': 'DEV',
        'experiment_name': config_object["experiment_name"],
        'fpath': config_object["geolocation_fpath"],
        'input_language': config_object["input_language"],
        'stochastic_models_debug_mode': 'False',
        'test_sample_only': 'False',
    })


def csv_to_dataframe(csv_path):
    data = pd.read_csv(csv_path)
    return data


def send_to_api(data):
    url = "https://edp-middleware.herokuapp.com"
    path = "/end_geolocation_pipeline"
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

# Geolocation Saving Chunks


try:
    config = {
        "experiment_name": experiment_name,
        "geolocation_fpath": geolocation_fpath,
        "cities_table": cities_table,
        "input_language": input_language
    }
    pipeline_result = pipeline_driver(config)
except Exception as e:
    print(e)
    send_telegram_error(
        f"Sucedió un problema dentro de la ejecución de geolocation pipeline con el siguiente error: {e}")

# COMMAND ----------

# MAGIC %md ##Updating Database

# COMMAND ----------

# Get Result
#result_dataframe = csv_to_dataframe("/dbfs/mnt/dif_diamond/geolocation/csv/inference_model_runs/global_october_20221028_dev/global_october_20221028_dev_results.csv")
result_dataframe = csv_to_dataframe(pipeline_result)
result_dataframe["is_analyzed"] = True
for col in result_dataframe.columns:
    result_dataframe[col] = result_dataframe[col].astype(str)
result_dataframe = result_dataframe.drop('Cleaned_text', axis=1)
if 'Unnamed: 0' in result_dataframe:
    result_dataframe = result_dataframe.drop('Unnamed: 0', axis=1)
if(input_language == "fr"):
    result_dataframe["Language"] = "french"
elif(input_language == "es"):
    result_dataframe["Language"] = "spanish"

if "country_based_model_predictions" not in result_dataframe:
    result_dataframe["country_based_model_predictions"] = "nan"

result_dataframe_ultra = result_dataframe[["Brand", "bio", "Permalink", "Language", "country_based_model_predictions",
                                           "SN_MSG_ID", "Created_Time", "combined_city_prediction", "Country", "combined_country_prediction"]]
print(result_dataframe.shape)

# COMMAND ----------

spark_df = spark.createDataFrame(result_dataframe_ultra)
spark_df.write.format("delta").mode("append").saveAsTable(
    f"geolocation_weekly.{table_results}")

# COMMAND ----------

response = send_to_api(
    {"geolocation_fpath": pipeline_result, "country": country})
send_telegram_error(f"Geolocation pipeline terminado: {country}")
print(response)

# COMMAND ----------

dbutils.notebook.exit(pipeline_result)

# COMMAND ----------
