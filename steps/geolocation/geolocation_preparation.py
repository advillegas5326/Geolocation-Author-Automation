# Databricks notebook source
import pandas as pd
import requests

# COMMAND ----------

dbutils.widgets.text("geolocation_fpath", '')
geolocation_fpath = dbutils.widgets.get("geolocation_fpath")

dbutils.widgets.text("country", '')
country = dbutils.widgets.get("country")

print(geolocation_fpath)

# COMMAND ----------

# Functions


def data_preparation():
    return dbutils.notebook.run(path='/Users/nick_altgelt@bat.com/DIF/v1.1/source/geolocation_data_prep/geolocation_data_prep', timeout_seconds=0, arguments={
        'input_file_path': geolocation_fpath,
        'output_file_path': geolocation_fpath,
        'user_column_name': 'SenderUserId',
        'channel_column_name': 'Message_Type',
        'post_link_column_name': 'Permalink',
        'post_text_column_name': 'cleaned_original_text',
    })


def csv_to_dataframe(csv_path):
    data = pd.read_csv(csv_path)
    return data


def send_to_api(data):
    url = "https://edp-middleware.herokuapp.com"
    path = "/end_geolocation_preparation"
    response = requests.post(url=url + path, json=data)
    final = response.json()
    return final


def send_telegram_error(text):
    dbutils.notebook.run(path='/Repos/nick_altgelt@bat.com/Geolocation-Author-Automation/steps/utils/telegram_live_notifications', timeout_seconds=0, arguments={
        'send_text': f"{text}",
    })

# COMMAND ----------


# Preparation
try:
    prepairing_result = data_preparation()
except Exception as e:
    print(e)
    send_telegram_error(
        f"Sucedió un problema dentro de la ejecución de geolocation preparation con el siguiente error: {e}")

# COMMAND ----------

response = send_to_api(
    {"geolocation_fpath": prepairing_result, "country": country})
send_telegram_error(f"Geolocation preparation terminado: {country}")
print(response)

# COMMAND ----------

dbutils.notebook.exit(prepairing_result)
