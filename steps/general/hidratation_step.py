# Databricks notebook source
import pandas as pd
import requests

# COMMAND ----------

dbutils.widgets.text("file_path", '')
file_path = dbutils.widgets.get("file_path")

dbutils.widgets.text("country", '')
country = dbutils.widgets.get("country")

# COMMAND ----------

# Functions


def inference_hidratation(file_path):
    return dbutils.notebook.run(path='/Users/nick_altgelt@bat.com/DIF/v1.1/source/data_hydratation_process', timeout_seconds=0, arguments={
        'channel_column_name': 'Message_Type',
        'creds_scope_name': 'geolocation',
        'creds_set_name': 'DEV',
        'input_file_path': file_path,
        'user_column_name': 'SenderUserId',
        'post_link_column_name': 'Permalink',
        'model_name': 'geolocation',
        'need_tw_timelines': "True",
    })


def csv_to_dataframe(csv_path):
    data = pd.read_csv(csv_path)
    return data


def send_to_api(data):
    url = "https://edp-middleware.herokuapp.com"
    path = "/end_hidratation"
    response = requests.post(url=url + path, json=data)
    final = response.json()
    return final


def send_telegram_error(text):
    dbutils.notebook.run(path='/Repos/nick_altgelt@bat.com/Geolocation-Author-Automation/steps/utils/telegram_live_notifications', timeout_seconds=0, arguments={
        'send_text': f"Sucedió un problema dentro de la ejecución con el siguiente error: {text}",
    })

# COMMAND ----------


# Hidratation
try:
    hidratation_result = inference_hidratation(file_path)
except Exception as e:
    print(e)
    send_telegram_error(e)

# COMMAND ----------

response = send_to_api({"fpath": hidratation_result, "country": country})
print(response)

# COMMAND ----------

dbutils.notebook.exit(hidratation_result)
