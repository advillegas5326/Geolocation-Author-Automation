# Databricks notebook source
import pandas as pd
import requests

# COMMAND ----------

dbutils.widgets.text("input_file_path", '')
input_file_path = dbutils.widgets.get("input_file_path")

#output_file_path = input_file_path.split('.csv')[0] + "_prepared" + '.csv'
output_file_path = input_file_path

dbutils.widgets.text("country", '')
country = dbutils.widgets.get("country")

print(input_file_path, output_file_path)

# COMMAND ----------

# Functions


def data_preparation(file_path):
    return dbutils.notebook.run(path='/Users/nick_altgelt@bat.com/DIF/v1.0/source/geolocation_data_prep/geolocation_data_prep', timeout_seconds=0, arguments={
        'input_file_path': input_file_path,
        'output_file_path': output_file_path,
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
    path = "/end_preparation"
    response = requests.post(url=url + path, json=data)
    final = response.json()
    return final


def send_telegram_error(text):
    dbutils.notebook.run(path='/Repos/nick_altgelt@bat.com/Geolocation-Author-Automation/steps/utils/telegram_live_notifications', timeout_seconds=0, arguments={
        'send_text': f"Sucedió un problema dentro de la ejecución con el siguiente error: {text}",
    })

# COMMAND ----------


# Preparation
try:
    prepairing_result = data_preparation(input_file_path)
except Exception as e:
    print(e)
    send_telegram_error(e)

# COMMAND ----------

response = send_to_api({"fpath": prepairing_result, "country": country})
print(response)

# COMMAND ----------

dbutils.notebook.exit(prepairing_result)
