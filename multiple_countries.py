# Databricks notebook source
# MAGIC %md #Auto Geolocation by Chunks

# COMMAND ----------

# MAGIC %md ##Imports

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS geolocation_weekly;
# MAGIC CREATE DATABASE IF NOT EXISTS author_weekly;

# COMMAND ----------

#Imports
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

# dbutils.widgets.text("compute_universe", '')
# compute_universe = dbutils.widgets.get("compute_universe")

# dbutils.widgets.text("splits", "")
# splits = dbutils.widgets.get("splits")

dbutils.widgets.text("country_prediction", '')
country_prediction = dbutils.widgets.get("country_prediction")

dbutils.widgets.text("month", '')
month = dbutils.widgets.get("month")

dbutils.widgets.text("manual_month", '')
manual_month = dbutils.widgets.get("manual_month")

dbutils.widgets.text("get_data", 'False')
get_data = dbutils.widgets.get("get_data")

dbutils.widgets.text("using_api", 'True')
using_api = dbutils.widgets.get("using_api")

dbutils.widgets.text("is_author", 'False')
is_author = dbutils.widgets.get("is_author")

# COMMAND ----------

# MAGIC %md ##Functions

# COMMAND ----------

#Old Notebooks
def paralllel_geolocation(config_object):
    return dbutils.notebook.run(path='/Users/nick_altgelt@bat.com/Geolocation/Geolocation Automation/v1.2/geolocation_job'
                         , timeout_seconds=0, arguments={
        'cities_table': f'/dbfs/mnt/geolocation/data/nodel/{config_object["cities_table"]}'
            ,
        'experiment_name': config_object["experiment_name"],
        'fpath': config_object["fpath"],
        'input_language': config_object["input_language"],
        'table_results': config_object["table_results"],
        })
  
def inference_hidratation(file_path):
    return dbutils.notebook.run(path='/Users/nick_altgelt@bat.com/DIF/v1.0/source/data_hydratation_process'
                         , timeout_seconds=0, arguments={
        'channel_column_name': 'Message_Type',
        'creds_scope_name': 'geolocation',
        'creds_set_name': 'DEV',
        'input_file_path': file_path,
        'user_column_name': 'SenderUserId',
        'post_link_column_name': 'Permalink',
        })
    
def data_preparation(file_path):
    return dbutils.notebook.run(path='/Users/nick_altgelt@bat.com/DIF/v1.0/source/geolocation_data_prep/geolocation_data_prep'
                         , timeout_seconds=0, arguments={
        'input_dataset_path': file_path,
        'post_owner_column_name': 'SenderScreenName',
        'post_social_network_column_name': 'Message_Type',
        'post_url_column_name': 'Permalink',
        'post_text_column_name': 'cleaned_original_text',
        })
    
def hidratation_preparation_geolocation_simple(country):
  
  shape = country[9].shape[0]
  print("\n---------------------Starting Hidratation--------------------\n")
  hidratation_result = inference_hidratation(country[8])
  print(hidratation_result)
  
  print("\n------------------------Prepairing Data----------------------\n")
  prepairing_result = data_preparation(country[8])
  print(prepairing_result)
  
  print("\n---------------------Starting Geolocation--------------------\n")
  result_geolocated = paralllel_geolocation({
                      'fpath': country[8],
                      'experiment_name': country[7],
                      'cities_table': country[5],
                      'input_language': country[2],
                      'table_results': country[3],
                      })
  print(result_geolocated)
  print("\n---------------------------Complete--------------------------\n")

# COMMAND ----------

# data = spark.sql("SELECT * From default.japan_october_2022112_w_author_preds").toPandas()
# print(data['Sentiment_New'].value_counts())
# print(data['Funnel_New'].value_counts())
# csv = data.to_csv("/dbfs/FileStore/shared_uploads/nick_altgelt@bat.com/japan_october_2022112_full.csv",index=False)

# COMMAND ----------

#Functions
def dataframe_to_csv(data, path_to_save, save_name):
  dbutils.fs.mkdirs(path_to_save+"/"+save_name)
  path = "/dbfs"+path_to_save+"/"+save_name+"/"+save_name+"_full"+".csv"
  data.to_csv(path, index=False)
  return path

def csv_to_dataframe(csv_path):
  data = pd.read_csv(csv_path)
  return data

def compute_universe():
  complete_dataframe = spark.sql(f"SELECT {final_table_path}.* FROM {final_table_path} LEFT JOIN {initial_db} ON ({final_table_path}.SN_MSG_ID = {initial_db}.SN_MSG_ID) WHERE {initial_db}.SN_MSG_ID = {final_table_path}.SN_MSG_ID").toPandas()
  complete_dataframe = complete_dataframe.drop_duplicates(subset='SN_MSG_ID', keep="first")
  print("Complete Size without dupicates: ",complete_dataframe.shape)
  file_path = "/dbfs"+path_to_save+"/"+save_name+"/"+save_name+"_universe"+".csv"
  complete_dataframe.to_csv(file_path, index=False)
  print("Path: ",file_path)
  dbutils.notebook.exit(file_path)
  
def create_execution_variables():
  if(is_author == "False"):
    database = "geolocation_weekly"
    path_to_save = "/FileStore/shared_uploads/nick_altgelt@bat.com/geolocation_chunks"
    today = datetime.datetime.now()
    if(manual_month == "False"):
      month = today.strftime('%B').lower()
    else:
      month = dbutils.widgets.get("month")
    country_array = country_prediction.split(",")
    complete_countries = []
    for country in country_array:
      initial_db = f"pei.{country}_full_data_table_{month}"
      save_name = f"{country}_{month}_{today.year}{today.month}{today.day}"
      if (country == "brazil"):
        input_language = "pt"
        cities_table = "20220811_br_clustered_cities.csv"
      elif (country == "global"):
        input_language = "en"
        cities_table = "20220719_us_clustered_cities.csv"
      elif (country == "japan"):
        input_language = "jp"
        cities_table = "20220719_us_clustered_cities.csv"
      elif (country == "germany"):
        input_language = "de"
        cities_table = "20220719_us_clustered_cities.csv"
      elif (country == "france"):
        input_language = "fr"
        cities_table = "20220719_us_clustered_cities.csv"
      elif (country == "mexico" or country == "chile"):
        input_language = "es"
        cities_table = "20220714_cl_clustered_cities.csv"
      elif (country == "russia"):
        input_language = "ru"
        cities_table = "20220719_us_clustered_cities.csv"
      else:
        raise Exception(f"Language {input_language} is not supported")

      table_results = f"geolocated_{month}_{country}"
      final_table_path = f"{database}.{table_results}"

      complete_countries.append((initial_db,country,input_language,table_results, final_table_path, cities_table, path_to_save, save_name))
  else:
    database = "author_weekly"
    path_to_save = "/FileStore/shared_uploads/nick_altgelt@bat.com/author_chunks"
    today = datetime.datetime.now()
    if(manual_month == "False"):
      month = today.strftime('%B').lower()
    else:
      month = dbutils.widgets.get("month")
    country_array = country_prediction.split(",")
    complete_countries = []
    for country in country_array:
      initial_db = f"pei.{country}_full_data_table_{month}"
      save_name = f"{country}_{month}_{today.year}{today.month}{today.day}"
      if (country == "brazil"):
        input_language = "pt"
        cities_table = "20220811_br_clustered_cities.csv"
      elif (country == "global"):
        input_language = "en"
        cities_table = "20220719_us_clustered_cities.csv"
      elif (country == "japan"):
        input_language = "jp"
        cities_table = "20220719_us_clustered_cities.csv"
      elif (country == "germany"):
        input_language = "de"
        cities_table = "20220719_us_clustered_cities.csv"
      elif (country == "france"):
        input_language = "fr"
        cities_table = "20220719_us_clustered_cities.csv"
      elif (country == "mexico" or country == "chile"):
        input_language = "es"
        cities_table = "20220714_cl_clustered_cities.csv"
      elif (country == "russia"):
        input_language = "ru"
        cities_table = "20220719_us_clustered_cities.csv"
      else:
        raise Exception(f"Language {input_language} is not supported")

      table_results = f"author_{month}_{country}"
      final_table_path = f"{database}.{table_results}"

      complete_countries.append((initial_db,country,input_language,table_results, final_table_path, cities_table, path_to_save, save_name))
    return complete_countries

def complement_or_complete_data(table_results, initial_db, final_table_path, path_to_save, save_name):
  if(is_author == "False"):
    temp_table = "geolocation_weekly"
  else:
    temp_table = "author_weekly"
  if not DeltaTable.isDeltaTable(spark, f'/user/hive/warehouse/{temp_table}.db/{table_results}'):
    non_analyzed_dataframe = spark.sql("select * from {}".format(initial_db)).toPandas()
    print("Old size: ",non_analyzed_dataframe.shape)
    non_analyzed_dataframe = non_analyzed_dataframe.drop_duplicates(subset='SN_MSG_ID', keep="first")
    print("New size: ",non_analyzed_dataframe.shape)
    file_path = dataframe_to_csv(non_analyzed_dataframe,path_to_save, save_name)
    print("Path: ",file_path)
  else:
    #MATCH TABLE WITH SAVED TABLE (HIDRATED AND GEOLOCATED)
    non_analyzed_dataframe = spark.sql(f"SELECT {initial_db}.* FROM {initial_db} LEFT JOIN {final_table_path} ON ({initial_db}.SN_MSG_ID = {final_table_path}.SN_MSG_ID) WHERE {final_table_path}.SN_MSG_ID IS NULL").toPandas()
    print("Old size: ",non_analyzed_dataframe.shape)
    non_analyzed_dataframe = non_analyzed_dataframe.drop_duplicates(subset='SN_MSG_ID', keep="first")
    print("New non analized size: ",non_analyzed_dataframe.shape)
    file_path = dataframe_to_csv(non_analyzed_dataframe, path_to_save, save_name)
    print("Path: ",file_path)
  
  return (file_path,non_analyzed_dataframe)

def getting_data():

  print("\n---------------------Execution Variables---------------------\n")
  countries_array = create_execution_variables()
  print(countries_array)
  
  #Get data of all global countries + non analyzed
  print("\n------------------------Getting Data-------------------------\n")
  for x,country in enumerate(countries_array):
    print(f"\ncountry: {country[1]}\n")
    file_path,non_analyzed_dataframe = complement_or_complete_data(country[3], country[0], country[4], country[6], country[7])
    countries_array[x] = countries_array[x] + (file_path,)
    countries_array[x] = countries_array[x] + (non_analyzed_dataframe,)
    countries_array[x] = countries_array[x] + (non_analyzed_dataframe.shape[0],)
  
  #Ordenar de menos records a más records
  countries_array.sort(key=lambda records: records[10])
  
  return countries_array

def send_to_api(data):
  url = "https://edp-middleware.herokuapp.com"
  path = "/create"
  response = requests.post(url = url+path, json = data)
  final = response.json()
  return final

def send_to_api_info(data):
  url = "https://edp-middleware.herokuapp.com"
  path = "/info"
  response = requests.post(url = url+path, json = data)
  final = response.json()
  return final

# COMMAND ----------

# MAGIC %md
# MAGIC ## Obtaining Execution variables

# COMMAND ----------

countries_array = getting_data()

# COMMAND ----------

data = []
if(get_data == "True"):
  if(is_author == "False"):
    for country in countries_array:
      data.append({
        'country':country[1],
        'records':f"Records sin geolocalizar: {country[9].shape[0]} - Columnas de la data : {country[9].shape[1]}",
      })
    print(data)
    send_to_api_info(data)
  else:
    for country in countries_array:
      data.append({
        'country':country[1],
        'records':f"Records sin aplicar author: {country[9].shape[0]} - Columnas de la data : {country[9].shape[1]}",
      })
    print(data)
    send_to_api_info(data)
else:
  if(using_api == "True"):
    if(is_author == "False"):
      #print(countries_array)
      for country in countries_array:
        data.append({
          'country':country[1],
          'records':country[10],
          'fpath': country[8],
          'experiment_name': country[7],
          'cities_table': country[5],
          'input_language': country[2],
          'table_results': country[3],
          'model': "geolocation",
        })
      print(data)
      send_to_api(data)
    else:
      #print(countries_array)
      for country in countries_array:
        data.append({
          'country':country[1],
          'records':country[10],
          'fpath': country[8],
          'experiment_name': country[7],
          'cities_table': country[5],
          'input_language': country[2],
          'table_results': country[3],
          'model': "author",
        })
      print(data)
      send_to_api(data)
  else:
    print("\n------------Hidratation/Preparation/Geolocation--------------\n")
    for country in countries_array:
      start_execution = time.time()
      print(f"\nActual Country: {country[1]} - With {country[10]} Records")
      hidratation_preparation_geolocation_simple(country)
      print(f"Execution Time: {(time.time() - start_execution)/60}")

# COMMAND ----------

dbutils.notebook.exit("success")