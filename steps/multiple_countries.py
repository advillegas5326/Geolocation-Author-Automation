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
import pandas as pd
import datetime
from delta.tables import *
import requests

# COMMAND ----------

# MAGIC %md ##Widgets

# COMMAND ----------

dbutils.widgets.text("country_prediction", '')
country_prediction = dbutils.widgets.get("country_prediction")

dbutils.widgets.text("month", '')
month = dbutils.widgets.get("month")

dbutils.widgets.text("manual_month", 'False')
manual_month = dbutils.widgets.get("manual_month")

dbutils.widgets.text("get_data", 'False')
get_data = dbutils.widgets.get("get_data")

dbutils.widgets.text("is_author", '')
is_author = dbutils.widgets.get("is_author")

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

            complete_countries.append((initial_db, country, input_language, table_results,
                                      final_table_path, cities_table, path_to_save, save_name))
        return complete_countries
    elif(is_author == "True"):
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

            complete_countries.append((initial_db, country, input_language, table_results,
                                      final_table_path, cities_table, path_to_save, save_name))
        return complete_countries
    else:

        country_array = country_prediction.split(",")
        complete_countries = []
        for country in country_array:

            today = datetime.datetime.now()
            if(manual_month == "False"):
                month = today.strftime('%B').lower()
            else:
                month = dbutils.widgets.get("month")

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

            # table_results = f"author_{month}_{country}"
            # final_table_path = f"{database}.{table_results}"

            complete_countries.append([(initial_db, country, input_language, f"author_{month}_{country}",
                                      f"author_weekly.author_{month}_{country}", cities_table, "/FileStore/shared_uploads/nick_altgelt@bat.com/author_chunks", save_name),
                                       (initial_db, country, input_language, f"geolocated_{month}_{country}",
                                       f"geolocation_weekly.geolocated_{month}_{country}", cities_table, "/FileStore/shared_uploads/nick_altgelt@bat.com/geolocation_chunks", save_name)])
        return complete_countries


def complement_or_complete_data(temp_table, table_results, initial_db, final_table_path, path_to_save, save_name):

    if not DeltaTable.isDeltaTable(spark, f'/user/hive/warehouse/{temp_table}.db/{table_results}'):

        # Select all dataframe
        non_analyzed_dataframe = spark.sql(
            "select * from {}".format(initial_db)).toPandas()
        print("Old size: ", non_analyzed_dataframe.shape)

        # Drop duplicates
        non_analyzed_dataframe = non_analyzed_dataframe.drop_duplicates(
            subset='SN_MSG_ID', keep="first")
        print("New size: ", non_analyzed_dataframe.shape)

        # Check column names of data - channel and user
        if 'Channel' in non_analyzed_dataframe.columns:
            non_analyzed_dataframe.columns.rename(
                columns={'Channel': 'Message_Type'}, inplace=True)

        if 'user_id' in non_analyzed_dataframe.columns:
            non_analyzed_dataframe.columns.rename(
                columns={'user_id': 'SenderUserId'}, inplace=True)

        # Save new dataframe
        file_path = dataframe_to_csv(
            non_analyzed_dataframe, path_to_save, save_name)
        print("Path: ", file_path)
    else:

        # Select all dataframe
        non_analyzed_dataframe = spark.sql(
            f"SELECT {initial_db}.* FROM {initial_db} LEFT JOIN {final_table_path} ON ({initial_db}.SN_MSG_ID = {final_table_path}.SN_MSG_ID) WHERE {final_table_path}.SN_MSG_ID IS NULL").toPandas()
        print("Old size: ", non_analyzed_dataframe.shape)

        # Drop duplicates
        non_analyzed_dataframe = non_analyzed_dataframe.drop_duplicates(
            subset='SN_MSG_ID', keep="first")
        print("New non analized size: ", non_analyzed_dataframe.shape)

        # Check column names of data - channel and user
        if 'Channel' in non_analyzed_dataframe.columns:
            non_analyzed_dataframe.columns.rename(
                columns={'Channel': 'Message_Type'}, inplace=True)

        if 'user_id' in non_analyzed_dataframe.columns:
            non_analyzed_dataframe.columns.rename(
                columns={'user_id': 'SenderUserId'}, inplace=True)

        # Save new dataframe to csv
        file_path = dataframe_to_csv(
            non_analyzed_dataframe, path_to_save, save_name)
        print("Path: ", file_path)

    return (file_path, non_analyzed_dataframe)


def getting_data():

    print("\n---------------------Execution Variables---------------------\n")
    countries_array = create_execution_variables()
    print(countries_array)

    # Get data of all global countries + non analyzed
    print("\n------------------------Getting Data-------------------------\n")
    if(is_author == "False"):

        temp_table = "geolocation_weekly"
        for x, country in enumerate(countries_array):
            print(f"\ncountry: {country[1]}\n")
            file_path, non_analyzed_dataframe = complement_or_complete_data(temp_table,
                                                                            country[3], country[0], country[4], country[6], country[7])
            countries_array[x] = countries_array[x] + (file_path,)
            countries_array[x] = countries_array[x] + (non_analyzed_dataframe,)
            countries_array[x] = countries_array[x] + \
                (non_analyzed_dataframe.shape[0],)
        # Ordenar de menos records a más records
        countries_array.sort(key=lambda records: records[10])

        return countries_array

    elif(is_author == "True"):

        temp_table = "author_weekly"
        for x, country in enumerate(countries_array):
            print(f"\ncountry: {country[1]}\n")
            file_path, non_analyzed_dataframe = complement_or_complete_data(temp_table,
                                                                            country[3], country[0], country[4], country[6], country[7])
            countries_array[x] = countries_array[x] + (file_path,)
            countries_array[x] = countries_array[x] + (non_analyzed_dataframe,)
            countries_array[x] = countries_array[x] + \
                (non_analyzed_dataframe.shape[0],)
        # Ordenar de menos records a más records
        countries_array.sort(key=lambda records: records[10])

        return countries_array

    else:

        for x, country in enumerate(countries_array):
            print("\n------------------------------------------------")
            print(f"\nCOUNTRY: {country[0][1]}\n")
            print("AUTHOR\n")

            file_path, non_analyzed_dataframe = complement_or_complete_data("author_weekly",
                                                                            country[0][3], country[0][0], country[0][4], country[0][6], country[0][7])

            countries_array[x][0] = countries_array[x][0] + (file_path,)
            countries_array[x][0] = countries_array[x][0] + \
                (non_analyzed_dataframe,)
            countries_array[x][0] = countries_array[x][0] + \
                (non_analyzed_dataframe.shape[0],)

            print("\nGEOLOCATION\n")

            file_path, non_analyzed_dataframe = complement_or_complete_data("geolocation_weekly",
                                                                            country[1][3], country[1][0], country[1][4], country[1][6], country[1][7])
            countries_array[x][1] = countries_array[x][1] + (file_path,)
            countries_array[x][1] = countries_array[x][1] + \
                (non_analyzed_dataframe,)
            countries_array[x][1] = countries_array[x][1] + \
                (non_analyzed_dataframe.shape[0],)

        # Ordenar de menos records a más records
        countries_array.sort(key=lambda records: records[0][10])

        return countries_array


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


def send_telegram_error(text):
    dbutils.notebook.run(path='/Repos/nick_altgelt@bat.com/Geolocation-Author-Automation/steps/utils/telegram_live_notifications', timeout_seconds=0, arguments={
        'send_text': f"Sucedió un problema dentro de la ejecución con el siguiente error: {text}",
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Obtaining Execution variables

# COMMAND ----------


try:
    countries_array = getting_data()
except Exception as e:
    print(e)
    send_telegram_error(e)


# COMMAND ----------

data = []
if(get_data == "True"):
    if(is_author == "False"):
        for country in countries_array:
            data.append({
                'country': country[1],
                'records': f"Records sin geolocalizar: {country[9].shape[0]} - Columnas de la data : {country[9].shape[1]}",
            })
        print(data)
        send_to_api_info(data)
    elif(is_author == "True"):
        for country in countries_array:
            data.append({
                'country': country[1],
                'records': f"Records sin aplicar author: {country[9].shape[0]} - Columnas de la data : {country[9].shape[1]}",
            })
        print(data)
        send_to_api_info(data)
    else:
        for country in countries_array:
            model = country[0][3].split('_')
            data.append({
                'country': country[0][1],
                'records': f"Records sin aplicar {model[0]}: {country[0][9].shape[0]} - Columnas de la data : {country[0][9].shape[1]}",
            })
            model = country[1][3].split('_')
            data.append({
                'country': country[1][1],
                'records': f"Records sin aplicar {model[0]}: {country[1][9].shape[0]} - Columnas de la data : {country[1][9].shape[1]}",
            })
        print(data)
        send_to_api_info(data)
else:
    if(is_author == "False"):

        for country in countries_array:

            model = country[3].split('_')
            if model[0] == "geolocated":
                model[0] = "geolocation"

            data.append({
                'country': country[1],
                'records': country[10],
                'geolocation_fpath': country[8],
                'experiment_name': country[7],
                'cities_table': country[5],
                'input_language': country[2],
                'geolocation_table_results': country[3],
                'model': model[0],
            })
        send_to_api(data)

    elif(is_author == "True"):

        for country in countries_array:

            model = country[3].split('_')
            if model[0] == "geolocated":
                model[0] = "geolocation"

            data.append({
                'country': country[1],
                'records': country[10],
                'author_fpath': country[8],
                'experiment_name': country[7],
                'cities_table': country[5],
                'input_language': country[2],
                'author_table_results': country[3],
                'model': model[0],
            })
        send_to_api(data)
    else:

        for country in countries_array:

            model = country[0][3].split('_')
            if model == "geolocated":
                model = "geolocation"

            data.append({
                'country': country[0][1],
                'geolocation_records': country[1][10],
                'author_records': country[0][10],
                'geolocation_fpath': country[1][8],
                'author_fpath': country[0][8],
                'experiment_name': country[1][7],
                'cities_table': country[1][5],
                'input_language': country[0][2],
                'geolocation_table_results': country[1][3],
                'author_table_results': country[0][3],
            })
        send_to_api(data)

# COMMAND ----------

final_frame = pd.DataFrame(data)
display(data)

# COMMAND ----------

dbutils.notebook.exit("Done")
