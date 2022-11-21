# Databricks notebook source
import requests
import json

# COMMAND ----------

url = "https://api.telegram.org/bot5586532150:AAHiKSMW8Su6w2suY9jgUdCnlZzBpVtWURg/sendMessage"
dbutils.widgets.text('send_text', '')
send_text = dbutils.widgets.get('send_text')

# COMMAND ----------

print(send_text)

# COMMAND ----------

payload = json.dumps({
    'chat_id': '-1001671035053',
    'text': send_text,
    'parse_mode': 'markdown',
})

headers = {'Content-Type': 'application/json'}
response = requests.request('POST', url, headers=headers, data=payload)

# COMMAND ----------

print(response.text)

# COMMAND ----------

dbutils.notebook.exit(response)
