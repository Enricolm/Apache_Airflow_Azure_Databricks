# Databricks notebook source
import requests
import json

# COMMAND ----------

key = 'fnks2xz3k6k8x22vunu399u3'
secret = '5VDfQd8qKCqJtahsRze8PrxChpXPK3NUVnB7JAWXMfByBvSXSN6rmQZV2Btr3Uhw'
redirect_uri = 'https://www.sprinklr.com/pt-br/'
auth_token='637d1e15f29eb122a26b4921'
code = '63867cf7b1afe75617a6e1b0'

# %%
def credentials ():
    with open("/dbfs/FileStore/tables/Tokens/access_token.json", 'r') as j:
        i = json.load(j)    
        ref_token = (i['access_token'])
        print(i)
        
    headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + ref_token,
                'Key': 'fnks2xz3k6k8x22vunu399u3'
            }    


    with open('/dbfs/FileStore/tables/Tokens/payload.json', 'r+') as j:
        payload = json.load(j)
    return ref_token, headers,payload

Data = []
Sentimento = []
Valor = []

i = 0
# while True:
#     payload['page'] = i
#     request_url = 'https://api2.sprinklr.com/prod2/api/v2/reports/query'
#     request = requests.post(request_url, json=payload, headers=headers, verify=False)
#     response = request.text
#     obj_json2 = json.loads(response)
#     print(obj_json2['data']['rows'])
#     spark.createDataFrame()


# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/tables/Tokens/access_token.json")

# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/tables/Tokens/payload.json")

# COMMAND ----------

ref_token,headers,payload = credentials()

# COMMAND ----------


