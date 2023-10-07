# Databricks notebook source
import requests
import json
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from pyspark.sql.types import StructType, StructField, StringType



dbutils.fs.mkdirs("dbfs:/FileStore/tables/Extraction_Data_Sprin/Tokens/")


# COMMAND ----------

display(dbutils.fs.head("dbfs:/FileStore/tables/Extraction_Data_Sprin/Tokens/access_token.json"))

# COMMAND ----------

display(dbutils.fs.head("dbfs:/FileStore/tables/Extraction_Data_Sprin/Tokens/payload.json"))

# COMMAND ----------

class extracao_valores():
    def __init__ (self):
        self.key = 'fnks2xz3k6k8x22vunu399u3'
        self.secret = '5VDfQd8qKCqJtahsRze8PrxChpXPK3NUVnB7JAWXMfByBvSXSN6rmQZV2Btr3Uhw'
        self.redirect_uri = 'https://www.sprinklr.com/pt-br/'
        self.auth_token='637d1e15f29eb122a26b4921'
        self.code = '63867cf7b1afe75617a6e1b0'
        super().__init__()



    def credentials (self):
        with open("/dbfs/FileStore/tables/Extraction_Data_Sprin/Tokens/access_token.json", 'r') as j:
            i = json.load(j)    
            ref_token = (i['access_token'])

        headers = {
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer ' + ref_token,
                    'Key': 'fnks2xz3k6k8x22vunu399u3'
                }    

        with open('/dbfs/FileStore/tables/Extraction_Data_Sprin/Tokens/payload.json', 'r+') as j:
            payload = json.load(j)
        self.headers = headers
        self.payload = payload

    def extraction_values_and_merge(self):
        i = 0
        data = []
        payload = self.payload
        headers = self.headers
        while True:
            print(i)
            payload['page'] = i
            request_url = 'https://api2.sprinklr.com/prod2/api/v2/reports/query'
            request = requests.post(request_url, json=payload, headers=headers, verify=False)
            response = request.text
            obj_json2 = json.loads(response)
            data.append(obj_json2['data']['rows'])
            i = i + 1
            if  120 != len(obj_json2['data']['rows']):
                break
        self.data = data
    
    def create_dataframe(self):
        self.credentials()
        self.extraction_values_and_merge()
        data_list = self.data

        schema = StructType([
            StructField("Data", StringType(), True),
            StructField("Sentimento", StringType(), True),
            StructField("Valor", StringType(), True)
            ])
        

        data = spark.createDataFrame([], schema=schema)
        for i in range (len(data_list)):
            
            for j in (data_list[i]):
                data = data.union(spark.createDataFrame([j], schema=schema))
        self.data = data
    
    def criando_pasta(self):
        self.create_dataframe()
        import datetime
        caminho = f"dbfs:/FileStore/tables/Extraction_Data_Sprin/Data/Bronze/{str(datetime.date.today().year)}/{str(datetime.date.today().month)}/{str(datetime.date.today().day)}"
        dbutils.fs.mkdirs(caminho)
        self.caminho = caminho


    def save_dataframe(self):
        self.criando_pasta()
        self.data.write.format("parquet")\
            .mode("overwrite")\
            .save(self.caminho)
    

        


extract = extracao_valores()
extract.save_dataframe()
    


# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/Extraction_Data_Sprin/Data/2023/10/4")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


