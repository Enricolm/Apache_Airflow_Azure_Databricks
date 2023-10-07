# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql.functions import from_unixtime,date_trunc




dbutils.fs.ls("dbfs:/FileStore/tables/Extraction_Data_Sprin/Data/Bronze/")

# COMMAND ----------

dados_with_out_filter = spark.read.parquet("dbfs:/FileStore/tables/Extraction_Data_Sprin/Data/Bronze/*/*/*/")
                                

# COMMAND ----------

display(dados_with_out_filter.show())

# COMMAND ----------

data_transform_date = dados_with_out_filter.withColumn("Data", (f.col("Data") / 1000 ).cast("timestamp"))


data_transform_date = data_transform_date.withColumn("Data", f.col("Data").cast("date"))

# COMMAND ----------

data_transform_date.select("Data").where(f.col("Data") == "2023-07-16").show()

# COMMAND ----------



# COMMAND ----------

data_transform_date.show()

# COMMAND ----------

data_transform_date.filter(data_transform_date.Sentimento.like("Positive")).show()

# COMMAND ----------

data_filter_per_day = data_transform_date.groupBy("Data")\
                    .pivot("Sentimento")\
                    .agg(f.first("Valor"))

# COMMAND ----------

data_filter_per_day.show()

# COMMAND ----------

data_filter_per_mounth = data_transform_date\
    .groupBy((f.date_format("Data", "yyyy-MM")).alias("Data"))\
    .pivot("Sentimento")\
    .agg(f.round(f.avg("Valor"), 2))

# COMMAND ----------

data_filter_per_mounth.show()

# COMMAND ----------

data_filter_per_day.write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep",";")\
    .option("header", "true")\
    .save("dbfs:/FileStore/tables/Extraction_Data_Sprin/Data/Silver/Per_Day")
    
    

# COMMAND ----------

data_filter_per_mounth.write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep",";")\
    .option("header", "true")\
    .save("dbfs:/FileStore/tables/Extraction_Data_Sprin/Data/Silver/Per_Mounth")
    
    

# COMMAND ----------


