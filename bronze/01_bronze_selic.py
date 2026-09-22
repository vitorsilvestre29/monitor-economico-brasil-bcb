# Databricks notebook source
import requests 
from pyspark.sql.functions import lit,current_timestamp

# COMMAND ----------

url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=json&dataInicial=01/01/2020"

# COMMAND ----------

resposta = requests.get(url)

# COMMAND ----------

resposta.status_code

# COMMAND ----------

dados = resposta.json()

# COMMAND ----------

dados [:3]

# COMMAND ----------

type(dados[0]['valor'] )

# COMMAND ----------

df_bronze = spark.createDataFrame(dados)

# COMMAND ----------

df_bronze = df_bronze.withColumn("indicador", lit("Selic"))
df_bronze = df_bronze.withColumn("serie_id", lit(432))
df_bronze = df_bronze.withColumn("data_ingestao", current_timestamp())
df_bronze = df_bronze.withColumn("fonte", lit("Banco Central do Brasil - SGS"))

# COMMAND ----------

display(df_bronze)

# COMMAND ----------

df_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_bcb_selic")

# COMMAND ----------

spark.table("bronze_bcb_selic").display()