# Databricks notebook source
import requests 
from pyspark.sql.functions import lit, current_timestamp
url_dolar = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados?formato=json&dataInicial=01/01/2020"

# COMMAND ----------

resposta_dolar = requests.get(url_dolar)

# COMMAND ----------

resposta_dolar.status_code

# COMMAND ----------

dados = resposta_dolar.json()

# COMMAND ----------

dados [:3]

# COMMAND ----------

type(dados[0]['valor'] )

# COMMAND ----------

df_bronze_dolar = spark.createDataFrame(dados)

# COMMAND ----------

df_bronze_dolar = df_bronze_dolar.withColumn("indicador", lit("Dólar"))
df_bronze_dolar = df_bronze_dolar.withColumn("fonte", lit("Banco Central do Brasil - SGS"))
df_bronze_dolar = df_bronze_dolar.withColumn("serie_id", lit(1))
df_bronze_dolar = df_bronze_dolar.withColumn("data_ingestao", current_timestamp())

# COMMAND ----------

df_bronze_dolar.write.mode("overwrite").options(overwriteSchema="true").saveAsTable("bronze_bcb_dolar")
spark.table("bronze_bcb_dolar").display()