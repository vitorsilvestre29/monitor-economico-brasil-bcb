# Databricks notebook source
import requests
from pyspark.sql.functions import current_timestamp, lit
url_ipca = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?formato=json&dataInicial=01/01/2020"

# COMMAND ----------

resposta_ipca = requests.get(url_ipca)

# COMMAND ----------

resposta_ipca.status_code

# COMMAND ----------

dados = resposta_ipca.json()

# COMMAND ----------

dados [:3]

# COMMAND ----------

type(dados[0]['valor'] )

# COMMAND ----------

df_bronze_ipca = spark.createDataFrame(dados)

# COMMAND ----------

df_bronze_ipca = df_bronze_ipca.withColumn('indicador' , lit('IPCA'))
df_bronze_ipca = df_bronze_ipca.withColumn('serie_id' , lit(433))
df_bronze_ipca = df_bronze_ipca.withColumn('fonte' , lit("Banco Central do Brasil - SGS"))
df_bronze_ipca = df_bronze_ipca.withColumn('data_ingestao' , current_timestamp())

# COMMAND ----------

df_bronze_ipca.write.mode("overwrite").option("overwriteSchema" , "True").saveAsTable("bronze_bcb_ipca")
spark.table("bronze_bcb_ipca").display()