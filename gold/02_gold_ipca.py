# Databricks notebook source
df_silver_ipca = spark.table("workspace.default.silver_bcb_ipca")

# COMMAND ----------

from pyspark.sql.functions import round

# COMMAND ----------

df_gold_ipca = df_silver_ipca.select(df_silver_ipca.data.alias('mes_referencia'), round(df_silver_ipca.valor,2).alias('ipca_mensal'))

# COMMAND ----------

display(df_gold_ipca)

# COMMAND ----------

df_gold_ipca.write.mode("overwrite").saveAsTable("gold_bcb_ipca_mensal")
spark.table("gold_bcb_ipca_mensal").display()