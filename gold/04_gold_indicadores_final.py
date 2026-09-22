# Databricks notebook source
from pyspark.sql.functions import when

df_selic = spark.table("workspace.default.gold_bcb_selic_mensal")
df_ipca = spark.table("workspace.default.gold_bcb_ipca_mensal")
df_dolar = spark.table("workspace.default.gold_bcb_dolar_mensal")

# COMMAND ----------

df_indicadores = df_selic.join(df_ipca, "mes_referencia","left")

# COMMAND ----------

df_indicadores = df_indicadores.join(df_dolar, "mes_referencia","left")

# COMMAND ----------

df_indicadores = df_indicadores.withColumn("ipca_disponivel" , when(df_indicadores.ipca_mensal.isNull(), False).otherwise(True))

# COMMAND ----------

display(df_indicadores)

# COMMAND ----------

df_indicadores = df_indicadores.orderBy("mes_referencia")

# COMMAND ----------

linhas_sem_ipca = df_indicadores.filter(df_indicadores.ipca_mensal.isNull()).count()
print(linhas_sem_ipca)

# COMMAND ----------

linhas_sem_ipca = df_indicadores.filter(df_indicadores.ipca_mensal.isNull())
display(linhas_sem_ipca)

# COMMAND ----------

df_indicadores.write.mode("overwrite").options(overwriteSchema="true").saveAsTable("gold_indicadores_economicos_mensal")
spark.table("gold_indicadores_economicos_mensal").display()