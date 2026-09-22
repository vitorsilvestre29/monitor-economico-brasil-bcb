# Databricks notebook source
df_bronze_ipca = spark.table("workspace.default.bronze_bcb_ipca")

# COMMAND ----------

from pyspark.sql.functions import col, to_date

# COMMAND ----------

df_silver_ipca = df_bronze_ipca.withColumn('data', to_date(col('data'), 'dd/MM/yyyy'))

# COMMAND ----------

df_silver_ipca = df_silver_ipca.withColumn('valor', col('valor').cast('double'))

# COMMAND ----------

display(df_silver_ipca)

# COMMAND ----------

total_de_linhas = df_silver_ipca.count()
print(total_de_linhas)

# COMMAND ----------

linhas_sem_data = df_silver_ipca.filter(df_silver_ipca.data.isNull()).count()
print(linhas_sem_data)

# COMMAND ----------

linhas_sem_valor = df_silver_ipca.filter(df_silver_ipca.valor.isNull()).count()
print(linhas_sem_valor)

# COMMAND ----------

datas_distintas = df_silver_ipca.select('data').distinct().count()
print (datas_distintas)

# COMMAND ----------

registros_duplicados = total_de_linhas - datas_distintas
print (registros_duplicados)


# COMMAND ----------

if linhas_sem_data > 0 or linhas_sem_valor > 0 or registros_duplicados > 0:
  raise ValueError("Dados inconsistentes")
else :
  print("Dados consistentes")

# COMMAND ----------

df_silver_ipca.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_bcb_ipca")
spark.table("silver_bcb_ipca").display()