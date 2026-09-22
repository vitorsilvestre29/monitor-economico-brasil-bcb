# Databricks notebook source
df_bronze = spark.table("workspace.default.bronze_bcb_selic")

# COMMAND ----------

from pyspark.sql.functions import col, to_date

# COMMAND ----------

df_silver = df_bronze.withColumn('data', to_date(col('data'), 'dd/MM/yyyy'))

# COMMAND ----------

df_silver = df_silver.withColumn('valor', col('valor').cast('double'))

# COMMAND ----------

display(df_silver)

# COMMAND ----------

count = df_silver.count()
print(count)

# COMMAND ----------

linhas_sem_data = df_silver.filter(df_silver.data.isNull()).count()
print(linhas_sem_data)

# COMMAND ----------

linhas_sem_valor = df_silver.filter(df_silver.valor.isNull()).count()
print(linhas_sem_valor)

# COMMAND ----------

datas_distintas = df_silver.select('data').distinct().count()
print (datas_distintas)

# COMMAND ----------

total_linhas = df_silver.count()

datas_distintas = df_silver.select('data').distinct().count()

registros_duplicados = total_linhas - datas_distintas

print(registros_duplicados)

# COMMAND ----------

if linhas_sem_data > 0 or linhas_sem_valor > 0 or registros_duplicados > 0 :
  raise ValueError("Dados inconsistentes")
else:
  print ("Dataframe Silver pronto para o próximo notebook")

# COMMAND ----------

df_silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_bcb_selic")
spark.table("silver_bcb_selic").display()