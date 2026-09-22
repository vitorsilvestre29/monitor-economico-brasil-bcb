# Databricks notebook source
df_bronze_dolar = spark.table("workspace.default.bronze_bcb_dolar")

# COMMAND ----------

from pyspark.sql.functions import col, to_date

# COMMAND ----------

df_silver_dolar = df_bronze_dolar.withColumn('data', to_date(col('data'), 'dd/MM/yyyy'))

# COMMAND ----------

df_silver_dolar = df_silver_dolar.withColumn('valor', col('valor').cast('double'))

# COMMAND ----------

display(df_silver_dolar)

# COMMAND ----------

total_de_linhas = df_silver_dolar.count()
print(total_de_linhas)

# COMMAND ----------

linhas_sem_data = df_silver_dolar.filter(df_silver_dolar.data.isNull()).count()
print(linhas_sem_data)

# COMMAND ----------

linhas_sem_valor = df_silver_dolar.filter(df_silver_dolar.valor.isNull()).count()
print(linhas_sem_valor)

# COMMAND ----------

datas_distintas = df_silver_dolar.select('data').distinct().count()
print (datas_distintas)

# COMMAND ----------

registros_duplicados = total_de_linhas - datas_distintas
display(registros_duplicados)

# COMMAND ----------

if linhas_sem_data > 0 or linhas_sem_valor > 0 or  registros_duplicados > 0 :
  raise ValueError("Dados inconsistentes")
else:
  print("Dados consistentes")


# COMMAND ----------

df_silver_dolar.write.mode("overwrite").options(overwriteSchema="true").saveAsTable("silver_bcb_dolar")
spark.table("silver_bcb_dolar").display()