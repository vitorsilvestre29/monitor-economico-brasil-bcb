# Databricks notebook source
df_silver_dolar = spark.table("workspace.default.silver_bcb_dolar")

# COMMAND ----------

from pyspark.sql.functions import date_trunc

# COMMAND ----------

df_com_mes = df_silver_dolar.withColumn("mes_referencia", date_trunc("month", df_silver_dolar.data))

# COMMAND ----------

from pyspark.sql.functions import avg, min, max, count

# COMMAND ----------

df_gold_dolar = df_com_mes.groupBy("mes_referencia").agg(avg("valor").alias("dolar_media"), min("valor").alias("dolar_minima"), max("valor").alias("dolar_maxima"), count("valor").alias("qtd_observacoes_dolar"))

# COMMAND ----------

from pyspark.sql.functions import to_date, round

# COMMAND ----------

df_gold_dolar = df_gold_dolar.withColumn("mes_referencia", to_date(df_gold_dolar.mes_referencia)).withColumn("dolar_media", round(df_gold_dolar.dolar_media, 2)).withColumn("dolar_minima", round(df_gold_dolar.dolar_minima, 2)).withColumn("dolar_maxima", round(df_gold_dolar.dolar_maxima, 2))

# COMMAND ----------

df_gold_dolar.write.mode("overwrite").options(overwriteSchema=True).saveAsTable("gold_bcb_dolar_mensal")
spark.table("gold_bcb_dolar_mensal").display()