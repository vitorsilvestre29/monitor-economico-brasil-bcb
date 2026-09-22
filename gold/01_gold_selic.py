# Databricks notebook source
df_silver = spark.table("workspace.default.silver_bcb_selic")

# COMMAND ----------

from pyspark.sql.functions import date_trunc

# COMMAND ----------

df_com_mes = df_silver.withColumn("mes_referencia", date_trunc("month", df_silver.data))

# COMMAND ----------

from pyspark.sql.functions import avg, min, max, count

# COMMAND ----------

df_gold = df_com_mes.groupBy("mes_referencia").agg(count("valor").alias("qtd_observacoes_selic"), avg("valor").alias("selic_media"), min("valor").alias("selic_minima"), max("valor").alias("selic_maxima"))

# COMMAND ----------

display(df_gold)

# COMMAND ----------

from pyspark.sql.functions import to_date, round

# COMMAND ----------

df_gold = df_gold.withColumn("mes_referencia", to_date(df_gold.mes_referencia)).withColumn("selic_media", round(df_gold.selic_media, 2)).withColumn("selic_minima", round(df_gold.selic_minima, 2)).withColumn("selic_maxima", round(df_gold.selic_maxima, 2))

# COMMAND ----------

df_gold.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_bcb_selic_mensal")
spark.table("gold_bcb_selic_mensal").display()