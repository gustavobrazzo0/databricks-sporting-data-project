# Databricks notebook source
# celula 1
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC
# MAGIC %run /Workspace/Users/guz.souz@gmail.com/Utilities
# MAGIC
# MAGIC

# COMMAND ----------

# celula 3
dbutils.widgets.text("catalog", "gana_pati", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f"/Volumes/gana_pati/raw/source_files/full_load/{data_source}/*.csv"
print(base_path)

# COMMAND ----------

# celula 4
df = (
    spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(base_path)
        .withColumn("read_timestamp", F.current_timestamp())
        .select("*", "_metadata.file_name", "_metadata.file_size")
)

df.printSchema()
display(df.limit(10))

# COMMAND ----------

df.write \
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM gana_pati.bronze.customers;

# COMMAND ----------

# celula 7: carrega o Bronze pra trabalhar na Silver
df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
df_bronze.printSchema()
display(df_bronze)

# COMMAND ----------

# celula 8: verifica quais customer_id aparecem mais de uma vez
df_duplicates = df_bronze.groupBy("customer_id").count().filter(F.col("count") > 1)
display(df_duplicates)

# COMMAND ----------

# celula 9: remove as duplicatas
print("Linhas antes de remover duplicatas: ", df_bronze.count())
df_silver = df_bronze.dropDuplicates(["customer_id"])
print("Linhas depois de remover duplicatas: ", df_silver.count())

# COMMAND ----------

# celula 10: verifica nomes de cliente com espaco sobrando no inicio/fim
display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)

# COMMAND ----------

# celula 11: remove esses espacos
df_silver = df_silver.withColumn(
    "customer_name",
    F.trim(F.col("customer_name"))
)

# COMMAND ----------

# celula 12: ve todos os valores distintos de cidade, antes de corrigir
df_silver.select("city").distinct().show()

# COMMAND ----------

# celula 13: dicionario de typo -> nome correto, e aplica a correcao
city_mapping = {
    "Sao Paulo": "São Paulo",
    "São Pauloo": "São Paulo",

    "Rio de Janeiroo": "Rio de Janeiro",
    "Rio de Janiro": "Rio de Janeiro",

    "BeloHorizonte": "Belo Horizonte",
    "Belo Horiznote": "Belo Horizonte",
    "BeloHorizontee": "Belo Horizonte",
}

allowed = ["São Paulo", "Rio de Janeiro", "Belo Horizonte"]

df_silver = (
    df_silver
    .replace(city_mapping, subset=["city"])
    .withColumn(
        "city",
        F.when(F.col("city").isNull(), None)
         .when(F.col("city").isin(allowed), F.col("city"))
         .otherwise(None)
    )
)

# COMMAND ----------

# celula 14: confirma que so restaram os 3 nomes corretos (e nulo)
df_silver.select("city").distinct().show()

# COMMAND ----------

# celula 15: nomes distintos, antes de corrigir capitalizacao
df_silver.select("customer_name").distinct().show()

# COMMAND ----------

# celula 16: corrige capitalizacao
df_silver = df_silver.withColumn(
    "customer_name",
    F.when(F.col("customer_name").isNull(), None)
     .otherwise(F.initcap("customer_name"))
)

# COMMAND ----------

# celula 17: confirma
df_silver.select("customer_name").distinct().show()

# COMMAND ----------

# celula 18: mostra as linhas com cidade nula
df_silver.filter(F.col("city").isNull()).show(truncate=False)

# COMMAND ----------

# celula 19: correcao confirmada pelo time de negocio, por customer_id
customer_city_fix = {
    789403: "Belo Horizonte",
    789420: "São Paulo",
    789521: "Rio de Janeiro",
    789603: "Rio de Janeiro",
}

df_fix = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()],
    ["customer_id", "fixed_city"]
)

display(df_fix)

# COMMAND ----------

# celula 20: aplica a correcao via join
df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left")
    .withColumn(
        "city",
        F.coalesce("city", "fixed_city")
    )
    .drop("fixed_city")
)

# COMMAND ----------

# celula 21: confirma que nao restou nenhuma cidade nula
df_silver.filter(F.col("city").isNull()).show(truncate=False)

# COMMAND ----------

# celula 22: converte customer_id pra texto
df_silver = df_silver.withColumn("customer_id", F.col("customer_id").cast("string"))
df_silver.printSchema()

# COMMAND ----------

# celula 23: padroniza colunas pro mesmo formato da tabela da Ganapati
df_silver = (
    df_silver
    .withColumn(
        "customer",
        F.concat_ws("-", "customer_name", F.coalesce(F.col("city"), F.lit("Unknown")))
    )
    .withColumn("market", F.lit("Brasil"))
    .withColumn("platform", F.lit("Hastinapur Nutrition"))
    .withColumn("channel", F.lit("Acquisition"))
)

display(df_silver.limit(5))

# COMMAND ----------

# celula 24: grava a tabela Silver
df_silver.write \
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- celula 25
# MAGIC SELECT count(*) FROM gana_pati.silver.customers;

# COMMAND ----------

# celula 26: recarrega a Silver e seleciona so as colunas finais
df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")

df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

display(df_gold.limit(5))

# COMMAND ----------

# celula 27: grava a tabela Gold da Hastinapur Nutrition, isolada
df_gold.write \
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.hn_dim_{data_source}")

# COMMAND ----------

# celula 28: junta com a tabela dim_customers da Ganapati
delta_table = DeltaTable.forName(spark, "gana_pati.gold.dim_customers")

df_child_customers = spark.table("gana_pati.gold.hn_dim_customers").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- celula 29: confirma o total depois do merge
# MAGIC SELECT count(*) FROM gana_pati.gold.dim_customers;