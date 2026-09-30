# Databricks notebook source
# celula 1
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/guz.souz@gmail.com/Utilities
# MAGIC
# MAGIC

# COMMAND ----------

# celula 3
dbutils.widgets.text("catalog", "gana_pati", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f"/Volumes/{catalog}/raw/source_files/full_load/{data_source}"
landing_path = f"{base_path}/landing"
processed_path = f"{base_path}/processed"
print("Landing:", landing_path)
print("Processed:", processed_path)

bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.hn_fact_{data_source}"

# COMMAND ----------

# celula 4
df = (
    spark.read.options(header=True, inferSchema=True).csv(f"{landing_path}/*.csv")
        .withColumn("read_timestamp", F.current_timestamp())
        .select("*", "_metadata.file_name", "_metadata.file_size")
)
print("Total de linhas:", df.count())
df.show(5)

# COMMAND ----------

# celula 5
df.write \
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("append") \
 .saveAsTable(bronze_table)

# COMMAND ----------

# celula 6: move os arquivos processados
files = dbutils.fs.ls(landing_path)
for file_info in files:
    dbutils.fs.mv(file_info.path, f"{processed_path}/{file_info.name}", True)

# COMMAND ----------

# celula 7: carrega o Bronze pra trabalhar na Silver
df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
df_bronze.printSchema()
display(df_bronze)

# COMMAND ----------

# celula 8: mantem so linhas com order_qty preenchido
df_silver = df_bronze.filter(F.col("order_qty").isNotNull())

# celula 9: customer_id, se nao for so digitos, cai no fallback 999999
df_silver = df_silver.withColumn(
    "customer_id",
    F.when(F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id"))
     .otherwise("999999")
     .cast("string")
)

# COMMAND ----------

# celula 10: remove o dia da semana, independente do idioma
df_silver = df_silver.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[^,]+,\s*", "")
)

# COMMAND ----------

# celula 11: confirma o que sobrou
df_silver.select("order_placement_date").distinct().show(30, truncate=False)

# COMMAND ----------

# celula 12: extrai dia, mes por extenso e ano do formato "01 de julho de 2025"
padrao_data_extenso = r'^(\d{1,2}) de ([a-zçãá]+) de (\d{4})$'

df_silver = df_silver.withColumn("dia_extenso", F.regexp_extract(F.col("order_placement_date"), padrao_data_extenso, 1))
df_silver = df_silver.withColumn("mes_extenso", F.regexp_extract(F.col("order_placement_date"), padrao_data_extenso, 2))
df_silver = df_silver.withColumn("ano_extenso", F.regexp_extract(F.col("order_placement_date"), padrao_data_extenso, 3))

# COMMAND ----------

# celula 13: mapeia o nome do mes pro numero correspondente
mapa_meses = {
    "janeiro": "01", "fevereiro": "02", "março": "03", "abril": "04",
    "maio": "05", "junho": "06", "julho": "07", "agosto": "08",
    "setembro": "09", "outubro": "10", "novembro": "11", "dezembro": "12"
}
df_silver = df_silver.replace(mapa_meses, subset=["mes_extenso"])

# COMMAND ----------

# celula 14: remonta a data em formato numerico yyyy-MM-dd
df_silver = df_silver.withColumn(
    "data_reconstruida",
    F.when(
        F.col("dia_extenso") != "",
        F.concat_ws("-", F.col("ano_extenso"), F.col("mes_extenso"), F.lpad(F.col("dia_extenso"), 2, "0"))
    )
)

# COMMAND ----------

# celula 15: coalesce final, juntando os formatos numericos com o reconstruido
df_silver = df_silver.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date(F.col("order_placement_date"), "yyyy/MM/dd"),
        F.try_to_date(F.col("order_placement_date"), "dd-MM-yyyy"),
        F.try_to_date(F.col("order_placement_date"), "dd/MM/yyyy"),
        F.try_to_date(F.col("data_reconstruida"), "yyyy-MM-dd")
    )
)

# COMMAND ----------

# celula 16: confirma que nao sobrou nenhuma data nula, e limpa colunas auxiliares
print("Linhas com data nula:", df_silver.filter(F.col("order_placement_date").isNull()).count())

df_silver = df_silver.drop("dia_extenso", "mes_extenso", "ano_extenso", "data_reconstruida")
df_silver.printSchema()

# COMMAND ----------

# celula 17: remove duplicatas exatas e converte product_id pra string
df_silver = df_silver.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])
df_silver = df_silver.withColumn("product_id", F.col("product_id").cast("string"))

# COMMAND ----------

# celula 18: confere o intervalo de datas depois do parse
df_silver.agg(
    F.min("order_placement_date").alias("data_minima"),
    F.max("order_placement_date").alias("data_maxima")
).show()

# COMMAND ----------

# celula 19: junta com products pra trazer o product_code (mesmo padrao de integridade referencial do gross_price)
df_products = spark.table(f"{catalog}.{silver_schema}.products")
df_joined = df_silver.join(df_products, on="product_id", how="inner").select(df_silver["*"], df_products["product_code"])
df_joined.show(5)
df_joined.count()

# COMMAND ----------

# celula 20: grava a silver, criando na primeira vez (ainda nao existe)
if not spark.catalog.tableExists(silver_table):
    df_joined.write.format("delta") \
        .option("delta.enableChangeDataFeed", "true") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .saveAsTable(silver_table)
else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("silver").merge(
        df_joined.alias("bronze"),
        "silver.order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_id = bronze.customer_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- celula 21: confirma a contagem
# MAGIC SELECT count(*) FROM gana_pati.silver.orders;

# COMMAND ----------

# celula 22: grava a gold isolada da hastinapur, ainda em grao diario
df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {silver_table};")

if not spark.catalog.tableExists(gold_table):
    df_gold.write.format("delta") \
        .option("delta.enableChangeDataFeed", "true") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("source").merge(
        df_gold.alias("gold"),
        "source.date = gold.date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- celula 23: confirma
# MAGIC SELECT count(*) FROM gana_pati.gold.hn_fact_orders;

# COMMAND ----------

# celula 24: recarrega so as colunas que vao pra reconciliacao
df_child = spark.sql(f"SELECT date, product_code, customer_code, sold_quantity FROM {gold_table}")
df_child.count()

# COMMAND ----------

# celula 25: reconcilia grao diario (hastinapur) para grao mensal (ganapati)
df_monthly = (
    df_child
    .withColumn("month_start", F.trunc("date", "MM"))
    .groupBy("month_start", "product_code", "customer_code")
    .agg(F.sum("sold_quantity").alias("sold_quantity"))
    .withColumnRenamed("month_start", "date")
)
df_monthly.show(5, truncate=False)
df_monthly.count()

# COMMAND ----------

# celula 26: merge final com a gold da ganapati
gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
gold_parent_delta.alias("parent_gold").merge(
    df_monthly.alias("child_gold"),
    "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- celula 27: confirma a contagem final
# MAGIC SELECT count(*) FROM gana_pati.gold.fact_orders;