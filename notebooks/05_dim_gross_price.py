# Databricks notebook source
# celula 1
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run /Workspace/Users/guz.souz@gmail.com/Utilities
# MAGIC
# MAGIC

# COMMAND ----------

# celula 3
dbutils.widgets.text("catalog", "gana_pati", "Catalog")
dbutils.widgets.text("data_source", "gross_price", "Data Source")

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

# celula 5
df.write \
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- celula 6
# MAGIC SELECT count(*) FROM gana_pati.bronze.gross_price;

# COMMAND ----------

# celula 7: carrega o Bronze pra trabalhar na Silver
df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
df_bronze.printSchema()

# COMMAND ----------

# celula 8: valores distintos de month, antes de normalizar
df_bronze.select("month").distinct().show()

# COMMAND ----------

# celula 9: converte todos os formatos pra data de verdade
date_formats = ["yyyy/MM/dd", "dd/MM/yyyy", "yyyy-MM-dd", "dd-MM-yyyy"]

df_silver = df_bronze.withColumn(
    "month",
    F.coalesce(
        F.try_to_date(F.col("month"), "yyyy/MM/dd"),
        F.try_to_date(F.col("month"), "dd/MM/yyyy"),
        F.try_to_date(F.col("month"), "yyyy-MM-dd"),
        F.try_to_date(F.col("month"), "dd-MM-yyyy")
    )
)

# COMMAND ----------

# celula 10: confirma, agora tudo como data
df_silver.select("month").distinct().show()
df_silver.printSchema()

# COMMAND ----------

# celula 11: ver os valores distintos e "estranhos" de gross_price antes da limpeza
df_silver.select("gross_price").distinct().show(50)

# COMMAND ----------

# celula 12: valida, corrige sinal e substitui valores invalidos por 0
df_silver = df_silver.withColumn(
    "gross_price",
    F.when(
        F.col("gross_price").rlike(r'^-?\d+(\.\d+)?$'),
        F.when(F.col("gross_price").cast("double") < 0, -1 * F.col("gross_price").cast("double"))
         .otherwise(F.col("gross_price").cast("double"))
    ).otherwise(0)
)

# COMMAND ----------

# celula 13: confirma o resultado
df_silver.select("gross_price").distinct().show(50)
df_silver.printSchema()

# COMMAND ----------

# celula 14: junta com products para validar product_id e trazer o product_code
df_products = spark.table(f"{catalog}.{silver_schema}.products")
df_joined = df_silver.join(df_products.select("product_id", "product_code"), on="product_id", how="inner")
df_joined = df_joined.select("product_id", "product_code", "month", "gross_price", "read_timestamp", "file_name", "file_size")

# COMMAND ----------

# celula 15: confirma a contagem
df_joined.count()

# COMMAND ----------

# celula 16: grava a silver de gross_price
df_joined.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- celula 17: confirma a contagem na tabela silver
# MAGIC SELECT count(*) FROM gana_pati.silver.gross_price;

# COMMAND ----------

# celula 18: grava a gold isolada da hastinapur (antes da reconciliacao)
df_gold = df_joined.select("product_code", "month", "gross_price")
df_gold.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{gold_schema}.hn_dim_{data_source}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- celula 19: confirma a contagem
# MAGIC SELECT count(*) FROM gana_pati.gold.hn_dim_gross_price;

# COMMAND ----------

# celula 20: prepara as colunas auxiliares pra reconciliacao
df_gold_price = spark.table(f"{catalog}.{gold_schema}.hn_dim_{data_source}")
df_gold_price = (
    df_gold_price
    .withColumn("year", F.year("month"))
    .withColumn("is_zero", F.when(F.col("gross_price") == 0, 1).otherwise(0))
)
df_gold_price.orderBy("product_code", "month").show(20)

# COMMAND ----------

# celula 21: define a janela e aplica row_number para escolher 1 linha por produto/ano
w = Window.partitionBy("product_code", "year").orderBy(F.col("is_zero"), F.col("month").desc())
df_gold_latest_price = (
    df_gold_price
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") == 1)
)
df_gold_latest_price.orderBy("product_code").show(20)

# COMMAND ----------

# celula 22: confirma a contagem, agora deve ser 18 (1 linha por produto)
df_gold_latest_price.count()

# COMMAND ----------

# celula 23: descobre qual produto nao tem nenhum registro de preco
df_products_all = spark.table(f"{catalog}.{silver_schema}.products").select("product_id", "product_code")
produtos_sem_preco = df_products_all.join(
    df_gold_latest_price.select("product_code").distinct(),
    on="product_code",
    how="left_anti"
)
produtos_sem_preco.show()

# COMMAND ----------

# celula 24: renomeia para price_brl e ajusta o tipo do year
df_final = (
    df_gold_latest_price
    .select("product_code", "year", "gross_price")
    .withColumnRenamed("gross_price", "price_brl")
)
df_final = df_final.withColumn("year", F.col("year").cast("string"))
df_final.printSchema()

# COMMAND ----------

# celula 25: merge no dim_gross_price da ganapati
delta_table = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.dim_gross_price")
delta_table.alias("target").merge(
    source=df_final.alias("source"),
    condition="target.product_code = source.product_code"
).whenMatchedUpdate(
    set={"price_brl": "source.price_brl", "year": "source.year"}
).whenNotMatchedInsert(
    values={"product_code": "source.product_code", "price_brl": "source.price_brl", "year": "source.year"}
).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- celula 26: confirma a contagem final
# MAGIC SELECT count(*) FROM gana_pati.gold.dim_gross_price;