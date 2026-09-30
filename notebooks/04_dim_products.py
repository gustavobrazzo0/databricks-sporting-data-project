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
dbutils.widgets.text("data_source", "products", "Data Source")

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
display(df.limit(20))

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
# MAGIC SELECT count(*) FROM gana_pati.bronze.products;

# COMMAND ----------

# celula 7: carrega o Bronze pra trabalhar na Silver
df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
df_bronze.printSchema()
display(df_bronze)

# COMMAND ----------

# celula 8: remove duplicatas por product_id
print("Linhas antes de remover duplicatas: ", df_bronze.count())
df_silver = df_bronze.dropDuplicates(["product_id"])
print("Linhas depois de remover duplicatas: ", df_silver.count())

# COMMAND ----------

# celula 9: categorias distintas, antes de corrigir
df_silver.select("category").distinct().show()

# COMMAND ----------

# celula 10: corrige capitalizacao
df_silver = df_silver.withColumn(
    "category",
    F.when(F.col("category").isNull(), None)
     .otherwise(F.initcap("category"))
)

# COMMAND ----------

# celula 11: confirma
df_silver.select("category").distinct().show()

# COMMAND ----------

# celula 12: nomes de produto antes de corrigir o typo
df_silver.select("product_name").distinct().show(truncate=False)

# COMMAND ----------

# celula 13: corrige "Protien" -> "Protein", em product_name e category
df_silver = (
    df_silver
    .withColumn(
        "product_name",
        F.regexp_replace(F.col("product_name"), "(?i)Protien", "Protein")
    )
    .withColumn(
        "category",
        F.regexp_replace(F.col("category"), "(?i)Protien", "Protein")
    )
)

# COMMAND ----------

# celula 14: confirma
df_silver.select("product_name", "category").distinct().show(truncate=False)

# COMMAND ----------

# celula 15: adiciona a divisao, baseado na categoria
df_silver = df_silver.withColumn(
    "division",
    F.when(F.col("category") == "Energy Bars", "Nutrition Bars")
     .when(F.col("category") == "Protein Bars", "Nutrition Bars")
     .when(F.col("category") == "Granola & Cereals", "Breakfast Foods")
     .when(F.col("category") == "Recovery Dairy", "Dairy & Recovery")
     .when(F.col("category") == "Healthy Snacks", "Healthy Snacks")
     .when(F.col("category") == "Electrolyte Mix", "Hydration & Electrolytes")
     .otherwise("Other")
)

# COMMAND ----------

# celula 16: extrai a variante/peso do nome do produto
df_silver = df_silver.withColumn(
    "variant",
    F.regexp_extract(F.col("product_name"), r"\((.*?)\)", 1)
)

display(df_silver.select("product_name", "category", "division", "variant"))

# COMMAND ----------

# celula 17: gera product_code (hash) e trata product_id invalido
df_silver = (
    df_silver
    .withColumn(
        "product_code",
        F.sha2(F.col("product_name").cast("string"), 256)
    )
    .withColumn(
        "product_id",
        F.when(
            F.col("product_id").cast("string").rlike("^[0-9]+$"),
            F.col("product_id").cast("string")
        ).otherwise(F.lit(999999).cast("string"))
    )
    .withColumnRenamed("product_name", "product")
)

display(df_silver.select("product_code", "product_id", "product"))

# COMMAND ----------

# celula 18: seleciona as colunas finais, na ordem
df_silver = df_silver.select("product_code", "division", "category", "product", "variant", "product_id", "read_timestamp", "file_name", "file_size")

display(df_silver)

# COMMAND ----------

# celula 19: grava a tabela Silver
df_silver.write \
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- celula 20: confirma
# MAGIC SELECT count(*) FROM gana_pati.silver.products;

# COMMAND ----------

# celula 21: recarrega a Silver e seleciona as colunas finais da Gold isolada
df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")

df_gold = df_silver.select("product_code", "product_id", "division", "category", "product", "variant")

display(df_gold)

# COMMAND ----------

# celula 22: grava a Gold isolada da Hastinapur Nutrition
df_gold.write \
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.hn_dim_{data_source}")

# COMMAND ----------

# celula 23: junta com a tabela dim_products da Ganapati
delta_table = DeltaTable.forName(spark, "gana_pati.gold.dim_products")

df_child_products = spark.sql("SELECT product_code, division, category, product, variant FROM gana_pati.gold.hn_dim_products;")

delta_table.alias("target").merge(
    source=df_child_products.alias("source"),
    condition="target.product_code = source.product_code"
).whenMatchedUpdate(
    set={
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }
).whenNotMatchedInsert(
    values={
        "product_code": "source.product_code",
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }
).execute()

# COMMAND ----------

# celula 23: junta com a tabela dim_products da Ganapati
delta_table = DeltaTable.forName(spark, "gana_pati.gold.dim_products")

df_child_products = spark.sql("SELECT product_code, division, category, product, variant FROM gana_pati.gold.hn_dim_products;")

delta_table.alias("target").merge(
    source=df_child_products.alias("source"),
    condition="target.product_code = source.product_code"
).whenMatchedUpdate(
    set={
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }
).whenNotMatchedInsert(
    values={
        "product_code": "source.product_code",
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }
).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- celula 24: confirma o total depois do merge
# MAGIC SELECT count(*) FROM gana_pati.gold.dim_products;