# Databricks notebook source
# celula 1
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/guz.souz@gmail.com/Utilities
# MAGIC

# COMMAND ----------

# celula 3
dbutils.widgets.text("catalog", "gana_pati", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")
catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

incremental_path = f"/Volumes/{catalog}/raw/source_files/incremental_load/{data_source}"
arquivo_do_dia = f"{incremental_path}/orders_2025_12_01.csv"

bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.hn_fact_{data_source}"
staging_bronze_table = f"{catalog}.{bronze_schema}.staging_{data_source}"
staging_silver_table = f"{catalog}.{silver_schema}.staging_{data_source}"

# COMMAND ----------

# celula 4: bronze so do dia
df = (
    spark.read.options(header=True, inferSchema=True).csv(arquivo_do_dia)
        .withColumn("read_timestamp", F.current_timestamp())
        .select("*", "_metadata.file_name", "_metadata.file_size")
)
print("Linhas do dia:", df.count())

# COMMAND ----------

# celula 5: acrescenta ao historico completo da bronze
df.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("append").saveAsTable(bronze_table)

# celula 6: staging, so o lote de hoje (sobrescrita a cada rodada)
df.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("overwrite").saveAsTable(staging_bronze_table)

# COMMAND ----------

# celula 7: silver so sobre o staging (nao sobre o historico completo)
df_orders = spark.sql(f"SELECT * FROM {staging_bronze_table};")

df_orders = df_orders.filter(F.col("order_qty").isNotNull())

df_orders = df_orders.withColumn(
    "customer_id",
    F.when(F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id"))
     .otherwise("999999")
     .cast("string")
)

df_orders = df_orders.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[^,]+,\s*", "")
)

padrao_data_extenso = r'^(\d{1,2}) de ([a-zçãá]+) de (\d{4})$'
df_orders = df_orders.withColumn("dia_extenso", F.regexp_extract(F.col("order_placement_date"), padrao_data_extenso, 1))
df_orders = df_orders.withColumn("mes_extenso", F.regexp_extract(F.col("order_placement_date"), padrao_data_extenso, 2))
df_orders = df_orders.withColumn("ano_extenso", F.regexp_extract(F.col("order_placement_date"), padrao_data_extenso, 3))

mapa_meses = {
    "janeiro": "01", "fevereiro": "02", "março": "03", "abril": "04",
    "maio": "05", "junho": "06", "julho": "07", "agosto": "08",
    "setembro": "09", "outubro": "10", "novembro": "11", "dezembro": "12"
}
df_orders = df_orders.replace(mapa_meses, subset=["mes_extenso"])

df_orders = df_orders.withColumn(
    "data_reconstruida",
    F.when(
        F.col("dia_extenso") != "",
        F.concat_ws("-", F.col("ano_extenso"), F.col("mes_extenso"), F.lpad(F.col("dia_extenso"), 2, "0"))
    )
)

df_orders = df_orders.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date(F.col("order_placement_date"), "yyyy/MM/dd"),
        F.try_to_date(F.col("order_placement_date"), "dd-MM-yyyy"),
        F.try_to_date(F.col("order_placement_date"), "dd/MM/yyyy"),
        F.try_to_date(F.col("data_reconstruida"), "yyyy-MM-dd")
    )
)
df_orders = df_orders.drop("dia_extenso", "mes_extenso", "ano_extenso", "data_reconstruida")

df_orders = df_orders.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])
df_orders = df_orders.withColumn("product_id", F.col("product_id").cast("string"))

# COMMAND ----------

# celula 8: join com products
df_products = spark.table(f"{catalog}.{silver_schema}.products")
df_joined = df_orders.join(df_products, on="product_id", how="inner").select(df_orders["*"], df_products["product_code"])
print("Linhas do dia apos silver:", df_joined.count())

# COMMAND ----------

# celula 9: acrescenta/mescla no historico completo da silver
if not spark.catalog.tableExists(silver_table):
    df_joined.write.format("delta").option("delta.enableChangeDataFeed", "true").option("mergeSchema", "true").mode("overwrite").saveAsTable(silver_table)
else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("silver").merge(
        df_joined.alias("bronze"),
        "silver.order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_id = bronze.customer_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# celula 10: staging da silver, so o lote de hoje
df_joined.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("overwrite").saveAsTable(staging_silver_table)

# COMMAND ----------

# celula 11: gold do dia, a partir do staging da silver (nao da silver inteira)
df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {staging_silver_table};")
print("Linhas do dia na gold:", df_gold.count())

if not spark.catalog.tableExists(gold_table):
    df_gold.write.format("delta").option("delta.enableChangeDataFeed", "true").option("mergeSchema", "true").mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("source").merge(
        df_gold.alias("gold"),
        "source.date = gold.date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# celula 12: descobre quais meses foram tocados por essa carga
meses_tocados = spark.sql(f"SELECT order_placement_date as date FROM {staging_silver_table}") \
    .select(F.trunc("date", "MM").alias("start_month")).distinct()
meses_tocados.createOrReplaceTempView("meses_tocados")
meses_tocados.show()

# celula 13: recupera TODO o acumulado desses meses na gold diaria (nao so hoje)
tabela_mensal_recalculada = spark.sql(f"""
    SELECT date, product_code, customer_code, sold_quantity
    FROM {gold_table} g
    INNER JOIN meses_tocados m ON trunc(g.date, 'MM') = m.start_month
""")
print("Linhas do mes inteiro acumuladas na gold diaria:", tabela_mensal_recalculada.count())

# celula 14: reagrupa por mes, somando tudo que ja existe pro mes (nao so o incremento de hoje)
df_monthly_recalc = (
    tabela_mensal_recalculada
    .withColumn("month_start", F.trunc("date", "MM"))
    .groupBy("month_start", "product_code", "customer_code")
    .agg(F.sum("sold_quantity").alias("sold_quantity"))
    .withColumnRenamed("month_start", "date")
)
print("Linhas apos reconciliar (produto/cliente unicos no mes):", df_monthly_recalc.count())

# COMMAND ----------

# celula 15: merge final, substitui (nao soma) o valor do mes na ganapati
gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
gold_parent_delta.alias("parent_gold").merge(
    df_monthly_recalc.alias("child_gold"),
    "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- celula 16: confirma
# MAGIC SELECT count(*) FROM gana_pati.gold.fact_orders;

# COMMAND ----------

# celula 17: limpeza das tabelas de staging (efemeras, nao devem persistir)
spark.sql(f"DROP TABLE IF EXISTS {staging_bronze_table}")
spark.sql(f"DROP TABLE IF EXISTS {staging_silver_table}")