# COMMAND ----------
from delta.tables import DeltaTable

df_ganapati_dez = spark.read.options(header=True, inferSchema=True).csv(
    "/Volumes/gana_pati/raw/source_files/parent_incremental/fact_orders.csv"
)

# COMMAND ----------
gold_delta = DeltaTable.forName(spark, "gana_pati.gold.fact_orders")
gold_delta.alias("target").merge(
    df_ganapati_dez.alias("source"),
    "target.date = source.date AND target.product_code = source.product_code AND target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
