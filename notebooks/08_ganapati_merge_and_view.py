# Databricks notebook source
from delta.tables import DeltaTable

# COMMAND ----------

# celula: carrega o incremental da ganapati direto na gold (simula pipeline dela ja em producao)
df_ganapati_dez = spark.read.options(header=True, inferSchema=True).csv(
    "/Volumes/gana_pati/raw/source_files/parent_incremental/fact_orders.csv"
)

gold_delta = DeltaTable.forName(spark, "gana_pati.gold.fact_orders")
gold_delta.alias("target").merge(
    df_ganapati_dez.alias("source"),
    "target.date = source.date AND target.product_code = source.product_code AND target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM gana_pati.gold.fact_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gana_pati.gold.vw_vendas_completo AS
# MAGIC SELECT
# MAGIC     f.date,
# MAGIC     f.product_code,
# MAGIC     f.customer_code,
# MAGIC     f.sold_quantity,
# MAGIC     c.customer,
# MAGIC     c.market,
# MAGIC     c.platform,
# MAGIC     c.channel,
# MAGIC     p.division,
# MAGIC     p.category,
# MAGIC     p.product,
# MAGIC     p.variant,
# MAGIC     gp.price_brl,
# MAGIC     (f.sold_quantity * gp.price_brl) AS receita
# MAGIC FROM gana_pati.gold.fact_orders f
# MAGIC LEFT JOIN gana_pati.gold.dim_customers c
# MAGIC     ON f.customer_code = c.customer_code
# MAGIC LEFT JOIN gana_pati.gold.dim_products p
# MAGIC     ON f.product_code = p.product_code
# MAGIC LEFT JOIN gana_pati.gold.dim_gross_price gp
# MAGIC     ON f.product_code = gp.product_code
# MAGIC     AND CAST(year(f.date) AS STRING) = gp.year;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM gana_pati.gold.vw_vendas_completo;
# MAGIC SELECT count(*) FROM gana_pati.gold.vw_vendas_completo WHERE receita IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM gana_pati.gold.vw_vendas_completo;