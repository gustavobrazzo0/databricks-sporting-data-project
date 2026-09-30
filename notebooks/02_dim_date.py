# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT count(*) FROM gana_pati.gold.fact_orders;

# COMMAND ----------

from pyspark.sql import functions as F

calendar_start_date = "2024-01-01"
calendar_end_date = "2025-12-01"

monthly_calendar_df = spark.sql(f"""
    SELECT explode(
        sequence(
            to_date('{calendar_start_date}'),
            to_date('{calendar_end_date}'),
            interval 1 month
        )
    ) AS month_start_date
""")

monthly_calendar_df = (
    monthly_calendar_df
    .withColumn("date_key", F.date_format("month_start_date", "yyyyMM").cast("int"))
    .withColumn("year", F.year("month_start_date"))
    .withColumn("month_name", F.date_format("month_start_date", "MMMM"))
    .withColumn("month_short_name", F.date_format("month_start_date", "MMM"))
    .withColumn("quarter", F.concat(F.lit("Q"), F.quarter("month_start_date")))
    .withColumn("year_quarter", F.concat(F.col("year"), F.lit("-Q"), F.quarter("month_start_date")))
)

monthly_calendar_df.write.mode("overwrite").format("delta").saveAsTable("gana_pati.gold.dim_date")