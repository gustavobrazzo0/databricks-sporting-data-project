# Ganapati Sporting Goods + Hastinapur Nutrition - Lakehouse (Medallion Architecture)

Portfolio project: a simulated merger between Ganapati Sporting Goods (a
mature sporting goods manufacturer, consolidated ERP, dimensional Star
Schema model) and Hastinapur Nutrition (a sports nutrition startup, messy
data coming from spreadsheets, shared drive folders and an API). Goal:
unify both data ecosystems into a single pipeline using Medallion
Architecture (Bronze/Silver/Gold) with Delta Lake, ending in a
consolidated dashboard.

Adapted from the Codebasics data engineering course project (originally
using the companies "Atlon"/"Sports Bar" and Indian data, see
NAMING_MAP.md for the full name/currency/region mapping used to
re-scenario it). The course's own reference material is not redistributed
in this repository.

## Stack

- Databricks Free Edition (managed free workspace, no credit card, no
  linked cloud account required)
- Unity Catalog + Volumes instead of S3, at no AWS cost
- PySpark + Delta Lake (native to the Databricks runtime)
- Databricks Jobs for orchestration, Databricks Dashboards and Genie for
  consumption

## Unity Catalog structure

```
gana_pati                       <- catalog
+-- raw
|   +-- source_files/            <- Volume: raw CSVs from Hastinapur Nutrition
|       +-- full_load/{customers,products,gross_price}/
|       +-- full_load/orders/{landing,processed}/
|       +-- incremental_load/orders/
+-- bronze                       <- Delta tables, one to one with the source file
+-- silver                       <- cleaned and standardized data
+-- gold                         <- consolidated Star Schema (dim_*, fact_orders,
                                     hn_* tables before the merge)
```

Startup tables carry the hn_ prefix (Hastinapur Nutrition) before being
merged into the parent company's tables, for example gold.hn_dim_customers.

## Repository structure

```
cairn-bolt-lakehouse/
+-- README.md
+-- NAMING_MAP.md               (full name, currency and city mapping, Portuguese)
+-- CONVENCOES.md               (documentation conventions used while building this, Portuguese)
+-- data/raw/                    (adapted sample data, ready to upload to the Volume)
|   +-- parent_company/full_load/        (dim_customers, dim_products, dim_gross_price, fact_orders)
|   +-- parent_company/incremental_load/
|   +-- child_company/{full_load,incremental_load}/  (customers, products, gross_price, orders)
+-- notebooks/                   (pipeline notebooks, one per stage, mirror what runs on Databricks)
+-- docker/, docker-compose.yml, spark/
    (a local Docker + MinIO route was explored first, then dropped in favor
    of Databricks Free Edition for a zero-cost managed setup; kept here for
    reference, not part of the current pipeline)
```

## How to load the sample data

1. In Databricks, create the catalog, schemas and volume (see the setup
   notebook).
2. Upload the contents of data/raw/ to /Volumes/gana_pati/raw/source_files/,
   keeping the folder structure (Databricks CLI batch upload; too many
   files to drag one by one through the UI).

## First test

Once the Volume is populated, run a simple read (spark.read.csv(...))
against one of the files in the Volume to confirm Databricks can see the
data before starting the real Bronze pipeline.

## Pipeline overview

- Bronze: raw ingestion with audit metadata (read timestamp, file name,
  file size). The fact table uses a landing/processed folder pattern with
  append writes, since each load adds to history instead of replacing it.
- Silver: per-source cleaning and standardization (deduplication, date
  parsing, invalid id fallback, price sign fixes, trim/initcap, category
  mapping, surrogate keys via SHA-256 hashing where the source has no
  reliable natural key).
- Gold: consolidated Star Schema. Dimensions merge on their natural or
  surrogate key; the fact table reconciles daily granularity into monthly
  before merging into the parent company's existing history, matching the
  grain already used there.
- Incremental loads use staging tables plus a full month recompute for
  idempotent reconciliation, so reprocessing a day never adds on top of an
  existing total.
- Orchestration: a Databricks Job with a fan-out dependency graph
  (customers -> products -> gross_price), scheduled to run daily.
- Consumption: a denormalized view (LEFT JOIN across the fact table and
  all three dimensions, preserving rows even without a full dimension
  match), a Genie space for natural language questions over that view,
  and a dashboard with five KPIs: total quantity, total revenue, revenue
  by channel, monthly revenue trend, and top products by revenue.

## Notable data engineering problems solved

- Locale-dependent date parsing: the Portuguese long-form date field
  (weekday name, day, month name, year) breaks both an ASCII-only weekday
  regex and native month-name date parsing, which depends on the JVM's
  default locale. Solved with a language-agnostic regex anchored on the
  first comma, plus a manual month-name-to-number mapping applied before
  parsing.
- Two different reconciliation strategies depending on whether the metric
  is additive: a window function picks one representative price per
  product per year (price is not additive across time), while group by
  plus sum reconciles daily order quantities into a monthly total (which
  is additive).
- Idempotent incremental reconciliation: reprocessing a day recomputes the
  affected months from scratch against the accumulated daily history and
  replaces the existing total, instead of naively adding the new day's
  amount on top of it (which would double count on any reprocessing).
