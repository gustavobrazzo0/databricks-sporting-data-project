# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS gana_pati;
# MAGIC USE CATALOG gana_pati;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS gana_pati.raw;
# MAGIC CREATE SCHEMA IF NOT EXISTS gana_pati.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS gana_pati.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gana_pati.gold;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS gana_pati.raw.source_files;
# MAGIC
# MAGIC SHOW SCHEMAS IN gana_pati;
# MAGIC