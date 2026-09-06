# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Create the raw Volume and ingest
# MAGIC Run on serverless notebook compute. All external requests include your BLS contact email.
# COMMAND ----------
import os
import sys
from pathlib import Path
# Databricks sets the working directory to this notebook's directory.
PROJECT = Path(os.getcwd()).resolve().parent
sys.path.insert(0, str(PROJECT))
from rearc_quest.ingest import HTTPClient, ingest

for name, default in [('catalog','workspace'), ('schema','rearc_quest'),
                      ('contact_email',''), ('volume','raw')]:
    dbutils.widgets.text(name, default)

catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')
volume = dbutils.widgets.get('volume')
contact = dbutils.widgets.get('contact_email')
import re
for value in [catalog, schema, volume]:
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', value):
        raise ValueError('Catalog/schema/volume must be simple SQL identifiers')
if not contact:
    raise ValueError('Set contact_email to your real email in the widget before running.')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`')
spark.sql(f'CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`')
root = f'/Volumes/{catalog}/{schema}/{volume}/rearc'
# COMMAND ----------
result = ingest(root, HTTPClient(contact))
print(result)
print(f'Pipeline configuration: quest.raw_root = {root}')
print(f'Pipeline destination: {catalog}.{schema}')
# No automatic pipeline side effect here. The bundle job chains the pipeline task.
