# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Validate after the pipeline succeeds
# MAGIC This checks the actual Gold tables, all three SQL alternatives and quality gates.
# COMMAND ----------
import os, sys
from pathlib import Path
PROJECT = Path(os.getcwd()).resolve().parent
sys.path.insert(0, str(PROJECT))
from rearc_quest.transforms import q1_population_stats, q2_best_year, q3_value_population
from pyspark.testing.utils import assertDataFrameEqual
for name, default in [('catalog','workspace'),('schema','rearc_quest')]:
    dbutils.widgets.text(name, default)
import re
catalog, schema = dbutils.widgets.get('catalog'), dbutils.widgets.get('schema')
assert all(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', s) for s in [catalog,schema])
spark.sql(f'USE CATALOG `{catalog}`')
spark.sql(f'USE SCHEMA `{schema}`')
p = spark.table('silver_population')
o = spark.table('silver_observations')
s = spark.table('silver_series')
checks = [
 ('q1_population_stats.sql','gold_population_stats',q1_population_stats(p)),
 ('q2_best_year.sql','gold_best_year',q2_best_year(o,s)),
 ('q3_value_population.sql','gold_series_population',q3_value_population(o,p))]
for sql_file, gold_table, py in checks:
    sql = spark.sql((PROJECT / 'alternatives' / sql_file).read_text())
    actual = spark.table(gold_table)
    assertDataFrameEqual(py, sql, checkRowOrder=False, rtol=1e-9, atol=1e-6)
    assertDataFrameEqual(actual, sql, checkRowOrder=False, rtol=1e-9, atol=1e-6)
    print('PASS:',gold_table,'matches SQL and PySpark')
    display(actual.orderBy('series_id') if 'series_id' in actual.columns else actual)
# COMMAND ----------
assert p.filter('year BETWEEN 2013 AND 2018').count() == 6
assert o.groupBy('series_id','year','period').count().filter('count > 1').count() == 0
assert p.groupBy('year').count().filter('count > 1').count() == 0
assert spark.table('gold_best_year').count() == o.select('series_id').union(s.select('series_id')).distinct().count()
for table in ['quarantine_observations','quarantine_population']:
    count = spark.table(table).count()
    print(table, count)
    assert count == 0, f'Review {table} before submission: {count} rejected records'
assert spark.table('gold_series_population').count() == o.filter("series_id = 'PRS30006032' AND period = 'Q01'").count()
print('All acceptance checks passed. Capture screenshots described in docs/RUNBOOK.md.')
