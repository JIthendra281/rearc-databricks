"""Add this file as the sole Python source of a serverless declarative pipeline.
Set quest.raw_root to /Volumes/<catalog>/<schema>/raw/rearc.
No network calls or imperative writes execute inside pipeline dataset functions.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window, SparkSession
from rearc_quest.transforms import (
    OBS_VALID, POP_VALID, observations_typed, observations_dedup,
    population_typed, population_dedup, series_dimension,
    q1_population_stats, q2_best_year, q3_value_population)

spark = SparkSession.getActiveSession()
ROOT = spark.conf.get('quest.raw_root').rstrip('/')
MANIFEST_SCHEMA = '''snapshot_id STRING, committed_at STRING, deleted ARRAY<STRING>,
 files ARRAY<STRUCT<source_name:STRING,url:STRING,sha256:STRING,object_id:STRING,
 raw_relpath:STRING,parsed_relpath:STRING,kind:STRING,etag:STRING,last_modified:STRING,bytes:BIGINT>>'''
RECORD_SCHEMA = 'object_id STRING, source_name STRING, kind STRING, row_number BIGINT, fields MAP<STRING,STRING>'

@dp.materialized_view(name='bronze_manifest', comment='Latest complete source inventory; older raw versions retained.')
@dp.expect_or_fail('manifest_keys', 'snapshot_id IS NOT NULL AND object_id IS NOT NULL')
def bronze_manifest():
    manifests = spark.read.schema(MANIFEST_SCHEMA).option('mode','FAILFAST').json(ROOT + '/manifests')
    latest = manifests.withColumn('_rank', F.row_number().over(Window.orderBy(F.col('snapshot_id').desc()))).filter('_rank = 1')
    return latest.select('snapshot_id','committed_at', F.explode('files').alias('file')).select('snapshot_id','committed_at','file.*')

@dp.materialized_view(name='bronze_records', comment='String records and provenance for the committed active raw files.')
@dp.expect_or_fail('record_envelope', 'object_id IS NOT NULL AND source_name IS NOT NULL AND kind IS NOT NULL AND fields IS NOT NULL')
def bronze_records():
    cached = spark.read.schema(RECORD_SCHEMA).option('mode','FAILFAST').json(ROOT + '/parsed')
    active = spark.read.table('bronze_manifest').select('object_id').distinct()
    return cached.join(active, 'object_id', 'left_semi')

@dp.temporary_view(name='typed_observations')
def typed_observations():
    return observations_typed(spark.read.table('bronze_records').filter("kind = 'observations'"))

@dp.materialized_view(name='quarantine_observations')
def quarantine_observations():
    return spark.read.table('typed_observations').filter(~F.coalesce(F.expr(OBS_VALID), F.lit(False)))

@dp.temporary_view(name='valid_observations')
@dp.expect_or_drop('valid_observation', OBS_VALID)
def valid_observations():
    return spark.read.table('typed_observations')

@dp.materialized_view(name='silver_observations')
@dp.expect_or_fail('non_null_keys', 'series_id IS NOT NULL AND year IS NOT NULL AND period IS NOT NULL')
@dp.expect_or_fail('consistent_overlap', 'value_versions = 1')
def silver_observations():
    return observations_dedup(spark.read.table('valid_observations'))

@dp.temporary_view(name='typed_population')
def typed_population():
    return population_typed(spark.read.table('bronze_records').filter("kind = 'population'"))

@dp.materialized_view(name='quarantine_population')
def quarantine_population():
    return spark.read.table('typed_population').filter(~F.coalesce(F.expr(POP_VALID), F.lit(False)))

@dp.temporary_view(name='valid_population')
@dp.expect_or_drop('valid_population', POP_VALID)
def valid_population():
    return spark.read.table('typed_population')

@dp.materialized_view(name='silver_population')
@dp.expect_or_fail('population_keys', 'year IS NOT NULL AND population IS NOT NULL')
@dp.expect_or_fail('consistent_population', 'population_versions = 1')
def silver_population():
    return population_dedup(spark.read.table('valid_population'))

@dp.materialized_view(name='silver_series')
@dp.expect_or_fail('series_keys', "series_id RLIKE '^PR[SU][0-9]{8}$'")
@dp.expect_or_fail('unique_series', 'series_versions = 1')
@dp.expect_or_fail('complete_labels', '''sector_name IS NOT NULL AND class_text IS NOT NULL AND
 measure_text IS NOT NULL AND duration_text IS NOT NULL AND seasonal_text IS NOT NULL''')
@dp.expect_or_fail('unique_lookups', '''sector_versions = 1 AND class_versions = 1 AND
 measure_versions = 1 AND duration_versions = 1 AND seasonal_versions = 1''')
def silver_series():
    records = spark.read.table('bronze_records')
    return series_dimension(records.filter("kind = 'series'"), records)

@dp.materialized_view(name='gold_population_stats')
@dp.expect_or_fail('six_year_coverage', 'year_count = 6 AND start_year = 2013 AND end_year = 2018')
def gold_population_stats():
    return q1_population_stats(spark.read.table('silver_population'))

@dp.materialized_view(name='gold_best_year')
@dp.expect_or_fail('human_readable_series', 'series_label IS NOT NULL')
def gold_best_year():
    return q2_best_year(spark.read.table('silver_observations'), spark.read.table('silver_series'))

@dp.materialized_view(name='gold_series_population')
def gold_series_population():
    return q3_value_population(spark.read.table('silver_observations'), spark.read.table('silver_population'))
