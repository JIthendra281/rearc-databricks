"""Pure Spark transformations shared by the managed pipeline and local tests."""
from pyspark.sql import functions as F, Window

QUARTERS = ['Q01', 'Q02', 'Q03', 'Q04']
OBS_VALID = "series_id RLIKE '^PR[SU][0-9]{8}$' AND year BETWEEN 1900 AND 2200 AND period IN ('Q01','Q02','Q03','Q04','Q05') AND (value IS NOT NULL OR missing_value)"
POP_VALID = 'year BETWEEN 1900 AND 2200 AND population IS NOT NULL AND population > 0 AND population_decimal = population AND nation IS NOT NULL'

def observations_typed(df):
    result = df.select('source_name', 'object_id', 'row_number',
        F.trim(F.col('fields')['series_id']).alias('series_id'),
        F.col('fields')['year'].alias('year_text'),
        F.trim(F.col('fields')['period']).alias('period'),
        F.trim(F.col('fields')['value']).alias('value_text'),
        F.trim(F.col('fields')['footnote_codes']).alias('footnote_codes'))
    return (result.withColumn('year', F.expr('try_cast(year_text AS INT)'))
            .withColumn('value', F.expr('try_cast(value_text AS DECIMAL(24,6))'))
            .withColumn('missing_value', F.coalesce(F.col('value_text').isin('', '-'), F.lit(True))))

def observations_dedup(df):
    # Identical Current/AllData overlaps collapse. Conflicting values fail expectations.
    # Preserve all footnote variants as evidence; do not silently choose a revision.
    return df.groupBy('series_id', 'year', 'period').agg(
        F.max('value').alias('value'),
        F.countDistinct(F.coalesce(F.col('value').cast('string'), F.lit('__NULL__'))).alias('value_versions'),
        F.sort_array(F.collect_set('footnote_codes')).alias('footnotes'),
        F.sort_array(F.collect_set('source_name')).alias('source_files'))

def population_typed(df):
    result = df.select(*[F.col('fields')[k].alias(k) for k in ['year', 'nation', 'nation_id', 'population']])
    return (result.withColumn('year', F.expr('try_cast(year AS INT)'))
            .withColumn('population_decimal', F.expr('try_cast(population AS DECIMAL(24,6))'))
            .withColumn('population', F.expr('try_cast(population_decimal AS BIGINT)')))

def population_dedup(df):
    return (df.filter((F.col('nation') == 'United States') & F.col('nation_id').isin('01000US', ''))
            .groupBy('year').agg(F.max('population').alias('population'),
                F.countDistinct('population').alias('population_versions')))

def series_dimension(series, lookups):
    cols = ['series_id','sector_code','class_code','measure_code','duration_code','seasonal','base_year']
    s = series.select(*[F.trim(F.col('fields')[k]).alias(k) for k in cols]).distinct()
    # A repeated ID with conflicting metadata survives DISTINCT and fails the uniqueness check.
    s = s.withColumn('series_versions', F.count('*').over(Window.partitionBy('series_id')))
    joins = [('sector','sector_code','sector_name'), ('class','class_code','class_text'),
             ('measure','measure_code','measure_text'), ('duration','duration_code','duration_text'),
             ('seasonal','seasonal_code','seasonal_text')]
    for kind, code, label in joins:
        m = lookups.filter(F.col('kind') == kind).select(
            F.col('fields')[code].alias(code), F.col('fields')[label].alias(label)).distinct()
        m = m.groupBy(code).agg(F.max(label).alias(label), F.count('*').alias(f'{kind}_versions'))
        if kind == 'seasonal':
            s = s.join(m, s.seasonal == m.seasonal_code, 'left').drop('seasonal_code')
        else:
            s = s.join(m, code, 'left')
    return s.withColumn('series_label', F.concat_ws(' | ', 'sector_name', 'class_text',
        'measure_text', 'duration_text', 'seasonal_text',
        F.when(~F.col('base_year').isin('', '-'), F.concat(F.lit('Index base: '), F.col('base_year')))))

def q1_population_stats(population):
    return population.filter(F.col('year').between(2013, 2018)).agg(
        F.count('*').alias('year_count'), F.min('year').alias('start_year'), F.max('year').alias('end_year'),
        F.avg('population').alias('mean_population'),
        F.stddev_pop('population').alias('stddev_population'),
        F.stddev_samp('population').alias('stddev_sample'))

def q2_best_year(observations, series):
    annual = observations.filter(F.col('period').isin(QUARTERS)).groupBy('series_id','year').agg(
        F.sum('value').alias('summed_value'), F.count('value').alias('quarters_available'))
    annual = annual.filter(F.col('quarters_available') > 0)
    best = (annual.withColumn('rank', F.row_number().over(Window.partitionBy('series_id').orderBy(
        F.col('summed_value').desc(), F.col('year').asc())))
        .filter('rank = 1').select('series_id', F.col('year').alias('best_year'),
                                  'summed_value','quarters_available'))
    # Include series in observations or metadata, including annual-only series.
    universe = observations.select('series_id').unionByName(series.select('series_id')).distinct()
    return (universe.join(series.select('series_id','series_label'), 'series_id', 'left')
        .join(best, 'series_id', 'left').withColumn('status',
        F.when(F.col('best_year').isNull(), F.lit('no_quarterly_observations')).otherwise(F.lit('ok'))))

def q3_value_population(observations, population):
    return (observations.filter((F.col('series_id') == 'PRS30006032') & (F.col('period') == 'Q01'))
        .select('series_id','year','period','value').join(population.select('year','population'), 'year', 'left')
        .select('series_id','year','period','value','population'))
