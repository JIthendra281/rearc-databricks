"""Validate actual ingested files on local Spark; never simulates a Databricks run."""
import argparse, json
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.testing.utils import assertDataFrameEqual
from rearc_quest.ingest import latest_manifest
from rearc_quest.transforms import *

def main(root, output):
    root=Path(root); output=Path(output); output.mkdir(parents=True,exist_ok=True)
    manifest=latest_manifest(root)
    if not manifest: raise ValueError('Run ingestion first')
    spark=(SparkSession.builder.master('local[2]').appName('rearc-live-validation')
           .config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    schema='object_id string, source_name string, kind string, row_number long, fields map<string,string>'
    records=spark.read.schema(schema).option('mode','FAILFAST').json(
        [str(root/f['parsed_relpath']) for f in manifest['files'] if f['parsed_relpath']]).cache()
    ot=observations_typed(records.filter("kind = 'observations'"))
    pt=population_typed(records.filter("kind = 'population'"))
    bad_o=ot.filter(~F.coalesce(F.expr(OBS_VALID),F.lit(False))).count()
    bad_p=pt.filter(~F.coalesce(F.expr(POP_VALID),F.lit(False))).count()
    assert bad_o==bad_p==0,(bad_o,bad_p)
    obs=observations_dedup(ot).cache(); pop=population_dedup(pt).cache()
    series=series_dimension(records.filter("kind = 'series'"),records).cache()
    assert obs.filter('value_versions != 1').count()==0,'Conflicting BLS overlaps'
    assert pop.filter('population_versions != 1').count()==0
    assert series.filter('series_versions != 1').count()==0
    for field in ['sector_name','class_text','measure_text','duration_text','seasonal_text']:
        assert series.filter(F.col(field).isNull()).count()==0,field
    for field in ['sector_versions','class_versions','measure_versions','duration_versions','seasonal_versions']:
        assert series.filter(F.col(field)!=1).count()==0,field
    for name,df in [('silver_observations',obs),('silver_population',pop),('silver_series',series)]:
        df.createOrReplaceTempView(name)
    pairs=[('q1_population_stats.sql',q1_population_stats(pop)),
           ('q2_best_year.sql',q2_best_year(obs,series)),('q3_value_population.sql',q3_value_population(obs,pop))]
    summary={'environment':'Local Apache Spark '+spark.version+'; not executed in Databricks',
             'snapshot_id':manifest['snapshot_id'], 'source_files':len(manifest['files']),
             'observations':obs.count(),'series':series.count(),'population_years':pop.count(),
             'quarantine_observations':bad_o,'quarantine_population':bad_p,'outputs':{}}
    import csv
    for filename,df in pairs:
        sql=spark.sql((Path(__file__).resolve().parents[1]/'alternatives'/filename).read_text())
        assertDataFrameEqual(df,sql,checkRowOrder=False,rtol=1e-9,atol=1e-6)
        rows=df.orderBy(*[c for c in ['series_id','year'] if c in df.columns]).collect() if 'series_id' in df.columns else df.collect()
        if filename.startswith('q1'): assert rows[0].year_count==6
        if filename.startswith('q2'): assert all(r.series_label for r in rows)
        with (output/(Path(filename).stem+'.csv')).open('w',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=df.columns); writer.writeheader()
            writer.writerows(r.asDict() for r in rows)
        summary['outputs'][Path(filename).stem]={'row_count':len(rows),'sql_pyspark_parity':'passed'}
    (output/'validation.json').write_text(json.dumps(summary,indent=2)+'\n')
    # URL + digest only; no private workspace identity or contact User-Agent.
    (output/'source_provenance.json').write_text(json.dumps(
        [{k:f[k] for k in ['source_name','url','sha256','bytes']} for f in manifest['files']],indent=2)+'\n')
    print(json.dumps(summary,indent=2)); spark.stop()
if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--root',required=True); p.add_argument('--output',required=True)
    a=p.parse_args(); main(a.root,a.output)
