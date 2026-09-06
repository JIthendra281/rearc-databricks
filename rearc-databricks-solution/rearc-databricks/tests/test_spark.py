import unittest, tempfile, json, uuid
from decimal import Decimal
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.testing.utils import assertDataFrameEqual
from rearc_quest.transforms import *
class SparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark=(SparkSession.builder.master('local[2]').appName('rearc-tests').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')
    @classmethod
    def tearDownClass(cls): cls.spark.stop()
    def frame(self, rows, schema):
        # Native JSON fixture readers exercise Catalyst without Python worker setup.
        from pyspark.sql.types import StructType
        names=StructType.fromDDL(schema).fieldNames()
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'fixture.json'
            p.write_text(''.join(json.dumps(dict(zip(names,r)),default=str)+'\n' for r in rows))
            result=self.spark.read.schema(schema).json(str(p)).cache()
            result.count()
            return result
    def test_sql_parity_and_analytical_edges(self):
        s=self.spark
        rows=[('PRS30006032',2013,'Q01',Decimal('1')),('PRS30006032',2013,'Q02',Decimal('2')),
              ('PRS30006032',2013,'Q05',Decimal('99999')),('PRS30006032',2014,'Q01',Decimal('3')),
              ('PRS30006032',2012,'Q01',Decimal('-4')),('PRS30006033',2013,'Q05',Decimal('10')),
              ('PRS30006034',2013,'Q01',None),('PRS30006035',2013,'Q01',Decimal('-3')),
              ('PRS30006035',2014,'Q01',Decimal('-1'))]
        obs=self.frame(rows,'series_id string,year int,period string,value decimal(24,6)')
        pop=self.frame([(y,100+(y-2013)*10) for y in range(2013,2019)],'year int,population long')
        series=self.frame([(f'PRS3000603{i}',f'Label {i}') for i in range(2,7)],'series_id string,series_label string')
        for name,df in [('silver_observations',obs),('silver_population',pop),('silver_series',series)]: df.createOrReplaceTempView(name)
        pairs=[('q1_population_stats.sql',q1_population_stats(pop)),('q2_best_year.sql',q2_best_year(obs,series)),('q3_value_population.sql',q3_value_population(obs,pop))]
        for filename,py in pairs:
            sql=s.sql((Path(__file__).resolve().parents[1]/'alternatives'/filename).read_text())
            assertDataFrameEqual(py,sql,checkRowOrder=False,rtol=1e-9,atol=1e-6)
        q1=pairs[0][1].first(); self.assertEqual(q1.mean_population,125)
        self.assertAlmostEqual(q1.stddev_population,17.0782512766,places=6)
        q2={r.series_id:r for r in pairs[1][1].collect()}
        self.assertEqual(q2['PRS30006032'].best_year,2013)
        self.assertEqual(q2['PRS30006032'].summed_value,Decimal('3'))
        self.assertEqual(q2['PRS30006033'].status,'no_quarterly_observations')
        self.assertEqual(q2['PRS30006034'].status,'no_quarterly_observations')
        self.assertEqual(q2['PRS30006035'].best_year,2014); self.assertIn('PRS30006036',q2)
        self.assertIsNone(pairs[2][1].filter('year = 2012').first().population)
    def test_overlap_conflicts_and_typing(self):
        fields={'series_id':' PRS30006032 ','year':'2013','period':'Q01','value':'1.0','footnote_codes':''}
        schema='source_name string,object_id string,row_number long,fields map<string,string>'
        df=self.frame([('current','a',1,fields),('all','b',1,fields)],schema)
        typed=observations_typed(df); self.assertEqual(typed.filter(F.expr(OBS_VALID)).count(),2)
        self.assertEqual(observations_dedup(typed).first().value_versions,1)
        conflict=self.frame([('other','c',1,{**fields,'value':'2'})],schema)
        self.assertEqual(observations_dedup(observations_typed(df.union(conflict))).first().value_versions,2)
        invalid=self.frame([('bad','d',1,{**fields,'value':'nonsense'})],schema)
        self.assertEqual(observations_typed(invalid).filter(F.expr(OBS_VALID)).count(),0)
    def test_population_integrality(self):
        df=self.frame([({'year':'2013','nation':'United States','nation_id':'01000US','population':'316128839.0'},),({'year':'2014','nation':'United States','nation_id':'01000US','population':'12.5'},)],'fields map<string,string>')
        valid=population_typed(df).filter(F.expr(POP_VALID)); self.assertEqual(valid.count(),1)
        self.assertEqual(valid.first().population,316128839)
