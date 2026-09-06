import json, tempfile, unittest, hashlib
from pathlib import Path
from rearc_quest.ingest import *
BASE=BLS_URL
POP='https://example.org/population'
DATA=b'series_id\tyear\tperiod\tvalue\tfootnote_codes\nPRS30006032\t2013\tQ01\t1.5\t\n'
POPULATION=json.dumps({'page':{'total':1,'offset':0},'data':[{'Year':2013,'Nation':'United States','Nation ID':'01000US','Population':316128839.0}]}).encode()
class FakeClient:
    def __init__(self):
        self.files={'pr.data.new':DATA,'pr.txt':b'provider documentation'}
        self.fail=None; self.conditional=True
    def get(self,url,headers=None):
        if self.fail and url.endswith(self.fail): raise RuntimeError('Injected failure')
        if url==BASE:
            return 200,{},''.join(f'<a href="{k}">{k}</a>' for k in self.files).encode()
        body=POPULATION if url==POP else self.files[url.rsplit('/',1)[-1]]
        etag=hashlib.sha256(body).hexdigest()
        if self.conditional and (headers or {}).get('If-None-Match')==etag: return 304,{},b''
        return 200,{'ETag':etag},body
class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.root=Path(self.tmp.name); self.client=FakeClient()
    def tearDown(self): self.tmp.cleanup()
    def run_ingest(self): return ingest(self.root,self.client,BASE,POP)
    def test_discovery_boundary(self):
        html=b'<a href="pr.future">ok</a><a href="../secret">bad</a><a href="https://evil.com/x">bad</a><a href="%2e%2e%2fsecret">bad</a>'
        self.assertEqual(list(discover(html)),['pr.future'])
        with self.assertRaises(ValueError): discover(b'<html>Access denied</html>')
    def test_unchanged_no_writes(self):
        first=self.run_ingest(); before={str(p):p.stat().st_mtime_ns for p in self.root.rglob('*') if p.is_file()}
        second=self.run_ingest()
        self.assertFalse(second['changed']); self.assertEqual(first['snapshot_id'],second['snapshot_id'])
        self.assertEqual(second['downloaded'],0)
        self.assertEqual(before,{str(p):p.stat().st_mtime_ns for p in self.root.rglob('*') if p.is_file()})
    def test_hash_fallback(self):
        self.run_ingest(); self.client.conditional=False
        self.assertFalse(self.run_ingest()['changed'])
        self.assertEqual(len(list((self.root/'manifests').glob('*'))),1)
    def test_add_change_delete(self):
        self.run_ingest(); old=latest_manifest(self.root)
        self.client.files['pr.data.new']=DATA.replace(b'1.5',b'9.5')
        self.client.files['pr.contacts']=b'new documentation'; del self.client.files['pr.txt']
        result=self.run_ingest(); current=latest_manifest(self.root)
        self.assertTrue(result['changed']); self.assertEqual(result['deleted'],['pr.txt'])
        self.assertNotIn('pr.txt',[f['source_name'] for f in current['files']])
        self.assertTrue(all((self.root/f['raw_relpath']).exists() for f in old['files']))
    def test_failure_no_commit(self):
        self.run_ingest(); before=latest_manifest(self.root)
        self.client.files['pr.data.new']=DATA.replace(b'1.5',b'9.5'); self.client.fail='pr.txt'
        with self.assertRaises(RuntimeError): self.run_ingest()
        self.assertEqual(before,latest_manifest(self.root))
    def test_trailing_tabs_and_drift(self):
        kind,rows=parse_bls('pr.data.test',DATA.replace(b'1.5\t\n',b' 1.5 \t\t\n'))
        self.assertEqual(rows[0]['value'],'1.5')
        with self.assertRaises(ValueError): parse_bls('pr.data.test',b'wrong\theader\nx\ty')
    def test_population_partial_page(self):
        body=json.loads(POPULATION); body['page']['total']=2
        with self.assertRaises(ValueError): parse_population(json.dumps(body).encode())
    def test_contact_required(self):
        with self.assertRaises(ValueError): HTTPClient('')
    def test_seasonal_header_case(self):
        kind,rows=parse_bls('pr.seasonal',b'Seasonal_code\tSeasonal_text\nS\tSeasonally adjusted\n')
        self.assertEqual(kind,'seasonal'); self.assertEqual(rows[0]['seasonal_code'],'S')
    def test_missing_parse_cache_is_repaired(self):
        self.run_ingest(); manifest=latest_manifest(self.root)
        target=next(f for f in manifest['files'] if f['parsed_relpath'])
        (self.root/target['parsed_relpath']).unlink()
        self.run_ingest()
        self.assertTrue((self.root/target['parsed_relpath']).exists())
