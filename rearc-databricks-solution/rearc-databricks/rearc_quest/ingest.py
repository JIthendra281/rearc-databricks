"""Single-writer, content-addressed ingestion; standard library only.

Raw bytes and parse caches are immutable. Publishing one manifest is the commit.
Run ingestion and the downstream pipeline sequentially; do not run concurrent writers.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import io
import json
import re
import time
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urljoin, urlsplit
from urllib.request import Request, build_opener, HTTPRedirectHandler

BLS_URL = 'https://download.bls.gov/pub/time.series/pr/'
POP_URL = ('https://honolulu-api.datausa.io/tesseract/data.jsonrecords?'
           'cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population')
DATA_COLS = {'series_id', 'year', 'period', 'value', 'footnote_codes'}
SERIES_COLS = {'series_id', 'sector_code', 'class_code', 'measure_code',
               'duration_code', 'seasonal', 'base_year', 'footnote_codes',
               'begin_year', 'begin_period', 'end_year', 'end_period'}
LOOKUPS = {'sector': ('sector_code', 'sector_name'), 'class': ('class_code', 'class_text'),
           'measure': ('measure_code', 'measure_text'), 'duration': ('duration_code', 'duration_text'),
           'seasonal': ('seasonal_code', 'seasonal_text'), 'period': ('period', 'period_name'),
           'footnote': ('footnote_code', 'footnote_text')}

class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # Inspect provider changes explicitly; never send contact info to redirects.

class HTTPClient:
    def __init__(self, contact: str, attempts=4, pause=0.5):
        if not re.fullmatch(r'[^\s@]+@[^\s@]+\.[^\s@]+', contact):
            raise ValueError('Supply a real contact email for the BLS User-Agent.')
        self.ua = f'RearcDataQuest/1.0 (contact: {contact})'
        self.attempts, self.pause = attempts, pause
        self.opener = build_opener(NoRedirect())

    def get(self, url, headers=None):
        for attempt in range(self.attempts):
            time.sleep(self.pause)
            try:
                with self.opener.open(Request(url, headers={'User-Agent': self.ua,
                      'Accept-Encoding': 'identity', **(headers or {})}), timeout=45) as r:
                    return r.status, dict(r.headers), r.read()
            except HTTPError as e:
                if e.code == 304:
                    return 304, dict(e.headers), b''
                if e.code == 403:
                    raise RuntimeError('BLS/provider denied access (403). Confirm your real '
                        'contact User-Agent; contact the provider if the restriction persists. '
                        'No proxy rotation or bypass is attempted.') from e
                if e.code not in (429, 500, 502, 503, 504) or attempt == self.attempts - 1:
                    raise
                retry = e.headers.get('Retry-After', '')
                try:
                    delay = float(retry)
                except ValueError:
                    try:
                        delay = (parsedate_to_datetime(retry) - datetime.now(timezone.utc)).total_seconds()
                    except (ValueError, TypeError):
                        delay = 2 ** attempt
                if delay > 60:
                    raise RuntimeError(f'Provider requests retry after {delay:.0f}s; rerun later.') from e
                time.sleep(max(0, delay))
            except (URLError, TimeoutError):
                if attempt == self.attempts - 1:
                    raise
                time.sleep(2 ** attempt)
        raise RuntimeError('HTTP retries exhausted')

class Links(HTMLParser):
    def __init__(self):
        super().__init__(); self.hrefs = []
    def handle_starttag(self, tag, attrs):
        if tag.lower() == 'a':
            self.hrefs.extend(v for k, v in attrs if k.lower() == 'href' and v)

def discover(body: bytes, base=BLS_URL):
    parser = Links(); parser.feed(body.decode('utf-8'))
    root = urlsplit(base)
    result = {}
    for href in parser.hrefs:
        u = urlsplit(urljoin(base, href))
        if (u.scheme, u.netloc) != (root.scheme, root.netloc) or u.query or u.fragment:
            continue
        if not u.path.startswith(root.path):
            continue
        name = unquote(u.path[len(root.path):])
        if not name or '/' in name or '\\' in name or name.startswith('.'):
            continue
        result[name] = u.geturl()
    if not result:
        raise ValueError('Empty/unrecognized BLS listing; refusing to infer mass deletion.')
    return dict(sorted(result.items()))

def digest(body):
    return hashlib.sha256(body).hexdigest()

def parse_bls(name, body):
    """Contract-aware tab parsing. Values remain strings until Silver."""
    if body.lstrip().lower().startswith((b'<!doctype html', b'<html')):
        raise ValueError(f'HTML returned instead of raw file: {name}')
    suffix = name.removeprefix('pr.')
    if name.startswith('pr.data.'):
        kind, required = 'observations', DATA_COLS
    elif name == 'pr.series':
        kind, required = 'series', SERIES_COLS
    elif suffix in LOOKUPS:
        kind, required = suffix, set(LOOKUPS[suffix])
    else:
        return None, []  # Documentation and future unknown files still land raw.
    text = body.decode('utf-8-sig')
    reader = csv.reader(io.StringIO(text), delimiter='\t')
    headers = [v.strip().lower() for v in next(reader, [])]
    if len(headers) != len(set(headers)) or not required.issubset(headers):
        raise ValueError(f'Schema drift in {name}: required {sorted(required)}, got {headers}')
    rows = []
    for line, cells in enumerate(reader, 2):
        if not cells or not any(v.strip() for v in cells):
            continue
        # BLS data files may carry one trailing tab after the declared fields.
        while len(cells) > len(headers) and cells[-1] == '':
            cells.pop()
        if len(cells) != len(headers):
            raise ValueError(f'{name}:{line}: wrong field count')
        rows.append({k: v.strip() for k, v in zip(headers, cells)})
    if not rows and kind != 'footnote':
        raise ValueError(f'Unexpected empty data table: {name}')
    return kind, rows

def parse_population(body):
    payload = json.loads(body)
    rows = payload.get('data')
    if not isinstance(rows, list) or not rows:
        raise ValueError('Population response must contain a nonempty data array')
    page = payload.get('page', {})
    if int(page.get('offset', 0)) != 0 or int(page.get('total', len(rows))) != len(rows):
        raise ValueError('Population API returned a partial page; refusing incomplete results')
    result = []
    for row in rows:
        if not {'Year', 'Nation', 'Population'}.issubset(row):
            raise ValueError('Population schema drift: Year/Nation/Population required')
        # Tesseract uses Nation ID; preserve old ID Nation as a documented compatibility alias.
        result.append({'year': str(row['Year']) if row['Year'] is not None else None,
                       'nation': row['Nation'],
                       'nation_id': str(row.get('Nation ID', row.get('ID Nation', ''))),
                       'population': str(row['Population']) if row['Population'] is not None else None})
    return 'population', result

def latest_manifest(root):
    paths = sorted((root / 'manifests').glob('*.json'))
    return json.loads(paths[-1].read_text()) if paths else None

def write_once(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != body:
            raise ValueError(f'Immutable object mismatch: {path}')
        return
    path.write_bytes(body)

def ingest(root, client, bls_url=BLS_URL, pop_url=POP_URL):
    root = Path(root); root.mkdir(parents=True, exist_ok=True)
    previous = latest_manifest(root)
    old = {f['source_name']: f for f in previous['files']} if previous else {}
    _, _, listing = client.get(bls_url)
    sources = discover(listing, bls_url)
    sources['population.json'] = pop_url
    files, downloaded, reused = [], 0, 0
    for name, url in sorted(sources.items()):
        before = old.get(name)
        headers = {}
        if before and before['url'] == url and (root / before['raw_relpath']).exists() and (
                not before.get('parsed_relpath') or (root / before['parsed_relpath']).exists()):
            if before.get('etag'):
                headers['If-None-Match'] = before['etag']
            elif before.get('last_modified'):
                headers['If-Modified-Since'] = before['last_modified']
        status, response_headers, body = client.get(url, headers)
        if status == 304:
            if not before or not headers:
                raise ValueError('Unexpected 304 without a cached representation')
            files.append(before); reused += 1; continue
        if status != 200:
            raise ValueError(f'Unexpected HTTP {status} for {name}')
        downloaded += 1
        sha = digest(body)
        object_id = digest(name.encode() + b'\0' + body)
        if before and before['sha256'] == sha and (root / before['raw_relpath']).exists() and (
                not before.get('parsed_relpath') or (root / before['parsed_relpath']).exists()):
            files.append(before); reused += 1; continue
        kind, rows = parse_population(body) if name == 'population.json' else parse_bls(name, body)
        raw_rel = f'raw/{object_id}/{name}'
        parsed_rel = f'parsed/{object_id}.json' if kind else None
        write_once(root / raw_rel, body)
        if kind:
            records = [{'object_id': object_id, 'source_name': name, 'kind': kind,
                        'row_number': i, 'fields': row} for i, row in enumerate(rows, 1)]
            # Empty lookup is allowed; a blank JSON file is valid for Spark's explicit schema.
            write_once(root / parsed_rel, ''.join(json.dumps(r) + '\n' for r in records).encode())
        h = {k.lower(): v for k, v in response_headers.items()}
        files.append({'source_name': name, 'url': url, 'sha256': sha, 'object_id': object_id,
                      'raw_relpath': raw_rel, 'parsed_relpath': parsed_rel, 'kind': kind,
                      'etag': h.get('etag'), 'last_modified': h.get('last-modified'), 'bytes': len(body)})
    # A changing directory invalidates this candidate. Previously committed state remains active.
    _, _, after_listing = client.get(bls_url)
    if digest(after_listing) != digest(listing):
        raise ValueError('BLS listing changed during ingestion; rerun to obtain a stable snapshot')
    same = previous and {(f['source_name'], f['sha256']) for f in files} == {
        (f['source_name'], f['sha256']) for f in previous['files']}
    if same:
        return {'changed': False, 'snapshot_id': previous['snapshot_id'],
                'downloaded': downloaded, 'reused': reused, 'deleted': []}
    snapshot = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '_' + uuid.uuid4().hex[:8]
    manifest = {'snapshot_id': snapshot, 'committed_at': datetime.now(timezone.utc).isoformat(),
                'files': files, 'deleted': sorted(set(old) - set(sources))}
    # One small commit file, written last. No overwrites/rename assumptions on Volume FUSE.
    write_once(root / 'manifests' / f'{snapshot}.json', (json.dumps(manifest) + '\n').encode())
    return {'changed': True, 'snapshot_id': snapshot, 'downloaded': downloaded,
            'reused': reused, 'deleted': manifest['deleted']}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True)
    parser.add_argument('--contact', required=True)
    args = parser.parse_args()
    print(json.dumps(ingest(args.root, HTTPClient(args.contact)), indent=2))
