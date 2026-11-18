"""Dated AMC holdings snapshots, isolated from trading research universes."""
import io
import re
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from urllib.parse import urljoin
from workspace_analytics.sqlite_store import connect_database


def worksheet_rows(payload):
    ns = {'m': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        if sum(x.file_size for x in archive.infolist()) > 30_000_000:
            raise ValueError('disclosure_too_large')
        strings = []
        if 'xl/sharedStrings.xml' in archive.namelist():
            strings = [''.join(x.itertext()) for x in ET.fromstring(archive.read('xl/sharedStrings.xml'))]
        for sheet in sorted(x for x in archive.namelist() if re.fullmatch(r'xl/worksheets/sheet\d+\.xml', x)):
            rows = []
            for row in ET.fromstring(archive.read(sheet)).findall('.//m:row', ns):
                values = {}
                for cell in row.findall('m:c', ns):
                    key = re.sub(r'\d', '', cell.get('r', ''))
                    text = cell.findtext('m:v', '', ns)
                    if cell.get('t') == 's':
                        text = strings[int(text)]
                    elif cell.get('t') == 'inlineStr':
                        inline = cell.find('m:is', ns)
                        text = ''.join(inline.itertext()) if inline is not None else ''
                    values[key] = text
                rows.append(values)
            yield rows


def ensure_disclosures(c):
    c.execute('''CREATE TABLE IF NOT EXISTS portfolio_fund_holdings (
        fund_isin TEXT, disclosure_date TEXT, security_isin TEXT, name TEXT,
        asset_class TEXT, weight_pct REAL, source TEXT, retrieved_at TEXT,
        PRIMARY KEY(fund_isin,disclosure_date,security_isin))''')


def lookthrough(path, holdings, total):
    c = connect_database(path)
    try:
        ensure_disclosures(c)
        rows = [dict(r) for r in c.execute('''SELECT h.* FROM portfolio_fund_holdings h
            WHERE disclosure_date=(SELECT MAX(disclosure_date) FROM portfolio_fund_holdings x WHERE x.fund_isin=h.fund_isin)''')]
        c.commit()
    finally:
        c.close()
    from collections import defaultdict
    groups = defaultdict(float)
    stock_exposures = {}
    fund_maps = {}
    coverage = []
    for holding in holdings:
        value = holding['market_value_inr'] or 0
        symbol = holding['symbol']
        if holding.get('asset_type') == 'mutual_fund':
            fund_rows = [r for r in rows if r['fund_isin'] == symbol]
            admitted = sum(r['weight_pct'] for r in fund_rows)
            coverage.append({'name': holding['display_name'], 'date': fund_rows[0]['disclosure_date'] if fund_rows else None, 'coverage_pct': admitted, 'source': fund_rows[0]['source'] if fund_rows else None})
            groups['Undisclosed / unresolved'] += value * max(0, 100-admitted)/100
            fund_maps[symbol] = {r['security_isin']: r['weight_pct'] for r in fund_rows if r['asset_class'] == 'Equity'}
            for row in fund_rows:
                exposure = value * row['weight_pct']/100
                groups[row['asset_class']] += exposure
                if row['asset_class'] == 'Equity':
                    stock = stock_exposures.setdefault(row['security_isin'], {'name': row['name'], 'value_inr': 0, 'fund_count': 0})
                    stock['value_inr'] += exposure
                    stock['fund_count'] += 1
        elif symbol in {'GOLDBEES', 'SILVERBEES'}:
            groups['Commodity'] += value
        elif symbol.endswith('-F'):
            groups['Debt'] += value
        else:
            groups['Direct equity / other listed securities'] += value
    pairs = []
    keys = [k for k,v in fund_maps.items() if v]
    names = {r['symbol']: r['display_name'] for r in holdings}
    for i, left in enumerate(keys):
        for right in keys[i+1:]:
            common = set(fund_maps[left]) & set(fund_maps[right])
            pairs.append({'left': names[left], 'right': names[right], 'common_stocks': len(common), 'overlap_pct': sum(min(fund_maps[left][k], fund_maps[right][k]) for k in common)})
    return {'allocation': [{'category': k, 'value_inr': v, 'weight_pct': 100*v/total if total else 0} for k,v in sorted(groups.items(), key=lambda x: -x[1]) if v > 0], 'stocks': sorted(stock_exposures.values(), key=lambda r: -r['value_inr']), 'pairs': pairs, 'coverage': coverage, 'loaded_funds': len(keys)}


def load_verified_user_disclosures(path):
    """Reviewed factsheet aggregates; never treat issuer bonds as stock holdings."""
    snapshots = [
        ('INF109K013N3', '2026-10-31', 'Uploaded ICICI Prudential Short Term Fund.pdf, page 1', [
            ('government', 'Government securities', 'Debt', 14.08),
            ('corporate', 'Corporate securities', 'Debt', 65.73),
            ('ptc', 'Pass through certificates', 'Debt', 7.52),
            ('small-debt', 'Debt below 0.5% of corpus', 'Debt', 8.92),
            ('aif', 'Corporate Debt Market Development Fund (Class A2)', 'Other / AIF', 0.35),
            ('cash', 'TREPS and net current assets', 'Cash / net current assets', 3.40)]),
        ('INF090I01JR0', '2026-09-30', 'Uploaded Franklin factsheet September 30 2026, page 32; underlying stock table is August 31 and top ten only', [
            ('underlying-fund', 'Franklin U.S. Opportunities Fund Class I (Acc)', 'Overseas fund units (look-through pending)', 95.83),
            ('cash', 'Call, cash and other current assets', 'Cash / net current assets', 4.17)])]
    c = connect_database(path)
    try:
        ensure_disclosures(c)
        stamp = datetime.now(timezone.utc).isoformat()
        for fund, day, source, rows in snapshots:
            if abs(sum(r[3] for r in rows)-100) > .01:
                raise ValueError('factsheet_allocation_does_not_reconcile')
            c.executemany('INSERT OR REPLACE INTO portfolio_fund_holdings VALUES (?,?,?,?,?,?,?,?)', [(fund, day, key, name, asset, weight, source, stamp) for key,name,asset,weight in rows])
        c.commit()
    finally:
        c.close()
    return {'factsheets_loaded': 2}


def _refresh_abbreviated_disclosures(path):
    import requests
    from workspace_analytics.portfolio import portfolio_overview
    funds = [r for r in portfolio_overview(path)['holdings'] if r.get('asset_type') == 'mutual_fund']
    supported = [r for r in funds if 'nippon' in r['display_name'].lower() and 'large cap' in r['display_name'].lower()]
    if not supported:
        return {'loaded_funds': 0, 'missing_funds': len(funds)}
    landing = 'https://mf.nipponindiaim.com/investor-service/downloads/factsheet-portfolio-and-other-disclosures/'
    page = requests.get(landing, timeout=20)
    page.raise_for_status()
    links = re.findall(r'href=["\x27]([^"\x27]*Weblink-file-([A-Za-z]+)-(\d{4})\.zip)', page.text, re.I)
    candidates = []
    for link, month, year in links:
        try:
            when = datetime.strptime(f'{month} {year}', '%B %Y')
        except ValueError:
            continue
        candidates.append((when, urljoin(landing, link)))
    if not candidates:
        raise ValueError('nippon_disclosure_link_missing')
    month, url = max(candidates)
    import calendar
    disclosure_date = month.replace(day=calendar.monthrange(month.year, month.month)[1]).date().isoformat()
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    stamp = datetime.now(timezone.utc).isoformat()
    parsed = []
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        filename = next(n for n in archive.namelist() if n.endswith(' - Nippon India Large Cap Fund.xlsx'))
        for rows in worksheet_rows(archive.read(filename)):
            headers = None
            for row in rows:
                if any('ISIN' == v.strip().upper() for v in row.values()):
                    headers = row
                    continue
                if not headers:
                    continue
                isin_col = next((k for k,v in headers.items() if v.strip().upper() == 'ISIN'), None)
                isin = row.get(isin_col, '').strip()
                if not re.fullmatch(r'IN[A-Z0-9]{10}', isin):
                    continue
                weight_col = next((k for k,v in headers.items() if '%' in v and ('NAV' in v.upper() or 'NET' in v.upper())), None)
                name_col = next((k for k,v in headers.items() if 'NAME' in v.upper() and ('INSTRUMENT' in v.upper() or 'ISSUER' in v.upper())), None)
                if not weight_col or not name_col:
                    raise ValueError('disclosure_columns_unrecognised')
                weight = float(row.get(weight_col, '').replace('%', ''))
                # Excel percentage cells store fractions.
                if '%' not in row.get(weight_col, ''):
                    weight *= 100
                if not 0 <= weight <= 100:
                    raise ValueError('disclosure_weight_invalid')
                parsed.append((isin, row.get(name_col, isin), 'Equity' if isin.startswith('INE') else 'Other', weight))
    total = sum(r[3] for r in parsed)
    if not parsed or not 50 <= total <= 100.5:
        raise ValueError(f'disclosure_coverage_invalid:{total:.2f}')
    c = connect_database(path)
    try:
        ensure_disclosures(c)
        for fund in supported:
            c.execute('DELETE FROM portfolio_fund_holdings WHERE fund_isin=? AND disclosure_date=?', (fund['symbol'], disclosure_date))
            c.executemany('INSERT INTO portfolio_fund_holdings VALUES (?,?,?,?,?,?,?,?)', [(fund['symbol'], disclosure_date, isin, name, asset, weight, url, stamp) for isin,name,asset,weight in parsed])
        c.commit()
    finally:
        c.close()
    return {'loaded_funds': len(supported), 'missing_funds': len(funds)-len(supported), 'disclosure_date': disclosure_date, 'securities': len(parsed)}


def refresh_disclosures(path):
    import requests
    from workspace_analytics.portfolio import portfolio_overview
    funds = [r for r in portfolio_overview(path)['holdings'] if r.get('asset_type') == 'mutual_fund']
    supported = [r for r in funds if 'nippon' in r['display_name'].lower() and 'large cap' in r['display_name'].lower()]
    if not supported:
        return {'loaded_funds': 0, 'missing_funds': len(funds)}
    landing = 'https://mf.nipponindiaim.com/investor-service/downloads/factsheet-portfolio-and-other-disclosures/'
    response = requests.get(landing, timeout=20)
    response.raise_for_status()
    links = re.findall(r'href=["\x27]([^"\x27]*NIMF-MONTHLY-PORTFOLIO-(\d{2}-[A-Za-z]{3}-\d{2})\.xls)', response.text, re.I)
    candidates = [(datetime.strptime(day, '%d-%b-%y'), urljoin(landing, link)) for link,day in links]
    if not candidates:
        raise ValueError('nippon_monthly_disclosure_missing')
    moment, url = max(candidates)
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    parsed = []
    for rows in worksheet_rows(response.content):
        if not any('nippon india large cap fund' in str(v).lower() for row in rows[:3] for v in row.values()):
            continue
        asset = None
        for row in rows[4:]:
            label = row.get('C', '').strip()
            lower = label.lower()
            if 'equity & equity' in lower:
                asset = 'Equity'
            elif any(word in lower for word in ('debt instruments', 'money market', 'treasury', 'government securities', 'corporate debt')):
                asset = 'Debt'
            elif 'gold' in lower or 'silver' in lower:
                asset = 'Commodity'
            isin = row.get('B', '').strip()
            if not re.fullmatch(r'IN[A-Z0-9]{10}', isin):
                continue
            weight = float(row.get('G', '0')) * 100
            if not 0 <= weight <= 100:
                raise ValueError('invalid_disclosure_weight')
            parsed.append((isin, label, asset or 'Other', weight))
    total = sum(r[3] for r in parsed)
    if not parsed or not 50 <= total <= 100.5:
        raise ValueError(f'disclosure_coverage_invalid:{total:.2f}')
    c = connect_database(path)
    try:
        ensure_disclosures(c)
        stamp = datetime.now(timezone.utc).isoformat()
        for fund in supported:
            c.execute('DELETE FROM portfolio_fund_holdings WHERE fund_isin=? AND disclosure_date=?', (fund['symbol'], moment.date().isoformat()))
            c.executemany('INSERT INTO portfolio_fund_holdings VALUES (?,?,?,?,?,?,?,?)', [(fund['symbol'], moment.date().isoformat(), isin, name, asset, weight, url, stamp) for isin,name,asset,weight in parsed])
        c.commit()
    finally:
        c.close()
    return {'loaded_funds': len(supported), 'missing_funds': len(funds)-len(supported), 'securities': len(parsed), 'disclosure_date': moment.date().isoformat()}
