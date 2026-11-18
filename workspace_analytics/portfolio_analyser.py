"""Position-based analysis, independent of cash-flow performance gates."""
from collections import defaultdict
from workspace_analytics.portfolio import portfolio_overview
from workspace_analytics.sqlite_store import connect_database
from datetime import datetime, timezone
import csv
import io


def ensure_fund_analysis(connection):
    connection.execute('''CREATE TABLE IF NOT EXISTS portfolio_fund_metadata (
        isin TEXT PRIMARY KEY, scheme_code TEXT, name TEXT, amc TEXT,
        category TEXT, nav REAL, nav_date TEXT, retrieved_at TEXT, source TEXT)''')


def refresh_held_funds(path):
    import requests
    url = 'https://portal.amfiindia.com/spages/NAVAll.txt'
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    holdings = portfolio_overview(path)['holdings']
    held = {r['symbol'] for r in holdings if r.get('asset_type') == 'mutual_fund'}
    amc = category = ''
    matched = {}
    stamp = datetime.now(timezone.utc).isoformat()
    for row in csv.reader(io.StringIO(response.text), delimiter=';'):
        if not row or not any(v.strip() for v in row):
            continue
        if len(row) == 1:
            label = row[0].strip()
            if label.startswith(('Open Ended', 'Close Ended', 'Interval Fund')):
                category = label
            elif label != 'Scheme Code':
                amc = label
            continue
        if len(row) < 6 or not row[0].strip().isdigit():
            continue
        keys = set(v.strip() for v in row[1:3]) | {f'MF-{row[0].strip()}'}
        for isin in held.intersection(keys):
            try:
                nav = float(row[-2])
                nav_date = datetime.strptime(row[-1].strip(), '%d-%b-%Y').date().isoformat()
            except (ValueError, TypeError):
                continue
            if nav > 0:
                name = ' · '.join(v.strip() for v in row[3:-2])
                matched[isin] = (isin, row[0].strip(), name, amc, category, nav, nav_date, stamp, url)
    connection = connect_database(path)
    try:
        ensure_fund_analysis(connection)
        connection.executemany('INSERT OR REPLACE INTO portfolio_fund_metadata VALUES (?,?,?,?,?,?,?,?,?)', matched.values())
        connection.commit()
    finally:
        connection.close()
    return {'ok': True, 'held_funds': len(held), 'matched': len(matched), 'missing': sorted(held-set(matched)), 'retrieved_at': stamp}


def held_fund_analysis(path, holdings, total):
    connection = connect_database(path)
    try:
        ensure_fund_analysis(connection)
        metadata = {r['isin']: dict(r) for r in connection.execute('SELECT * FROM portfolio_fund_metadata')}
        connection.commit()
    finally:
        connection.close()
    funds = []
    amcs = defaultdict(float)
    categories = defaultdict(float)
    for row in holdings:
        if row.get('asset_type') != 'mutual_fund':
            continue
        info = metadata.get(row['symbol'], {})
        value = row['market_value_inr'] or 0
        amcs[info.get('amc') or 'Unmatched AMC'] += value
        categories[info.get('category') or 'Unmatched category'] += value
        funds.append({**row, 'metadata': info, 'weight_pct': 100*value/total if total else 0})
    fund_total = sum(r['market_value_inr'] or 0 for r in funds)
    def weights(groups):
        return [{'name': k, 'value_inr': v, 'portfolio_weight_pct': 100*v/total if total else 0, 'fund_weight_pct': 100*v/fund_total if fund_total else 0} for k,v in sorted(groups.items(), key=lambda x: -x[1])]
    return {'funds': funds, 'amcs': weights(amcs), 'categories': weights(categories), 'matched_count': sum(bool(r['metadata']) for r in funds), 'fund_count': len(funds)}


def portfolio_analyser(path):
    overview = portfolio_overview(path)
    holdings = overview['holdings']
    total = sum(row['market_value_inr'] or 0 for row in holdings)
    groups = defaultdict(float)
    exposures = []
    missing = []
    for row in holdings:
        name = row['display_name'].upper()
        symbol = row['symbol']
        # These are instrument wrappers, not inferred underlying fund allocations.
        if row.get('asset_type') == 'mutual_fund':
            category = 'Mutual funds'
            missing.append({'symbol': symbol, 'name': row['display_name'],
                            'value_inr': row['market_value_inr'],
                            'reason': 'Underlying AMC portfolio disclosure not loaded'})
        elif symbol in {'GOLDBEES', 'SILVERBEES'}:
            category = 'Gold ETF' if symbol == 'GOLDBEES' else 'Silver ETF'
        elif symbol.startswith('INF') or 'ETF' in name or 'BEES' in symbol:
            category = 'Other funds / ETFs'
            missing.append({'symbol': symbol, 'name': row['display_name'], 'value_inr': row['market_value_inr'], 'reason': 'Underlying portfolio not loaded'})
        elif symbol.startswith('INE') or 'DEBT' in name or symbol.endswith('-F'):
            category = 'Debt / other listed securities'
        else:
            category = 'Direct stocks / other securities'
        value = row['market_value_inr'] or 0
        groups[category] += value
        exposures.append({**row, 'category': category,
                          'weight_pct': 100 * value / total if total else None})
    ranked = sorted(exposures, key=lambda r: -(r['market_value_inr'] or 0))
    weights = [(r['market_value_inr'] or 0) / total for r in ranked] if total else []
    hhi = sum(w*w for w in weights)
    from workspace_analytics.fund_disclosures import lookthrough
    return {'ok': True, 'lookthrough': lookthrough(path, holdings, total), 'fund_analysis': held_fund_analysis(path, holdings, total), 'holdings': ranked, 'total_value_inr': total,
            'largest_weight_pct': 100 * max(weights) if weights else None,
            'top_three_pct': 100 * sum(weights[:3]) if weights else None,
            'effective_positions': 1 / hhi if hhi else None,
            'unpriced_count': sum(r['market_value_inr'] is None for r in holdings),
            'allocation': [{'category': key, 'value_inr': value, 'weight_pct': 100*value/total if total else 0} for key, value in sorted(groups.items(), key=lambda item: -item[1])],
            'fund_disclosure_gaps': missing,
            'overlap_status': 'unavailable' if missing else 'no_fund_holdings',
            'price_dates': sorted({r['price_as_of_date'] for r in holdings if r['price_as_of_date']}),
            'method': 'Current priced holdings only. Fund wrappers are not underlying asset allocation. Unpriced holdings excluded from weights. Concentration includes each fund as one position.'}
