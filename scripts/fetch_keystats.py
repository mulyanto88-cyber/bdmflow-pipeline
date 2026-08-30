# =============================================================================
# STOCKBIT KEY STATS & FUNDAMENTALS PIPELINE — Burst + Cooldown Edition
#
# KEY INSIGHT: Stockbit throttles after ~50 consecutive requests per token,
# returning empty 200 OK payloads. Solution: process in bursts of 45 stocks
# with 60-second cooldowns between bursts to let the rate-limit window reset.
#
# Math: 978 stocks / 45 per burst = 22 bursts
#       Each burst: 45 × 1.5s = 68s + 60s cooldown = 128s
#       Total: 22 × 128s = 2816s ≈ 47 minutes (fits GitHub Actions 60m limit)
#       Expected success rate: ~95%+ (vs 14% with continuous processing)
#
# Endpoint: https://exodus.stockbit.com/keystats/ratio/v1/{STOCK_CODE}?year_limit=10
# =============================================================================
import os
import sys
import json
import time
import random
import re
import requests
import duckdb
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build

from stockbit_auth import get_or_refresh_token

# =============================================================================
# CONFIG
# =============================================================================
SA_JSON          = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'])
MOTHERDUCK_TOKEN = os.environ['MOTHERDUCK_TOKEN']
TOKEN_SHEET_ID   = os.environ['TOKEN_SHEET_ID']

MOTHERDUCK_DB    = 'my_db'
MD_SCHEMA        = 'market'
MD_TABLE         = 'company_keystats'
BASE_URL         = 'https://exodus.stockbit.com/keystats/ratio/v1'

BURST_SIZE       = 40   # Max requests before Stockbit throttles
COOLDOWN_SECS    = 30   # Seconds to wait between bursts for rate-limit reset
INTER_REQ_DELAY  = 1.3  # Seconds between individual requests within a burst
RUN_LIMIT        = 120  # Total stocks to fetch per run in Rolling Update mode

# All 50 columns in the exact order they exist in MotherDuck.
DB_COLUMNS = [
    'stock_code', 'market_cap_b', 'enterprise_value_b', 'shares_outstanding_b',
    'free_float_pct', 'pe_ratio_ttm', 'pe_ratio_annualized', 'forward_pe',
    'pbv_ratio', 'ps_ratio', 'ev_ebitda', 'ev_ebit', 'peg_ratio',
    'earnings_yield_pct', 'p_fcf_ratio', 'eps_ttm', 'eps_annualized', 'bvps',
    'revenue_per_share', 'cash_per_share', 'fcf_per_share', 'roe_ttm_pct',
    'roa_ttm_pct', 'roce_ttm_pct', 'gpm_quarter_pct', 'opm_quarter_pct',
    'npm_quarter_pct', 'revenue_growth_yoy_pct', 'gross_profit_growth_yoy',
    'net_income_growth_yoy', 'debt_to_equity', 'current_ratio', 'quick_ratio',
    'interest_coverage', 'piotroski_f_score', 'altman_z_score', 'revenue_ttm_b',
    'gross_profit_ttm_b', 'ebitda_ttm_b', 'net_income_ttm_b', 'cash_quarter_b',
    'total_assets_b', 'total_liabilities_b', 'total_equity_b', 'total_debt_b',
    'net_debt_b', 'cash_from_ops_ttm_b', 'free_cash_flow_ttm_b',
    'period_latest', 'updated_at',
]

# =============================================================================
# HELPERS & PARSERS
# =============================================================================
def authenticate():
    creds = service_account.Credentials.from_service_account_info(
        SA_JSON,
        scopes=[
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets'
        ]
    )
    return build('sheets', 'v4', credentials=creds, cache_discovery=False)

def make_headers(token, stock_code=None):
    ref = f'https://stockbit.com/symbol/{stock_code}/keystats' if stock_code else 'https://stockbit.com/'
    return {
        'Authorization':   f'Bearer {token}',
        'Accept':          'application/json, text/plain, */*',
        'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Referer':         ref,
        'Origin':          'https://stockbit.com',
        'sec-ch-ua':       '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        'sec-ch-ua-mobile':'?0',
        'sec-ch-ua-platform':'"Windows"',
        'sec-fetch-dest':  'empty',
        'sec-fetch-mode':  'cors',
        'sec-fetch-site':  'same-site',
    }

def clean_val(val_str):
    if val_str is None:
        return None
    s = str(val_str).strip()
    if not s or s in ('-', 'N/A', 'null', 'None'):
        return None
    is_negative = False
    if s.startswith('(') and s.endswith(')'):
        is_negative = True
        s = s[1:-1].strip()
    elif s.startswith('-'):
        is_negative = True
        s = s[1:].strip()
    s = s.replace(',', '').replace('%', '').replace('+', '').strip()
    s = re.sub(r'[BMTK]$', '', s).strip()
    try:
        num = float(s)
        return -num if is_negative else num
    except Exception:
        return None

def get_stale_stock_codes(con, limit=120):
    """Fetch the most stale stocks (oldest updated_at or missing) for rolling update."""
    try:
        rows = con.execute(f"""
            WITH canonical_stocks AS (
                SELECT DISTINCT stock_code
                FROM market.company_profile
                WHERE stock_code IS NOT NULL AND LENGTH(stock_code) = 4
            )
            SELECT c.stock_code
            FROM canonical_stocks c
            LEFT JOIN {MD_SCHEMA}.{MD_TABLE} k ON c.stock_code = k.stock_code
            ORDER BY k.updated_at ASC NULLS FIRST
            LIMIT {limit}
        """).fetchall()
        if rows:
            return [r[0] for r in rows]
    except Exception as e:
        print(f"⚠️ Gagal ambil list stale stocks: {e}")
        
    # Fallback if company_profile fails
    try:
        rows = con.execute(f"""
            WITH canonical_stocks AS (
                SELECT DISTINCT stock_code
                FROM market.daily_transactions
                WHERE stock_code NOT IN ('COMPOSITE', 'IHSG', 'IDX30', 'LQ45')
                  AND stock_code IS NOT NULL AND LENGTH(stock_code) = 4
            )
            SELECT c.stock_code
            FROM canonical_stocks c
            LEFT JOIN {MD_SCHEMA}.{MD_TABLE} k ON c.stock_code = k.stock_code
            ORDER BY k.updated_at ASC NULLS FIRST
            LIMIT {limit}
        """).fetchall()
        if rows:
            return [r[0] for r in rows]
    except Exception as e:
        print(f"⚠️ Gagal ambil list stale stocks fallback: {e}")
        return []


def fetch_one(session, token, stock_code, sheets_svc=None, sheet_id=None):
    """Fetch key stats for one stock. Returns (row_dict, token)."""
    headers = make_headers(token, stock_code)
    url = f'{BASE_URL}/{stock_code}?year_limit=10'

    for attempt in range(3):
        try:
            r = session.get(url, headers=headers, timeout=20)

            if r.status_code in (401, 403) and sheets_svc and sheet_id:
                print(f" [token refresh]", end='', flush=True)
                token = get_or_refresh_token(sheets_svc, sheet_id)
                headers = make_headers(token, stock_code)
                time.sleep(2)
                continue

            if r.status_code == 429:
                wait = 8 + attempt * 5
                print(f" [429→{wait}s]", end='', flush=True)
                time.sleep(wait)
                continue

            r.raise_for_status()
            data = (r.json().get('data') or {})
            stats = data.get('stats') or {}
            closure_results = data.get('closure_fin_items_results') or []

            if not stats and not closure_results:
                if attempt < 2:
                    time.sleep(2.0 + attempt * 2.0 + random.uniform(0.5, 1.5))
                    continue

            item_map = {}
            for group in closure_results:
                for sub in group.get('fin_name_results', []):
                    fitem = sub.get('fitem', {})
                    name = fitem.get('name')
                    if name:
                        item_map[name.strip()] = clean_val(fitem.get('value'))

            latest_period = 'Latest'
            for group in data.get('financial_year_parent', {}).get('financial_year_groups', []):
                mrq = group.get('most_recent_quarter', {})
                if mrq.get('quarter') and mrq.get('date'):
                    latest_period = f"{mrq['quarter']} ({mrq['date']})"
                    break

            row = {
                'stock_code':              stock_code,
                'market_cap_b':            clean_val(stats.get('market_cap')),
                'enterprise_value_b':      clean_val(stats.get('enterprise_value')),
                'shares_outstanding_b':    clean_val(stats.get('current_share_outstanding')),
                'free_float_pct':          clean_val(stats.get('free_float')),
                'pe_ratio_ttm':            item_map.get('Current PE Ratio (TTM)'),
                'pe_ratio_annualized':     item_map.get('Current PE Ratio (Annualised)'),
                'forward_pe':              item_map.get('Forward PE Ratio'),
                'pbv_ratio':               item_map.get('Current Price to Book Value'),
                'ps_ratio':                item_map.get('Current Price to Sales (TTM)'),
                'ev_ebitda':               item_map.get('EV to EBITDA (TTM)'),
                'ev_ebit':                 item_map.get('EV to EBIT (TTM)'),
                'peg_ratio':               item_map.get('PEG Ratio'),
                'earnings_yield_pct':      item_map.get('Earnings Yield (TTM)'),
                'p_fcf_ratio':             item_map.get('Current Price To Free Cashflow (TTM)'),
                'eps_ttm':                 item_map.get('Current EPS (TTM)'),
                'eps_annualized':          item_map.get('Current EPS (Annualised)'),
                'bvps':                    item_map.get('Current Book Value Per Share'),
                'revenue_per_share':       item_map.get('Revenue Per Share (TTM)'),
                'cash_per_share':          item_map.get('Cash Per Share (Quarter)'),
                'fcf_per_share':           item_map.get('Free Cashflow Per Share (TTM)'),
                'roe_ttm_pct':             item_map.get('Return on Equity (TTM)'),
                'roa_ttm_pct':             item_map.get('Return on Assets (TTM)'),
                'roce_ttm_pct':            item_map.get('Return on Capital Employed (TTM)'),
                'gpm_quarter_pct':         item_map.get('Gross Profit Margin (Quarter)'),
                'opm_quarter_pct':         item_map.get('Operating Profit Margin (Quarter)'),
                'npm_quarter_pct':         item_map.get('Net Profit Margin (Quarter)'),
                'revenue_growth_yoy_pct':  item_map.get('Revenue (Quarter YoY Growth)'),
                'gross_profit_growth_yoy': item_map.get('Gross Profit (Quarter YoY Growth)'),
                'net_income_growth_yoy':   item_map.get('Net Income (Quarter YoY Growth)'),
                'debt_to_equity':          item_map.get('Debt to Equity Ratio (Quarter)'),
                'current_ratio':           item_map.get('Current Ratio (Quarter)'),
                'quick_ratio':             item_map.get('Quick Ratio (Quarter)'),
                'interest_coverage':       item_map.get('Interest Coverage (TTM)'),
                'piotroski_f_score':       item_map.get('Piotroski F-Score'),
                'altman_z_score':          item_map.get('Altman Z-Score (Modified)'),
                'revenue_ttm_b':           item_map.get('Revenue (TTM)'),
                'gross_profit_ttm_b':      item_map.get('Gross Profit (TTM)'),
                'ebitda_ttm_b':            item_map.get('EBITDA (TTM)'),
                'net_income_ttm_b':        item_map.get('Net Income (TTM)'),
                'cash_quarter_b':          item_map.get('Cash (Quarter)'),
                'total_assets_b':          item_map.get('Total Assets (Quarter)'),
                'total_liabilities_b':     item_map.get('Total Liabilities (Quarter)'),
                'total_equity_b':          item_map.get('Total Equity (Quarter)'),
                'total_debt_b':            item_map.get('Total Debt (Quarter)'),
                'net_debt_b':              item_map.get('Net Debt (Quarter)'),
                'cash_from_ops_ttm_b':     item_map.get('Cash From Operations (TTM)'),
                'free_cash_flow_ttm_b':    item_map.get('Free Cash Flow (TTM)'),
                'period_latest':           latest_period,
                'updated_at':              datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }
            return row, token
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(1.5 + attempt * 2)

    raise RuntimeError(f"Max retries for {stock_code}")

def commit_batch(con, rows):
    """Column-safe upsert into MotherDuck."""
    if not rows:
        return 0
    import pandas as pd
    valid = [r for r in rows if any(r.get(k) is not None for k in
             ('pe_ratio_ttm', 'pbv_ratio', 'roe_ttm_pct', 'market_cap_b', 'free_float_pct'))]
    if not valid:
        return 0
    for row in valid:
        for col in DB_COLUMNS:
            row.setdefault(col, None)
    df = pd.DataFrame(valid, columns=DB_COLUMNS)
    con.register('df_batch', df)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {MD_SCHEMA}.{MD_TABLE} AS SELECT * FROM df_batch LIMIT 0;
        DELETE FROM {MD_SCHEMA}.{MD_TABLE} WHERE stock_code IN (SELECT stock_code FROM df_batch);
        INSERT INTO {MD_SCHEMA}.{MD_TABLE} SELECT * FROM df_batch;
    """)
    con.unregister('df_batch')
    return len(valid)

def is_valid(row):
    return any(row.get(k) is not None for k in
               ('pe_ratio_ttm', 'pbv_ratio', 'roe_ttm_pct', 'market_cap_b', 'free_float_pct'))

# =============================================================================
# MAIN PIPELINE — BURST + COOLDOWN ARCHITECTURE
# =============================================================================
def main():
    print("=" * 65)
    print("🚀 STOCKBIT KEY STATS PIPELINE (BURST + COOLDOWN EDITION)")
    print(f"⏰ Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📐 Strategy: {BURST_SIZE} stocks/burst → {COOLDOWN_SECS}s cooldown")
    print("=" * 65)

    # 1. Auth
    print("\n🔐 Google Sheets Auth...")
    sheets_svc = authenticate()
    token = get_or_refresh_token(sheets_svc, TOKEN_SHEET_ID)

    # 2. MotherDuck
    print("🦆 MotherDuck...")
    con = duckdb.connect(f'md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}')
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {MD_SCHEMA}")

    # 3. Stock list (Rolling Update Strategy)
    args = sys.argv[1:]
    if '--stocks' in args:
        idx = args.index('--stocks')
        stocks = [s.strip().upper() for s in args[idx + 1].split(',')] if idx + 1 < len(args) else []
        print(f"🎯 Custom: {len(stocks)} saham")
    else:
        limit = RUN_LIMIT
        if '--limit' in args:
            idx = args.index('--limit')
            if idx + 1 < len(args):
                limit = int(args[idx + 1])
                
        stocks = get_stale_stock_codes(con, limit=limit)
        print(f"📋 Mode Rolling Update: Menarik {len(stocks)} saham paling usang (limit={limit})")

    # 4. Split into bursts
    bursts = [stocks[i:i + BURST_SIZE] for i in range(0, len(stocks), BURST_SIZE)]
    num_bursts = len(bursts)
    est_time = num_bursts * (BURST_SIZE * INTER_REQ_DELAY + COOLDOWN_SECS)
    print(f"📦 {num_bursts} burst × {BURST_SIZE} saham (est. {est_time/60:.0f} menit)")

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=1)
    session.mount('https://', adapter)

    start_time = time.time()
    total_success = 0
    total_empty = 0
    total_fail = 0
    all_empty_codes = []
    committed_total = 0
    global_idx = 0
    total_stocks = len(stocks)

    for burst_num, burst_stocks in enumerate(bursts, 1):
        burst_success = 0
        burst_empty = 0
        batch_buffer = []

        print(f"\n{'─' * 50}")
        print(f"🔥 BURST {burst_num}/{num_bursts} ({len(burst_stocks)} saham)")
        print(f"{'─' * 50}")

        for code in burst_stocks:
            global_idx += 1
            print(f"[{global_idx}/{total_stocks}] {code}... ", end='', flush=True)
            try:
                row, token = fetch_one(session, token, code, sheets_svc, TOKEN_SHEET_ID)
                if is_valid(row):
                    burst_success += 1
                    batch_buffer.append(row)
                    print(f"✅ PER:{row['pe_ratio_ttm'] or '-'} PBV:{row['pbv_ratio'] or '-'} ROE:{row['roe_ttm_pct'] or '-'}%")
                else:
                    burst_empty += 1
                    all_empty_codes.append(code)
                    print("⚠️ KOSONG")
            except Exception as e:
                total_fail += 1
                print(f"❌ {str(e)[:50]}")

            time.sleep(INTER_REQ_DELAY + random.uniform(0.0, 0.4))

        # Commit this burst's results
        if batch_buffer:
            n = commit_batch(con, batch_buffer)
            committed_total += n
            print(f"   💾 Burst {burst_num}: {n} saham → MotherDuck (total: {committed_total})")

        total_success += burst_success
        total_empty += burst_empty

        # Early termination detection: if burst had >80% empty, the throttle
        # is active — increase cooldown for this pause
        empty_pct = burst_empty / max(len(burst_stocks), 1)
        if empty_pct > 0.8 and burst_num < num_bursts:
            extended = COOLDOWN_SECS + 30
            print(f"   ⚠️ Burst ke-{burst_num} terlalu banyak KOSONG ({burst_empty}/{len(burst_stocks)}) → extra cooldown {extended}s")
            time.sleep(extended)
        elif burst_num < num_bursts:
            print(f"   ✅ Burst {burst_num}: {burst_success}/{len(burst_stocks)} sukses — cooldown {COOLDOWN_SECS}s...")
            time.sleep(COOLDOWN_SECS)

    elapsed_main = time.time() - start_time
    print("\n" + "=" * 65)
    print(f"🏁 Ronde Utama: {elapsed_main:.0f}s ({elapsed_main/60:.1f}m)")
    print(f"   ✅ {total_success} · ⚠️ {total_empty} · ❌ {total_fail}")
    print("=" * 65)

    # 5. Retry pass for empty codes — same burst+cooldown approach
    retry_rounds = 1
    if '--retry-rounds' in args:
        idx = args.index('--retry-rounds')
        if idx + 1 < len(args):
            try: retry_rounds = max(0, min(3, int(args[idx + 1])))
            except: pass

    recovered_total = 0
    for rnd in range(1, retry_rounds + 1):
        if not all_empty_codes:
            break

        print(f"\n🔁 Retry ronde {rnd}: {len(all_empty_codes)} saham...")
        time.sleep(30)  # Initial cooldown before retry

        retry_bursts = [all_empty_codes[i:i + BURST_SIZE] for i in range(0, len(all_empty_codes), BURST_SIZE)]
        still_empty = []

        for rb_num, rb_stocks in enumerate(retry_bursts, 1):
            retry_buffer = []
            rb_empty = []

            print(f"   🔄 Retry burst {rb_num}/{len(retry_bursts)}...")
            for code in rb_stocks:
                print(f"   ↻ {code}... ", end='', flush=True)
                try:
                    row, token = fetch_one(session, token, code, sheets_svc, TOKEN_SHEET_ID)
                    if is_valid(row):
                        recovered_total += 1
                        retry_buffer.append(row)
                        print("✅ PULIH!")
                    else:
                        rb_empty.append(code)
                        print("masih KOSONG")
                except Exception as e:
                    rb_empty.append(code)
                    print(f"gagal: {str(e)[:30]}")
                time.sleep(INTER_REQ_DELAY + random.uniform(0.1, 0.5))

            still_empty.extend(rb_empty)

            if retry_buffer:
                n = commit_batch(con, retry_buffer)
                committed_total += n
                print(f"   💾 Retry burst {rb_num}: {n} saham → MotherDuck!")

            if rb_num < len(retry_bursts):
                time.sleep(COOLDOWN_SECS)

        all_empty_codes = still_empty

    # 6. Final summary
    total_in_db = con.execute(f"SELECT COUNT(*) FROM {MD_SCHEMA}.{MD_TABLE}").fetchone()[0]
    elapsed_total = time.time() - start_time
    print("\n" + "=" * 65)
    print(f"🎉 PIPELINE SELESAI dalam {elapsed_total:.0f}s ({elapsed_total/60:.1f}m)")
    print(f"   ✅ Sukses: {total_success + recovered_total} · ⚠️ Kosong: {len(all_empty_codes)} · ❌ Gagal: {total_fail}")
    print(f"   📊 Total di {MD_SCHEMA}.{MD_TABLE}: {total_in_db} emiten")
    print("=" * 65)
    con.close()

if __name__ == '__main__':
    main()
