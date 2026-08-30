# =============================================================================
# STOCKBIT KEY STATS & FUNDAMENTALS PIPELINE — High-Performance Parallel Edition
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
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
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

# Global Lock for DB Writes & Token Refreshes
db_lock          = threading.Lock()
token_lock       = threading.Lock()
global_token     = None

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

def get_stock_codes(con):
    try:
        rows = con.execute("""
            SELECT DISTINCT stock_code
            FROM market.daily_transactions
            WHERE stock_code NOT IN ('COMPOSITE', 'IHSG', 'IDX30', 'LQ45')
              AND stock_code IS NOT NULL
              AND LENGTH(stock_code) = 4
            ORDER BY stock_code ASC
        """).fetchall()
        if rows:
            return [r[0] for r in rows]
    except Exception as e:
        print(f"⚠️ Gagal ambil list dari daily_transactions: {e}")

    try:
        rows = con.execute("""
            SELECT DISTINCT stock_code
            FROM market.company_profile
            WHERE stock_code IS NOT NULL
            ORDER BY stock_code ASC
        """).fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        print(f"⚠️ Gagal ambil list dari company_profile: {e}")
        return ['BBCA', 'BBRI', 'BMRI', 'BBNI', 'ASII', 'TLKM', 'BRMS', 'AMMN', 'BRIS', 'ADRO']

def fetch_keystats_single(session, stock_code, sheets_svc=None, sheet_id=None):
    global global_token
    token = global_token
    headers = make_headers(token, stock_code)
    url = f'{BASE_URL}/{stock_code}?year_limit=10'
    
    for attempt in range(3):
        try:
            r = session.get(url, headers=headers, timeout=15)
            
            # Handle token expiration (401/403)
            if r.status_code in (401, 403) and sheets_svc and sheet_id:
                with token_lock:
                    if token == global_token: # only one thread triggers refresh
                        print(f"\n🔄 Token refresh otomatis untuk {stock_code}...", flush=True)
                        global_token = get_or_refresh_token(sheets_svc, sheet_id)
                    token = global_token
                headers = make_headers(token, stock_code)
                time.sleep(1.5)
                continue

            # Handle rate limiting (429)
            if r.status_code == 429:
                time.sleep(3 + attempt * 2 + random.uniform(0.5, 1.5))
                continue

            r.raise_for_status()
            res_json = r.json()
            data = res_json.get('data') or {}

            stats = data.get('stats') or {}
            closure_results = data.get('closure_fin_items_results') or []

            # If payload is empty, jitter and retry once
            if not stats and not closure_results:
                if attempt < 2:
                    time.sleep(1.5 + attempt * 1.5)
                    continue

            mcap_b        = clean_val(stats.get('market_cap'))
            shares_out_b  = clean_val(stats.get('current_share_outstanding'))
            ev_b          = clean_val(stats.get('enterprise_value'))
            free_float    = clean_val(stats.get('free_float'))

            item_map = {}
            for group in closure_results:
                for sub in group.get('fin_name_results', []):
                    fitem = sub.get('fitem', {})
                    name = fitem.get('name')
                    val = fitem.get('value')
                    if name:
                        item_map[name.strip()] = clean_val(val)

            latest_period = 'Latest'
            for group in data.get('financial_year_parent', {}).get('financial_year_groups', []):
                mrq = group.get('most_recent_quarter', {})
                if mrq.get('quarter') and mrq.get('date'):
                    latest_period = f"{mrq.get('quarter')} ({mrq.get('date')})"
                    break

            return {
                'stock_code':              stock_code,
                'market_cap_b':            mcap_b,
                'enterprise_value_b':      ev_b,
                'shares_outstanding_b':    shares_out_b,
                'free_float_pct':          free_float,
                
                # Valuation Ratios
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

                # Per Share
                'eps_ttm':                 item_map.get('Current EPS (TTM)'),
                'eps_annualized':          item_map.get('Current EPS (Annualised)'),
                'bvps':                    item_map.get('Current Book Value Per Share'),
                'revenue_per_share':       item_map.get('Revenue Per Share (TTM)'),
                'cash_per_share':          item_map.get('Cash Per Share (Quarter)'),
                'fcf_per_share':           item_map.get('Free Cashflow Per Share (TTM)'),

                # Profitability & Margins
                'roe_ttm_pct':             item_map.get('Return on Equity (TTM)'),
                'roa_ttm_pct':             item_map.get('Return on Assets (TTM)'),
                'roce_ttm_pct':            item_map.get('Return on Capital Employed (TTM)'),
                'gpm_quarter_pct':         item_map.get('Gross Profit Margin (Quarter)'),
                'opm_quarter_pct':         item_map.get('Operating Profit Margin (Quarter)'),
                'npm_quarter_pct':         item_map.get('Net Profit Margin (Quarter)'),

                # Growth
                'revenue_growth_yoy_pct':  item_map.get('Revenue (Quarter YoY Growth)'),
                'gross_profit_growth_yoy': item_map.get('Gross Profit (Quarter YoY Growth)'),
                'net_income_growth_yoy':   item_map.get('Net Income (Quarter YoY Growth)'),

                # Solvency & Financial Health
                'debt_to_equity':          item_map.get('Debt to Equity Ratio (Quarter)'),
                'current_ratio':           item_map.get('Current Ratio (Quarter)'),
                'quick_ratio':             item_map.get('Quick Ratio (Quarter)'),
                'interest_coverage':       item_map.get('Interest Coverage (TTM)'),
                'piotroski_f_score':       item_map.get('Piotroski F-Score'),
                'altman_z_score':          item_map.get('Altman Z-Score (Modified)'),

                # Financial Statements (in Billion IDR)
                'revenue_ttm_b':           item_map.get('Revenue (TTM)'),
                'gross_profit_ttm_b':      item_map.get('Gross Profit (TTM)'),
                'ebitda_ttm_b':            item_map.get('EBITDA (TTM)'),
                'net_income_ttm_b':        item_map.get('Net Income (TTM)'),
                'cash_quarter_b':          item_map.get('Cash (Quarter)'),
                'total_assets_b':          item_map.get('Total Assets (Quarter)'),
                'total_liabilities_b':     item_map.get('Total Liabilities (Quarter)'),
                'total_equity_b':          item_map.get('Total Equity (Quarter)'),
                'free_cash_flow_ttm_b':    item_map.get('Free Cash Flow (TTM)'),
                
                'period_latest':           latest_period,
                'updated_at':              datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        except Exception as e:
            if attempt == 2:
                return {'stock_code': stock_code, 'error': str(e)}
            time.sleep(1.0 + attempt)

    return {'stock_code': stock_code, 'error': 'Max attempts reached'}

def commit_batch_to_db(con, batch_rows):
    if not batch_rows:
        return
    import pandas as pd
    valid_rows = [
        r for r in batch_rows 
        if r.get('pe_ratio_ttm') is not None 
        or r.get('pbv_ratio') is not None 
        or r.get('roe_ttm_pct') is not None 
        or r.get('market_cap_b') is not None
        or r.get('free_float_pct') is not None
    ]
    if not valid_rows:
        return

    with db_lock:
        df = pd.DataFrame(valid_rows)
        con.register('df_batch', df)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {MD_SCHEMA}.{MD_TABLE} AS SELECT * FROM df_batch LIMIT 0;
            DELETE FROM {MD_SCHEMA}.{MD_TABLE} WHERE stock_code IN (SELECT stock_code FROM df_batch);
            INSERT INTO {MD_SCHEMA}.{MD_TABLE} SELECT * FROM df_batch;
        """)
        con.unregister('df_batch')
        print(f"   💾 [DB] Committed chunk of {len(valid_rows)} valid stocks to MotherDuck!", flush=True)

# =============================================================================
# MAIN PIPELINE
# =============================================================================
def main():
    global global_token
    print("=" * 65)
    print("🚀 STOCKBIT KEY STATS & FUNDAMENTALS PIPELINE (PARALLEL EDITION)")
    print(f"⏰ Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    # 1. Google Sheets Auth & Token
    print("\n🔐 Menghubungkan ke Google Sheets...")
    sheets_svc = authenticate()
    global_token = get_or_refresh_token(sheets_svc, TOKEN_SHEET_ID)

    # 2. MotherDuck Connection
    print("\n🦆 Menghubungkan ke MotherDuck...")
    con = duckdb.connect(f'md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}')
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {MD_SCHEMA}")

    # 3. Stock List & CLI Arguments
    args = sys.argv[1:]
    custom_stocks = None
    if '--stocks' in args:
        idx = args.index('--stocks')
        if idx + 1 < len(args):
            custom_stocks = [s.strip().upper() for s in args[idx + 1].split(',')]

    workers = 6
    if '--workers' in args:
        idx = args.index('--workers')
        if idx + 1 < len(args):
            try:
                workers = max(1, min(12, int(args[idx + 1])))
            except ValueError:
                pass

    if custom_stocks:
        stocks = custom_stocks
        print(f"🎯 Menjalankan mode custom ({len(stocks)} saham): {', '.join(stocks)}")
    else:
        stocks = get_stock_codes(con)
        if '--limit' in args:
            idx = args.index('--limit')
            if idx + 1 < len(args):
                limit_n = int(args[idx + 1])
                stocks = stocks[:limit_n]
        print(f"📋 Total {len(stocks)} emiten akan diproses dengan {workers} Thread Workers...")

    # 4. Multi-threaded Parallel Extraction
    session = requests.Session()
    # Adapter with connection pool
    adapter = requests.adapters.HTTPAdapter(pool_connections=workers * 2, pool_maxsize=workers * 4, max_retries=2)
    session.mount('https://', adapter)

    start_time = time.time()
    success_count = 0
    empty_count = 0
    fail_count = 0
    empty_codes = []
    
    batch_buffer = []
    processed_count = 0
    total_stocks = len(stocks)

    print(f"\n⚡ Memulai penarikan paralel ({workers} workers)...")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_stock = {
            executor.submit(fetch_keystats_single, session, code, sheets_svc, TOKEN_SHEET_ID): code
            for code in stocks
        }

        for future in as_completed(future_to_stock):
            code = future_to_stock[future]
            processed_count += 1
            try:
                row = future.result()
                if row.get('error'):
                    fail_count += 1
                    print(f"❌ [{processed_count}/{total_stocks}] {code}: {row['error'][:30]}", flush=True)
                else:
                    has_any = any(
                        row.get(k) is not None
                        for k in ('pe_ratio_ttm', 'pbv_ratio', 'roe_ttm_pct', 'market_cap_b', 'free_float_pct')
                    )
                    if has_any:
                        success_count += 1
                        batch_buffer.append(row)
                        print(f"✅ [{processed_count}/{total_stocks}] {code} (PER: {row['pe_ratio_ttm'] or '-'}, PBV: {row['pbv_ratio'] or '-'}, ROE: {row['roe_ttm_pct'] or '-'}%)", flush=True)
                    else:
                        empty_count += 1
                        empty_codes.append(code)
                        print(f"⚠️ [{processed_count}/{total_stocks}] {code} [KOSONG / 200 OK]", flush=True)
            except Exception as e:
                fail_count += 1
                print(f"❌ [{processed_count}/{total_stocks}] {code} Exception: {str(e)[:30]}", flush=True)

            # Atomic commit every 25 stocks
            if len(batch_buffer) >= 25:
                to_commit = list(batch_buffer)
                batch_buffer = []
                commit_batch_to_db(con, to_commit)

    # Commit any remaining buffered stocks
    if batch_buffer:
        commit_batch_to_db(con, batch_buffer)
        batch_buffer = []

    elapsed = time.time() - start_time
    print("\n" + "=" * 65)
    print(f"🏁 Ronde Utama Selesai dalam {elapsed:.1f} detik ({elapsed/60:.1f} menit)!")
    print(f"📊 Hasil: ✅ Berhasil: {success_count} · ⚠️ Kosong: {empty_count} · ❌ Gagal: {fail_count}")
    print("=" * 65)

    # 5. Fast Retry Pass for Empty/Failed Codes
    if empty_codes:
        print(f"\n🔁 Melakukan Quick Retry untuk {len(empty_codes)} saham kosong...")
        retry_rows = []
        with ThreadPoolExecutor(max_workers=min(4, workers)) as executor:
            future_retry = {
                executor.submit(fetch_keystats_single, session, code, sheets_svc, TOKEN_SHEET_ID): code
                for code in empty_codes
            }
            for future in as_completed(future_retry):
                code = future_retry[future]
                try:
                    row = future.result()
                    has_any = any(
                        row.get(k) is not None
                        for k in ('pe_ratio_ttm', 'pbv_ratio', 'roe_ttm_pct', 'market_cap_b', 'free_float_pct')
                    )
                    if has_any:
                        retry_rows.append(row)
                        print(f"   ↻ ✅ PULIH {code}!", flush=True)
                except Exception:
                    pass

        if retry_rows:
            commit_batch_to_db(con, retry_rows)
            print(f"   💾 [DB] Berhasil memulihkan {len(retry_rows)} saham dari retry pass!")

    total_in_db = con.execute(f"SELECT COUNT(*) FROM {MD_SCHEMA}.{MD_TABLE}").fetchone()[0]
    print("\n" + "=" * 65)
    print(f"🎉 PIPELINE SELESAI! Total emiten tersimpan di {MD_SCHEMA}.{MD_TABLE}: {total_in_db} emiten.")
    print("=" * 65)
    con.close()

if __name__ == '__main__':
    main()
