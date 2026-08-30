# =============================================================================
# STOCKBIT KEY STATS & FUNDAMENTALS PIPELINE — Chunked Micro-Batch Edition
# Strategy: Process stocks in small sequential chunks (batch of 3) with
#           controlled pacing. Stockbit's backend throttles concurrent requests
#           from the same token aggressively, returning empty 200s. Sequential
#           with micro-pacing (1.2-1.8s between requests) is the most reliable
#           approach — confirmed by the old sequential pipeline's 62% success
#           rate vs the parallel 6-worker's 4% success rate.
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

# All 50 columns in the exact order they exist in MotherDuck.
# This guarantees INSERT always matches the table schema.
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

def get_existing_codes(con):
    """Return set of stock codes that already have valid data in the DB."""
    try:
        rows = con.execute(f"""
            SELECT stock_code FROM {MD_SCHEMA}.{MD_TABLE}
            WHERE pe_ratio_ttm IS NOT NULL
               OR pbv_ratio IS NOT NULL
               OR roe_ttm_pct IS NOT NULL
               OR market_cap_b IS NOT NULL
        """).fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()

def fetch_keystats_stock(session, token, stock_code, sheets_svc=None, sheet_id=None):
    """Fetch key stats for a single stock. Returns dict or raises."""
    headers = make_headers(token, stock_code)
    url = f'{BASE_URL}/{stock_code}?year_limit=10'
    
    for attempt in range(3):
        try:
            r = session.get(url, headers=headers, timeout=20)
            
            # Auto-refresh token if 401/403
            if r.status_code in (401, 403) and sheets_svc and sheet_id:
                print(f"\n🔄 Token refresh for {stock_code}...", end='', flush=True)
                token = get_or_refresh_token(sheets_svc, sheet_id)
                headers = make_headers(token, stock_code)
                time.sleep(2)
                continue

            if r.status_code == 429:
                wait = 5 + attempt * 5 + random.uniform(1, 3)
                print(f" [429 throttle, wait {wait:.0f}s]", end='', flush=True)
                time.sleep(wait)
                continue

            r.raise_for_status()
            res_json = r.json()
            data = res_json.get('data') or {}

            stats = data.get('stats') or {}
            closure_results = data.get('closure_fin_items_results') or []

            # Empty payload → jitter retry
            if not stats and not closure_results:
                if attempt < 2:
                    time.sleep(2.0 + attempt * 2.0 + random.uniform(0.5, 1.5))
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

                # Financial Statements (in Billion IDR) — all 50 cols
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
            }, token  # return updated token in case it was refreshed
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(1.5 + attempt * 2)

    raise RuntimeError(f"Max retries for {stock_code}")

def commit_batch_to_db(con, batch_rows):
    """Column-safe upsert: only update matching columns, fill missing with NULL."""
    if not batch_rows:
        return 0
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
        return 0

    # Ensure every row has all 50 columns (fill missing with None)
    for row in valid_rows:
        for col in DB_COLUMNS:
            if col not in row:
                row[col] = None

    # Build DataFrame with columns in exact DB order
    df = pd.DataFrame(valid_rows, columns=DB_COLUMNS)
    con.register('df_batch', df)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {MD_SCHEMA}.{MD_TABLE} AS SELECT * FROM df_batch LIMIT 0;
        DELETE FROM {MD_SCHEMA}.{MD_TABLE} WHERE stock_code IN (SELECT stock_code FROM df_batch);
        INSERT INTO {MD_SCHEMA}.{MD_TABLE} SELECT * FROM df_batch;
    """)
    con.unregister('df_batch')
    return len(valid_rows)

# =============================================================================
# MAIN PIPELINE
# =============================================================================
def main():
    print("=" * 65)
    print("🚀 STOCKBIT KEY STATS PIPELINE (CHUNKED MICRO-BATCH EDITION)")
    print(f"⏰ Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    # 1. Google Sheets Auth & Token
    print("\n🔐 Menghubungkan ke Google Sheets...")
    sheets_svc = authenticate()
    token = get_or_refresh_token(sheets_svc, TOKEN_SHEET_ID)

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

    skip_existing = '--incremental' in args

    if custom_stocks:
        stocks = custom_stocks
        print(f"🎯 Mode custom ({len(stocks)} saham): {', '.join(stocks)}")
    else:
        stocks = get_stock_codes(con)
        if '--limit' in args:
            idx = args.index('--limit')
            if idx + 1 < len(args):
                stocks = stocks[:int(args[idx + 1])]

        # Incremental mode: skip stocks that already have valid data
        if skip_existing:
            existing = get_existing_codes(con)
            before = len(stocks)
            stocks = [s for s in stocks if s not in existing]
            print(f"📋 Incremental: {before} total → {len(stocks)} belum terisi (skip {before - len(stocks)} yang sudah ada)")
        else:
            print(f"📋 Total {len(stocks)} emiten akan diproses (full refresh)...")

    # 4. Sequential Extraction with Micro-Pacing
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=1)
    session.mount('https://', adapter)

    start_time = time.time()
    success_count = 0
    empty_count = 0
    fail_count = 0
    empty_codes = []
    batch_buffer = []
    total_stocks = len(stocks)
    committed_total = 0

    print(f"\n⚡ Memulai penarikan data ({total_stocks} saham)...")

    for i, code in enumerate(stocks, 1):
        print(f"[{i}/{total_stocks}] {code}... ", end='', flush=True)
        try:
            row, token = fetch_keystats_stock(
                session, token, code,
                sheets_svc=sheets_svc, sheet_id=TOKEN_SHEET_ID
            )
            has_any = any(
                row.get(k) is not None
                for k in ('pe_ratio_ttm', 'pbv_ratio', 'roe_ttm_pct', 'market_cap_b', 'free_float_pct')
            )
            if has_any:
                success_count += 1
                batch_buffer.append(row)
                print(f"✅ PER:{row['pe_ratio_ttm'] or '-'} PBV:{row['pbv_ratio'] or '-'} ROE:{row['roe_ttm_pct'] or '-'}%")
            else:
                empty_count += 1
                empty_codes.append(code)
                print("⚠️ KOSONG")
        except Exception as e:
            fail_count += 1
            print(f"❌ {str(e)[:50]}")

        # Pacing: 1.2 – 1.8 seconds between requests
        time.sleep(1.2 + random.uniform(0.1, 0.6))

        # Commit every 30 stocks
        if len(batch_buffer) >= 30 or i == total_stocks:
            if batch_buffer:
                n = commit_batch_to_db(con, batch_buffer)
                committed_total += n
                print(f"   💾 [DB] Batch commit: {n} saham → MotherDuck (total: {committed_total})")
                batch_buffer = []

        # Every 200 stocks, do a brief cooldown to avoid long-term throttling
        if i % 200 == 0 and i < total_stocks:
            cooldown = random.uniform(5, 10)
            print(f"   ⏸️ Cooldown {cooldown:.0f}s setelah {i} saham...", flush=True)
            time.sleep(cooldown)

    elapsed_main = time.time() - start_time
    print("\n" + "=" * 65)
    print(f"🏁 Ronde Utama: {elapsed_main:.0f}s ({elapsed_main/60:.1f}m)")
    print(f"   ✅ Berhasil: {success_count} · ⚠️ Kosong: {empty_count} · ❌ Gagal: {fail_count}")
    print("=" * 65)

    # 5. Retry Pass — empty codes often succeed on second attempt after cooldown
    retry_rounds = 2
    if '--retry-rounds' in args:
        idx = args.index('--retry-rounds')
        if idx + 1 < len(args):
            try:
                retry_rounds = max(0, min(5, int(args[idx + 1])))
            except ValueError:
                pass

    recovered_total = 0
    for rnd in range(1, retry_rounds + 1):
        if not empty_codes:
            break
        wait = 15 if rnd == 1 else 60 * rnd
        print(f"\n🔁 Retry ronde {rnd}/{retry_rounds}: {len(empty_codes)} saham — jeda {wait}s...")
        time.sleep(wait)

        retry_buffer = []
        still_empty = []
        for code in empty_codes:
            print(f"   ↻ {code}... ", end='', flush=True)
            try:
                row, token = fetch_keystats_stock(
                    session, token, code,
                    sheets_svc=sheets_svc, sheet_id=TOKEN_SHEET_ID
                )
                has_any = any(
                    row.get(k) is not None
                    for k in ('pe_ratio_ttm', 'pbv_ratio', 'roe_ttm_pct', 'market_cap_b', 'free_float_pct')
                )
                if has_any:
                    recovered_total += 1
                    retry_buffer.append(row)
                    print(f"✅ PULIH!")
                else:
                    still_empty.append(code)
                    print("masih KOSONG")
            except Exception as e:
                still_empty.append(code)
                print(f"gagal: {str(e)[:40]}")
            time.sleep(2.0 + random.uniform(0.5, 1.5))

        empty_codes = still_empty
        if retry_buffer:
            n = commit_batch_to_db(con, retry_buffer)
            committed_total += n
            print(f"   💾 [DB] Ronde {rnd}: {n} saham pulih → MotherDuck!")

    # 6. Final Summary
    total_in_db = con.execute(f"SELECT COUNT(*) FROM {MD_SCHEMA}.{MD_TABLE}").fetchone()[0]
    elapsed_total = time.time() - start_time
    print("\n" + "=" * 65)
    print(f"🎉 PIPELINE SELESAI dalam {elapsed_total:.0f}s ({elapsed_total/60:.1f}m)")
    print(f"   ✅ Sukses: {success_count + recovered_total} · ⚠️ Masih kosong: {len(empty_codes)} · ❌ Gagal: {fail_count}")
    print(f"   📊 Total di {MD_SCHEMA}.{MD_TABLE}: {total_in_db} emiten")
    print("=" * 65)
    con.close()

if __name__ == '__main__':
    main()
