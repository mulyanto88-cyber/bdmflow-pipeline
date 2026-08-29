# =============================================================================
# STOCKBIT KEY STATS & FUNDAMENTALS PIPELINE — Production Edition
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

def make_headers(token):
    return {
        'Authorization': f'Bearer {token}',
        'Accept':        'application/json',
        'User-Agent':    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Referer':       'https://stockbit.com/',
        'Origin':        'https://stockbit.com',
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

def fetch_keystats_stock(headers, stock_code):
    url = f'{BASE_URL}/{stock_code}?year_limit=10'
    r = requests.get(url, headers=headers, timeout=20)
    
    if r.status_code == 429:
        time.sleep(5)
        r = requests.get(url, headers=headers, timeout=20)

    r.raise_for_status()
    res_json = r.json()
    data = res_json.get('data', {})

    stats = data.get('stats', {})
    mcap_b        = clean_val(stats.get('market_cap'))
    shares_out_b  = clean_val(stats.get('current_share_outstanding'))
    ev_b          = clean_val(stats.get('enterprise_value'))
    free_float    = clean_val(stats.get('free_float'))

    item_map = {}
    for group in data.get('closure_fin_items_results', []):
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
        'total_equity_b':          item_map.get('Total Equity'),
        'total_debt_b':            item_map.get('Total Debt (Quarter)'),
        'net_debt_b':              item_map.get('Net Debt (Quarter)'),
        'cash_from_ops_ttm_b':     item_map.get('Cash From Operations (TTM)'),
        'free_cash_flow_ttm_b':    item_map.get('Free cash flow (TTM)'),

        # Metadata
        'period_latest':           latest_period,
        'updated_at':              datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 65)
    print("📊 STOCKBIT KEY STATS & FUNDAMENTALS PIPELINE (v1/ratio)")
    print("=" * 65)

    # 1. Auth & Token
    sheets_svc = authenticate()
    token = get_or_refresh_token(sheets_svc, TOKEN_SHEET_ID)
    headers = make_headers(token)

    # 2. Connect MotherDuck
    con = duckdb.connect(f'md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}')
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {MD_SCHEMA}")

    # Check if existing table has outdated column count, auto drop to migrate
    try:
        col_count = len(con.execute(f"PRAGMA table_info('{MD_SCHEMA}.{MD_TABLE}')").fetchall())
        if 0 < col_count != 46:
            print(f"🔄 Menyesuaikan schema tabel {MD_SCHEMA}.{MD_TABLE} ({col_count} -> 46 kolom)...")
            con.execute(f"DROP TABLE {MD_SCHEMA}.{MD_TABLE}")
    except Exception:
        pass

    # 3. Stock List
    args = sys.argv[1:]
    custom_stocks = None
    if '--stocks' in args:
        idx = args.index('--stocks')
        if idx + 1 < len(args):
            custom_stocks = [s.strip().upper() for s in args[idx + 1].split(',')]

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
        print(f"📋 Total {len(stocks)} emiten akan diproses...")

    # 4. Extraction Loop
    results = []
    success_count = 0
    fail_count = 0

    for i, code in enumerate(stocks, 1):
        print(f"[{i}/{len(stocks)}] Fetching Key Stats {code}... ", end='', flush=True)
        try:
            row = fetch_keystats_stock(headers, code)
            results.append(row)
            success_count += 1
            print(f"✅ OK (PER: {row['pe_ratio_ttm'] or '-'}, PBV: {row['pbv_ratio'] or '-'}, ROE: {row['roe_ttm_pct'] or '-'}%)")
        except Exception as e:
            fail_count += 1
            print(f"⚠️ Gagal: {str(e)[:50]}")

        # Politeness delay
        time.sleep(0.5 + random.uniform(0.1, 0.3))

        # Batch insert to MotherDuck
        if len(results) >= 25 or i == len(stocks):
            if results:
                import pandas as pd
                df = pd.DataFrame(results)
                con.register('df_batch', df)
                con.execute(f"""
                    CREATE TABLE IF NOT EXISTS {MD_SCHEMA}.{MD_TABLE} AS SELECT * FROM df_batch LIMIT 0;
                    DELETE FROM {MD_SCHEMA}.{MD_TABLE} WHERE stock_code IN (SELECT stock_code FROM df_batch);
                    INSERT INTO {MD_SCHEMA}.{MD_TABLE} SELECT * FROM df_batch;
                """)
                con.unregister('df_batch')
                print(f"   💾 [DB] Tersimpan batch {len(results)} saham ke MotherDuck!")
                results = []

    total_in_db = con.execute(f"SELECT COUNT(*) FROM {MD_SCHEMA}.{MD_TABLE}").fetchone()[0]
    print("=" * 65)
    print(f"🎉 SELESAI! Sukses: {success_count} · Gagal: {fail_count}")
    print(f"📊 Total emiten di {MD_SCHEMA}.{MD_TABLE}: {total_in_db} rows")
    print("=" * 65)
    con.close()

if __name__ == '__main__':
    main()
