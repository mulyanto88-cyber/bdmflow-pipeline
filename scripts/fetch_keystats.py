# =============================================================================
# STOCKBIT KEY STATS & FUNDAMENTALS PIPELINE — GitHub Actions Edition
# =============================================================================
import os
import sys
import json
import time
import random
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
BASE_URL         = 'https://exodus.stockbit.com'

# =============================================================================
# HELPERS
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

def clean_num(val):
    """Safely converts numeric or string number representations to float/int."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).replace(',', '').replace('%', '').replace('+', '').strip()
    if not s or s == '-' or s == 'N/A' or s == 'null':
        return None
    try:
        return float(s)
    except Exception:
        return None

def extract_metric(items_dict, *candidate_keys):
    """Searches for candidate keys in a key-stats dictionary or nested item list."""
    for k in candidate_keys:
        if k in items_dict and items_dict[k] is not None:
            v = items_dict[k]
            if isinstance(v, dict):
                v = v.get('value') or v.get('val') or v.get('raw') or v.get('formatted')
            res = clean_num(v)
            if res is not None:
                return res
    return None

def get_stock_codes(con):
    """Fetches list of active stocks from daily_transactions or company_profile."""
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
        return ['BBCA', 'BBRI', 'BMRI', 'BBNI', 'ASII', 'TLKM', 'TPIA', 'AMMN', 'BRIS', 'ADRO', 'PTBA', 'ICBP', 'UNVR', 'GOTO', 'KLBF']

def fetch_company_keystats(headers, stock_code):
    """
    Calls Stockbit Exodus Key Stats API for a single stock code.
    Tries primary keystats endpoint and company financials.
    """
    url = f'{BASE_URL}/keystats/company/{stock_code}'
    r = requests.get(url, headers=headers, timeout=20)
    
    if r.status_code == 404 or r.status_code == 400:
        # Fallback to alternate endpoint if any
        url = f'{BASE_URL}/company/keystats/{stock_code}'
        r = requests.get(url, headers=headers, timeout=20)
        
    r.raise_for_status()
    res_json = r.json()
    data = res_json.get('data', {})
    
    # Stockbit typically returns keystats as a flat dict or categorized sections
    # Flatten all fields for easy key lookup
    flattened = {}
    
    def walk(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    walk(v)
                else:
                    flattened[k.lower().replace(' ', '_').replace('-', '_')] = v
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict):
                    # Check for { "name": "PE Ratio", "value": 15.2 } structure
                    name = item.get('name') or item.get('title') or item.get('key') or item.get('label')
                    val = item.get('value') or item.get('val') or item.get('raw') or item.get('formatted')
                    if name and val is not None:
                        clean_name = str(name).lower().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '').replace('/', '_')
                        flattened[clean_name] = val
                    walk(item)

    walk(data)

    # Extract Key Metrics
    market_cap        = extract_metric(flattened, 'market_cap', 'market_capitalization', 'mcap')
    shares_out        = extract_metric(flattened, 'shares_outstanding', 'total_shares', 'shares')
    pe_ttm            = extract_metric(flattened, 'pe_ratio_ttm', 'pe_ratio', 'per_ttm', 'pe_ttm', 'current_pe_ratio_ttm', 'p_e_ratio_ttm')
    pe_ann            = extract_metric(flattened, 'pe_ratio_annualized', 'per_annualized', 'forward_pe', 'pe_annualised')
    pbv               = extract_metric(flattened, 'pbv_ratio', 'price_to_book_value', 'price_to_book', 'pbv', 'pb_ratio')
    ps_ratio          = extract_metric(flattened, 'price_to_sales_ttm', 'price_to_sales', 'ps_ratio', 'ps_ttm', 'p_s_ratio')
    ev_ebitda         = extract_metric(flattened, 'ev_ebitda', 'ev_to_ebitda', 'ev_ebitda_ttm', 'ev_to_ebitda_ttm')
    eps_ttm           = extract_metric(flattened, 'eps_ttm', 'earnings_per_share_ttm', 'eps_trailing_12m')
    eps_ann           = extract_metric(flattened, 'eps_annualized', 'eps_ann')
    bvps              = extract_metric(flattened, 'bvps', 'book_value_per_share', 'book_value_share')
    dps               = extract_metric(flattened, 'dividend_per_share', 'dps', 'dps_ttm')
    div_yield         = extract_metric(flattened, 'dividend_yield', 'div_yield', 'dividend_yield_ttm')
    payout_ratio      = extract_metric(flattened, 'payout_ratio', 'dividend_payout_ratio', 'dpr')
    roe               = extract_metric(flattened, 'roe_ttm', 'return_on_equity_ttm', 'roe', 'return_on_equity')
    roa               = extract_metric(flattened, 'roa_ttm', 'return_on_assets_ttm', 'roa', 'return_on_assets')
    gpm               = extract_metric(flattened, 'gpm_ttm', 'gross_profit_margin_ttm', 'gross_margin', 'gpm')
    opm               = extract_metric(flattened, 'opm_ttm', 'operating_profit_margin_ttm', 'operating_margin', 'opm')
    npm               = extract_metric(flattened, 'npm_ttm', 'net_profit_margin_ttm', 'net_margin', 'npm')
    revenue_ttm       = extract_metric(flattened, 'revenue_ttm', 'total_revenue_ttm', 'revenue', 'sales_ttm')
    revenue_growth    = extract_metric(flattened, 'revenue_growth_yoy', 'quarterly_revenue_growth_yoy', 'revenue_growth')
    net_income_ttm    = extract_metric(flattened, 'net_income_ttm', 'net_profit_ttm', 'net_income')
    net_income_growth = extract_metric(flattened, 'net_income_growth_yoy', 'quarterly_net_income_growth_yoy', 'earnings_growth_yoy')
    total_assets      = extract_metric(flattened, 'total_assets', 'assets')
    total_liabilities = extract_metric(flattened, 'total_liabilities', 'liabilities')
    total_equity      = extract_metric(flattened, 'total_equity', 'equity', 'stockholders_equity')
    cash_equiv        = extract_metric(flattened, 'cash_and_equivalents', 'cash', 'cash_and_cash_equivalents')
    total_debt        = extract_metric(flattened, 'total_debt', 'debt')
    der               = extract_metric(flattened, 'debt_to_equity', 'debt_to_equity_ratio', 'der', 'debt_equity')
    current_ratio     = extract_metric(flattened, 'current_ratio', 'cr')
    free_cash_flow    = extract_metric(flattened, 'free_cash_flow_ttm', 'free_cash_flow', 'fcf')
    
    # Financial Period string
    period_str = str(data.get('quarter') or data.get('period') or flattened.get('latest_quarter') or flattened.get('period') or 'Latest')

    return {
        'stock_code':            stock_code,
        'market_cap':            market_cap,
        'shares_outstanding':    int(shares_out) if shares_out else None,
        'pe_ratio_ttm':          pe_ttm,
        'pe_ratio_annualized':   pe_ann,
        'pbv_ratio':             pbv,
        'ps_ratio':              ps_ratio,
        'ev_ebitda':             ev_ebitda,
        'eps_ttm':               eps_ttm,
        'eps_annualized':        eps_ann,
        'bvps':                  bvps,
        'dps':                   dps,
        'dividend_yield':        div_yield,
        'payout_ratio':          payout_ratio,
        'roe_ttm':               roe,
        'roa_ttm':               roa,
        'gpm_ttm':               gpm,
        'opm_ttm':               opm,
        'npm_ttm':               npm,
        'revenue_ttm':           revenue_ttm,
        'revenue_growth_yoy':    revenue_growth,
        'net_income_ttm':        net_income_ttm,
        'net_income_growth_yoy': net_income_growth,
        'total_assets':          total_assets,
        'total_liabilities':     total_liabilities,
        'total_equity':          total_equity,
        'cash_and_equivalents':  cash_equiv,
        'total_debt':            total_debt,
        'debt_to_equity':        der,
        'current_ratio':         current_ratio,
        'free_cash_flow_ttm':    free_cash_flow,
        'period_latest':         period_str[:20],
        'updated_at':            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 65)
    print("📊 STOCKBIT KEY STATS & FUNDAMENTALS PIPELINE")
    print("=" * 65)

    # 1. Auth & Refresh Token
    sheets_svc = authenticate()
    token = get_or_refresh_token(sheets_svc, TOKEN_SHEET_ID)
    headers = make_headers(token)

    # 2. Connect MotherDuck
    con = duckdb.connect(f'md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}')
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {MD_SCHEMA}")

    # Create target table if not exists
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {MD_SCHEMA}.{MD_TABLE} (
            stock_code VARCHAR PRIMARY KEY,
            market_cap DOUBLE,
            shares_outstanding BIGINT,
            pe_ratio_ttm DOUBLE,
            pe_ratio_annualized DOUBLE,
            pbv_ratio DOUBLE,
            ps_ratio DOUBLE,
            ev_ebitda DOUBLE,
            eps_ttm DOUBLE,
            eps_annualized DOUBLE,
            bvps DOUBLE,
            dps DOUBLE,
            dividend_yield DOUBLE,
            payout_ratio DOUBLE,
            roe_ttm DOUBLE,
            roa_ttm DOUBLE,
            gpm_ttm DOUBLE,
            opm_ttm DOUBLE,
            npm_ttm DOUBLE,
            revenue_ttm DOUBLE,
            revenue_growth_yoy DOUBLE,
            net_income_ttm DOUBLE,
            net_income_growth_yoy DOUBLE,
            total_assets DOUBLE,
            total_liabilities DOUBLE,
            total_equity DOUBLE,
            cash_and_equivalents DOUBLE,
            total_debt DOUBLE,
            debt_to_equity DOUBLE,
            current_ratio DOUBLE,
            free_cash_flow_ttm DOUBLE,
            period_latest VARCHAR,
            updated_at TIMESTAMP
        )
    """)

    # 3. Get Stock List
    # Check if a custom list or limit is passed
    args = sys.argv[1:]
    custom_stocks = None
    if '--stocks' in args:
        idx = args.index('--stocks')
        if idx + 1 < len(args):
            custom_stocks = [s.strip().toUpperCase() for s in args[idx + 1].split(',')]

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

    # 4. Ingestion Loop
    results = []
    success_count = 0
    fail_count = 0

    for i, code in enumerate(stocks, 1):
        print(f"[{i}/{len(stocks)}] Fetching Key Stats {code}... ", end='', flush=True)
        try:
            row = fetch_company_keystats(headers, code)
            results.append(row)
            success_count += 1
            print(f"✅ OK (PER: {row['pe_ratio_ttm'] or '-'}, PBV: {row['pbv_ratio'] or '-'}, ROE: {row['roe_ttm'] or '-'}%)")
        except Exception as e:
            fail_count += 1
            print(f"⚠️ Gagal: {str(e)[:50]}")

        # Sleep jitter to be polite to Stockbit API
        time.sleep(0.35 + random.uniform(0.1, 0.25))

        # Batch insert to MotherDuck every 50 stocks or at the end
        if len(results) >= 50 or i == len(stocks):
            if results:
                import pandas as pd
                df = pd.DataFrame(results)
                con.register('df_batch', df)
                con.execute(f"""
                    INSERT OR REPLACE INTO {MD_SCHEMA}.{MD_TABLE}
                    SELECT * FROM df_batch
                """)
                con.unregister('df_batch')
                print(f"   💾 [DB] Berhasil menyimpan batch {len(results)} saham ke MotherDuck!")
                results = []

    total_in_db = con.execute(f"SELECT COUNT(*) FROM {MD_SCHEMA}.{MD_TABLE}").fetchone()[0]
    print("=" * 65)
    print(f"🎉 SELESAI! Sukses: {success_count} · Gagal: {fail_count}")
    print(f"📊 Total emiten di {MD_SCHEMA}.{MD_TABLE}: {total_in_db} rows")
    print("=" * 65)
    con.close()

if __name__ == '__main__':
    main()
