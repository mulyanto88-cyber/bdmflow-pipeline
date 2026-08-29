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

def fetch_keystats_stock(session, token, stock_code, sheets_svc=None, sheet_id=None, preview=True):
    headers = make_headers(token, stock_code)
    
    # 1. Trigger Paywall Eligibility check (same as browser client flow)
    pw_url = f'https://exodus.stockbit.com/paywall/eligibility/check?features=PAYWALL_FEATURE_KEYSTATS&company={stock_code}'
    try:
        pw = session.get(pw_url, headers=headers, timeout=8)
        try:
            pw_body = pw.json()
            pw_data = (pw_body or {}).get('data') or {}
            feats = pw_data.get('features') or []
            elig = next((f.get('is_eligible') for f in feats if isinstance(f, dict)), None)
            print(f"[PW: eligible={elig} subs={pw_data.get('last_subscription')}] ", end='', flush=True)
        except Exception:
            print("[PW: ?] ", end='', flush=True)
    except Exception:
        pass

    # 2. Fetch Key Stats
    url = f'{BASE_URL}/{stock_code}?year_limit=10'
    
    for attempt in range(3):
        try:
            r = session.get(url, headers=headers, timeout=20)
            
            # Auto-refresh token if 401/403
            if r.status_code in (401, 403) and sheets_svc and sheet_id:
                print(f"\n🔄 Token refresh otomatis untuk {stock_code}...", end='', flush=True)
                new_token = get_or_refresh_token(sheets_svc, sheet_id)
                headers = make_headers(new_token, stock_code)
                time.sleep(2)
                continue

            if r.status_code == 429:
                time.sleep(5 + attempt * 3)
                continue

            r.raise_for_status()
            res_json = r.json()
            data = res_json.get('data') or {}

            stats = data.get('stats') or {}
            closure_results = data.get('closure_fin_items_results') or []

            # If payload is empty, log preview and retry
            if not stats and not closure_results:
                if attempt < 2:
                    time.sleep(2 + attempt * 2)
                    continue
                else:
                    # Log response diagnostic
                    preview = str(res_json)[:120]
                    print(f"[Resp: {preview}] ", end='', flush=True)

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

            # Diagnostic: a 200 response that parses to nothing is a paywall /
            # tier / coverage signal, not a success — print the raw shape once.
            parsed_any = (
                mcap_b is not None or shares_out_b is not None or ev_b is not None
                or free_float is not None or bool(item_map)
            )
            if not parsed_any and preview:
                preview_str = str(res_json)[:400]
                print(f"[Resp: {preview_str}] ", end='', flush=True)

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
        except Exception as e:
            if attempt == 2:
                raise e
            time.sleep(2)

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

    # Persistent HTTP Session
    session = requests.Session()

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
    empty_codes = []
    success_count = 0
    empty_count = 0
    fail_count = 0

    for i, code in enumerate(stocks, 1):
        print(f"[{i}/{len(stocks)}] Fetching Key Stats {code}... ", end='', flush=True)
        try:
            row = fetch_keystats_stock(session, token, code, sheets_svc=sheets_svc, sheet_id=TOKEN_SHEET_ID)
            results.append(row)
            has_any = any(
                row.get(k) is not None
                for k in ('pe_ratio_ttm', 'pbv_ratio', 'roe_ttm_pct', 'market_cap_b', 'free_float_pct')
            )
            if has_any:
                success_count += 1
                print(f"✅ DATA (PER: {row['pe_ratio_ttm'] or '-'}, PBV: {row['pbv_ratio'] or '-'}, ROE: {row['roe_ttm_pct'] or '-'}%)")
            else:
                empty_count += 1
                empty_codes.append(code)
                print(f"⚠️ KOSONG — 200 OK tapi tidak ada data ter-parse (throttle? lihat [Resp:] di atas)")
        except Exception as e:
            fail_count += 1
            print(f"❌ Gagal: {str(e)[:50]}")

        # Politeness delay between stocks — the ratio endpoint serves
        # empty-but-200 payloads when hit too fast, so pace gently.
        time.sleep(1.2 + random.uniform(0.2, 0.6))

        # Batch insert to MotherDuck
        if len(results) >= 25 or i == len(stocks):
            if results:
                # Only insert valid records so failures never wipe existing data
                valid_results = [
                    r for r in results 
                    if r.get('pe_ratio_ttm') is not None 
                    or r.get('pbv_ratio') is not None 
                    or r.get('roe_ttm_pct') is not None 
                    or r.get('market_cap_b') is not None
                    or r.get('free_float_pct') is not None
                ]
                if valid_results:
                    import pandas as pd
                    df = pd.DataFrame(valid_results)
                    con.register('df_batch', df)
                    con.execute(f"""
                        CREATE TABLE IF NOT EXISTS {MD_SCHEMA}.{MD_TABLE} AS SELECT * FROM df_batch LIMIT 0;
                        DELETE FROM {MD_SCHEMA}.{MD_TABLE} WHERE stock_code IN (SELECT stock_code FROM df_batch);
                        INSERT INTO {MD_SCHEMA}.{MD_TABLE} SELECT * FROM df_batch;
                    """)
                    con.unregister('df_batch')
                    print(f"   💾 [DB] Tersimpan batch {len(valid_results)} saham valid ke MotherDuck!")
                results = []

    # 5. Retry passes — the Stockbit endpoint is cache/throttle-sensitive:
    # ~25-33% of requests come back 200-but-empty even though eligibility is
    # granted. Empirically, retrying LATER recovers a meaningful fraction each
    # round (run 3: +6/18 on round 1). More rounds + longer pauses = more
    # coverage; the table persists between weekly runs, so coverage also
    # accumulates over time.
    args = sys.argv[1:]
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
        wait = 20 if rnd == 1 else 90 * (rnd - 1)
        print(f"\n🔁 Retry ronde {rnd}/{retry_rounds} untuk {len(empty_codes)} saham KOSONG — jeda {wait} detik...")
        time.sleep(wait)
        retry_rows = []
        still_empty = []
        for code in empty_codes:
            print(f"   ↻ {code}... ", end='', flush=True)
            try:
                row = fetch_keystats_stock(session, token, code, preview=False)
                has_any = any(
                    row.get(k) is not None
                    for k in ('pe_ratio_ttm', 'pbv_ratio', 'roe_ttm_pct', 'market_cap_b', 'free_float_pct')
                )
                if has_any:
                    recovered_total += 1
                    retry_rows.append(row)
                    print(f"✅ PULIH (PER: {row['pe_ratio_ttm'] or '-'}, PBV: {row['pbv_ratio'] or '-'})")
                else:
                    still_empty.append(code)
                    print("masih KOSONG")
            except Exception as e:
                still_empty.append(code)
                print(f"gagal: {str(e)[:40]}")
            time.sleep(2.5)
        empty_codes = still_empty

        if retry_rows:
            import pandas as pd
            df = pd.DataFrame(retry_rows)
            con.register('df_retry', df)
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {MD_SCHEMA}.{MD_TABLE} AS SELECT * FROM df_retry LIMIT 0;
                DELETE FROM {MD_SCHEMA}.{MD_TABLE} WHERE stock_code IN (SELECT stock_code FROM df_retry);
                INSERT INTO {MD_SCHEMA}.{MD_TABLE} SELECT * FROM df_retry;
            """)
            con.unregister('df_retry')
            print(f"   💾 [DB] Ronde {rnd} tersimpan {len(retry_rows)} saham pulih!")

    total_in_db = con.execute(f"SELECT COUNT(*) FROM {MD_SCHEMA}.{MD_TABLE}").fetchone()[0]
    print("=" * 65)
    print(f"🎉 SELESAI! Data: {success_count} · KOSONG: {empty_count} · Pulih total (retry): {recovered_total} · Gagal: {fail_count}")
    print(f"📊 Total emiten di {MD_SCHEMA}.{MD_TABLE}: {total_in_db} rows")
    print("=" * 65)
    con.close()

if __name__ == '__main__':
    main()
