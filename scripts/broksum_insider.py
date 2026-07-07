# =============================================================================
# BROKSUM + INSIDER PIPELINE — GitHub Actions Edition
# Token dibaca dari Google Sheet — tidak perlu buka laptop!
# =============================================================================
import os, io, json, time, base64, random, requests
from datetime import datetime, timedelta, timezone
import pandas as pd
import duckdb

from google.oauth2 import service_account
from googleapiclient.discovery import build

# =============================================================================
# CONFIG
# =============================================================================
SA_JSON          = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'])
MOTHERDUCK_TOKEN = os.environ['MOTHERDUCK_TOKEN']
TOKEN_SHEET_ID   = os.environ['TOKEN_SHEET_ID']

MOTHERDUCK_DB    = 'my_db'
MD_TABLE_BROKER  = 'broker_activity'
MD_TABLE_INSIDER = 'insider_major_holder'

BASE_URL         = 'https://exodus.stockbit.com'
TOP_LIMIT        = 100
ACTIVITY_LIMIT   = 50
ACTIVITY_MAX_PAGE= 10
INSIDER_LIMIT    = 50
INSIDER_MAX_PAGES= 200
PERIOD           = 'TB_PERIOD_LAST_1_DAY'

DEFAULT_DAYS_BACK_BROKER  = 5
DEFAULT_DAYS_BACK_INSIDER = 30

# =============================================================================
# AUTH
# =============================================================================
def authenticate():
    creds = service_account.Credentials.from_service_account_info(
        SA_JSON,
        scopes=[
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets'
        ]
    )
    sheets = build('sheets', 'v4', credentials=creds, cache_discovery=False)
    return sheets

# =============================================================================
# BACA TOKEN DARI GOOGLE SHEET
# =============================================================================
def read_token_from_sheet(sheets_service):
    print("📋 Membaca token dari Google Sheet...")
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=TOKEN_SHEET_ID,
            range='A1'
        ).execute()
        values = result.get('values', [])
        if not values or not values[0] or not values[0][0].strip():
            print("❌ Token kosong di Google Sheet!")
            print("   → Buka stockbit.com di HP → tap bookmarklet → paste token ke Sheet A1")
            return None
        token = values[0][0].strip()
        print(f"✅ Token ditemukan ({len(token)} chars)")
        return token
    except Exception as e:
        print(f"❌ Gagal baca sheet: {e}")
        return None

# =============================================================================
# VALIDASI TOKEN
# =============================================================================
def validate_token(token):
    try:
        parts = token.split('.')
        pad   = 4 - len(parts[1]) % 4
        body  = json.loads(base64.b64decode(parts[1] + '=' * (pad % 4)))
        exp   = body.get('exp', 0)
        sisa  = exp - datetime.now().timestamp()
        if sisa <= 0:
            print("❌ Token EXPIRED! Update token di Google Sheet.")
            return False
        h, m = int(sisa // 3600), int((sisa % 3600) // 60)
        print(f"✅ Token valid — sisa {h}j {m}m")
        return True
    except Exception as e:
        print(f"⚠️ Tidak bisa validasi token: {e}")
        return True  # tetap coba

# =============================================================================
# HELPERS
# =============================================================================
def make_headers(token):
    return {
        'Authorization': f'Bearer {token}',
        'Accept':        'application/json',
        'User-Agent':    'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Referer':       'https://stockbit.com/',
        'Origin':        'https://stockbit.com',
    }

def is_weekday(d):
    return d.weekday() < 5

def get_date_list(default_days=7):
    now   = datetime.now(timezone.utc) + timedelta(hours=7)
    dates = []
    d     = now
    while len(dates) < default_days:
        if is_weekday(d):
            dates.append(d.strftime('%Y-%m-%d'))
        d -= timedelta(days=1)
    return sorted(dates)

def get_existing_dates(con, table):
    try:
        rows = con.execute(
            f"SELECT DISTINCT CAST(date AS VARCHAR) FROM {table}"
        ).fetchall()
        return set(r[0] for r in rows)
    except Exception:
        return set()

# =============================================================================
# FETCH BROKER
# =============================================================================
def get_top_brokers(headers, target_date):
    r = requests.get(f'{BASE_URL}/order-trade/broker/top', headers=headers,
        timeout=15, params={
            'sort': 'TB_SORT_BY_TOTAL_VALUE', 'order': 'ORDER_BY_DESC',
            'period': PERIOD, 'market_type': 'MARKET_TYPE_ALL',
            'eod_only': 'true', 'limit': TOP_LIMIT, 'offset': 0,
        })
    r.raise_for_status()
    data = r.json().get('data', {})
    return data.get('list', [])

def get_broker_activity(headers, broker_code, target_date):
    all_buys, all_sells = [], []
    for page in range(1, ACTIVITY_MAX_PAGE + 1):
        r = requests.get(f'{BASE_URL}/order-trade/broker/activity',
            headers=headers, timeout=15, params={
                'broker_code': broker_code, 'limit': ACTIVITY_LIMIT,
                'page': page, 'from': target_date, 'to': target_date,
                'transaction_type': 'TRANSACTION_TYPE_NET',
                'market_board': 'MARKET_TYPE_REGULER',
                'investor_type': 'INVESTOR_TYPE_ALL',
            })
        r.raise_for_status()
        tx    = r.json().get('data', {}).get('broker_activity_transaction', {})
        buys  = tx.get('brokers_buy', [])
        sells = tx.get('brokers_sell', [])
        all_buys.extend(buys)
        all_sells.extend(sells)
        if len(buys) < ACTIVITY_LIMIT and len(sells) < ACTIVITY_LIMIT:
            break
        time.sleep(0.7 + random.uniform(0.2, 0.5))
    return all_buys, all_sells

def normalize_broker(stock, broker_code, broker_name, side, default_date):
    d = str(stock.get('date', ''))
    return {
        'date':        d[:10] if 'T' in d else (d or default_date),
        'broker_code': broker_code,
        'broker_name': broker_name,
        'side':        side,
        'stock_code':  stock.get('stock_code'),
        'value':       int(stock.get('value',     0)),
        'lot':         int(stock.get('lot',       0)),
        'avg_price':   round(float(stock.get('avg_price', 0)), 2),
        'freq':        int(stock.get('freq',      0)),
    }

# =============================================================================
# FETCH INSIDER
# =============================================================================
def normalize_insider(item, target_date):
    badges = item.get('badges', [])
    broker = item.get('broker_detail', {})
    return {
        'insider_id':        item.get('id', ''),
        'insider_name':      item.get('name', ''),
        'stock_code':        item.get('symbol', ''),
        'transaction_date':  item.get('date', target_date),
        'action_type':       item.get('action_type', '').replace('ACTION_TYPE_', ''),
        'nationality':       item.get('nationality', '').replace('NATIONALITY_TYPE_', ''),
        'shares_previous':   int(float(str(item.get('previous',{}).get('value','0')).replace(',',''))),
        'pct_previous':      float(str(item.get('previous',{}).get('percentage','0')).replace(',','')),
        'shares_current':    int(float(str(item.get('current',{}).get('value','0')).replace(',',''))),
        'pct_current':       float(str(item.get('current',{}).get('percentage','0')).replace(',','')),
        'shares_change':     int(float(str(item.get('changes',{}).get('value','0')).replace(',','').replace('+',''))),
        'pct_change':        float(str(item.get('changes',{}).get('percentage','0')).replace(',','').replace('+','')),
        'price_formatted':   float(str(item.get('price_formatted','0')).replace(',','') or 0),
        'data_source':       item.get('data_source',{}).get('label',''),
        'source_type':       item.get('data_source',{}).get('type','').replace('SOURCE_TYPE_',''),
        'broker_code':       broker.get('code',''),
        'broker_group':      broker.get('group','').replace('BROKER_GROUP_',''),
        'is_pengendali':     'SHAREHOLDER_BADGE_PENGENDALI' in badges,
        'is_komisaris':      'SHAREHOLDER_BADGE_KOMISARIS' in badges,
        'is_direksi':        'SHAREHOLDER_BADGE_DIREKSI' in badges,
        'badges':            ','.join([b.replace('SHAREHOLDER_BADGE_','') for b in badges]),
        'target_date':       target_date,
    }

def fetch_insider_for_date(headers, target_date):
    all_items = []
    page      = 1
    while page <= INSIDER_MAX_PAGES:
        try:
            r = requests.get(f'{BASE_URL}/insider/company/majorholder',
                headers=headers, timeout=30, params={
                    'date_start':  target_date, 'date_end': target_date,
                    'page': page, 'limit': INSIDER_LIMIT,
                    'action_type': 'ACTION_TYPE_UNSPECIFIED',
                    'source_type': 'SOURCE_TYPE_UNSPECIFIED',
                })
            r.raise_for_status()
            data      = r.json().get('data', {})
            movements = data.get('movement', [])
            if not movements:
                break
            for item in movements:
                all_items.append(normalize_insider(item, target_date))
            if not data.get('is_more', False):
                break
            page += 1
            time.sleep(0.5 + random.uniform(0.2, 0.4))
        except Exception as e:
            print(f"   ⚠️ Error insider page {page}: {str(e)[:60]}")
            break
    return all_items

# =============================================================================
# MAIN
# =============================================================================
def main():
    start = time.time()
    print("="*60)
    print("🚀 BROKSUM + INSIDER — GitHub Actions Edition")
    print("="*60)

    # 1. Auth & baca token
    sheets_svc = authenticate()
    token      = read_token_from_sheet(sheets_svc)

    if not token:
        print("\n💡 CARA UPDATE TOKEN:")
        print("   1. Buka stockbit.com di HP → Login")
        print("   2. Tap bookmark 'Get Stockbit Token'")
        print("   3. Copy token yang muncul")
        print("   4. Buka Google Sheet BDMFlow-Token → paste di cell A1")
        raise SystemExit("❌ Token tidak tersedia — update di Google Sheet")

    if not validate_token(token):
        raise SystemExit("❌ Token expired — update di Google Sheet")

    headers = make_headers(token)

    # 2. Connect MotherDuck
    con = duckdb.connect(f'md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}')

    # 3. Cek tanggal yang sudah ada
    existing_broker  = get_existing_dates(con, MD_TABLE_BROKER)
    existing_insider = get_existing_dates(con, MD_TABLE_INSIDER)

    dates_broker  = [d for d in get_date_list(DEFAULT_DAYS_BACK_BROKER)  if d not in existing_broker]
    dates_insider = [d for d in get_date_list(DEFAULT_DAYS_BACK_INSIDER) if d not in existing_insider]

    print(f"\n📊 Broker  : {len(dates_broker)} hari baru")
    print(f"🕵️ Insider : {len(dates_insider)} hari baru")

    if not dates_broker and not dates_insider:
        print("✅ Semua data sudah up-to-date!")
        con.close()
        return

    # 4. Fetch Broker
    COLS_BROKER = ['date','broker_code','broker_name','side','stock_code','value','lot','avg_price','freq']
    all_broker_rows = []

    if dates_broker:
        print(f"\n📊 Fetching broker data...")
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {MD_TABLE_BROKER} (
                date DATE, broker_code VARCHAR, broker_name VARCHAR,
                side VARCHAR, stock_code VARCHAR, value BIGINT,
                lot BIGINT, avg_price DOUBLE, freq BIGINT)
        """)

        for target_date in dates_broker:
            print(f"   📅 {target_date}...")
            try:
                brokers    = get_top_brokers(headers, target_date)
                rows_today = []
                for b in brokers:
                    code, name = b.get('code'), b.get('name','')
                    try:
                        buys, sells = get_broker_activity(headers, code, target_date)
                        for s in buys:  rows_today.append(normalize_broker(s, code, name, 'BUY',  target_date))
                        for s in sells: rows_today.append(normalize_broker(s, code, name, 'SELL', target_date))
                    except Exception as e:
                        if '401' in str(e) or '403' in str(e):
                            raise
                    time.sleep(0.8 + random.uniform(0.2, 0.6))

                if rows_today:
                    df_today = pd.DataFrame(rows_today, columns=COLS_BROKER)
                    df_today['date']      = pd.to_datetime(df_today['date']).dt.date
                    df_today['value']     = df_today['value'].astype('int64')
                    df_today['lot']       = df_today['lot'].astype('int64')
                    df_today['avg_price'] = df_today['avg_price'].astype('float64')
                    df_today['freq']      = df_today['freq'].astype('int64')
                    df_today = df_today.dropna(subset=['stock_code','broker_code'])
                    df_today = df_today.drop_duplicates(subset=['date','broker_code','side','stock_code'])

                    con.execute(f"DELETE FROM {MD_TABLE_BROKER} WHERE date = '{target_date}'")
                    con.register("temp_broker", df_today)
                    con.execute(f"INSERT INTO {MD_TABLE_BROKER} SELECT * FROM temp_broker")
                    all_broker_rows.append(len(df_today))
                    print(f"   ✅ {target_date}: {len(df_today):,} rows")

            except Exception as e:
                if '401' in str(e) or '403' in str(e):
                    print(f"❌ Token expired saat proses! Update token di Google Sheet.")
                    break
                print(f"   ❌ {target_date}: {str(e)[:60]}")

    # 5. Fetch Insider
    all_insider_rows = []

    if dates_insider:
        print(f"\n🕵️ Fetching insider data...")
        COLS_INSIDER = [
            'insider_id','insider_name','stock_code','transaction_date','action_type',
            'nationality','shares_previous','pct_previous','shares_current','pct_current',
            'shares_change','pct_change','price_formatted','data_source','source_type',
            'broker_code','broker_group','is_pengendali','is_komisaris','is_direksi',
            'badges','target_date'
        ]
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {MD_TABLE_INSIDER} (
                insider_id VARCHAR, insider_name VARCHAR, stock_code VARCHAR,
                transaction_date DATE, action_type VARCHAR, nationality VARCHAR,
                shares_previous BIGINT, pct_previous DOUBLE,
                shares_current BIGINT, pct_current DOUBLE,
                shares_change BIGINT, pct_change DOUBLE,
                price_formatted DOUBLE, data_source VARCHAR, source_type VARCHAR,
                broker_code VARCHAR, broker_group VARCHAR,
                is_pengendali BOOLEAN, is_komisaris BOOLEAN, is_direksi BOOLEAN,
                badges VARCHAR, target_date DATE)
        """)

        for target_date in dates_insider:
            print(f"   📅 {target_date}...")
            try:
                items = fetch_insider_for_date(headers, target_date)
                if items:
                    df_today = pd.DataFrame(items, columns=COLS_INSIDER)
                    df_today['transaction_date'] = pd.to_datetime(
                        df_today['transaction_date'], format='%d %b %y', errors='coerce').dt.date
                    df_today['target_date'] = pd.to_datetime(df_today['target_date']).dt.date
                    df_today = df_today.drop_duplicates(
                        subset=['insider_id','stock_code','transaction_date'])

                    con.execute(f"DELETE FROM {MD_TABLE_INSIDER} WHERE target_date = '{target_date}'")
                    con.register("temp_insider", df_today)
                    con.execute(f"INSERT INTO {MD_TABLE_INSIDER} SELECT * FROM temp_insider")
                    all_insider_rows.append(len(df_today))
                    print(f"   ✅ {target_date}: {len(items)} rows")
            except Exception as e:
                if '401' in str(e) or '403' in str(e):
                    print(f"❌ Token expired! Update token di Google Sheet.")
                    break
                print(f"   ❌ {target_date}: {str(e)[:60]}")

    # 6. Summary
    broker_total  = con.execute(f"SELECT COUNT(*) FROM {MD_TABLE_BROKER}").fetchone()[0]
    insider_total = con.execute(f"SELECT COUNT(*) FROM {MD_TABLE_INSIDER}").fetchone()[0]
    con.close()

    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"🎉 SELESAI! ⏱️ {elapsed/60:.1f} menit")
    print(f"   📊 Broker total  : {broker_total:,} rows")
    print(f"   🕵️ Insider total : {insider_total:,} rows")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
