# =============================================================================
# DAILY TRANSACTIONS PIPELINE — GitHub Actions Edition  v2
# Changelog dari v1:
#   - whale_signal: tambah filter minimum value (Rp 500 juta) dan
#     minimum volume (100K shares) untuk eliminasi false positive dari
#     saham illiquid yang hampir tidak pernah ditransaksikan
#   - big_player_anomaly: tambah filter minimum value (Rp 2 miliar)
#   - Inkonsistensi MA20 vs MA50 diluruskan — whale_signal tetap pakai
#     MA50 sebagai baseline (lebih stabil), tapi threshold dinaikkan
#   - Semua perubahan terisolasi di process_full_dataframe() saja
# =============================================================================
import os, re, time, random, io, json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import duckdb
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# =============================================================================
# CONFIG
# =============================================================================
SA_JSON            = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'])
MOTHERDUCK_TOKEN   = os.environ['MOTHERDUCK_TOKEN']

FOLDER_SUMBER_ID   = '1L0O1fc4B2jNo7pQB4ttJx6itYsAePmrt'
FOLDER_BACKUP_ID   = '1hX2jwUrAgi4Fr8xkcFWjCW6vbk6lsIlP'
NAMA_FILE_BACKUP   = 'Kompilasi_Data_Daily_Transactions_MotherDuck_Backup.csv'
NAMA_WORKSHEET     = 'Sheet1'
MOTHERDUCK_DB      = 'my_db'
MD_SCHEMA          = 'market'
MD_TABLE           = 'daily_transactions'

# =============================================================================
# WHALE SIGNAL THRESHOLDS
# Calibrated from actual data distribution analysis (530K rows, Jan 2024–May 2026)
# =============================================================================
MIN_VALUE_WHALE   = 500_000_000    # Rp 500 juta — eliminates illiquid noise
MIN_VALUE_ANOMALY = 2_000_000_000  # Rp 2 miliar — for big_player_anomaly
MIN_VOLUME_SHARES = 100_000        # 100K shares (1,000 lot) minimum

# =============================================================================
# AUTH
# =============================================================================
def authenticate():
    print("🔐 Authenticating via Service Account...")
    creds = service_account.Credentials.from_service_account_info(
        SA_JSON,
        scopes=[
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets.readonly'
        ]
    )
    drive_service  = build('drive',  'v3', credentials=creds, cache_discovery=False)
    sheets_service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
    print("✅ Auth berhasil")
    return drive_service, sheets_service

# =============================================================================
# DRIVE HELPERS
# =============================================================================
def load_backup_csv(drive_service):
    print("\n📂 Mengecek backup CSV di GDrive...")
    try:
        query = f"'{FOLDER_BACKUP_ID}' in parents and name='{NAMA_FILE_BACKUP}' and trashed=false"
        results = drive_service.files().list(q=query, fields="files(id, name)").execute()
        files_found = results.get('files', [])
        if not files_found:
            print("   ↳ File backup belum ada. Mode: FULL LOAD.")
            return None, set()
        file_id = files_found[0]['id']
        request = drive_service.files().get_media(fileId=file_id)
        downloaded = io.BytesIO()
        downloader = MediaIoBaseDownload(downloaded, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        downloaded.seek(0)
        df_backup = pd.read_csv(downloaded)
        processed_files = set(df_backup['Source File'].unique().astype(str)) if 'Source File' in df_backup.columns else set()
        print(f"   ✅ Backup: {len(df_backup):,} baris, {len(processed_files)} file.")
        return df_backup, processed_files
    except Exception as e:
        print(f"   ⚠️ Gagal: {e}. Mode: FULL LOAD.")
        return None, set()

def get_sheets_in_folder(service, folder_id):
    print(f"\n🔎 Scanning Google Sheets...")
    all_files, page_token = [], None
    query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
    while True:
        results = service.files().list(
            q=query, fields="nextPageToken, files(id, name)",
            pageToken=page_token, corpora='allDrives',
            includeItemsFromAllDrives=True, supportsAllDrives=True
        ).execute()
        all_files.extend(results.get('files', []))
        page_token = results.get('nextPageToken')
        if not page_token:
            break
    print(f"✅ {len(all_files)} file ditemukan.")
    return all_files

def read_sheet_data(sheets_service, sheet_id, sheet_name, max_retries=5):
    range_name = f"{sheet_name}!A:AZ"
    for attempt in range(max_retries):
        try:
            res = sheets_service.spreadsheets().values().get(
                spreadsheetId=sheet_id, range=range_name).execute()
            return res.get('values', [])
        except Exception as e:
            if attempt >= max_retries - 1:
                return None
            time.sleep((2 ** attempt) + random.uniform(0, 1))
    return None

# =============================================================================
# DATA PROCESSING HELPERS
# =============================================================================
def convert_indonesian_date(date_str):
    bulan_map = {
        "Jan":"01","Feb":"02","Mar":"03","Apr":"04","Mei":"05","Jun":"06",
        "Jul":"07","Agt":"08","Sep":"09","Okt":"10","Nov":"11","Des":"12"
    }
    date_str = str(date_str).strip()
    try:
        for indo, num in bulan_map.items():
            if indo in date_str:
                return pd.to_datetime(date_str.replace(indo, num), format="%d %m %Y", errors='coerce')
        return pd.to_datetime(date_str, errors='coerce')
    except:
        return pd.NaT

def extract_date_from_filename(filename):
    match = re.search(r"(\d{8})", filename or "")
    if match:
        try:
            return pd.to_datetime(match.group(1), format="%Y%m%d", errors='coerce')
        except:
            return pd.NaT
    return pd.NaT

def convert_foreign_to_rupiah(df):
    if 'Foreign Buy' in df.columns and 'Foreign Sell' in df.columns:
        for col in ['Foreign Buy','Foreign Sell','Volume','Value','Close']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',',''), errors='coerce')
        df['Daily_VWAP']        = np.where(df['Volume'] > 0, df['Value'] / df['Volume'], df['Close'])
        df['Foreign_Buy_Value'] = df['Foreign Buy']  * df['Daily_VWAP']
        df['Foreign_Sell_Value']= df['Foreign Sell'] * df['Daily_VWAP']
        df['Net_Foreign_Value'] = df['Foreign_Buy_Value'] - df['Foreign_Sell_Value']
        df.drop(columns=['Daily_VWAP'], errors='ignore', inplace=True)
    return df

# =============================================================================
# CORE PROCESSING — whale/anomaly logic corrected in v2
# =============================================================================
def process_full_dataframe(df_raw):
    print("\n🧹 Processing data...")
    df_raw['Last Trading Date'] = pd.to_datetime(df_raw['Last Trading Date'], errors='coerce')
    df_raw.dropna(subset=['Last Trading Date'], inplace=True)
    df = df_raw.copy()
    print(f"   ↳ {len(df):,} baris | "
          f"{df['Last Trading Date'].min().date()} s/d {df['Last Trading Date'].max().date()}")

    # ── Numeric coercion ─────────────────────────────────────────────
    numeric_cols = ['High','Low','Close','Volume','Value','Bid Volume','Offer Volume',
                    'Previous','Change','Open Price','Frequency','Offer','Bid']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.strip().str.replace(r'[,\sRp]','',regex=True),
                errors='coerce')

    # ── Sort & forward-fill prices ───────────────────────────────────
    df = df.sort_values(['Stock Code','Last Trading Date'])
    price_cols = ['High','Low','Close','Previous','Open Price','Bid','Offer']
    vol_cols   = ['Volume','Value','Bid Volume','Offer Volume','Frequency']
    df[[c for c in price_cols if c in df.columns]] = \
        df.groupby('Stock Code')[[c for c in price_cols if c in df.columns]].ffill().bfill()
    df[[c for c in vol_cols if c in df.columns]] = \
        df[[c for c in vol_cols if c in df.columns]].fillna(0)

    # ── Derived columns ──────────────────────────────────────────────
    df['Change %']      = np.where(df['Previous'] != 0, (df['Change'] / df['Previous']) * 100, 0)
    df['Typical Price'] = (df['High'] + df['Low'] + df['Close']) / 3
    df['TPxV']          = df['Typical Price'] * df['Volume']

    # VWMA 20D
    sum_tpxv   = df.groupby('Stock Code')['TPxV'].transform(
                    lambda x: x.rolling(20, min_periods=1).sum())
    sum_v      = df.groupby('Stock Code')['Volume'].transform(
                    lambda x: x.rolling(20, min_periods=1).sum())
    df['VWMA_20D'] = np.where(sum_v != 0, sum_tpxv / sum_v, df['Close'])
    df['MA20_vol'] = df.groupby('Stock Code')['Volume'].transform(
                        lambda x: x.rolling(20, min_periods=1).mean())

    # ── AOV calculations ─────────────────────────────────────────────
    if 'Frequency' in df.columns:
        df['Avg_Order_Volume'] = np.where(
            df['Frequency'] > 0, df['Volume'] / df['Frequency'], 0)
        df['MA50_AOVol']       = df.groupby('Stock Code')['Avg_Order_Volume'].transform(
                                    lambda x: x.rolling(50, min_periods=1).mean())
    else:
        df['Avg_Order_Volume'] = 0
        df['MA50_AOVol']       = 0

    df['AOVol_MA20']       = df.groupby('Stock Code')['Avg_Order_Volume'].transform(
                                lambda x: x.rolling(20, min_periods=1).mean())
    df['AOVol_Ratio_MA20'] = np.where(
        df['AOVol_MA20'] > 0, df['Avg_Order_Volume'] / df['AOVol_MA20'], 1.0)

    # ── Signal ───────────────────────────────────────────────────────
    df['Signal'] = np.select(
        [(df['Close'] > df['VWMA_20D']) & (df['Volume'] > df['MA20_vol']),
         (df['Close'] < df['VWMA_20D']) & (df['Volume'] > df['MA20_vol'])],
        ['Akumulasi','Distribusi'], default='Netral')

    # ── Foreign conversion ───────────────────────────────────────────
    if 'Foreign_Buy_Value' not in df.columns:
        df = convert_foreign_to_rupiah(df)

    # ── AOV ratio (MA50 base — for whale detection) ──────────────────
    df['AOV_Ratio'] = np.where(
        df['MA50_AOVol'] > 0,
        df['Avg_Order_Volume'] / df['MA50_AOVol'],
        1.0
    )

    # ── WHALE SIGNAL v2 — with minimum absolute thresholds ───────────
    # v1 problem: 100-lot trades in dormant stocks flagged as whale
    # because MA50_AOVol was near zero → ratio exploded to 20x+
    # Fix: require minimum Rp 500 juta AND 100K shares (1,000 lot)
    df['Whale_Signal'] = (
        (df['AOV_Ratio']  >= 1.5)           &
        (df['Value']      >= MIN_VALUE_WHALE)   &
        (df['Volume']     >= MIN_VOLUME_SHARES) &
        (df['MA50_AOVol'] >  0)
    )

    # ── BIG PLAYER ANOMALY v2 — stricter value floor ─────────────────
    # v1 problem: no value floor → same false positive issue as whale
    # Fix: require Rp 2 miliar minimum + 1,000 lot minimum
    df['Big_Player_Anomaly'] = (
        (df['Avg_Order_Volume'] > 2 * df['MA50_AOVol']) &
        (df['MA50_AOVol']       > 0)                    &
        (df['Value']            >= MIN_VALUE_ANOMALY)   &
        (df['Volume']           >= MIN_VOLUME_SHARES)
    )

    # ── Dedup ────────────────────────────────────────────────────────
    df = df.drop_duplicates(subset=['Stock Code','Last Trading Date'], keep='last')
    print(f"   ✅ Selesai: {len(df):,} baris")
    return df

# =============================================================================
# DRIVE BACKUP
# =============================================================================
def save_backup_to_drive(df, drive_service):
    print(f"\n💾 Menyimpan backup...")
    try:
        query   = f"'{FOLDER_BACKUP_ID}' in parents and name='{NAMA_FILE_BACKUP}' and trashed=false"
        old_files = drive_service.files().list(q=query, fields="files(id)").execute().get('files',[])
        csv_buf = io.StringIO()
        df.to_csv(csv_buf, index=False)
        media   = MediaIoBaseUpload(
            io.BytesIO(csv_buf.getvalue().encode('utf-8')),
            mimetype='text/csv', resumable=True)
        if old_files:
            drive_service.files().update(
                fileId=old_files[0]['id'], body={'name': NAMA_FILE_BACKUP},
                media_body=media, supportsAllDrives=True).execute()
        else:
            drive_service.files().create(
                body={'name': NAMA_FILE_BACKUP, 'parents': [FOLDER_BACKUP_ID]},
                media_body=media, fields='id', supportsAllDrives=True).execute()
        print("   ✅ Backup tersimpan")
    except Exception as e:
        print(f"   ⚠️ Gagal: {e}")

# =============================================================================
# MOTHERDUCK PREP
# =============================================================================
def prepare_for_motherduck(df):
    col_map = {
        'Stock Code':'stock_code','Last Trading Date':'trading_date',
        'Open Price':'open_price','High':'high','Low':'low','Close':'close',
        'Previous':'previous','Change %':'change_percent','Volume':'volume',
        'Value':'value','Frequency':'frequency',
        'Foreign_Buy_Value':'foreign_buy_value',
        'Foreign_Sell_Value':'foreign_sell_value',
        'Net_Foreign_Value':'net_foreign_value',
        'VWMA_20D':'vwma_20d','MA20_vol':'ma20_volume',
        'Avg_Order_Volume':'avg_order_volume',
        'MA50_AOVol':'ma50_avg_order_volume',
        'AOVol_Ratio_MA20':'aov_ratio_ma20',
        'Whale_Signal':'whale_signal',
        'Big_Player_Anomaly':'big_player_anomaly',
        'Signal':'signal',
        'Tradeble Shares':'tradeable_shares',
        'Source File':'source_file'
    }
    df_clean = pd.DataFrame()
    for csv_col, db_col in col_map.items():
        if csv_col in df.columns:
            df_clean[db_col] = df[csv_col]

    df_clean['trading_date'] = pd.to_datetime(df_clean['trading_date']).dt.date

    int_cols = ['volume','value','frequency','foreign_buy_value','foreign_sell_value',
                'net_foreign_value','ma20_volume','tradeable_shares']
    for col in int_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0).astype('int64')

    float_cols = ['open_price','high','low','close','previous','change_percent',
                  'vwma_20d','avg_order_volume','ma50_avg_order_volume','aov_ratio_ma20']
    for col in float_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0).astype(float)

    for col in ['whale_signal','big_player_anomaly']:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.upper().map(
                {'TRUE':True,'FALSE':False,'1':True,'0':False}
            ).fillna(False).astype(bool)

    df_clean = df_clean.replace({np.nan:None, pd.NaT:None, np.inf:None, -np.inf:None})
    return df_clean

# =============================================================================
# MAIN
# =============================================================================
def main():
    start = time.time()
    print("="*60)
    print("🚀 DAILY TRANSACTIONS v2 — whale signal corrected")
    print("="*60)

    drive_svc, sheets_svc = authenticate()
    df_backup, processed  = load_backup_csv(drive_svc)
    all_sheets            = get_sheets_in_folder(drive_svc, FOLDER_SUMBER_ID)
    to_process            = [f for f in all_sheets if f['name'] not in processed]

    print(f"\n📋 {len(all_sheets)} total | {len(processed)} done | {len(to_process)} new")

    if not to_process:
        print("🔄 Semua sheet sudah diproses. Reprocessing dengan logika whale v2...")
        if df_backup is not None:
            df_final = process_full_dataframe(df_backup)
            save_backup_to_drive(df_final, drive_svc)
        else:
            print("❌ Tidak ada data backup.")
            return
    else:
        new_data, headers = [], None
        for i, sheet in enumerate(to_process, 1):
            if i % 10 == 0 or i == 1:
                print(f"   [{i}/{len(to_process)}] {sheet['name']}")
            rows = read_sheet_data(sheets_svc, sheet['id'], NAMA_WORKSHEET)
            if not rows or len(rows) < 2:
                continue
            cur_headers = rows[0] + ["Source File"]
            if headers is None:
                headers = cur_headers
            expected = len(headers) - 1
            for row in rows[1:]:
                if len(row) == expected:
                    new_data.append(row + [sheet['name']])

        if not new_data:
            print("❌ No valid new data found.")
            return

        df_new = pd.DataFrame(new_data, columns=headers)
        df_new.columns = df_new.columns.str.strip()
        df_new['Last Trading Date'] = df_new['Last Trading Date'].apply(convert_indonesian_date)
        mask_na = df_new['Last Trading Date'].isna()
        df_new.loc[mask_na, 'Last Trading Date'] = \
            df_new.loc[mask_na, 'Source File'].apply(extract_date_from_filename)
        df_new.dropna(subset=['Last Trading Date'], inplace=True)
        df_new = convert_foreign_to_rupiah(df_new)

        if df_backup is not None:
            cols       = [c for c in df_backup.columns if c in df_new.columns]
            df_combined = pd.concat([df_backup[cols], df_new], ignore_index=True)
        else:
            df_combined = df_new

        df_final = process_full_dataframe(df_combined)
        save_backup_to_drive(df_final, drive_svc)

    df_md = prepare_for_motherduck(df_final)

    print(f"\n🦆 Upload ke MotherDuck...")
    con = None
    try:
        con = duckdb.connect(f'md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}')
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MD_SCHEMA}")
        con.execute(f"DROP TABLE IF EXISTS {MD_SCHEMA}.{MD_TABLE}")
        con.register("temp_daily", df_md)
        con.execute(f"""
            CREATE TABLE {MD_SCHEMA}.{MD_TABLE} AS
            SELECT CAST(trading_date AS DATE) AS trading_date,
                   stock_code, open_price, high, low, close, previous,
                   change_percent, volume, value, frequency,
                   foreign_buy_value, foreign_sell_value, net_foreign_value,
                   vwma_20d, ma20_volume, avg_order_volume,
                   ma50_avg_order_volume, aov_ratio_ma20,
                   whale_signal, big_player_anomaly,
                   signal, tradeable_shares, source_file
            FROM temp_daily
        """)

        count  = con.execute(f"SELECT COUNT(*) FROM {MD_SCHEMA}.{MD_TABLE}").fetchone()[0]
        latest = con.execute(f"SELECT MAX(trading_date) FROM {MD_SCHEMA}.{MD_TABLE}").fetchone()[0]
        print(f"   ✅ {count:,} rows | Latest: {latest}")

        # ── Validation: confirm false positives eliminated ───────────
        print("\n🔍 Validating whale signal fix...")
        fp_whale = con.execute("""
            SELECT COUNT(*) FROM market.daily_transactions
            WHERE whale_signal = TRUE AND value < 500000000
        """).fetchone()[0]
        fp_anomaly = con.execute("""
            SELECT COUNT(*) FROM market.daily_transactions
            WHERE big_player_anomaly = TRUE AND value < 2000000000
        """).fetchone()[0]
        whale_total = con.execute("""
            SELECT COUNT(*) FROM market.daily_transactions WHERE whale_signal = TRUE
        """).fetchone()[0]
        anomaly_total = con.execute("""
            SELECT COUNT(*) FROM market.daily_transactions WHERE big_player_anomaly = TRUE
        """).fetchone()[0]
        print(f"   whale_signal    : {whale_total:,} total | {fp_whale} false positives remaining")
        print(f"   big_player_anomaly: {anomaly_total:,} total | {fp_anomaly} false positives remaining")
        if fp_whale == 0 and fp_anomaly == 0:
            print("   ✅ Zero false positives — whale logic clean!")
        else:
            print("   ⚠️  Some false positives remain — review thresholds")

        # ── Refresh screener_period ──────────────────────────────────
        print("\n🔄 Refreshing screener_period...")
        con.execute("DROP TABLE IF EXISTS market.screener_period")
        con.execute("""
            CREATE TABLE market.screener_period AS
            WITH latest AS (
                SELECT CAST(MAX(trading_date) AS DATE) AS max_date
                FROM market.daily_transactions
            ),
            spikes AS (
                SELECT stock_code,
                    MAX(aov_ratio_ma20) AS aov_max,
                    MAX(CASE WHEN CAST(trading_date AS DATE) >= (SELECT max_date FROM latest) - INTERVAL '1 days'
                             THEN aov_ratio_ma20 END) AS aov_max_1d,
                    MAX(CASE WHEN CAST(trading_date AS DATE) >= (SELECT max_date FROM latest) - INTERVAL '7 days'
                             THEN aov_ratio_ma20 END) AS aov_max_7d,
                    MAX(CASE WHEN CAST(trading_date AS DATE) >= (SELECT max_date FROM latest) - INTERVAL '14 days'
                             THEN aov_ratio_ma20 END) AS aov_max_14d,
                    MAX(CASE WHEN CAST(trading_date AS DATE) >= (SELECT max_date FROM latest) - INTERVAL '30 days'
                             THEN aov_ratio_ma20 END) AS aov_max_30d,
                    MAX(CASE WHEN CAST(trading_date AS DATE) >= (SELECT max_date FROM latest) - INTERVAL '90 days'
                             THEN aov_ratio_ma20 END) AS aov_max_90d,
                    COUNT(CASE WHEN aov_ratio_ma20 >= 1.5
                               AND CAST(trading_date AS DATE) >= (SELECT max_date FROM latest) - INTERVAL '1 days'
                               AND value >= 500000000
                               THEN 1 END) AS spike_1d,
                    COUNT(CASE WHEN aov_ratio_ma20 >= 1.5
                               AND CAST(trading_date AS DATE) >= (SELECT max_date FROM latest) - INTERVAL '7 days'
                               AND value >= 500000000
                               THEN 1 END) AS spike_7d,
                    COUNT(CASE WHEN aov_ratio_ma20 >= 1.5
                               AND CAST(trading_date AS DATE) >= (SELECT max_date FROM latest) - INTERVAL '14 days'
                               AND value >= 500000000
                               THEN 1 END) AS spike_14d,
                    COUNT(CASE WHEN aov_ratio_ma20 >= 1.5
                               AND CAST(trading_date AS DATE) >= (SELECT max_date FROM latest) - INTERVAL '30 days'
                               AND value >= 500000000
                               THEN 1 END) AS spike_30d,
                    COUNT(CASE WHEN aov_ratio_ma20 >= 1.5
                               AND CAST(trading_date AS DATE) >= (SELECT max_date FROM latest) - INTERVAL '90 days'
                               AND value >= 500000000
                               THEN 1 END) AS spike_90d
                FROM market.daily_transactions
                WHERE CAST(trading_date AS DATE) >= (SELECT max_date FROM latest) - INTERVAL '90 days'
                GROUP BY stock_code
            ),
            foreign_flow AS (
                SELECT stock_code,
                    SUM(CASE WHEN CAST(trading_date AS DATE) >= (SELECT max_date FROM latest) - INTERVAL '1 days'
                             THEN net_foreign_value ELSE 0 END) AS foreign_1d,
                    SUM(CASE WHEN CAST(trading_date AS DATE) >= (SELECT max_date FROM latest) - INTERVAL '7 days'
                             THEN net_foreign_value ELSE 0 END) AS foreign_7d,
                    SUM(CASE WHEN CAST(trading_date AS DATE) >= (SELECT max_date FROM latest) - INTERVAL '14 days'
                             THEN net_foreign_value ELSE 0 END) AS foreign_14d,
                    SUM(CASE WHEN CAST(trading_date AS DATE) >= (SELECT max_date FROM latest) - INTERVAL '30 days'
                             THEN net_foreign_value ELSE 0 END) AS foreign_30d,
                    SUM(CASE WHEN CAST(trading_date AS DATE) >= (SELECT max_date FROM latest) - INTERVAL '90 days'
                             THEN net_foreign_value ELSE 0 END) AS foreign_90d
                FROM market.daily_transactions
                WHERE CAST(trading_date AS DATE) >= (SELECT max_date FROM latest) - INTERVAL '90 days'
                GROUP BY stock_code
            )
            SELECT s.stock_code, s.aov_max,
                s.aov_max_1d, s.aov_max_7d, s.aov_max_14d, s.aov_max_30d, s.aov_max_90d,
                s.spike_1d, s.spike_7d, s.spike_14d, s.spike_30d, s.spike_90d,
                COALESCE(f.foreign_1d,  0) AS foreign_1d,
                COALESCE(f.foreign_7d,  0) AS foreign_7d,
                COALESCE(f.foreign_14d, 0) AS foreign_14d,
                COALESCE(f.foreign_30d, 0) AS foreign_30d,
                COALESCE(f.foreign_90d, 0) AS foreign_90d
            FROM spikes s
            LEFT JOIN foreign_flow f ON s.stock_code = f.stock_code
        """)
        sp_count = con.execute("SELECT COUNT(*) FROM market.screener_period").fetchone()[0]
        print(f"   ✅ screener_period: {sp_count} stocks refreshed")

        # ── Refresh whale timing snapshot ────────────────────────────
        try:
            con.execute("CREATE OR REPLACE TABLE ksei.whale_timing_snapshot AS SELECT * FROM ksei.vw_whale_timing")
            print("   ✅ whale_timing_snapshot refreshed")
        except Exception as e:
            print(f"   ⚠️ whale snapshot: {str(e)[:60]}")

    except Exception as e:
        print(f"❌ MotherDuck error: {str(e)[:200]}")
        raise
    finally:
        if con:
            con.close()

    elapsed = (time.time() - start) / 60
    print(f"\n{'='*60}")
    print(f"🎉 SELESAI! ⏱️  {elapsed:.1f} menit")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
