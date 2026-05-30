# =============================================================================
# KSEI FULL MONTHLY PIPELINE — GitHub Actions Edition
# =============================================================================
import os, io, json, zipfile, time
import pandas as pd
import numpy as np
import duckdb
from tqdm import tqdm

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# =============================================================================
# CONFIG
# =============================================================================
SA_JSON          = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'])
MOTHERDUCK_TOKEN = os.environ['MOTHERDUCK_TOKEN']

FOLDER_ZIP_ID    = 'FOLDER_ID_KSEI_FULL_ZIP'  # ← ganti dengan folder ID ZIP KSEI
FOLDER_OUTPUT_ID = '1hX2jwUrAgi4Fr8xkcFWjCW6vbk6lsIlP'
OUTPUT_CSV_NAME  = 'KSEI_Shareholder_Pure_KSEI_Only.csv'
MOTHERDUCK_DB    = 'my_db'

OWNERSHIP_COLS = [
    'Local_IS','Local_CP','Local_PF','Local_IB','Local_ID',
    'Local_MF','Local_SC','Local_FD','Local_OT',
    'Foreign_IS','Foreign_CP','Foreign_PF','Foreign_IB','Foreign_ID',
    'Foreign_MF','Foreign_SC','Foreign_FD','Foreign_OT'
]

# =============================================================================
# AUTH
# =============================================================================
def authenticate():
    creds = service_account.Credentials.from_service_account_info(
        SA_JSON, scopes=['https://www.googleapis.com/auth/drive'])
    return build('drive', 'v3', credentials=creds, cache_discovery=False)

# =============================================================================
# HELPERS
# =============================================================================
def list_zips(service, folder_id):
    files, page_token = [], None
    query = f"'{folder_id}' in parents and trashed=false"
    while True:
        res = service.files().list(
            q=query, fields="nextPageToken, files(id, name)",
            pageToken=page_token, includeItemsFromAllDrives=True,
            supportsAllDrives=True).execute()
        files.extend(res.get('files', []))
        page_token = res.get('nextPageToken')
        if not page_token:
            break
    return [f for f in files if 'balancepos' in f['name'].lower()]

def download_and_extract_zip(service, file_id, file_name):
    try:
        request = service.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        dl = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = dl.next_chunk()
        buf.seek(0)
        with zipfile.ZipFile(buf, 'r') as z:
            txt_file = next((n for n in z.namelist() if n.lower().endswith('.txt')), None)
            if txt_file:
                with z.open(txt_file) as f:
                    df = pd.read_csv(f, delimiter='|', encoding='latin1', thousands=',')
                    if 'Type' in df.columns:
                        df = df[df['Type'] == 'EQUITY'].copy()
                    df.rename(columns={'Total':'Total_Local','Total.1':'Total_Foreign'}, inplace=True)
                    return df
    except Exception as e:
        print(f"   ⚠️ Skip '{file_name}': {str(e)[:60]}")
    return None

def parse_ksei_date(date_str):
    date_str = str(date_str).lower().strip()
    mapping  = {
        'mei':'05','may':'05','agt':'08','aug':'08','okt':'10','oct':'10',
        'des':'12','dec':'12','jan':'01','feb':'02','mar':'03','apr':'04',
        'jun':'06','jul':'07','sep':'09','nov':'11'
    }
    for k, v in mapping.items():
        if k in date_str:
            date_str = date_str.replace(k, v)
            break
    return pd.to_datetime(date_str, format='%d-%m-%Y', errors='coerce')

def save_output(df, service):
    buf   = io.StringIO()
    df.to_csv(buf, index=False)
    media = MediaIoBaseUpload(
        io.BytesIO(buf.getvalue().encode('utf-8')),
        mimetype='text/csv', resumable=True)
    query = f"'{FOLDER_OUTPUT_ID}' in parents and name='{OUTPUT_CSV_NAME}' and trashed=false"
    old   = service.files().list(q=query, fields="files(id)").execute().get('files',[])
    if old:
        service.files().update(
            fileId=old[0]['id'], body={'name': OUTPUT_CSV_NAME},
            media_body=media, supportsAllDrives=True).execute()
    else:
        service.files().create(
            body={'name': OUTPUT_CSV_NAME, 'parents': [FOLDER_OUTPUT_ID]},
            media_body=media, fields='id', supportsAllDrives=True).execute()
    print("   ✅ Output tersimpan ke Drive")

# =============================================================================
# MAIN
# =============================================================================
def main():
    start = time.time()
    print("="*60)
    print("🚀 KSEI FULL MONTHLY — GitHub Actions Edition")
    print("="*60)

    service = authenticate()
    print("✅ Auth berhasil")

    zip_files = list_zips(service, FOLDER_ZIP_ID)
    print(f"📦 {len(zip_files)} ZIP files ditemukan")

    dfs = []
    for f in tqdm(zip_files, desc="Extracting ZIPs"):
        d = download_and_extract_zip(service, f['id'], f['name'])
        if d is not None:
            for c in OWNERSHIP_COLS:
                if c not in d.columns:
                    d[c] = 0
            cols_clean = OWNERSHIP_COLS + ['Price','Total_Local','Total_Foreign']
            for c in cols_clean:
                if c in d.columns:
                    d[c] = pd.to_numeric(d[c], errors='coerce').fillna(0)
            dfs.append(d)

    if not dfs:
        print("❌ Tidak ada data.")
        return

    final = pd.concat(dfs, ignore_index=True)
    print(f"   ✅ {len(final):,} baris terkumpul")

    # Processing
    final['Date'] = final['Date'].apply(parse_ksei_date)
    final['Date'] = pd.to_datetime(final['Date']).dt.date
    final.dropna(subset=['Date'], inplace=True)
    final.sort_values(['Code','Date'], inplace=True)

    final['Total_Shares']        = final[OWNERSHIP_COLS].sum(axis=1)
    grp                          = final.groupby('Code')
    final['Price_Chg_Pct']       = grp['Price'].pct_change()
    final['Shares_Chg_Pct']      = grp['Total_Shares'].pct_change()
    final['Is_Split_Suspect']    = (final['Price_Chg_Pct'] < -0.7) & (final['Shares_Chg_Pct'] > 2.0)
    final['Is_Reverse_Suspect']  = (final['Price_Chg_Pct'] > 2.0) & (final['Shares_Chg_Pct'] < -0.7)

    df_diff                = grp[OWNERSHIP_COLS].diff().fillna(0)
    final['Top_Buyer']     = df_diff.idxmax(axis=1)
    final['Top_Buyer_Vol'] = df_diff.max(axis=1)
    final['Top_Seller']    = df_diff.idxmin(axis=1)
    final['Top_Seller_Vol']= df_diff.min(axis=1)
    final['Top_Buyer_Val'] = final['Top_Buyer_Vol'] * final['Price']
    final['Top_Seller_Val']= final['Top_Seller_Vol'] * final['Price']

    for col in OWNERSHIP_COLS:
        final[f"{col}_Chg_Vol"] = df_diff[col]
        final[f"{col}_Chg_Val"] = df_diff[col] * final['Price']

    final = final.drop(columns=['Price_Chg_Pct','Shares_Chg_Pct'], errors='ignore')
    final.columns = final.columns.str.replace(' ', '_')
    print(f"   ✅ Processing selesai: {len(final):,} baris, {len(final.columns)} kolom")

    save_output(final, service)

    # Upload ke MotherDuck
    print("\n🦆 Upload ke MotherDuck...")
    con = duckdb.connect(f'md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}')
    con.execute("CREATE SCHEMA IF NOT EXISTS ksei")
    con.execute("DROP TABLE IF EXISTS ksei.monthly_snapshot")
    con.register("temp_ksei_full", final)
    con.execute("""
        CREATE TABLE ksei.monthly_snapshot AS
        SELECT CAST(Date AS DATE) AS Date, * EXCLUDE(Date)
        FROM temp_ksei_full
    """)
    count = con.execute("SELECT COUNT(*) FROM ksei.monthly_snapshot").fetchone()[0]
    dates = con.execute("SELECT COUNT(DISTINCT Date) FROM ksei.monthly_snapshot").fetchone()[0]
    codes = con.execute("SELECT COUNT(DISTINCT Code) FROM ksei.monthly_snapshot").fetchone()[0]
    con.close()

    print(f"   ✅ {count:,} rows | {dates} dates | {codes} codes")
    print(f"\n🎉 SELESAI! ⏱️ {(time.time()-start)/60:.1f} menit")

if __name__ == "__main__":
    main()
