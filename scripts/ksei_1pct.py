# =============================================================================
# KSEI 1% MONTHLY PIPELINE — GSheet/Excel Edition
# =============================================================================
import os, io, json, shutil, time, re
import pandas as pd
import numpy as np
from tqdm import tqdm
import duckdb

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# =============================================================================
# CONFIG
# =============================================================================
SA_JSON          = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'])
MOTHERDUCK_TOKEN = os.environ['MOTHERDUCK_TOKEN']

FOLDER_PDF_ID    = '1lS90X8fvJ87oFDjvdz4JXUNAE6ettaxa'  # Google Drive folder ID
FOLDER_BACKUP_ID = '1hX2jwUrAgi4Fr8xkcFWjCW6vbk6lsIlP'
BACKUP_CSV_NAME  = 'KSE_1Persen_Monthly_Snapshot.csv'
MOTHERDUCK_DB    = 'my_db'
TEMP_DIR         = '/tmp/ksei_1pct'

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
def list_ksei_files(service, folder_id):
    files, page_token = [], None
    # Query Google Sheets, native Excel, or files with .xlsx/.xls in the name
    query = (
        f"'{folder_id}' in parents and ("
        f"mimeType='application/vnd.google-apps.spreadsheet' or "
        f"mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or "
        f"name contains '.xlsx' or name contains '.xls'"
        f") and trashed=false"
    )
    while True:
        res = service.files().list(
            q=query, fields="nextPageToken, files(id, name, mimeType)",
            pageToken=page_token, includeItemsFromAllDrives=True,
            supportsAllDrives=True).execute()
        files.extend(res.get('files', []))
        page_token = res.get('nextPageToken')
        if not page_token:
            break
    return sorted(files, key=lambda x: x['name'])

def download_file(service, file_id, mime_type, dest_path):
    # If the file is a native Google Sheet, we export it as Excel (.xlsx)
    if mime_type == 'application/vnd.google-apps.spreadsheet':
        request = service.files().export_media(
            fileId=file_id,
            mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    else:
        request = service.files().get_media(fileId=file_id)
        
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = dl.next_chunk()
    buf.seek(0)
    with open(dest_path, 'wb') as f:
        f.write(buf.read())

def load_backup_csv(service):
    query = f"'{FOLDER_BACKUP_ID}' in parents and name='{BACKUP_CSV_NAME}' and trashed=false"
    res   = service.files().list(q=query, fields="files(id)").execute()
    files = res.get('files', [])
    if not files:
        return pd.DataFrame(), set()
    request = service.files().get_media(fileId=files[0]['id'])
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = dl.next_chunk()
    buf.seek(0)
    df = pd.read_csv(buf)
    processed = set(df['Data Source'].astype(str).tolist()) if 'Data Source' in df.columns else set()
    return df, processed

def clean_numeric_safe(df, columns, is_float=False):
    for col in columns:
        if col in df.columns:
            # If the column is already read as a numeric data type, just fill NaNs
            if pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].fillna(0)
            else:
                # If it's a string, clean it safely
                s = df[col].astype(str).str.strip().str.replace(r'\s+','',regex=True)
                if is_float:
                    s = s.str.replace('.','',regex=False).str.replace(',','.',regex=False)
                else:
                    s = s.str.replace('.','',regex=False).str.replace(',','',regex=False)
                df[col] = pd.to_numeric(s, errors='coerce').fillna(0)
            
            if is_float:
                df[col] = df[col].astype('float64')
            else:
                df[col] = df[col].astype('int64')
    return df

def process_excel(file_path, filename):
    try:
        df = pd.read_excel(file_path, sheet_name=0)
        df = df.dropna(how='all')
        
        # Scan first 15 rows to detect header row containing key columns
        header_idx = None
        for i in range(min(15, len(df))):
            row_vals = [str(val).upper().strip() for val in df.iloc[i].values]
            if 'SHARE_CODE' in row_vals or 'SHARE CODE' in row_vals or 'INVESTOR_NAME' in row_vals or 'INVESTOR NAME' in row_vals:
                header_idx = i
                break
                
        if header_idx is not None:
            cols = [str(c).upper().strip().replace(' ', '_') for c in df.iloc[header_idx].values]
            df_clean = df.iloc[header_idx + 1:].copy()
            df_clean.columns = cols
            df = df_clean
        else:
            df.columns = [str(c).upper().strip().replace(' ', '_') for c in df.columns]
            
        # Standardize column naming variations
        df = df.rename(columns={
            'SHARECODE': 'SHARE_CODE',
            'ISSUERNAME': 'ISSUER_NAME',
            'INVESTORNAME': 'INVESTOR_NAME',
            'INVESTORTYPE': 'INVESTOR_TYPE',
            'LOCALFOREIGN': 'LOCAL_FOREIGN',
            'HOLDINGSSCRIPLESS': 'HOLDINGS_SCRIPLESS',
            'HOLDINGSSCRIP': 'HOLDINGS_SCRIP',
            'TOTALHOLDINGSHARES': 'TOTAL_HOLDING_SHARES',
            'TOTAL_HOLDING': 'TOTAL_HOLDING_SHARES',
            'TOTAL_SHARES': 'TOTAL_HOLDING_SHARES',
        })
        
        # Filter rows: Keep only rows with valid stock code (4-6 chars uppercase alphanumeric/hyphen)
        if 'SHARE_CODE' in df.columns:
            df['SHARE_CODE'] = df['SHARE_CODE'].astype(str).str.strip().str.upper()
            df = df[df['SHARE_CODE'].str.match(r'^[A-Z0-9-]{4,6}$', na=False)]
            
        return df
    except Exception as e:
        print(f"   ⚠️ Error processing Excel '{filename}': {e}")
        return None

def save_backup(df, service):
    buf   = io.StringIO()
    df.to_csv(buf, index=False)
    media = MediaIoBaseUpload(
        io.BytesIO(buf.getvalue().encode('utf-8')),
        mimetype='text/csv', resumable=True)
    query = f"'{FOLDER_BACKUP_ID}' in parents and name='{BACKUP_CSV_NAME}' and trashed=false"
    old   = service.files().list(q=query, fields="files(id)").execute().get('files',[])
    if old:
        service.files().update(
            fileId=old[0]['id'], body={'name': BACKUP_CSV_NAME},
            media_body=media, supportsAllDrives=True).execute()
    else:
        service.files().create(
            body={'name': BACKUP_CSV_NAME, 'parents': [FOLDER_BACKUP_ID]},
            media_body=media, fields='id', supportsAllDrives=True).execute()
    print("   ✅ Backup tersimpan")

# =============================================================================
# MAIN
# =============================================================================
def main():
    start = time.time()
    print("="*60)
    print("🚀 KSEI 1% MONTHLY — GSheet/Excel Edition")
    print("="*60)

    os.makedirs(TEMP_DIR, exist_ok=True)
    service = authenticate()
    print("✅ Auth berhasil")

    df_existing, processed_sources = load_backup_csv(service)
    print(f"📂 Existing: {len(df_existing):,} rows | {len(processed_sources)} batches")

    all_files = list_ksei_files(service, FOLDER_PDF_ID)
    
    # Track unprocessed files: matches either full name, name without extension, or first 8 chars
    new_files = []
    for f in all_files:
        name = f['name']
        name_no_ext = os.path.splitext(name)[0]
        prefix_8 = name[:8]
        if prefix_8 not in processed_sources and name_no_ext not in processed_sources and name not in processed_sources:
            new_files.append(f)
            
    print(f"📄 {len(all_files)} total | {len(new_files)} baru")

    EXPECTED_COLS = [
        'DATE','SHARE_CODE','ISSUER_NAME','INVESTOR_NAME','INVESTOR_TYPE',
        'LOCAL_FOREIGN','NATIONALITY','DOMICILE','HOLDINGS_SCRIPLESS',
        'HOLDINGS_SCRIP','TOTAL_HOLDING_SHARES','PERCENTAGE'
    ]

    if not new_files:
        print("✅ Semua file sudah diproses di GDrive. Memastikan data di MotherDuck lengkap...")
        df_final = df_existing
    else:
        all_new = []
        for f_info in tqdm(new_files, desc="Processing files"):
            local_path = os.path.join(TEMP_DIR, f_info['name'])
            download_file(service, f_info['id'], f_info['mimeType'], local_path)
            df_temp = process_excel(local_path, f_info['name'])
            if df_temp is not None and not df_temp.empty:
                # Map column names to EXPECTED_COLS in order, filling missing ones with default values
                df_target = pd.DataFrame()
                for col in EXPECTED_COLS:
                    if col in df_temp.columns:
                        df_target[col] = df_temp[col]
                    else:
                        if col in ['HOLDINGS_SCRIPLESS','HOLDINGS_SCRIP','TOTAL_HOLDING_SHARES']:
                            df_target[col] = 0
                        elif col == 'PERCENTAGE':
                            df_target[col] = 0.0
                        else:
                            df_target[col] = ''
                            
                # Insert Data Source column (use filename without extension for cleaner tracking)
                data_source_val = os.path.splitext(f_info['name'])[0]
                df_target.insert(0, 'Data Source', data_source_val)
                df_target = df_target.replace(r'\n', ' ', regex=True)
                
                # Clean numeric columns safely
                df_target = clean_numeric_safe(df_target,
                    ['HOLDINGS_SCRIPLESS','HOLDINGS_SCRIP','TOTAL_HOLDING_SHARES'])
                df_target = clean_numeric_safe(df_target, ['PERCENTAGE'], is_float=True)
                all_new.append(df_target)

        if not all_new:
            print("❌ Tidak ada data baru.")
            return

        df_new_batch = pd.concat(all_new, ignore_index=True)
        df_final = pd.concat([df_existing, df_new_batch], ignore_index=True) \
                   if not df_existing.empty else df_new_batch
        save_backup(df_final, service)

    # Upload ke MotherDuck (Always sync the complete df_final dataset to prevent out-of-sync)
    print("\n🦆 Upload ke MotherDuck...")
    df_md = df_final.copy()
    df_md.columns = [c.lower().replace(' ', '_') for c in df_md.columns]
    
    # Force string columns to VARCHAR/object in pandas to avoid mixed-type scanner crashes in DuckDB
    for col in ['data_source', 'share_code', 'issuer_name', 'investor_name', 'investor_type', 'local_foreign', 'nationality', 'domicile']:
        if col in df_md.columns:
            df_md[col] = df_md[col].astype(str).str.strip()
            
    # Handle multiple date formatting (e.g. YYYY-MM-DD or DD/MM/YYYY)
    df_md['date'] = pd.to_datetime(df_md['date'], errors='coerce').dt.date
    df_md = df_md.dropna(subset=['date'])

    for col in ['holdings_scripless','holdings_scrip','total_holding_shares']:
        if col in df_md.columns:
            df_md[col] = pd.to_numeric(df_md[col], errors='coerce').fillna(0).astype('int64')
    if 'percentage' in df_md.columns:
        df_md['percentage'] = pd.to_numeric(df_md['percentage'], errors='coerce').fillna(0)

    con = duckdb.connect(f'md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}')
    con.execute("CREATE SCHEMA IF NOT EXISTS ksei")
    
    # Drop and recreate table directly from standardized dataframe to enforce correct schema types
    con.register("temp_ksei1", df_md)
    con.execute("DROP TABLE IF EXISTS ksei.ownership_1pct")
    con.execute("CREATE TABLE ksei.ownership_1pct AS SELECT * FROM temp_ksei1")

    count = con.execute("SELECT COUNT(*) FROM ksei.ownership_1pct").fetchone()[0]
    dates = con.execute("SELECT COUNT(DISTINCT date) FROM ksei.ownership_1pct").fetchone()[0]
    con.close()

    shutil.rmtree(TEMP_DIR, ignore_errors=True)
    print(f"   ✅ {count:,} rows | {dates} dates")
    print(f"\n🎉 SELESAI! ⏱️ {(time.time()-start)/60:.1f} menit")

if __name__ == "__main__":
    main()
