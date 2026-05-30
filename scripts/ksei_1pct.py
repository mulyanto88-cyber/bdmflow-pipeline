# =============================================================================
# KSEI 1% MONTHLY PIPELINE — GitHub Actions Edition
# =============================================================================
import os, io, json, shutil, time, re
import pandas as pd
import numpy as np
import pdfplumber
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

FOLDER_PDF_ID    = 'FOLDER_ID_KSEI_1PCT'  # ← ganti dengan folder ID Data 1%
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
def list_pdfs(service, folder_id):
    files, page_token = [], None
    query = f"'{folder_id}' in parents and name contains '.pdf' and trashed=false"
    while True:
        res = service.files().list(
            q=query, fields="nextPageToken, files(id, name)",
            pageToken=page_token, includeItemsFromAllDrives=True,
            supportsAllDrives=True).execute()
        files.extend(res.get('files', []))
        page_token = res.get('nextPageToken')
        if not page_token:
            break
    return sorted(files, key=lambda x: x['name'])

def download_file(service, file_id, dest_path):
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

def clean_numeric(df, columns, is_float=False):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r'\s+','',regex=True)
            df[col] = df[col].str.replace('.','',regex=False)
            df[col] = df[col].str.replace(',','.' if is_float else '',regex=False)
            if is_float:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('float64')
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
    return df

def process_pdf(file_path, filename):
    rows = []
    try:
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if table and len(table[0]) > 5:
                    for row in table:
                        if not row:
                            continue
                        if all(x is None or str(x).strip() == '' for x in row):
                            continue
                        row_str = str(row).lower()
                        if any(kw in row_str for kw in ['share_code','investor_name','total_holding','halaman']):
                            continue
                        rows.append(row)
    except Exception as e:
        print(f"   ⚠️ {filename}: {e}")
    return rows

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
    print("🚀 KSEI 1% MONTHLY — GitHub Actions Edition")
    print("="*60)

    os.makedirs(TEMP_DIR, exist_ok=True)
    service = authenticate()
    print("✅ Auth berhasil")

    df_existing, processed_sources = load_backup_csv(service)
    print(f"📂 Existing: {len(df_existing):,} rows | {len(processed_sources)} batches")

    all_pdfs = list_pdfs(service, FOLDER_PDF_ID)
    new_pdfs = [f for f in all_pdfs if f['name'][:8] not in processed_sources]
    print(f"📄 {len(all_pdfs)} total | {len(new_pdfs)} baru")

    if not new_pdfs:
        print("✅ Semua PDF sudah diproses.")
        return

    EXPECTED_COLS = [
        'DATE','SHARE_CODE','ISSUER_NAME','INVESTOR_NAME','INVESTOR_TYPE',
        'LOCAL_FOREIGN','NATIONALITY','DOMICILE','HOLDINGS_SCRIPLESS',
        'HOLDINGS_SCRIP','TOTAL_HOLDING_SHARES','PERCENTAGE'
    ]

    all_new = []
    for pdf_file in tqdm(new_pdfs, desc="Processing PDFs"):
        local_path = os.path.join(TEMP_DIR, pdf_file['name'])
        download_file(service, pdf_file['id'], local_path)
        rows = process_pdf(local_path, pdf_file['name'])
        if rows:
            df_temp = pd.DataFrame(rows)
            if len(df_temp.columns) >= 12:
                df_temp = df_temp.iloc[:, :12]
                df_temp.columns = EXPECTED_COLS
                df_temp.insert(0, 'Data Source', pdf_file['name'][:8])
                df_temp = df_temp.replace(r'\n', ' ', regex=True)
                df_temp = clean_numeric(df_temp,
                    ['HOLDINGS_SCRIPLESS','HOLDINGS_SCRIP','TOTAL_HOLDING_SHARES'])
                df_temp = clean_numeric(df_temp, ['PERCENTAGE'], is_float=True)
                all_new.append(df_temp)

    if not all_new:
        print("❌ Tidak ada data baru.")
        return

    df_new_batch = pd.concat(all_new, ignore_index=True)
    df_final = pd.concat([df_existing, df_new_batch], ignore_index=True) \
               if not df_existing.empty else df_new_batch

    save_backup(df_final, service)

    # Upload ke MotherDuck
    print("\n🦆 Upload ke MotherDuck...")
    df_md = df_new_batch.copy()
    df_md.columns = [c.lower().replace(' ', '_') for c in df_md.columns]
    df_md['date'] = pd.to_datetime(df_md['date'], dayfirst=True, errors='coerce').dt.date
    df_md = df_md.dropna(subset=['date'])

    for col in ['holdings_scripless','holdings_scrip','total_holding_shares']:
        if col in df_md.columns:
            df_md[col] = pd.to_numeric(df_md[col], errors='coerce').fillna(0).astype('int64')
    if 'percentage' in df_md.columns:
        df_md['percentage'] = pd.to_numeric(df_md['percentage'], errors='coerce').fillna(0)

    con = duckdb.connect(f'md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}')
    con.execute("CREATE SCHEMA IF NOT EXISTS ksei")
    table_exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema='ksei' AND table_name='ownership_1pct'
    """).fetchone()[0]

    for ds in df_md['data_source'].unique():
        if table_exists:
            con.execute(f"DELETE FROM ksei.ownership_1pct WHERE data_source='{ds}'")

    con.register("temp_ksei1", df_md)
    if table_exists:
        con.execute("INSERT INTO ksei.ownership_1pct SELECT * FROM temp_ksei1")
    else:
        con.execute("CREATE TABLE ksei.ownership_1pct AS SELECT * FROM temp_ksei1")

    count = con.execute("SELECT COUNT(*) FROM ksei.ownership_1pct").fetchone()[0]
    dates = con.execute("SELECT COUNT(DISTINCT date) FROM ksei.ownership_1pct").fetchone()[0]
    con.close()

    shutil.rmtree(TEMP_DIR, ignore_errors=True)
    print(f"   ✅ {count:,} rows | {dates} dates")
    print(f"\n🎉 SELESAI! ⏱️ {(time.time()-start)/60:.1f} menit")

if __name__ == "__main__":
    main()
