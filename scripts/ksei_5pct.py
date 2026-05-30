# =============================================================================
# KSEI 5% DAILY PIPELINE — GitHub Actions Edition
# =============================================================================
import os, io, json, shutil, time
import pandas as pd
import numpy as np
import pdfplumber
from datetime import datetime as dt
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

FOLDER_PDF_ID    = '13cvZII7kxFqfeEMUS8TuQMpgkAY8vZg8'  # ← ganti dengan folder ID KSEI 5%
FOLDER_BACKUP_ID = '1nuolJ2j2bTZOJwUt2frU6YnGzTIxR39k'
BACKUP_CSV_NAME  = 'MASTER_DATABASE_5persen_CLEAN.csv'
MOTHERDUCK_DB    = 'my_db'
TEMP_DIR         = '/tmp/ksei_pdfs'

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
def list_pdfs_in_folder(service, folder_id):
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
    return files

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
    request    = service.files().get_media(fileId=files[0]['id'])
    buf        = io.BytesIO()
    dl         = MediaIoBaseDownload(buf, request)
    done       = False
    while not done:
        _, done = dl.next_chunk()
    buf.seek(0)
    df = pd.read_csv(buf)
    for col in ['Jumlah Saham (Prev)','Jumlah Saham (Curr)','Perubahan_Saham']:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(',','').str.replace(' ',''),
                errors='coerce').fillna(0).astype('int64')
    df['Tanggal_Data'] = pd.to_datetime(df['Tanggal_Data'], errors='coerce')
    df = df.dropna(subset=['Tanggal_Data'])
    existing = set(df['Tanggal_Data'].dropna().apply(
        lambda d: pd.Timestamp(d).strftime('%Y%m%d')))
    return df, existing

def process_pdf(file_path, filename):
    if len(filename) < 8 or not filename[:8].isdigit():
        return []
    d        = filename[:8]
    date_str = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    rows     = []
    try:
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages[1:]:
                table = page.extract_table()
                if not table or len(table[0]) < 6:
                    continue
                for row in table:
                    if not row or len(row) < 6:
                        continue
                    row_str = str(row).lower()
                    if any(x in row_str for x in ['kode efek','nama emiten','halaman']):
                        continue
                    if all(x is None or str(x).strip() in ['','—'] for x in row[:4]):
                        continue
                    rows.append([date_str] + list(row))
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
            fileId=old[0]['id'], body={'name':BACKUP_CSV_NAME},
            media_body=media, supportsAllDrives=True).execute()
    else:
        service.files().create(
            body={'name':BACKUP_CSV_NAME,'parents':[FOLDER_BACKUP_ID]},
            media_body=media, fields='id', supportsAllDrives=True).execute()
    print(f"   ✅ Backup tersimpan")

# =============================================================================
# MAIN
# =============================================================================
def main():
    start = time.time()
    print("="*60)
    print("🚀 KSEI 5% PIPELINE — GitHub Actions Edition")
    print("="*60)

    os.makedirs(TEMP_DIR, exist_ok=True)
    service = authenticate()
    print("✅ Auth berhasil")

    df_base, existing_dates = load_backup_csv(service)
    print(f"📂 Base: {len(df_base):,} rows | {len(existing_dates)} dates")

    all_pdfs  = list_pdfs_in_folder(service, FOLDER_PDF_ID)
    new_pdfs  = [f for f in all_pdfs
                 if len(f['name']) >= 8 and f['name'][:8].isdigit()
                 and f['name'][:8] not in existing_dates]

    print(f"📄 {len(all_pdfs)} total PDFs | {len(new_pdfs)} baru")

    if not new_pdfs:
        print("✅ Semua PDF sudah diproses.")
        return

    df_combined = df_base.copy()
    FULL_COLS = ['Tanggal_Data','No','Kode Efek','Nama Emiten',
                 'Nama Pemegang Rekening Efek','Nama Pemegang Saham',
                 'Nama Rekening Efek','Alamat_1','Alamat_2','Kebangsaan','Domisili','Status',
                 'Jumlah Saham (Prev)','Saham Gabungan (Prev)','% (Prev)',
                 'Jumlah Saham (Curr)','Saham Gabungan (Curr)','% (Curr)','Perubahan']

    for pdf_file in tqdm(new_pdfs, desc="Processing PDFs"):
        local_path = os.path.join(TEMP_DIR, pdf_file['name'])
        download_file(service, pdf_file['id'], local_path)
        raw_rows = process_pdf(local_path, pdf_file['name'])
        if not raw_rows:
            continue

        df_new = pd.DataFrame(raw_rows)
        if len(df_new.columns) >= len(FULL_COLS):
            df_new = df_new.iloc[:, :len(FULL_COLS)]
        df_new.columns = FULL_COLS[:len(df_new.columns)]
        df_new['Tanggal_Data'] = pd.to_datetime(df_new['Tanggal_Data'], errors='coerce')

        for col in ['Jumlah Saham (Prev)','Jumlah Saham (Curr)']:
            if col in df_new.columns:
                df_new[col] = pd.to_numeric(
                    df_new[col].astype(str).str.replace(',','').str.replace(' ','').str.replace('-','0'),
                    errors='coerce').fillna(0).astype('int64')

        df_new = df_new.dropna(subset=['Kode Efek','Nama Pemegang Saham'])

        def tentukan_aksi(prev, curr):
            if prev == 0 and curr > 0:      return 'Buying'
            elif curr > prev > 0:            return 'Accumulation'
            elif curr < prev:                return 'Reduction'
            elif curr == prev and curr > 0:  return 'Holding'
            else:                            return 'Skip'

        df_new['Aksi'] = df_new.apply(
            lambda r: tentukan_aksi(r.get('Jumlah Saham (Prev)',0), r.get('Jumlah Saham (Curr)',0)), axis=1)
        df_new = df_new[df_new['Aksi'] != 'Skip']

        # Ghost Whale Detection
        if not df_combined.empty:
            last_date    = df_combined['Tanggal_Data'].max()
            last_state   = df_combined[df_combined['Tanggal_Data'] == last_date]
            active_whales = last_state[last_state['Jumlah Saham (Curr)'] > 0]
            today_whales  = set(zip(df_new['Kode Efek'], df_new['Nama Pemegang Saham']))
            dropouts = []
            for _, row in active_whales.iterrows():
                if (row['Kode Efek'], row['Nama Pemegang Saham']) not in today_whales:
                    drop_row = row.copy()
                    drop_row['Tanggal_Data']        = df_new['Tanggal_Data'].iloc[0]
                    drop_row['Jumlah Saham (Prev)'] = row['Jumlah Saham (Curr)']
                    drop_row['Jumlah Saham (Curr)'] = 0
                    drop_row['Aksi']                = 'Reduction'
                    dropouts.append(drop_row)
            if dropouts:
                df_new = pd.concat([df_new, pd.DataFrame(dropouts)], ignore_index=True)

        df_new['Perubahan_Saham'] = df_new['Jumlah Saham (Curr)'] - df_new['Jumlah Saham (Prev)']
        df_new['first_date']      = df_new.groupby(['Nama Pemegang Saham','Kode Efek'])['Tanggal_Data'].transform('min')
        df_new['is_baseline']     = df_new['Tanggal_Data'] == df_new['first_date']
        df_new = df_new[(df_new['Aksi'] != 'Holding') | df_new['is_baseline']]
        df_new = df_new.drop(columns=['first_date','is_baseline'], errors='ignore')
        df_combined = pd.concat([df_combined, df_new], ignore_index=True)

    cols_keep = ['Tanggal_Data','Kode Efek','Nama Pemegang Rekening Efek',
                 'Nama Pemegang Saham','Nama Rekening Efek','Status',
                 'Jumlah Saham (Prev)','Jumlah Saham (Curr)','Perubahan_Saham','Aksi']
    df_final = df_combined[[c for c in cols_keep if c in df_combined.columns]]
    df_final = df_final.sort_values(['Tanggal_Data','Kode Efek','Nama Pemegang Saham'])

    save_backup(df_final, service)

    # Upload ke MotherDuck
    print("\n🦆 Upload ke MotherDuck...")
    cm = {'Tanggal_Data':'tanggal_data','Kode Efek':'kode_efek',
          'Nama Pemegang Rekening Efek':'nama_pemegang_rekening_efek',
          'Nama Pemegang Saham':'nama_pemegang_saham',
          'Nama Rekening Efek':'nama_rekening_efek','Status':'status',
          'Jumlah Saham (Prev)':'jumlah_saham_prev','Jumlah Saham (Curr)':'jumlah_saham_curr',
          'Perubahan_Saham':'perubahan_saham','Aksi':'aksi'}

    processed_dates = [f"{f['name'][:4]}-{f['name'][4:6]}-{f['name'][6:8]}" for f in new_pdfs]

    con = duckdb.connect(f'md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}')
    con.execute("CREATE SCHEMA IF NOT EXISTS ksei")
    con.execute("""
        CREATE TABLE IF NOT EXISTS ksei.data5_mutasi (
            tanggal_data DATE, kode_efek VARCHAR,
            nama_pemegang_rekening_efek VARCHAR, nama_pemegang_saham VARCHAR,
            nama_rekening_efek VARCHAR, status VARCHAR,
            jumlah_saham_prev BIGINT, jumlah_saham_curr BIGINT,
            perubahan_saham BIGINT, aksi VARCHAR)
    """)

    df_final['_tgl'] = pd.to_datetime(df_final['Tanggal_Data']).dt.date
    total = 0
    for date_str in processed_dates:
        target = dt.strptime(date_str, '%Y-%m-%d').date()
        df_d   = df_final[df_final['_tgl'] == target].copy()
        if len(df_d) == 0:
            continue
        con.execute(f"DELETE FROM ksei.data5_mutasi WHERE tanggal_data = '{date_str}'")
        df_md  = df_d.rename(columns=cm)[[v for v in cm.values() if v in df_d.rename(columns=cm).columns]]
        df_md['tanggal_data'] = pd.to_datetime(df_md['tanggal_data']).dt.date
        df_md  = df_md.drop(columns=['_tgl'], errors='ignore')
        con.register("temp_ksei5", df_md)
        con.execute("INSERT INTO ksei.data5_mutasi SELECT CAST(tanggal_data AS DATE), * EXCLUDE(tanggal_data) FROM temp_ksei5")
        total += len(df_md)
        print(f"   ✅ {date_str}: {len(df_md):,} rows")

    count = con.execute("SELECT COUNT(*) FROM ksei.data5_mutasi").fetchone()[0]
    con.close()

    shutil.rmtree(TEMP_DIR, ignore_errors=True)
    print(f"\n{'='*60}")
    print(f"🎉 SELESAI! {total:,} rows uploaded | Total: {count:,}")
    print(f"⏱️ {(time.time()-start)/60:.1f} menit")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
