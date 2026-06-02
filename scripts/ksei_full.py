# =============================================================================
# KSEI MONTHLY PIPELINE — v2 (Two-Stage, Fail-Loud)
# =============================================================================
# Architecture:
#   Phase 1 (Python) : Drive → parse → registered DataFrame (in-memory, no DB table)
#   Phase 2 (SQL)    : DataFrame → ksei.monthly_snapshot   (one atomic CREATE OR REPLACE)
#
# Design notes:
#   - Only ONE persistent table in the DB: ksei.monthly_snapshot. No extra tables.
#   - Raw audit trail = the CSV pushed to Google Drive (upload_csv_to_drive).
#   - Derivation (deltas, top buyer/seller, split flags) lives in SQL, generated
#     once per run and applied via window functions.
#   - Atomic CREATE OR REPLACE replaces fragile DROP-then-CREATE.
# =============================================================================
import os, io, json, zipfile, time
import pandas as pd
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

FOLDER_ZIP_ID    = '1MnKL8m75GRH_WO1fllPTyGK9LJVDfuwP'
FOLDER_OUTPUT_ID = '1hX2jwUrAgi4Fr8xkcFWjCW6vbk6lsIlP'
OUTPUT_CSV_NAME  = 'KSEI_Shareholder_Pure_KSEI_Only.csv'
MOTHERDUCK_DB    = 'my_db'

# Set to False to skip the Drive CSV audit upload entirely.
# Service Accounts have no Drive storage quota, so CSV upload only works if
# FOLDER_OUTPUT_ID lives in a Shared Drive. The MotherDuck update does NOT
# depend on this; failure here is logged but does not fail the pipeline.
UPLOAD_CSV_BACKUP = True

# Position columns AFTER header normalisation (spaces -> underscores).
LOCALS   = ['Local_IS','Local_CP','Local_PF','Local_IB','Local_ID',
            'Local_MF','Local_SC','Local_FD','Local_OT']
FOREIGNS = ['Foreign_IS','Foreign_CP','Foreign_PF','Foreign_IB','Foreign_ID',
            'Foreign_MF','Foreign_SC','Foreign_FD','Foreign_OT']
OWNERSHIP_COLS = LOCALS + FOREIGNS

# Every column we expect to find in a normalised raw file. If any is missing,
# we ABORT — do NOT silently substitute zeros (the v1 bug).
REQUIRED_RAW_COLS = ['Date','Code','Type','Sec._Num','Price',
                     'Total_Local','Total_Foreign'] + OWNERSHIP_COLS

# Split / reverse detection thresholds (ratio of Sec._Num month-over-month).
SPLIT_RATIO_HI   = 1.9      # >= 1.9x   -> split  (e.g. 1:2 split ratio = 2.0)
SPLIT_RATIO_LO   = 0.55     # <= 0.55x  -> reverse (e.g. 2:1 reverse  = 0.5)

# =============================================================================
# AUTH + DRIVE HELPERS
# =============================================================================
def authenticate():
    creds = service_account.Credentials.from_service_account_info(
        SA_JSON, scopes=['https://www.googleapis.com/auth/drive'])
    return build('drive', 'v3', credentials=creds, cache_discovery=False)

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
        if not page_token: break
    return [f for f in files if 'balancepos' in f['name'].lower()]

# =============================================================================
# PHASE 1 — RAW INGEST (no derivation here, on purpose)
# =============================================================================
def normalise_headers(df):
    """Map raw KSEI headers -> canonical names. Runs FIRST, before anything else.

    Raw headers (spaces):       Date|Code|Type|Sec. Num|Price|Local IS|...|Total|Foreign IS|...|Total
    pandas auto-disambiguates duplicate 'Total' -> 'Total.1'.
    """
    # First handle the two ambiguous 'Total' columns explicitly.
    df = df.rename(columns={'Total': 'Total_Local', 'Total.1': 'Total_Foreign'})
    # Then normalise everything else: 'Local IS' -> 'Local_IS', 'Sec. Num' -> 'Sec._Num'.
    df.columns = df.columns.str.replace(' ', '_', regex=False)
    return df

def parse_ksei_date(date_str):
    s = str(date_str).lower().strip()
    months = {'jan':'01','feb':'02','mar':'03','apr':'04','mei':'05','may':'05',
              'jun':'06','jul':'07','agt':'08','aug':'08','sep':'09',
              'okt':'10','oct':'10','nov':'11','des':'12','dec':'12'}
    for k, v in months.items():
        if k in s:
            s = s.replace(k, v); break
    return pd.to_datetime(s, format='%d-%m-%Y', errors='coerce')

def download_and_parse_zip(service, file_id, file_name):
    """Returns a normalised, EQUITY-only DataFrame, or None on failure."""
    try:
        buf = io.BytesIO()
        dl  = MediaIoBaseDownload(buf, service.files().get_media(fileId=file_id))
        done = False
        while not done: _, done = dl.next_chunk()
        buf.seek(0)

        with zipfile.ZipFile(buf, 'r') as z:
            txt = next((n for n in z.namelist() if n.lower().endswith('.txt')), None)
            if not txt: return None
            with z.open(txt) as f:
                df = pd.read_csv(f, delimiter='|', encoding='latin1', thousands=',')
    except Exception as e:
        print(f"   ⚠️  Skip '{file_name}': {str(e)[:80]}")
        return None

    # Step 1: normalise headers IMMEDIATELY. This is the v1 bug fix.
    df = normalise_headers(df)

    # Step 2: EQUITY only.
    if 'Type' not in df.columns:
        raise AssertionError(f"'{file_name}': missing 'Type' column after normalisation")
    df = df[df['Type'] == 'EQUITY'].copy()

    # Step 3: fail loud if any required column is missing (no silent zero-fill).
    missing = [c for c in REQUIRED_RAW_COLS if c not in df.columns]
    if missing:
        raise AssertionError(
            f"'{file_name}': missing required columns after normalisation: {missing}\n"
            f"   Actual columns: {sorted(df.columns.tolist())}"
        )

    # Step 4: coerce numerics (now we know the columns are real).
    for c in OWNERSHIP_COLS + ['Price','Total_Local','Total_Foreign','Sec._Num']:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

    # Step 5: parse date.
    df['Date'] = df['Date'].apply(parse_ksei_date).dt.date
    df = df.dropna(subset=['Date'])

    # Step 6: integrity check — sum of 9 Local positions must equal Total_Local.
    diff_local = (df[LOCALS].sum(axis=1) - df['Total_Local']).abs()
    diff_for   = (df[FOREIGNS].sum(axis=1) - df['Total_Foreign']).abs()
    bad = int((diff_local > 1).sum() + (diff_for > 1).sum())
    if bad > 0:
        print(f"   ⚠️  '{file_name}': {bad} rows where positions don't reconcile with Total")

    return df

def upload_csv_to_drive(df, service):
    buf = io.StringIO(); df.to_csv(buf, index=False)
    media = MediaIoBaseUpload(io.BytesIO(buf.getvalue().encode('utf-8')),
                              mimetype='text/csv', resumable=True)
    q   = f"'{FOLDER_OUTPUT_ID}' in parents and name='{OUTPUT_CSV_NAME}' and trashed=false"
    old = service.files().list(q=q, fields="files(id)").execute().get('files',[])
    if old:
        service.files().update(fileId=old[0]['id'], media_body=media,
                               supportsAllDrives=True).execute()
    else:
        service.files().create(body={'name': OUTPUT_CSV_NAME,'parents':[FOLDER_OUTPUT_ID]},
                               media_body=media, fields='id', supportsAllDrives=True).execute()
    print("   ✅ CSV snapshot uploaded to Drive")

# =============================================================================
# PHASE 2 — SQL TRANSFORM (single source of truth for derivation)
# =============================================================================
def build_transform_sql():
    """Returns the CREATE OR REPLACE TABLE statement that derives
       ksei.monthly_snapshot from ksei.monthly_snapshot_raw."""

    types = OWNERSHIP_COLS
    q = lambda c: f'"{c}"'

    lag_lines = ['    LAG("Sec._Num") OVER w AS _sec_prev']
    for t in types: lag_lines.append(f'    LAG({q(t)}) OVER w AS _p_{t}')

    d_lines = []
    for t in types:
        d_lines.append(
            f'    CASE WHEN _p_{t} IS NULL OR _is_split OR _is_reverse '
            f'THEN 0 ELSE COALESCE({q(t)},0) - _p_{t} END AS d_{t}'
        )

    arr_lines = [
        f"      {{'t': '{t}', 'v': d_{t}::DOUBLE, 'val': (d_{t} * Price)::DOUBLE}}"
        for t in types
    ]

    final_cols = ['  Date, Code, Type, "Sec._Num", Price']
    for t in LOCALS:   final_cols.append(f'  {q(t)}')
    final_cols.append('  Total_Local')
    for t in FOREIGNS: final_cols.append(f'  {q(t)}')
    final_cols.append('  Total_Foreign')
    for t in types:    final_cols.append(f'  COALESCE(_p_{t}, 0) AS {q(t+"_1")}')
    final_cols += [
        '  (COALESCE("Local_IS",0)+COALESCE("Local_CP",0)+COALESCE("Local_PF",0)'
        '+COALESCE("Local_IB",0)+COALESCE("Local_ID",0)+COALESCE("Local_MF",0)'
        '+COALESCE("Local_SC",0)+COALESCE("Local_FD",0)+COALESCE("Local_OT",0)'
        '+COALESCE("Foreign_IS",0)+COALESCE("Foreign_CP",0)+COALESCE("Foreign_PF",0)'
        '+COALESCE("Foreign_IB",0)+COALESCE("Foreign_ID",0)+COALESCE("Foreign_MF",0)'
        '+COALESCE("Foreign_SC",0)+COALESCE("Foreign_FD",0)+COALESCE("Foreign_OT",0))'
        ' AS Total_Shares',
        '  _is_split   AS Is_Split_Suspect',
        '  _is_reverse AS Is_Reverse_Suspect',
        '  CASE WHEN _best.v  > 0 THEN _best.t   END AS Top_Buyer',
        '  CASE WHEN _best.v  > 0 THEN _best.v   END AS Top_Buyer_Vol',
        '  CASE WHEN _worst.v < 0 THEN _worst.t  END AS Top_Seller',
        '  CASE WHEN _worst.v < 0 THEN _worst.v  END AS Top_Seller_Vol',
        '  CASE WHEN _best.v  > 0 THEN _best.val END AS Top_Buyer_Val',
        '  CASE WHEN _worst.v < 0 THEN _worst.val END AS Top_Seller_Val',
    ]
    for t in types:
        final_cols.append(f'  d_{t}         AS {q(t+"_Chg_Vol")}')
        final_cols.append(f'  d_{t} * Price AS {q(t+"_Chg_Val")}')

    return f"""
CREATE OR REPLACE TABLE ksei.monthly_snapshot AS
WITH
raw AS (
  SELECT CAST(Date AS DATE) AS Date, * EXCLUDE(Date) FROM raw_df
),
lagged AS (
  SELECT *,
{",".join(chr(10) + L for L in lag_lines)}
  FROM raw
  WINDOW w AS (PARTITION BY Code ORDER BY Date)
),
flagged AS (
  SELECT *,
    (_sec_prev IS NOT NULL AND _sec_prev > 0 AND "Sec._Num"::DOUBLE / _sec_prev >= {SPLIT_RATIO_HI}) AS _is_split,
    (_sec_prev IS NOT NULL AND _sec_prev > 0 AND "Sec._Num"::DOUBLE / _sec_prev <= {SPLIT_RATIO_LO}) AS _is_reverse
  FROM lagged
),
deltas AS (
  SELECT *,
{",".join(chr(10) + L for L in d_lines)}
  FROM flagged
),
arr AS (
  SELECT *,
    [
{",".join(chr(10) + L for L in arr_lines)}
    ] AS _arr
  FROM deltas
),
picks AS (
  SELECT *,
    list_reduce(_arr, (a, b) -> CASE WHEN b.v > a.v THEN b ELSE a END) AS _best,
    list_reduce(_arr, (a, b) -> CASE WHEN b.v < a.v THEN b ELSE a END) AS _worst
  FROM arr
)
SELECT
{",".join(chr(10) + L for L in final_cols)}
FROM picks;
"""

# =============================================================================
# MAIN
# =============================================================================
def main():
    start = time.time()
    print("="*70)
    print("🚀 KSEI MONTHLY PIPELINE v2  (raw → SQL transform)")
    print("="*70)

    service = authenticate()
    print("✅ Drive auth OK")

    zips = list_zips(service, FOLDER_ZIP_ID)
    print(f"📦 {len(zips)} ZIP files found")
    if not zips:
        print("❌ No ZIPs to process. Aborting."); return

    # ---- Phase 1a: parse all ZIPs ----
    dfs = []
    for f in tqdm(zips, desc="Parsing ZIPs"):
        d = download_and_parse_zip(service, f['id'], f['name'])
        if d is not None: dfs.append(d)
    if not dfs:
        print("❌ No data parsed. Aborting."); return

    raw = pd.concat(dfs, ignore_index=True)

    # Dedup: latest record wins per (Code, Date). Protects against duplicated uploads.
    before = len(raw)
    raw = (raw.sort_values(['Code','Date'])
              .drop_duplicates(['Code','Date'], keep='last')
              .reset_index(drop=True))
    if before != len(raw):
        print(f"   ⚠️  Removed {before-len(raw)} duplicate (Code, Date) rows")

    print(f"✅ Raw parsed: {len(raw):,} rows, "
          f"{raw['Date'].nunique()} months, {raw['Code'].nunique()} codes")

    # ---- Phase 1b: register DataFrame to MotherDuck connection (NOT a table) ----
    print("\n🦆 Connecting to MotherDuck...")
    con = duckdb.connect(f'md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}')
    con.execute("CREATE SCHEMA IF NOT EXISTS ksei")
    con.register("raw_df", raw)  # in-memory only, no table is created in DB

    # ---- Phase 2: SQL transform — single atomic CREATE OR REPLACE ----
    print("\n🔄 Building ksei.monthly_snapshot from raw_df (one atomic statement)...")
    con.execute(build_transform_sql())

    # Validation right after transform
    val = con.execute("""
        SELECT
          COUNT(*)                                                      AS n,
          COUNT(*) FILTER (WHERE "Local_ID_Chg_Val"<>0
                              OR "Foreign_CP_Chg_Val"<>0)              AS rows_with_chg,
          COUNT(*) FILTER (WHERE Is_Split_Suspect)                      AS split_months,
          COUNT(*) FILTER (WHERE Is_Reverse_Suspect)                    AS reverse_months,
          COUNT(*) FILTER (WHERE Top_Buyer IS NOT NULL)                 AS rows_with_top_buyer
        FROM ksei.monthly_snapshot
    """).fetchone()
    print(f"   ✅ transformed: {val[0]:,} rows | with_chg={val[1]:,} | "
          f"splits={val[2]} | reverses={val[3]} | top_buyer_filled={val[4]:,}")

    if val[1] == 0:
        raise RuntimeError(
            "TRANSFORM PRODUCED ZERO DELTAS — likely a column-name regression. "
            "Inspect raw_df column names before transform."
        )

    # ---- Phase 3 (optional, non-fatal): CSV audit backup to Drive ----
    # Exported FROM MotherDuck (after transform) so it includes all derived columns:
    # _1 (prev month positions), _Chg_Vol, _Chg_Val, Top_Buyer/Seller, split flags.
    if UPLOAD_CSV_BACKUP:
        print("\n📤 Exporting full snapshot from DB → Drive CSV (optional)...")
        try:
            df_export = con.execute("SELECT * FROM ksei.monthly_snapshot ORDER BY Code, Date").df()
            upload_csv_to_drive(df_export, service)
        except Exception as e:
            print(f"   ⚠️  CSV upload skipped (NON-FATAL): {str(e)[:200]}")
            print("       → To fix: move FOLDER_OUTPUT_ID to a Shared Drive,")
            print("         or set UPLOAD_CSV_BACKUP=False to silence this step.")
    else:
        print("\n⏭️  CSV audit upload disabled by config (UPLOAD_CSV_BACKUP=False)")

    con.close()
    print(f"\n🎉 DONE in {(time.time()-start)/60:.1f} min")

if __name__ == "__main__":
    main()
