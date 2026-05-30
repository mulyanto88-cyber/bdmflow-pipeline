# =============================================================================
# DAILY TRANSACTIONS PIPELINE — GitHub Actions Edition
# Konversi dari Colab ke standalone Python
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
# CONFIG — dari Environment Variables (GitHub Secrets)
# =============================================================================
SA_JSON             = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'])
MOTHERDUCK_TOKEN    = os.environ['MOTHERDUCK_TOKEN']

FOLDER_SUMBER_ID    = '1L0O1fc4B2jNo7pQB4ttJx6itYsAePmrt'
FOLDER_BACKUP_ID    = '1hX2jwUrAgi4Fr8xkcFWjCW6vbk6lsIlP'
NAMA_FILE_BACKUP    = 'Kompilasi_Data_Daily_Transactions_MotherDuck_Backup.csv'
NAMA_WORKSHEET      = 'Sheet1'
MOTHERDUCK_DB       = 'my_db'
MD_SCHEMA           = 'market'
MD_TABLE            = 'daily_transactions'

# =============================================================================
# AUTH — Service Account (bukan google.colab.auth)
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
