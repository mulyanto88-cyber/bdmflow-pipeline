# =============================================================================
# STOCKBIT AUTO-LOGIN & TOKEN REFRESHER (Playwright Headless)
# =============================================================================
import os
import json
import time
import base64
from datetime import datetime
from playwright.sync_api import sync_playwright

def validate_token(token: str) -> bool:
    """Check if JWT token is still valid with at least 1 hour remaining."""
    if not token or len(token) < 30:
        return False
    try:
        parts = token.split('.')
        if len(parts) < 2:
            return False
        pad = 4 - len(parts[1]) % 4
        body = json.loads(base64.b64decode(parts[1] + '=' * (pad % 4)))
        exp = body.get('exp', 0)
        sisa = exp - datetime.now().timestamp()
        if sisa <= 3600:  # If less than 1 hour remaining, treat as expired
            return False
        h, m = int(sisa // 3600), int((sisa % 3600) // 60)
        print(f"✅ Token masih aktif ({h}j {m}m tersisa).")
        return True
    except Exception:
        return False

def write_token_to_sheet(sheets_service, sheet_id: str, token: str):
    """Write fresh token back to Google Sheet cell A1."""
    try:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range='A1',
            valueInputOption='RAW',
            body={'values': [[token]]}
        ).execute()
        print("💾 Token baru berhasil disimpan ke Google Sheet A1!")
    except Exception as e:
        print(f"⚠️ Gagal update token ke Sheet: {e}")

def get_stockbit_token_via_playwright(username: str, password: str) -> str:
    """
    Automates Stockbit login using Headless Chromium and extracts the Bearer JWT token.
    """
    if not username or not password:
        raise ValueError("STOCKBIT_USERNAME and STOCKBIT_PASSWORD environment variables are required!")

    print("\n🤖 Memulai Headless Browser (Playwright) untuk Auto-Login Stockbit...")
    captured_token = None

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
            ]
        )
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            viewport={'width': 1280, 'height': 800}
        )
        page = context.new_page()

        # Intercept network responses to capture auth tokens
        def handle_response(response):
            nonlocal captured_token
            try:
                if 'login' in response.url or 'token' in response.url or 'auth' in response.url:
                    if response.status == 200 and 'application/json' in response.headers.get('content-type', ''):
                        json_data = response.json()
                        token_candidate = (
                            json_data.get('data', {}).get('token') or
                            json_data.get('data', {}).get('access_token') or
                            json_data.get('token') or
                            json_data.get('access_token')
                        )
                        if token_candidate and isinstance(token_candidate, str) and len(token_candidate) > 40:
                            captured_token = token_candidate
                            print("🎯 Token berhasil ditangkap dari Network Response!")
            except Exception:
                pass

        page.on("response", handle_response)

        try:
            print("🌐 Membuka https://stockbit.com/login ...")
            page.goto('https://stockbit.com/login', wait_until='networkidle', timeout=45000)
            time.sleep(2)

            # Look for username input
            user_input = page.locator('input#username, input[name="username"], input[type="text"], input[type="email"]').first
            pass_input = page.locator('input#password, input[name="password"], input[type="password"]').first

            if not user_input.is_visible(timeout=10000):
                print("⚠️ Form username tidak langsung terlihat, mencoba klik tombol login...")
                login_nav_btn = page.locator('a[href*="login"], button:has-text("Log in"), button:has-text("Masuk")').first
                if login_nav_btn.is_visible():
                    login_nav_btn.click()
                    time.sleep(2)

            print("🔑 Mengisi kredensial username & password...")
            user_input.fill(username)
            time.sleep(0.5)
            pass_input.fill(password)
            time.sleep(0.5)

            # Click login submit button
            submit_btn = page.locator('button#loginbutton, button[type="submit"], button:has-text("Log in"), button:has-text("Masuk")').first
            print("🚀 Menekan tombol Masuk/Login...")
            submit_btn.click()

            # Wait for navigation or token capture
            page.wait_for_timeout(6000)

            # If not captured via network, inspect localStorage
            if not captured_token:
                print("🔍 Memeriksa LocalStorage browser...")
                local_storage = page.evaluate("() => JSON.stringify(window.localStorage)")
                ls_dict = json.loads(local_storage)
                for key, val in ls_dict.items():
                    if 'token' in key.lower() and isinstance(val, str) and len(val) > 40:
                        # Clean if wrapped in json string
                        try:
                            val_json = json.loads(val)
                            if isinstance(val_json, str): val = val_json
                            elif isinstance(val_json, dict) and 'token' in val_json: val = val_json['token']
                        except Exception:
                            pass
                        captured_token = val
                        print(f"🎯 Token ditemukan di LocalStorage [{key}]!")
                        break

            # If still not captured, check cookies
            if not captured_token:
                print("🔍 Memeriksa Cookies browser...")
                cookies = context.cookies()
                for c in cookies:
                    if 'token' in c['name'].lower() and len(c['value']) > 40:
                        captured_token = c['value']
                        print(f"🎯 Token ditemukan di Cookie [{c['name']}]!")
                        break

        except Exception as e:
            print(f"❌ Error selama proses Playwright Auto-Login: {e}")
        finally:
            browser.close()

    if captured_token and len(captured_token) > 40:
        print(f"✅ Auto-Login Sukses! Token baru didapatkan ({len(captured_token)} chars).")
        return captured_token

    raise RuntimeError("❌ Gagal mendapatkan token Stockbit via Playwright.")

def get_or_refresh_token(sheets_service, sheet_id: str) -> str:
    """
    Main entry point:
    1. Reads token from Google Sheet.
    2. If valid, returns it.
    3. If invalid/expired, runs Playwright Auto-Login, updates Google Sheet, and returns the fresh token.
    """
    print("📋 [Auth] Memeriksa status token saat ini...")
    existing_token = None
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range='A1'
        ).execute()
        values = result.get('values', [])
        if values and values[0] and values[0][0].strip():
            existing_token = values[0][0].strip()
    except Exception as e:
        print(f"⚠️ Gagal membaca Google Sheet: {e}")

    # Check validity
    if existing_token and validate_token(existing_token):
        return existing_token

    print("⚡ Token di Google Sheet kosong / sudah kedaluwarsa. Menjalankan Auto-Login otomatis...")
    sb_user = os.environ.get('STOCKBIT_USERNAME') or os.environ.get('STOCKBIT_EMAIL')
    sb_pass = os.environ.get('STOCKBIT_PASSWORD')

    if not sb_user or not sb_pass:
        if existing_token:
            print("⚠️ STOCKBIT_USERNAME / STOCKBIT_PASSWORD tidak diset di secrets. Mencoba menggunakan token lama...")
            return existing_token
        raise ValueError("❌ STOCKBIT_USERNAME dan STOCKBIT_PASSWORD belum diset di GitHub Secrets!")

    fresh_token = get_stockbit_token_via_playwright(sb_user, sb_pass)
    write_token_to_sheet(sheets_service, sheet_id, fresh_token)
    return fresh_token

if __name__ == '__main__':
    # Local test
    user = os.environ.get('STOCKBIT_USERNAME', '')
    pwd = os.environ.get('STOCKBIT_PASSWORD', '')
    if user and pwd:
        tok = get_stockbit_token_via_playwright(user, pwd)
        print("Result Token:", tok[:30] + "...")
    else:
        print("Set STOCKBIT_USERNAME and STOCKBIT_PASSWORD to test locally.")
