import os
import sys
import json
import time
import gc
from datetime import datetime
from DrissionPage import ChromiumPage, ChromiumOptions
import firebase_admin
from firebase_admin import credentials, db

# ==============================================================================
# KONFIGURASI TINGKAT TINGGI
# ==============================================================================
DATABASE_URL = "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/"
URL_TARGET = "https://orionterminal.com/screener"
TIMEOUT_LIMIT = 280 # 4.5 Menit

# 36 VARIABEL TARGET
COLUMNS_KEYS = [
    "price", "ticks_5m", "change_5m", "volume_5m", "volatility_15m",
    "volume_1h", "vdelta_1h", "oi_change_8h", "change_1d", "funding_rate",
    "open_interest", "oi_mc_ratio", "btc_corr_1d", "eth_corr_1d", "btc_corr_3d",
    "eth_corr_3d", "btc_beta_1d", "eth_beta_1d", "change_15m", "change_1h",
    "change_8h", "oi_change_15m", "oi_change_1d", "oi_change_1h", "oi_change_5m",
    "volatility_1h", "volatility_5m", "ticks_15m", "ticks_1h", "vdelta_15m",
    "vdelta_1d", "vdelta_5m", "vdelta_8h", "volume_15m", "volume_1d", "volume_8h"
]

# Daftar kata yang harus diabaikan (Bukan koin)
BLACKLIST = [
    "SCREENER", "ALERTS", "CHARTS", "CLI", "SYMBOL", "PRICE", "TICKS", "CHANGE", 
    "VOLUME", "VOLATILITY", "VDELTA", "OI", "FUNDING", "OPENINTEREST", "MARKETCAP",
    "ORION", "TERMINAL", "LOGIN", "SIGNUP", "SETTINGS", "PORTFOLIO", "CONNECT"
]

def init_firebase():
    json_str = os.environ.get("FIREBASE_KEY_JSON")
    if not json_str: return False
    try:
        cred_dict = json.loads(json_str)
        if not firebase_admin._apps:
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred, {'databaseURL': DATABASE_URL})
        return True
    except: return False

def is_numeric_line(text):
    """Mengecek apakah baris ini berisi data angka (ciri data koin)"""
    clean = text.replace('$', '').replace(',', '').replace('%', '').replace('+', '').replace('-', '').replace('.', '').strip()
    if not clean: return False
    # Jika baris mengandung banyak angka, berarti ini baris data
    return sum(c.isdigit() for c in clean) > 3

def run_ghost_bypass():
    print("🔥 INITIALIZING ORION GHOST BYPASS v2.1 (ADAPTIVE PARSING)...")
    start_global = time.time()
    
    if not init_firebase():
        print("❌ FIREBASE_KEY_JSON NOT FOUND!")
        return

    co = ChromiumOptions()
    co.set_argument('--headless=new')
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    co.set_argument('--disable-dev-shm-usage')
    co.set_argument('--window-size=3840,2160')
    co.set_user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")

    try:
        page = ChromiumPage(addr_or_opts=co)
        page.set.timeouts(page_load=60)
        
        print(f"🌐 ATTACKING TARGET: {URL_TARGET}")
        page.get(URL_TARGET)
        time.sleep(25)
        
        # Deteksi Cloudflare
        title = page.title
        if "Just a moment" in title or "Security" in title:
            print("🚨 CLOUDFLARE DETECTED! Refreshing...")
            page.refresh()
            time.sleep(20)

        page.run_js("document.body.style.zoom = '10%'")
        
        ref = db.reference('screener_full_data')
        status_ref = db.reference('bot_status')

        print("👀 STARTING ADAPTIVE EXTRACTION...")

        while True:
            if (time.time() - start_global) > TIMEOUT_LIMIT:
                print("🏁 SESSION TIMEOUT - EXITING")
                break

            try:
                page.run_js("window.scrollTo(0, 5000);")
                time.sleep(1)
                page.run_js("window.scrollTo(0, 0);")
                time.sleep(1)
                
                raw_extracted = page.run_js("return document.body.innerText")
                lines = [l.strip() for l in raw_extracted.split('\n') if l.strip()]
                
                data_batch = {}
                count = 0
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # LOGIKA PARSING BARU: Mendeteksi koin yang simbol dan datanya terpisah baris
                i = 0
                while i < len(lines):
                    line = lines[i]
                    
                    # 1. Deteksi Simbol (Harus mengandung huruf besar, minimal 2 karakter, tidak di blacklist)
                    if (len(line) >= 2 and any(c.isalpha() for c in line) and 
                        line.isupper() and line not in BLACKLIST):
                        
                        # 2. Lihat baris berikutnya, apakah berisi kumpulan angka?
                        if i + 1 < len(lines):
                            next_line = lines[i+1]
                            
                            if is_numeric_line(next_line):
                                # INI ADALAH BARIS DATA KOIN VALID
                                symbol = line
                                # Pecah baris data menjadi list berdasarkan spasi/tab
                                values = next_line.split()
                                
                                coin_data = {'updated': ts}
                                
                                # Masukkan ke 36 variabel
                                for idx, key in enumerate(COLUMNS_KEYS):
                                    if idx < len(values):
                                        coin_data[key] = values[idx].strip()
                                    else:
                                        coin_data[key] = "-"
                                
                                # Bersihkan Simbol untuk Firebase path
                                sym_clean = symbol.replace('.', '_').replace('/', '_').replace('$', '')
                                data_batch[sym_clean] = coin_data
                                count += 1
                                i += 1 # Loncat karena baris i+1 sudah diproses
                        
                    i += 1

                if data_batch:
                    ref.update(data_batch)
                    status_ref.set({
                        'status': 'ONLINE',
                        'last_active': ts,
                        'coins_found': count,
                        'environment': 'GITHUB_ACTION'
                    })
                    print(f"✅ [{ts}] SUCCESS: Extracted {count} coins.")
                    sys.stdout.flush()
                else:
                    print(f"⚠️ [{ts}] ZERO ROWS: Format data berubah atau halaman belum muat.")
                    if len(lines) > 10:
                        print(f"📄 PREVIEW 5 BARIS PERTAMA: {lines[:5]}")

                gc.collect()
                time.sleep(15)

            except Exception as e:
                print(f"❌ LOOP ERROR: {e}")
                time.sleep(5)
        
        page.quit()

    except Exception as e:
        print(f"❌ FATAL ERROR: {e}")

if __name__ == "__main__":
    run_ghost_bypass()
    sys.exit(0)
