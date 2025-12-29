from DrissionPage import ChromiumPage, ChromiumOptions
import firebase_admin
from firebase_admin import credentials, db
import os
import json
import time
import sys
from datetime import datetime

# KONFIGURASI
DATABASE_URL = "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/"
URL_TARGET = "https://orionterminal.com/screener"
TIMEOUT_LIMIT = 280 # 4.5 Menit

COLUMNS_KEYS = [
    "price", "ticks_5m", "change_5m", "volume_5m", "volatility_15m",
    "volume_1h", "vdelta_1h", "oi_change_8h", "change_1d", "funding_rate",
    "open_interest", "oi_mc_ratio", "btc_corr_1d", "eth_corr_1d", "btc_corr_3d",
    "eth_corr_3d", "btc_beta_1d", "eth_beta_1d", "change_15m", "change_1h",
    "change_8h", "oi_change_15m", "oi_change_1d", "oi_change_1h", "oi_change_5m",
    "volatility_1h", "volatility_5m", "ticks_15m", "ticks_1h", "vdelta_15m",
    "vdelta_1d", "vdelta_5m", "vdelta_8h", "volume_15m", "volume_1d", "volume_8h"
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

def run_safe_loop():
    print("🤖 BOT GITHUB CLEAN FILTER...")
    start_global = time.time()
    
    if not init_firebase(): return

    co = ChromiumOptions()
    co.set_argument('--headless=new')
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    co.set_user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    try:
        page = ChromiumPage(addr_or_opts=co)
        page.set.timeouts(page_load=30)
        
        try: page.get(URL_TARGET)
        except: pass

        time.sleep(20)
        ref = db.reference('screener_full_data')
        
        while True:
            durasi = time.time() - start_global
            if durasi > TIMEOUT_LIMIT: break

            try:
                # Scroll
                page.run_js("window.scrollTo(0, 10000);")
                time.sleep(0.5)
                
                # Baca Teks
                raw_text = page.run_js("return document.body.innerText")
                lines = raw_text.split('\n')
                
                data_batch = {}
                count = 0
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                for i, line in enumerate(lines):
                    line = line.strip()
                    
                    # FILTER KETAT: Hanya ambil jika Huruf Besar (A-Z) dan panjang 2-6
                    # Ini membuang angka sampah "13"
                    if 2 <= len(line) <= 6 and line.isalpha() and line.isupper():
                        symbol = line
                        try:
                            # Cek data angka di bawahnya
                            if i + 35 < len(lines):
                                raw_values = lines[i+1 : i+37]
                                first_val = raw_values[0].replace('$', '').replace(',', '').strip()
                                
                                # Validasi: Data pertama harus mengandung angka
                                if any(c.isdigit() for c in first_val):
                                    c_data = {'updated': ts}
                                    for k, key_name in enumerate(COLUMNS_KEYS):
                                        val = raw_values[k] if k < len(raw_values) else "-"
                                        c_data[key_name] = val.strip()
                                    
                                    # Bersihkan ID dari titik
                                    symbol_clean = symbol.replace('.', '_')
                                    data_batch[symbol_clean] = c_data
                                    count += 1
                        except: continue

                if data_batch:
                    ref.update(data_batch)
                    print(f"✅ Upload Clean Data: {count} Koin")
                
                time.sleep(10) 

            except Exception: pass
        
        page.quit()

    except Exception: pass

if __name__ == "__main__":
    run_safe_loop()
