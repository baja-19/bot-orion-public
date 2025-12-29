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
# KONFIGURASI
# ==============================================================================
DATABASE_URL = "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/"
URL_TARGET = "https://orionterminal.com/screener"
TIMEOUT_LIMIT = 280

# LIST BLACKLIST (Menu yang harus dihindari)
BLACKLIST = [
    "ALERTS", "CHARTS", "CLI", "SCREENER", "PORTFOLIO", "SETTINGS",
    "LOGIN", "SIGNUP", "CONNECT", "WALLET", "SEARCH", "FILTER",
    "COLUMNS", "EXPORT", "SHARE", "FEEDBACK", "HELP", "MARKET",
    "TYPE", "PRICE", "CHANGE", "VOLUME", "HIGH", "LOW", "OPENINTEREST"
]

# 36 VARIABEL
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
        if not firebase_admin._apps:
            cred = credentials.Certificate(json.loads(json_str))
            firebase_admin.initialize_app(cred, {'databaseURL': DATABASE_URL})
        return True
    except: return False

def run_linux_adaptive():
    print("🚀 BOT ORION: LINUX ADAPTIVE MODE...")
    start_global = time.time()
    
    if not init_firebase(): return

    co = ChromiumOptions()
    co.set_argument('--headless=new')
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    co.set_argument('--window-size=1920,1080') # Layar Full HD
    co.set_user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    try:
        page = ChromiumPage(addr_or_opts=co)
        page.set.timeouts(page_load=60)
        
        try: page.get(URL_TARGET)
        except: pass

        print("⏳ Menunggu Loading (25 detik)...")
        time.sleep(25)
        
        # Validasi Halaman
        if "Orion" not in page.title:
            print("❌ Gagal Load (Judul salah).")
            return

        # Zoom Out
        try: page.run_js("document.body.style.zoom = '25%'")
        except: pass

        ref = db.reference('screener_full_data')
        print("👀 MONITORING AKTIF...")

        cycle_count = 0
        while True:
            if (time.time() - start_global) > TIMEOUT_LIMIT:
                print("🏁 Selesai.")
                break

            try:
                # Scroll
                page.run_js("window.scrollTo(0, 10000);")
                time.sleep(0.5)
                page.run_js("window.scrollTo(0, 0);")
                time.sleep(0.5)
                
                # BACA TEXT (Metode InnerText)
                raw_text = page.run_js("return document.body.innerText")
                lines = [line.strip() for line in raw_text.split('\n') if line.strip()]
                
                data_batch = {}
                count = 0
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                i = 0
                while i < len(lines):
                    line = lines[i]
                    
                    # LOGIKA CARI KOIN:
                    # 1. Panjang 2-6 (BTC)
                    # 2. Huruf Besar Semua
                    # 3. Tidak ada di Blacklist
                    # 4. Bukan Angka
                    if (2 <= len(line) <= 6 and 
                        line.isalpha() and 
                        line.isupper() and 
                        line not in BLACKLIST):
                        
                        # VALIDASI GANDA: Cek baris bawahnya
                        if i + 1 < len(lines):
                            next_line = lines[i+1].replace('$', '').replace(',', '').strip()
                            
                            # Baris bawah HARUS angka (Harga)
                            # Bisa bentuk: "96000" atau "0.05"
                            is_number = False
                            try:
                                float(next_line)
                                is_number = True
                            except: 
                                is_number = False
                            
                            if is_number:
                                # INI KOIN VALID!
                                symbol = line
                                coin_data = {'updated': ts}
                                
                                # Ambil data mulai dari i+1
                                data_idx = i + 1
                                for key in COLUMNS_KEYS:
                                    if data_idx < len(lines):
                                        val = lines[data_idx]
                                        coin_data[key] = val
                                        data_idx += 1
                                    else:
                                        coin_data[key] = "-"
                                
                                symbol_clean = symbol.replace('.', '_')
                                data_batch[symbol_clean] = coin_data
                                count += 1
                                
                                # Lompat index
                                i = data_idx - 1
                            else:
                                i += 1
                        else:
                            i += 1
                    else:
                        i += 1

                if data_batch:
                    try:
                        ref.update(data_batch)
                        print(f"✅ [{ts}] Upload: {count} Koin (Linux)")
                        sys.stdout.flush()
                    except: pass
                else:
                    # Jika kosong, coba refresh
                    print("⚠️ Data kosong. Refreshing...")
                    page.refresh()
                    time.sleep(15)

                cycle_count += 1
                if cycle_count >= 10: gc.collect(); cycle_count = 0
                
                time.sleep(5)

            except Exception:
                time.sleep(5)
        
        page.quit()

    except Exception: pass

if __name__ == "__main__":
    run_linux_adaptive()
    sys.exit(0)
