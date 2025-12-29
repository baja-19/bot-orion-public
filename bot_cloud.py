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

BLACKLIST = [
    "ALERTS", "CHARTS", "CLI", "SCREENER", "PORTFOLIO", "SETTINGS",
    "LOGIN", "SIGNUP", "CONNECT", "WALLET", "SEARCH", "FILTER",
    "COLUMNS", "EXPORT", "SHARE", "FEEDBACK", "HELP", "MARKET", "OPENINTEREST"
]

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

def run_extreme_extraction():
    print("🚀 BOT ORION: EXTREME EXTRACTION MODE (GITHUB ACTIONS)...")
    start_global = time.time()
    
    if not init_firebase():
        print("❌ Firebase Init Failed")
        return

    co = ChromiumOptions()
    co.set_argument('--headless=new')
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    co.set_argument('--window-size=2560,1440') # Resolusi 2K agar tabel melebar
    co.set_user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    try:
        page = ChromiumPage(addr_or_opts=co)
        page.set.timeouts(page_load=60)
        
        print(f"🌐 Membuka Target: {URL_TARGET}")
        page.get(URL_TARGET)

        print("⏳ Menunggu Website Stabil (30 detik)...")
        time.sleep(30)
        
        # Perkecil zoom agar semua kolom ter-render oleh browser
        try: page.run_js("document.body.style.zoom = '20%'")
        except: pass

        ref = db.reference('screener_full_data')
        print("👀 MEMULAI SCANNING MENDALAM...")

        while True:
            if (time.time() - start_global) > TIMEOUT_LIMIT:
                print("🏁 Selesai.")
                break

            try:
                # Manuver Scroll untuk memicu lazy loading data
                page.run_js("window.scrollTo(0, 5000);")
                time.sleep(1)
                page.run_js("window.scrollTo(5000, 0);") # Geser kanan
                time.sleep(1)
                page.run_js("window.scrollTo(0, 0);")
                
                # METODE SCANNING BARU: Ambil semua teks dari element 'div' 
                # Ini lebih akurat daripada innerText global di Linux
                raw_text = page.run_js("return document.body.innerText")
                lines = [line.strip() for line in raw_text.split('\n') if line.strip()]
                
                # Debugging Log singkat
                if len(lines) > 0:
                    print(f"📄 Berhasil menangkap {len(lines)} baris teks.")
                
                data_batch = {}
                count = 0
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                i = 0
                while i < len(lines):
                    line = lines[i]
                    
                    # Logika Pencarian Simbol Koin
                    if (2 <= len(line) <= 6 and 
                        line.isalpha() and 
                        line.isupper() and 
                        line not in BLACKLIST):
                        
                        # Pastikan baris berikutnya bukan menu tapi angka/harga
                        if i + 1 < len(lines):
                            next_val = lines[i+1].replace('$', '').replace(',', '').strip()
                            
                            is_valid_coin = False
                            try:
                                float(next_val)
                                is_valid_coin = True
                            except:
                                is_valid_coin = False
                            
                            if is_valid_coin:
                                symbol = line
                                coin_data = {'updated': ts}
                                
                                # Ambil 36 variabel ke bawah
                                data_idx = i + 1
                                for key in COLUMNS_KEYS:
                                    if data_idx < len(lines):
                                        coin_data[key] = lines[data_idx].strip()
                                        data_idx += 1
                                    else:
                                        coin_data[key] = "-"
                                
                                symbol_clean = symbol.replace('.', '_')
                                data_batch[symbol_clean] = coin_data
                                count += 1
                                i = data_idx - 1 # Lompat ke akhir blok data koin ini
                            else:
                                i += 1
                        else:
                            i += 1
                    else:
                        i += 1

                if data_batch:
                    ref.update(data_batch)
                    print(f"✅ [{ts}] Berhasil Kirim: {count} Koin.")
                    sys.stdout.flush()
                else:
                    print("⚠️ Data belum terdeteksi. Mencoba refresh browser...")
                    page.refresh()
                    time.sleep(15)

                gc.collect()
                time.sleep(5)

            except Exception as e:
                print(f"⚠️ Kesalahan Loop: {e}")
                time.sleep(5)
        
        page.quit()

    except Exception as e:
        print(f"❌ Error Fatal: {e}")

if __name__ == "__main__":
    run_extreme_extraction()
    sys.exit(0)
