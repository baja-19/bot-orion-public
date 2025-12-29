import os
import sys
from DrissionPage import ChromiumPage, ChromiumOptions
import firebase_admin
from firebase_admin import credentials, db
import json
import time
from datetime import datetime

# KONFIGURASI
DATABASE_URL = "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/"
URL_TARGET = "https://orionterminal.com/screener"
TIMEOUT_LIMIT = 280

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
    print("--- DEBUGGING KONEKSI ---")
    # Cek apakah Kunci ada di Brankas GitHub
    json_str = os.environ.get("FIREBASE_KEY_JSON")
    
    if not json_str:
        print("❌ GAGAL FATAL: Secret 'FIREBASE_KEY_JSON' Kosong/Tidak Ditemukan!")
        print("👉 Solusi: Masuk Settings -> Secrets -> Actions -> New Repository Secret")
        return False

    print(f"✅ Kunci ditemukan. Panjang karakter: {len(json_str)}")

    try:
        cred_dict = json.loads(json_str)
        if not firebase_admin._apps:
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred, {'databaseURL': DATABASE_URL})
        print("✅ Firebase Login Sukses!")
        return True
    except Exception as e:
        print(f"❌ GAGAL LOGIN FIREBASE: {e}")
        print("👉 Solusi: Isi Secret Key mungkin salah copy-paste (kurang kurung kurawal).")
        return False

def run_debug_loop():
    print("BOT GITHUB DEBUG MODE...")
    
    # Konek dulu
    if not init_firebase():
        print("⛔ Bot berhenti karena gagal konek database.")
        return

    # Setup Browser
    print("⚙️ Menyiapkan Browser...")
    co = ChromiumOptions()
    co.set_argument('--headless=new')
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    co.set_user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    try:
        page = ChromiumPage(addr_or_opts=co)
        page.set.timeouts(page_load=30)
        
        print(f"🌐 Membuka URL: {URL_TARGET}")
        try:
            page.get(URL_TARGET)
        except:
            print("⚠️ Website lambat, lanjut...")

        print("⏳ Menunggu 15 detik...")
        time.sleep(15)

        # Cek Body
        if not page.ele('tag:body'):
            print("❌ GAGAL: Website tidak terbuka (Layar Putih/Blokir Cloudflare).")
            # Ambil Screenshot buat bukti
            page.get_screenshot(path='error.png', full_page=True)
            print("📸 Screenshot error diambil.")
            return

        print("✅ Website Terbuka! Mulaiambil data...")
        ref = db.reference('screener_full_data')
        
        start_global = time.time()
        
        while True:
            if (time.time() - start_global) > TIMEOUT_LIMIT:
                print("🏁 Waktu Habis. Selesai.")
                break

            try:
                page.run_js("window.scrollTo(0, 10000);")
                time.sleep(0.5)
                
                raw_text = page.run_js("return document.body.innerText")
                lines = raw_text.split('\n')
                
                data_batch = {}
                count = 0
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                for line in lines:
                    line = line.strip()
                    if 2 <= len(line) <= 6 and line.isalpha() and line.isupper():
                        # Simpan data mentah
                        data_batch[line] = {'raw': line, 'updated': ts}
                        count += 1

                if data_batch:
                    ref.update(data_batch)
                    print(f"✅ Update: {count} Koin")
                else:
                    print("⚠️ Data Kosong (Mungkin Cloudflare).")
                
                time.sleep(5) 

            except Exception as e:
                print(f"Error Loop: {e}")
        
        page.quit()

    except Exception as e:
        print(f"❌ Error Utama: {e}")

if __name__ == "__main__":
    run_debug_loop()
    sys.exit(0)
