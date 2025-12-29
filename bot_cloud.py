from DrissionPage import ChromiumPage, ChromiumOptions
import firebase_admin
from firebase_admin import credentials, db
import os
import json
import time
from datetime import datetime

# KONFIGURASI
DATABASE_URL = "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/"
URL_TARGET = "https://orionterminal.com/screener"

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
    # Ambil kunci dari "Brankas" GitHub (Environment Variable)
    json_str = os.environ.get("FIREBASE_KEY_JSON")
    
    if not json_str:
        print("❌ Kunci tidak ditemukan di GitHub Secret!")
        return False

    try:
        cred_dict = json.loads(json_str)
        if not firebase_admin._apps:
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred, {'databaseURL': DATABASE_URL})
        print("✅ Firebase Terhubung!")
        return True
    except Exception as e:
        print(f"❌ Gagal Konek: {e}")
        return False

def run_once():
    print("🤖 BOT GITHUB (SNAPSHOT)...")
    if not init_firebase(): return

    # Settingan Linux Headless (Tanpa Layar)
    co = ChromiumOptions()
    co.set_argument('--headless=new')
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    
    # Gunakan User Agent agar tidak langsung diblokir
    co.set_user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    try:
        # DrissionPage di Linux akan otomatis cari Chrome
        page = ChromiumPage(addr_or_opts=co)
        
        print(f"\n🌐 Membuka Orion...")
        page.get(URL_TARGET)

        print("⏳ Menunggu Loading (30 detik)...")
        time.sleep(30)
        
        # Cek apakah website terbuka
        if not page.ele('tag:body'):
            print("❌ Gagal Load (Mungkin diblokir Cloudflare).")
            return

        # Zoom Out dikit
        try: page.run_js("document.body.style.zoom = '25%'")
        except: pass

        ref = db.reference('screener_full_data')
        
        # Scroll Pancingan
        page.scroll.to_bottom(); time.sleep(1)
        page.run_js("window.scrollTo(10000, 0);"); time.sleep(1)
        page.scroll.to_top(); time.sleep(1)

        # Baca Data
        full_text = page.run_js("return document.body.innerText")
        lines = full_text.split('\n')
        
        data_batch = {}
        count = 0
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for i, line in enumerate(lines):
            line = line.strip()
            # Logika cari koin (Huruf Besar 2-6 digit)
            if 2 <= len(line) <= 6 and line.isalpha() and line.isupper():
                try:
                    # Cek baris-baris di bawahnya
                    if i + 35 < len(lines):
                        raw_values = lines[i+1 : i+37]
                        
                        # Validasi angka pertama
                        first = raw_values[0].replace('$', '').replace(',', '').strip()
                        if any(c.isdigit() for c in first):
                            c_data = {'updated': ts}
                            for k, key_name in enumerate(COLUMNS_KEYS):
                                c_data[key_name] = raw_values[k] if k < len(raw_values) else "-"
                            
                            data_batch[line] = c_data
                            count += 1
                except: continue

        if data_batch:
            try:
                ref.update(data_batch)
                print(f"✅ SUKSES UPLOAD: {count} Koin!")
            except Exception as e:
                print(f"⚠️ Gagal Upload: {e}")
        else:
            print("⚠️ Data Kosong.")

        page.quit()

    except Exception as e:
        print(f"❌ Error Fatal: {e}")

if __name__ == "__main__":
    run_once()