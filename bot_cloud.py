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

def run_ghost_bypass():
    print("🔥 INITIALIZING ORION GHOST BYPASS v2.0...")
    start_global = time.time()
    
    if not init_firebase():
        print("❌ FIREBASE_KEY_JSON NOT FOUND!")
        return

    co = ChromiumOptions()
    co.set_argument('--headless=new')
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    co.set_argument('--disable-dev-shm-usage')
    # Resolusi Super Lebar agar tabel tidak tertekuk
    co.set_argument('--window-size=3840,2160')
    # Menyamar sebagai Browser User sungguhan
    co.set_user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")

    try:
        page = ChromiumPage(addr_or_opts=co)
        page.set.timeouts(page_load=60)
        
        print(f"🌐 ATTACKING TARGET: {URL_TARGET}")
        page.get(URL_TARGET)

        # Tunggu Rendering Pertama
        time.sleep(25)
        
        # Deteksi Cloudflare
        title = page.title
        print(f"🏷️ PAGE TITLE: {title}")
        if "Just a moment" in title or "Security" in title:
            print("🚨 CLOUDFLARE DETECTED! GitHub IP is blocked. Trying to refresh...")
            page.refresh()
            time.sleep(20)

        # Perkecil Zoom secara sistemis
        page.run_js("document.body.style.zoom = '10%'")
        
        ref = db.reference('screener_full_data')
        status_ref = db.reference('bot_status')

        print("👀 STARTING DEEP DATA EXTRACTION...")

        while True:
            if (time.time() - start_global) > TIMEOUT_LIMIT:
                print("🏁 SESSION TIMEOUT - EXITING GRACEFULLY")
                break

            try:
                # Manuver Scroll Gila (Untuk memicu data Lazy Loading)
                for i in range(3):
                    page.run_js(f"window.scrollTo(0, {i * 2000});")
                    time.sleep(0.5)
                
                # JAVASCRIPT INJECTION: Ekstraksi data langsung dari DOM Tree
                # Ini jauh lebih akurat daripada membaca teks biasa
                js_script = """
                let results = [];
                let divs = Array.from(document.querySelectorAll('div'));
                // Cari elemen yang berisi teks kapital (BTC, ETH, dll)
                divs.forEach(d => {
                    let txt = d.innerText ? d.innerText.trim() : "";
                    if (txt.length >= 2 && txt.length <= 6 && /^[A-Z0-9]+$/.test(txt)) {
                        // Jika ketemu simbol koin, ambil 36 data berikutnya yang ada di sekitar
                        let parent = d.parentElement;
                        if(parent) {
                            results.push(parent.innerText);
                        }
                    }
                });
                return results;
                """
                
                raw_extracted = page.run_js("return document.body.innerText")
                lines = [l.strip() for l in raw_extracted.split('\n') if l.strip()]
                
                data_batch = {}
                count = 0
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # LOGIKA PARSING ADAPTIF
                i = 0
                while i < len(lines):
                    line = lines[i]
                    # Syarat Koin: Huruf Besar, 2-6 karakter, bukan kata menu umum
                    if (2 <= len(line) <= 6 and line.isupper() and line.isalpha() and 
                        line not in ["HOME", "PRICE", "VOL", "TOTAL", "OPEN"]):
                        
                        # Cek apakah baris berikutnya adalah harga (mengandung angka/$)
                        if i + 1 < len(lines):
                            next_line = lines[i+1]
                            if any(c.isdigit() for c in next_line) or "$" in next_line:
                                # KOIN VALID DITEMUKAN
                                symbol = line
                                coin_data = {'updated': ts}
                                
                                # Sedot 36 variabel
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
                                i = data_idx - 1
                            else: i += 1
                        else: i += 1
                    else: i += 1

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
                    print(f"⚠️ [{ts}] ZERO ROWS: Page content might be blocked or empty.")
                    # Jika zonk, ambil screenshot mini (log as text)
                    print(f"📄 DEBUG CONTENT: {raw_extracted[:300]}...")
                    # Coba paksa scroll ulang
                    page.run_js("window.scrollTo(0, document.body.scrollHeight);")

                gc.collect()
                time.sleep(10)

            except Exception as e:
                print(f"❌ LOOP ERROR: {e}")
                time.sleep(5)
        
        page.quit()

    except Exception as e:
        print(f"❌ FATAL ERROR: {e}")

if __name__ == "__main__":
    run_ghost_bypass()
    sys.exit(0)
