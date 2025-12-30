import os
import sys
import json
import time
import gc
import re
from datetime import datetime
from DrissionPage import ChromiumPage, ChromiumOptions
import firebase_admin
from firebase_admin import credentials, db

# ==============================================================================
# KONFIGURASI TINGKAT TINGGI
# ==============================================================================
DATABASE_URL = "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/"
URL_TARGET = "https://orionterminal.com/screener"
TIMEOUT_LIMIT = 285 # 4.7 Menit

# DAFTAR 36 VARIABEL LENGKAP SESUAI URUTAN TABEL
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
    """Inisialisasi Firebase menggunakan Secret GitHub"""
    json_str = os.environ.get("FIREBASE_KEY_JSON")
    if not json_str: 
        print("❌ Kunci FIREBASE_KEY_JSON tidak ditemukan di Secrets!")
        return False
    try:
        cred_dict = json.loads(json_str)
        if not firebase_admin._apps:
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred, {'databaseURL': DATABASE_URL})
        return True
    except Exception as e:
        print(f"❌ Gagal Init Firebase: {e}")
        return False

def is_clean_symbol(text):
    """Filter untuk membuang koin dengan karakter aneh (Mandarin/Sampah)"""
    if not text: return False
    # Hanya izinkan huruf Latin, Angka, dan simbol trading standar
    return bool(re.match(r'^[A-Z0-9/_$-]+$', text))

def run_super_scraper():
    print("🔥 INITIALIZING ORION SUPER SCRAPER v4.0 (36 VARS + HORIZONTAL SCROLL)...")
    if not init_firebase(): return

    co = ChromiumOptions()
    co.set_argument('--headless=new')
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    co.set_argument('--disable-dev-shm-usage')
    # Set resolusi Ultra Wide agar kolom tidak tertutup
    co.set_argument('--window-size=5120,2880') 
    co.set_user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")

    try:
        page = ChromiumPage(addr_or_opts=co)
        page.set.timeouts(page_load=60)
        
        print(f"🌐 Membuka Target: {URL_TARGET}")
        page.get(URL_TARGET)
        time.sleep(30) # Tunggu rendering awal yang berat

        # Trik Zoom out agar elemen dimuat lebih banyak dalam satu tampilan
        page.run_js("document.body.style.zoom = '10%'")
        
        ref = db.reference('screener_full_data')
        status_ref = db.reference('bot_status')
        start_time = time.time()

        while (time.time() - start_time) < TIMEOUT_LIMIT:
            try:
                # --- MANUVER SUPER SCROLL (4 ARAH) ---
                print("🔄 Melakukan Manuver Scroll untuk memancing 36 variabel...")
                page.scroll.to_bottom(); time.sleep(1)
                # Geser Kanan Mentok (JavaScript)
                page.run_js("window.scrollTo(10000, document.body.scrollHeight);"); time.sleep(1)
                page.scroll.to_top(); time.sleep(1)
                # Geser balik ke Kiri
                page.run_js("window.scrollTo(0, 0);"); time.sleep(1)

                # JAVASCRIPT INJECTION: Ekstraksi sel-demi-sel untuk presisi tinggi
                extraction_script = """
                let results = [];
                let rows = Array.from(document.querySelectorAll('div[role="row"], .table-row, .rt-tr-group'));
                
                rows.forEach(row => {
                    // Cari semua elemen yang mengandung teks di dalam baris
                    let cells = Array.from(row.querySelectorAll('div, span, p')).map(c => {
                        return c.childNodes.length === 1 ? c.innerText.trim() : "";
                    }).filter(v => v !== "");
                    
                    // Filter unik berurutan untuk menghindari data ganda dalam satu baris
                    let uniqueCells = cells.filter((v, i, a) => a.indexOf(v) === i);
                    if(uniqueCells.length > 3) {
                        results.push(uniqueCells);
                    }
                });
                return results;
                """
                
                raw_extracted_data = page.run_js(extraction_script)
                
                data_batch = {}
                count = 0
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                for entry in raw_extracted_data:
                    if len(entry) < 2: continue
                    
                    symbol = entry[0].upper().strip()
                    
                    # Filter Keamanan Nama Koin
                    if (is_clean_symbol(symbol) and len(symbol) >= 2 and 
                        symbol not in ["HOME", "PRICE", "ALERTS", "CLI", "SCREENER", "VOL"]):
                        
                        # Pastikan baris data koin valid (ada angka harganya)
                        if any(c.isdigit() for c in entry[1]):
                            coin_data = {'updated': ts}
                            
                            # Mapping 36 Variabel secara Dinamis
                            data_idx = 1 # Lewati simbol (index 0)
                            for key in COLUMNS_KEYS:
                                if data_idx < len(entry):
                                    coin_data[key] = entry[data_idx]
                                    data_idx += 1
                                else:
                                    coin_data[key] = "-"
                            
                            # Bersihkan ID simbol agar tidak error di Firebase
                            safe_sym = symbol.replace('.', '_').replace('/', '_').replace('$', '')
                            data_batch[safe_sym] = coin_data
                            count += 1

                if data_batch:
                    ref.update(data_batch)
                    status_ref.set({
                        'status': 'ONLINE',
                        'last_active': ts,
                        'coins_found': count,
                        'vars_captured': len(COLUMNS_KEYS),
                        'environment': 'GitHub Actions 4K'
                    })
                    print(f"✅ [{ts}] BERHASIL: Mengambil {count} koin dengan {len(COLUMNS_KEYS)} variabel.")
                    sys.stdout.flush()
                else:
                    print("⚠️ Data belum terdeteksi. Mencoba memicu ulang...")
                    page.refresh()
                    time.sleep(15)

                gc.collect()
                time.sleep(20) # Jeda antar siklus agar tidak dianggap DDOS

            except Exception as e:
                print(f"⚠️ Kesalahan Loop: {e}")
                time.sleep(10)

        page.quit()

    except Exception as e:
        print(f"❌ Error Fatal: {e}")

if __name__ == "__main__":
    run_super_scraper()
    sys.exit(0)
