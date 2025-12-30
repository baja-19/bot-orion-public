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
# KONFIGURASI TITAN
# ==============================================================================
DATABASE_URL = "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/"
URL_TARGET = "https://orionterminal.com/screener"
TIMEOUT_LIMIT = 285 

# DAFTAR 36 VARIABEL LENGKAP
COLUMNS_KEYS = [
    "price", "ticks_5m", "change_5m", "volume_5m", "volatility_15m",
    "volume_1h", "vdelta_1h", "oi_change_8h", "change_1d", "funding_rate",
    "open_interest", "oi_mc_ratio", "btc_corr_1d", "eth_corr_1d", "btc_corr_3d",
    "eth_corr_3d", "btc_beta_1d", "eth_beta_1d", "change_15m", "change_1h",
    "change_8h", "oi_change_15m", "oi_change_1d", "oi_change_1h", "oi_change_5m",
    "volatility_1h", "volatility_5m", "ticks_15m", "ticks_1h", "vdelta_15m",
    "vdelta_1d", "vdelta_5m", "vdelta_8h", "volume_15m", "volume_1d", "volume_8h"
]

BLACKLIST = ["ALERTS", "CHARTS", "CLI", "SCREENER", "PORTFOLIO", "SETTINGS", "LOGIN", "SIGNUP", "CONNECT", "WALLET", "SEARCH", "FILTER", "COLUMNS", "MARKET"]

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

def is_clean_symbol(text):
    if not text or len(text) < 2: return False
    return bool(re.match(r'^[A-Z0-9/_$-]+$', text))

def force_activate_columns(page):
    """Fungsi paling penting: Memaksa Orion mencentang semua 36 variabel"""
    print("🛠️ Memaksa aktivasi semua kolom variabel (36 metrik)...")
    js_logic = """
    try {
        // 1. Temukan dan klik tombol 'Columns'
        let btns = Array.from(document.querySelectorAll('button'));
        let target = btns.find(b => b.innerText.includes('Columns') || b.innerHTML.includes('layout'));
        if(target) {
            target.click();
            // 2. Tunggu sebentar dan centang SEMUA checkbox yang mati
            setTimeout(() => {
                let checkBoxes = document.querySelectorAll('input[type="checkbox"]');
                checkBoxes.forEach(cb => {
                    if(!cb.checked) cb.click();
                });
                console.log('Semua kolom diaktifkan.');
            }, 2000);
        }
    } catch(e) { console.error(e); }
    """
    page.run_js(js_logic)
    time.sleep(5)

def run_titan_scraper():
    print("🔥 INITIALIZING ORION TITAN v7.0 (MAX PRECISION)...")
    if not init_firebase(): return

    co = ChromiumOptions()
    co.set_argument('--headless=new')
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    co.set_argument('--disable-dev-shm-usage')
    # RESOLUSI ULTRA-WIDE (5000px) AGAR SEMUA KOLOM TERPAKSA RENDER
    co.set_argument('--window-size=5000,3000') 
    co.set_user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")

    try:
        page = ChromiumPage(addr_or_opts=co)
        page.set.timeouts(page_load=60)
        
        print(f"🌐 Menyerbu Target: {URL_TARGET}")
        page.get(URL_TARGET)
        time.sleep(30)

        # AKTIFKAN SEMUA KOLOM DULU
        force_activate_columns(page)
        
        # ZOOM OUT EKSTREM AGAR DOM MEMUAT SEMUA DATA
        page.run_js("document.body.style.zoom = '5%'")
        
        ref = db.reference('screener_full_data')
        status_ref = db.reference('bot_status')
        start_time = time.time()

        while (time.time() - start_time) < TIMEOUT_LIMIT:
            try:
                # MANUVER GRID SCROLLING (Sangat Penting untuk Lazy Loading)
                print("🔄 Syncing Grid (Down -> Right -> Top)...")
                page.run_js("window.scrollTo(0, 15000);")
                time.sleep(1)
                page.run_js("window.scrollTo(10000, 15000);") # Geser kanan mentok
                time.sleep(1)
                page.run_js("window.scrollTo(0, 0);")
                time.sleep(2)

                # EKSTRAKSI SURGICAL: Mencari element 'div' yang berfungsi sebagai row
                # Kita mengambil teks sel secara berurutan tanpa filter unik
                js_extract = """
                let results = [];
                let rows = Array.from(document.querySelectorAll('div[role="row"], .table-row, .rt-tr-group'));
                rows.forEach(row => {
                    let cells = Array.from(row.querySelectorAll('div, span')).map(c => {
                        // Hanya ambil div/span yang isinya murni teks (bukan container lagi)
                        return (c.children.length === 0) ? c.innerText.trim() : "";
                    }).filter(v => v !== "");
                    
                    if(cells.length > 5) results.push(cells);
                });
                return results;
                """
                grid_data = page.run_js(js_extract)
                
                # Jika DOM extraction gagal, gunakan Brute Force Text
                if not grid_data:
                    raw_text = page.run_js("return document.body.innerText")
                    lines = [l.strip() for l in raw_text.split('\n') if l.strip()]
                else:
                    # Gabungkan data grid menjadi flat list untuk parser
                    lines = []
                    for r in grid_data: lines.extend(r)

                data_batch = {}
                count = 0
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # PARSING ADAPTIVE (Mendeteksi koin dan 36 metrik)
                idx = 0
                while idx < len(lines):
                    line = lines[idx]
                    
                    # Logika: Simbol koin adalah kata kapital, bukan menu, panjang 2-10
                    if (2 <= len(line) <= 12 and line.isupper() and is_clean_symbol(line) and line not in BLACKLIST):
                        
                        # Cek baris berikutnya (Harus angka/harga)
                        if idx + 1 < len(lines):
                            price_val = lines[idx+1].replace('$', '').replace(',', '').strip()
                            
                            is_valid_coin = False
                            try:
                                if any(c.isdigit() for c in price_val): is_valid_coin = True
                            except: pass
                            
                            if is_valid_coin:
                                symbol = line
                                coin_data = {'updated': ts}
                                
                                # SEDOT 36 VARIABEL BERIKUTNYA
                                ptr = idx + 1
                                vars_captured = 0
                                while vars_captured < 36 and ptr < len(lines):
                                    val = lines[ptr]
                                    # Jika menabrak koin selanjutnya, stop
                                    if (vars_captured > 5 and len(val) <= 12 and val.isupper() and 
                                        is_clean_symbol(val) and val not in BLACKLIST):
                                        break
                                    
                                    coin_data[COLUMNS_KEYS[vars_captured]] = val
                                    vars_captured += 1
                                    ptr += 1
                                
                                safe_sym = symbol.replace('.', '_').replace('/', '_')
                                data_batch[safe_sym] = coin_data
                                count += 1
                                idx = ptr - 1
                            else: idx += 1
                        else: idx += 1
                    else: idx += 1

                if data_batch:
                    ref.update(data_batch)
                    status_ref.set({
                        'status': 'ONLINE',
                        'last_active': ts,
                        'coins': count,
                        'vars': 36,
                        'msg': 'Titan Scraper Success'
                    })
                    print(f"✅ [{ts}] TITAN SUCCESS: Terkirim {count} Koin (Full 36 Data).")
                    sys.stdout.flush()
                else:
                    print("⚠️ Data belum terdeteksi. Refreshing browser...")
                    page.refresh()
                    time.sleep(15)

                gc.collect()
                time.sleep(15)

            except Exception as e:
                print(f"⚠️ Loop Error: {e}")
                time.sleep(5)

        page.quit()

    except Exception as e:
        print(f"❌ Fatal Error: {e}")

if __name__ == "__main__":
    run_titan_scraper()
    sys.exit(0)
