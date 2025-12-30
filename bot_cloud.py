
import os
import sys
import json
import time
import random
import re
import signal
import traceback
from datetime import datetime
import requests
import firebase_admin
from firebase_admin import credentials, db
from DrissionPage import ChromiumPage, ChromiumOptions

# ==============================================================================
# KONFIGURASI UTAMA (sesuaikan bila perlu)
# ==============================================================================
DATABASE_URL = "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/"
ORION_API_URL = "https://orionterminal.com/api/screener"
ORION_UI_URL = "https://orionterminal.com/screener"

GLOBAL_TIMEOUT = 270            # Batas total eksekusi (detik) agar aman di GitHub Actions
FETCH_INTERVAL = 60             # Interval reload data (detik)
TIMEOUT_REQUEST = 20            # Timeout untuk request API (detik)

# Browser hard timeouts (detik) — jika page.get atau run_js lebih lama dari ini, akan dibatalkan
BROWSER_PAGELOAD_TIMEOUT = 25
BROWSER_JS_TIMEOUT = 8

# Snapshot management
SNAPSHOT_RETENTION_HOURS = 24
CLEANUP_INTERVAL = 900          # Cleanup tiap 15 menit

# Regex simbol valid (Latin + angka + underscore setelah sanitasi)
SYMBOL_REGEX = re.compile(r"^[A-Z0-9_]+$")

# Sleep chunk untuk adaptive sleep (detik)
SLEEP_CHUNK = 0.5

# ==============================================================================
# HELPER: LOG + FLUSH
# ==============================================================================
def lg(msg):
    """Log singkat + flush agar tampil real-time di CI."""
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")
    sys.stdout.flush()

# ==============================================================================
# HELPER: Timeout (signal-based) untuk operasi sinkron (hanya Linux / main thread)
# ==============================================================================
class HardTimeout(Exception):
    pass

def _timeout_handler(signum, frame):
    raise HardTimeout("Operation timed out by hard timeout (signal)")

def run_with_hard_timeout(fn, timeout_sec, *args, **kwargs):
    """
    Jalankan fungsi `fn` dengan batas waktu `timeout_sec` detik.
    Menggunakan signal.alarm — hanya bekerja di main thread pada OS yang support (Linux).
    """
    old_handler = signal.getsignal(signal.SIGALRM)
    try:
        signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(int(timeout_sec))
        return fn(*args, **kwargs)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)

# ==============================================================================
# HELPER: Adaptive sleep (memecah sleep jadi chunk kecil dan cek waktu global)
# ==============================================================================
def adaptive_sleep(total_seconds, start_time):
    """
    Tidur total_seconds tapi dipecah ke chunk kecil (SLEEP_CHUNK) sehingga
    loop bisa lebih cepat merespon jika GLOBAL_TIMEOUT tercapai.
    """
    end = time.time() + total_seconds
    while time.time() < end:
        # jika waktu global hampir habis, break segera
        if time.time() - start_time >= GLOBAL_TIMEOUT:
            break
        time.sleep(SLEEP_CHUNK)

# ==============================================================================
# FIREBASE INIT (menggunakan env FIREBASE_KEY_JSON)
# ==============================================================================
def init_firebase():
    key_json = os.environ.get("FIREBASE_KEY_JSON")
    if not key_json:
        raise RuntimeError("FIREBASE_KEY_JSON tidak ditemukan di environment")
    cred_dict = json.loads(key_json)
    if not firebase_admin._apps:
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred, {"databaseURL": DATABASE_URL})
    lg("Firebase initialized")

# ==============================================================================
# ORION API CLIENT (MODE UTAMA) — pure XHR
# ==============================================================================
class OrionAPI:
    def __init__(self):
        self.session = requests.Session()
        self.rotate_headers()

    def rotate_headers(self):
        """Set header acak (rotate) untuk mengurangi fingerprint."""
        self.session.headers.clear()
        self.session.headers.update({
            "accept": "application/json, text/javascript, */*; q=0.01",
            "x-requested-with": "XMLHttpRequest",
            "referer": ORION_UI_URL,
            "user-agent": random.choice([
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Mozilla/5.0 (X11; Linux x86_64)",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
            ])
        })

    def set_cookies_header(self, cookies: dict):
        """Jika dipanen cookies dari browser fallback, injeksi header cookie."""
        if cookies:
            self.session.headers["cookie"] = "; ".join(f"{k}={v}" for k, v in cookies.items())

    def fetch(self):
        """Ambil data JSON dari endpoint XHR Orion dengan timeout."""
        r = self.session.get(ORION_API_URL, timeout=TIMEOUT_REQUEST)
        if r.status_code == 200 and r.text:
            try:
                return r.json()
            except Exception:
                raise RuntimeError("Gagal parse JSON dari API")
        else:
            raise RuntimeError(f"XHR failed with status {r.status_code}")

# ==============================================================================
# BROWSER FALLBACK: harvest cookie + force render table (snake scrolling, klik columns)
# ==============================================================================
def browser_fallback(start_time):
    """
    Jalankan DrissionPage headless untuk:
    - harvest cookies (mis. cf_clearance)
    - paksa render tabel dengan resolusi besar
    - snake scrolling
    - klik Columns & centang (gunakan JS injeksi jika tersedia)
    Fungsi ini memakai run_with_hard_timeout untuk page.get & run_js agar tidak hang.
    """
    lg("Browser fallback: memulai DrissionPage untuk harvest cookie dan render")
    co = ChromiumOptions()
    co.set_argument("--headless=new")
    co.set_argument("--no-sandbox")
    co.set_argument("--disable-gpu")
    co.set_argument("--window-size=5000,3000")  # wajib sesuai master prompt
    co.set_user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

    page = None
    try:
        page = ChromiumPage(addr_or_opts=co)
        # Hard timeout untuk membuka halaman
        try:
            run_with_hard_timeout(lambda: page.get(ORION_UI_URL), BROWSER_PAGELOAD_TIMEOUT)
        except HardTimeout:
            lg("⚠️ Browser page.get timeout — membatalkan fallback browser")
            try:
                page.quit()
            except:
                pass
            raise

        # Short wait tapi responsive
        adaptive_sleep(3, start_time)

        # Inject JS: coba klik Columns & centang semua (jika markup ada)
        js_try_click_columns = """
        (function(){
            try {
                // Cari tombol Columns (berbagai kemungkinan selector)
                const selectors = [
                  'button[aria-label="Columns"]',
                  'button[title*="Columns"]',
                  'button:contains("Columns")',
                  '.columns-btn',
                  '.btn-columns'
                ];
                function clickIf(el){
                  if(el && typeof el.click === 'function'){ el.click(); return true; }
                  return false;
                }
                for (const s of selectors){
                  try {
                    const el = document.querySelector(s);
                    if(el){ clickIf(el); }
                  } catch(e){}
                }
                // Jika ada panel columns, coba cek semua input checkbox di dalamnya
                const panels = document.querySelectorAll('div,section');
                panels.forEach(p=>{
                  const inputs = p.querySelectorAll('input[type=checkbox]');
                  inputs.forEach(i=>{ if(!i.checked) i.click(); });
                });
                return true;
            } catch(e){ return false; }
        })();
        """

        # run_js dengan hard timeout (agar tidak bengong)
        try:
            run_with_hard_timeout(lambda: page.run_js(js_try_click_columns), BROWSER_JS_TIMEOUT)
        except HardTimeout:
            lg("⚠️ JS injection timeout (Columns) — melanjutkan tanpa klik")

        # Snake scrolling: Bawah -> Kanan -> Atas -> Kiri (dipecah agar responsive)
        for _ in range(2):
            if time.time() - start_time >= GLOBAL_TIMEOUT:
                lg("⏹️ Waktu global habis selama snake-scrolling, abort fallback")
                break
            try:
                run_with_hard_timeout(lambda: page.run_js("window.scrollTo(0, document.body.scrollHeight);"), BROWSER_JS_TIMEOUT)
            except HardTimeout:
                lg("⚠️ scrollTo bottom timeout")
            adaptive_sleep(0.8, start_time)
            try:
                run_with_hard_timeout(lambda: page.run_js("window.scrollTo(document.body.scrollWidth, 0);"), BROWSER_JS_TIMEOUT)
            except HardTimeout:
                lg("⚠️ scrollTo right timeout")
            adaptive_sleep(0.8, start_time)
            try:
                run_with_hard_timeout(lambda: page.run_js("window.scrollTo(0, 0);"), BROWSER_JS_TIMEOUT)
            except HardTimeout:
                lg("⚠️ scrollTo top timeout")
            adaptive_sleep(0.8, start_time)
            try:
                run_with_hard_timeout(lambda: page.run_js("window.scrollTo(0, 0);"), BROWSER_JS_TIMEOUT)
            except HardTimeout:
                pass

        # Ambil cookies (cegah halaman hang dengan batas)
        cookies = {}
        try:
            # run_with_hard_timeout tidak perlu untuk page.cookies (biasanya cepat)
            cookies = {c['name']: c['value'] for c in page.cookies}
        except Exception:
            lg("⚠️ Gagal baca cookies dari page, tapi melanjutkan")

        try:
            page.quit()
        except:
            pass

        if not cookies:
            lg("⚠️ Tidak ditemukan cookie (hasil kosong dari browser fallback)")
        else:
            lg("✅ Browser fallback selesai: cookie dipanen")

        return cookies

    except HardTimeout:
        lg("⚠️ Hard timeout di browser fallback — keluar")
        if page:
            try:
                page.quit()
            except:
                pass
        raise
    except Exception as e:
        lg(f"❌ Browser fallback error: {e}")
        lg(traceback.format_exc())
        if page:
            try:
                page.quit()
            except:
                pass
        raise

# ==============================================================================
# NORMALISASI DATA (semua variabel, filter simbol non-latin)
# ==============================================================================
def normalize_data(raw):
    """
    Menyimpan semua field yang dikirim API (future-proof).
    Filtering: hanya symbol yang mengandung huruf/angka latin setelah sanitasi.
    """
    rows = raw.get("data") or raw.get("rows") or raw
    coins = {}

    if not isinstance(rows, list):
        return coins

    for row in rows:
        # Ambil symbol (beberapa API menggunakan key berbeda)
        symbol_raw = row.get("symbol") or row.get("market") or row.get("pair") or row.get("name")
        if not symbol_raw:
            continue

        symbol = (
            symbol_raw.replace("/", "_")
                      .replace("-", "_")
                      .replace(".", "_")
                      .upper()
        )

        # Hanya simpan simbol Latin/A-Z/0-9/underscore
        if not SYMBOL_REGEX.match(symbol):
            continue

        coins[symbol] = {
            "symbol": symbol,
            "updated_utc": datetime.utcnow().isoformat(),
            **row
        }

    return coins

# ==============================================================================
# PUSH KE FIREBASE (struktur teratur)
# ==============================================================================
def fetch_and_push(api, root_ref, start_time):
    """
    Coba fetch via API; jika gagal (HTTP bukan 200) maka jalankan browser_fallback
    untuk harvest cookie lalu ulangi fetch.
    """
    raw = None
    try:
        raw = api.fetch()
    except Exception as e:
        lg(f"⚠️ API fetch gagal: {e} — memicu browser fallback")
        try:
            # jika waktu hampir habis, jangan mulai fallback
            if time.time() - start_time >= GLOBAL_TIMEOUT - 5:
                raise RuntimeError("Waktu global hampir habis — skip browser fallback")
            cookies = browser_fallback(start_time)
            api.rotate_headers()
            api.set_cookies_header(cookies)
            lg("🔁 Mencoba fetch ulang setelah harvest cookie")
            raw = api.fetch()
        except Exception as e2:
            lg(f"❌ Gagal fetch setelah fallback: {e2}")
            raise

    coins = normalize_data(raw)
    if not coins:
        raise RuntimeError("No coins parsed from API response")

    ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")

    # Update firebase secara terstruktur
    coins_ref = root_ref.child("coins")
    meta_ref = root_ref.child("metadata")
    snap_ref = root_ref.child("snapshots")

    coins_ref.update(coins)
    meta_ref.update({
        "last_update": ts,
        "total_coins": len(coins),
        "source": "orion_xhr_api"
    })
    snap_ref.child(ts).set(coins)

    lg(f"✅ {len(coins)} coins pushed to Firebase @ {ts}")

# ==============================================================================
# CLEANUP SNAPSHOT LAMA (berdasarkan timestamp key)
# ==============================================================================
def cleanup_old_snapshots(root_ref):
    snap_ref = root_ref.child("snapshots")
    snaps = snap_ref.get()
    if not snaps:
        return 0

    now = datetime.utcnow()
    deleted = 0
    for key in list(snaps.keys()):
        try:
            ts = datetime.strptime(key, "%Y-%m-%d_%H-%M-%S")
            age_hours = (now - ts).total_seconds() / 3600.0
            if age_hours > SNAPSHOT_RETENTION_HOURS:
                snap_ref.child(key).delete()
                deleted += 1
        except Exception:
            # jika key tidak match format, skip
            continue

    if deleted:
        lg(f"🧹 Cleanup: {deleted} old snapshots removed")
    return deleted

# ==============================================================================
# MAIN LOOP (with Strict Global Timer & Signal Watchdog)
# ==============================================================================
def run():
    lg("🚀 ORION QUANT BOT START")
    try:
        init_firebase()
    except Exception as e:
        lg(f"❌ Firebase init failed: {e}")
        lg(traceback.format_exc())
        return

    api = OrionAPI()
    root_ref = db.reference("screener_orion")

    start_time = time.time()
    last_cleanup = 0

    # Loop utama: jalankan sampai GLOBAL_TIMEOUT tercapai
    while True:
        # cek global timeout
        if time.time() - start_time >= GLOBAL_TIMEOUT:
            lg("⏹️ Global timeout tercapai — bot akan berhenti sekarang")
            break

        try:
            fetch_and_push(api, root_ref, start_time)
        except Exception as e:
            lg(f"⚠️ Error during fetch_and_push: {e}")
            lg(traceback.format_exc())
            # rotate headers & continue; jangan langsung exit kecuali waktu habis
            try:
                api.rotate_headers()
            except:
                pass

        # Periodic cleanup (tidak setiap loop)
        if time.time() - last_cleanup > CLEANUP_INTERVAL:
            try:
                cleanup_old_snapshots(root_ref)
            except Exception as e:
                lg(f"⚠️ Cleanup error: {e}")
            last_cleanup = time.time()

        # Adaptive sleep with early-exit responsiveness (Signal Watchdog)
        adaptive_sleep(FETCH_INTERVAL, start_time)

    lg("🏁 ORION QUANT BOT FINISHED (graceful exit)")

# ==============================================================================
# Entrypoint
# ==============================================================================
if __name__ == "__main__":
    try:
        run()
    except Exception as ex:
        lg(f"FATAL ERROR: {ex}")
        lg(traceback.format_exc())
    finally:
        # pastikan flush lagi sebelum exit
        sys.stdout.flush()
        sys.exit(0)
