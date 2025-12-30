import os
import sys

# ==========================================
# 🔥 BAGIAN INI YANG WAJIB ANDA ISI (FIX)
# ==========================================
# Masukkan URL Firebase Anda di sini
os.environ["FIREBASE_DB_URL"] = "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app"

# Masukkan Cookie Orion (Opsional tapi disarankan agar tidak error 403)
# Format harus string JSON yang valid
os.environ["ORION_COOKIES_JSON"] = '{"_ga": "GA1.1.1647406654.1766912547"}' 

# ==========================================
# KODE PROGRAM UTAMA (JANGAN UBAH DI BAWAH INI)
# ==========================================
import time
import json
import random
import requests
import logging
import hashlib
import statistics
import threading
from datetime import datetime, timedelta
from collections import deque
from typing import List, Dict, Any, Tuple, Optional, Set

# Konfigurasi Global (Mengambil dari settingan di atas)
FIREBASE_DB_URL = os.environ.get("FIREBASE_DB_URL")
INITIAL_COOKIES = os.environ.get("ORION_COOKIES_JSON", "{}") 

# Internal Config
ORION_API_URL = "https://orionterminal.com/api/screener"
CYCLE_ACTIVE_SEC = 270   
CYCLE_PAUSE_SEC = 30     
POLL_INTERVAL = 3        
DATA_LIMIT = 1000
MAX_SNAPSHOTS_TO_KEEP = 48
SCHEMA_VERSION = "5.3.0"
SCHEMA_WARMUP_CYCLES = 3

# Memory Limits
MAX_MEMORY_KEYS = 2000 
FREEZE_THRESHOLD_PCT = 0.2

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Orion-v5.3")

# --- CLASS 1: STATE TRACKER ---
class StateTracker:
    def __init__(self, firebase_url):
        self.firebase_url = firebase_url.rstrip('/') if firebase_url else ""
        self.prev_hash = self._load_last_hash()
        self.price_memory: Dict[str, deque] = {} 
        self.max_memory = 10
        self.epsilon = 1e-8 

    def _load_last_hash(self) -> str:
        if not self.firebase_url: return "0"*64
        try:
            url = f"{self.firebase_url}/orion_snapshots.json?orderBy=\"$key\"&limitToLast=1"
            res = requests.get(url, timeout=10)
            if res.status_code == 200 and res.json():
                last_key = list(res.json().keys())[0]
                last_data = res.json()[last_key]
                if 'integrity' in last_data and 'chain_hash' in last_data['integrity']:
                    last_hash = last_data['integrity']['chain_hash']
                    logger.info(f"🔗 Chain Resumed from: {last_hash[:8]}...")
                    return last_hash
        except Exception as e:
            logger.warning(f"⚠️ Failed to load last hash: {e}")
        return "0"*64 

    def check_soft_freeze_and_gc(self, current_snapshot: Dict) -> int:
        frozen_count = 0
        current_keys = set(current_snapshot.keys())
        
        for symbol, data in current_snapshot.items():
            price = data['price']
            if symbol not in self.price_memory:
                self.price_memory[symbol] = deque(maxlen=self.max_memory)
            mem = self.price_memory[symbol]
            mem.append(price)
            if len(mem) == self.max_memory:
                min_p, max_p = min(mem), max(mem)
                if (max_p - min_p) < self.epsilon:
                    frozen_count += 1
        
        existing_keys = set(self.price_memory.keys())
        stale_keys = existing_keys - current_keys
        if len(stale_keys) > 100:
            for k in stale_keys: del self.price_memory[k]
        if len(self.price_memory) > MAX_MEMORY_KEYS:
            self.price_memory.clear()
        return frozen_count

# --- CLASS 2: MARKET STATS ---
class MarketStats:
    def __init__(self):
        self.btc_hist = deque(maxlen=10)
        self.eth_hist = deque(maxlen=10)
        self.global_median_hist = deque(maxlen=10)
        
    def update_and_validate(self, btc_p: float, eth_p: float, all_prices: List[float]) -> bool:
        current_median = statistics.median(all_prices) if all_prices else 0
        if btc_p > 0: self.btc_hist.append(btc_p)
        if eth_p > 0: self.eth_hist.append(eth_p)
        if current_median > 0: self.global_median_hist.append(current_median)
        if len(self.btc_hist) < 3: return True 
        anomalies = 0
        if self.btc_hist:
            btc_med = statistics.median(self.btc_hist)
            if btc_med > 0 and abs(btc_p - btc_med) / btc_med > 0.2: anomalies += 1
        if self.eth_hist:
            eth_med = statistics.median(self.eth_hist)
            if eth_med > 0 and abs(eth_p - eth_med) / eth_med > 0.2: anomalies += 1
        if self.global_median_hist:
            glob_med = statistics.median(self.global_median_hist)
            if glob_med > 0 and abs(current_median - glob_med) / glob_med > 0.3: anomalies += 1
        if anomalies >= 2:
            logger.error(f"📉 MARKET ANOMALY DETECTED (Flags: {anomalies}/3)")
            return False
        return True

# --- CLASS 3: SCHEMA GUARD ---
class SchemaGuard:
    def __init__(self):
        self.ref_keys: Set[str] = set()
        self.locked = False
        self.cycle_count = 0 
        self.warmup_cycles = 3
        
    def validate_batch(self, items: List[Tuple[str, Dict]]) -> bool:
        if not items: return True
        if not self.locked:
            self.cycle_count += 1
            current_batch_keys = set()
            for _, data in items:
                current_batch_keys.update(data.keys())
            if len(current_batch_keys) > len(self.ref_keys):
                self.ref_keys = current_batch_keys
            if self.cycle_count >= self.warmup_cycles:
                self.locked = True
                logger.info(f"🛡️ Schema Locked. Fields: {len(self.ref_keys)}")
            return True

        sample_size = min(len(items), 5)
        samples = random.sample(items, sample_size)
        valid_votes = 0
        required = {'ticker', 'last_price'}
        for _, data in samples:
            current_keys = set(data.keys())
            missing = required - current_keys
            price_val = data.get('last_price') or data.get('price')
            type_ok = isinstance(price_val, (int, float)) or \
                      (isinstance(price_val, str) and price_val.replace('.','').isdigit())
            if not missing and type_ok: valid_votes += 1
        
        if valid_votes / sample_size < 0.5:
            logger.critical(f"🚨 SCHEMA DRIFT! Valid: {valid_votes}/{sample_size}")
            return False
        return True

# --- CLASS 4: PARSER ---
class DataParser:
    SCHEMA_MAP = {
        'price':        ['11', 'last_price', 'close', 'price', 0.0],
        'volume_24h':   ['10', 'volume_24h', 'volume', 0.0],
        'change_24h':   ['6', 'change_24h', 'change', 0.0],
        'rsi':          ['rsi', 'rsi_14', 50.0],
        'funding':      ['funding_rate', 'funding', 0.0],
        'oi':           ['open_interest', 'oi', 0.0]
    }
    @staticmethod
    def _extract_heuristic(raw_data: Dict, keys: List[Any], field_type: str) -> float:
        default = keys[-1]
        for k in keys[:-1]:
            if k not in raw_data: continue
            val = raw_data[k]
            if isinstance(val, list):
                if not val: return float(default)
                nums = [v for v in val if isinstance(v, (int, float))]
                if not nums: return float(default)
                chosen = nums[0]
                if field_type == 'volume': chosen = max(nums)
                elif field_type == 'percent':
                     candidates = [n for n in nums if -100 <= n <= 100]
                     chosen = candidates[0] if candidates else nums[0]
                return float(chosen)
            try:
                if val is None: continue
                return float(val)
            except: continue
        return float(default)

    @staticmethod
    def normalize(raw_key: str, raw_data: Dict) -> Optional[Dict]:
        clean = {}
        for out_key, mapping in DataParser.SCHEMA_MAP.items():
            ftype = 'volume' if 'volume' in out_key else ('percent' if 'change' in out_key or 'funding' in out_key else 'price')
            val = DataParser._extract_heuristic(raw_data, mapping, ftype)
            clean[out_key] = val
        if clean['price'] <= 0: return None
        raw_lower = raw_key.lower()
        clean['type'] = 'futures' if ('perp' in raw_lower or 'usdm' in raw_lower or clean['funding'] != 0) else 'spot'
        return clean

# --- CLASS 5: NETWORK CLIENT ---
class RobustOrionClient:
    def __init__(self, firebase_url):
        self.session = requests.Session()
        self.firebase_url = firebase_url
        self._lock = threading.Lock()
        self._inject_initial_cookies()
        self._set_base_headers()

    def _set_base_headers(self):
        self.session.headers.update({
            'accept': 'application/json',
            'referer': 'https://orionterminal.com/screener',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest'
        })

    def _inject_initial_cookies(self):
        try:
            cookies = json.loads(INITIAL_COOKIES)
            self.session.cookies.update(cookies)
        except: pass

    def _validate_cookies(self, cookies: Dict) -> bool:
        try:
            test_url = f"{ORION_API_URL}?limit=1"
            res = requests.get(test_url, cookies=cookies, headers=self.session.headers, timeout=5)
            return res.status_code == 200
        except: return False

    def _scavenge_cookies(self) -> bool:
        logger.info("♻️ Scavenging cookies...")
        try:
            url = f"{self.firebase_url}/config/orion_cookies.json"
            res = requests.get(url, timeout=10)
            if res.status_code == 200 and res.json():
                new_cookies = res.json()
                if not self._validate_cookies(new_cookies):
                    logger.warning("⚠️ Scavenged cookies are INVALID.")
                    return False
                with self._lock:
                    self.session.cookies.clear()
                    self.session.cookies.update(new_cookies)
                    logger.info("✅ Cookies Hot-Reloaded & Verified.")
                return True
        except: pass
        return False

    def fetch(self) -> Tuple[Any, str]:
        params = {"limit": DATA_LIMIT, "sort": "volume", "order": "desc"}
        backoff = 2
        for _ in range(3):
            try:
                with self._lock:
                    res = self.session.get(ORION_API_URL, params=params, timeout=20)
                if res.status_code == 429:
                    time.sleep(int(res.headers.get("Retry-After", 30)))
                    continue
                if res.status_code in [403, 401]:
                    if self._scavenge_cookies():
                        time.sleep(2); continue
                    return {}, "AUTH_DEAD"
                if res.status_code == 200:
                    return res.json(), "OK"
                if res.status_code >= 500:
                    time.sleep(backoff); backoff *= 2; continue
            except: time.sleep(backoff)
        return {}, "FAIL"

# --- CLASS 6: FIREBASE CLIENT ---
class SecureFirebaseClient:
    def __init__(self, db_url):
        self.db_url = db_url.rstrip('/') if db_url else ""

    def _compute_canonical_hash(self, market_data: Dict) -> str:
        # Rounded & Sorted for deterministic hash
        def recursive_round(obj):
            if isinstance(obj, float): return round(obj, 8)
            if isinstance(obj, dict): return {k: recursive_round(v) for k, v in obj.items()}
            return obj
        
        rounded = recursive_round(market_data)
        canonical_str = json.dumps(rounded, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(canonical_str.encode()).hexdigest()

    def push(self, market_data: Dict, meta_data: Dict, prev_hash: str) -> str:
        if not self.db_url: return ""
        
        data_hash = self._compute_canonical_hash(market_data)
        chain_hash = hashlib.sha256(f"{prev_hash}{data_hash}".encode()).hexdigest()
        
        container = {
            "market": market_data,
            "meta": meta_data,
            "integrity": {
                "hash": data_hash,
                "chain_hash": chain_hash,
                "prev_hash": prev_hash,
                "ts_iso": datetime.now().isoformat()
            }
        }
        ts_key = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        url = f"{self.db_url}/orion_snapshots/{ts_key}.json"
        try:
            res = requests.put(url, json=container, timeout=10)
            if res.status_code == 200:
                logger.info(f"✅ Pushed: {ts_key} | Coins: {len(market_data)}")
                self.cleanup()
                return chain_hash
            logger.error(f"❌ Push Fail: {res.status_code}")
            return prev_hash
        except: return prev_hash

    def cleanup(self):
        try:
            url = f"{self.db_url}/orion_snapshots.json?shallow=true"
            res = requests.get(url, timeout=5)
            if res.status_code != 200: return
            keys = sorted(list(res.json().keys()))
            if len(keys) <= MAX_SNAPSHOTS_TO_KEEP: return
            for k in keys[:-MAX_SNAPSHOTS_TO_KEEP][:5]:
                requests.delete(f"{self.db_url}/orion_snapshots/{k}.json")
        except: pass

# ==========================================
# 7. MAIN RUN
# ==========================================
def run():
    if not FIREBASE_DB_URL:
        logger.critical("🔥 MISSING ENV: FIREBASE_DB_URL")
        return

    client = RobustOrionClient(FIREBASE_DB_URL)
    fb = SecureFirebaseClient(FIREBASE_DB_URL)
    schema = SchemaGuard()
    stats = MarketStats()
    state = StateTracker(FIREBASE_DB_URL)
    
    logger.info(f"🚀 ENGINE v5.3 STARTED | Chain: {state.prev_hash[:8]}")

    while True:
        cycle_start = time.time()
        logger.info("🟢 CYCLE START")
        while (time.time() - cycle_start) < CYCLE_ACTIVE_SEC:
            loop_s = time.time()
            raw, status = client.fetch()
            
            if status == "AUTH_DEAD":
                logger.critical("💀 AUTH DEAD. Waiting..."); time.sleep(30); continue
            if not raw:
                time.sleep(POLL_INTERVAL); continue

            items = []
            if isinstance(raw, dict):
                if 'data' in raw and isinstance(raw['data'], list):
                    items = [(x.get('ticker', 'UNK'), x) for x in raw['data']]
                else: items = raw.items()
            elif isinstance(raw, list):
                items = [(x.get('ticker', 'UNK'), x) for x in raw]

            if items and not schema.validate_batch(items):
                logger.warning("⚠️ Schema Drift!")

            market_snapshot = {}
            btc_p, eth_p = 0, 0
            all_prices = []

            for k, v in items:
                clean_k = str(k).split('-')[0].upper().replace('/', '_')
                norm = DataParser.normalize(str(k), v)
                if norm:
                    market_snapshot[clean_k] = norm
                    all_prices.append(norm['price'])
                    if 'BTC' in clean_k: btc_p = norm['price']
                    if 'ETH' in clean_k: eth_p = norm['price']

            frozen_count = state.check_soft_freeze_and_gc(market_snapshot)
            
            if not stats.update_and_validate(btc_p, eth_p, all_prices):
                logger.error("⛔ STATS ANOMALY.")
                time.sleep(5); continue

            total = len(market_snapshot)
            freeze_ratio = frozen_count / total if total > 0 else 0
            
            if total > 50 and freeze_ratio < FREEZE_THRESHOLD_PCT:
                meta = {'total': total, 'frozen': frozen_count, 'ts': datetime.now().isoformat()}
                new_hash = fb.push(market_snapshot, meta, state.prev_hash)
                if new_hash != state.prev_hash: state.prev_hash = new_hash
            else:
                logger.warning(f"⚠️ Low Data: {total} coins")

            elapsed = time.time() - loop_s
            time.sleep(max(0, POLL_INTERVAL - elapsed))

        logger.info("⏸️ COOLDOWN")
        time.sleep(CYCLE_PAUSE_SEC)

if __name__ == "__main__":
    try: run()
    except KeyboardInterrupt: pass
    except Exception as e: logger.critical(f"🔥 FATAL: {e}"); sys.exit(1)
