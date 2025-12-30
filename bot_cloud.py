import os
import sys
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

# ==========================================
# 1. KONFIGURASI GLOBAL (SUDAH DIISI)
# ==========================================

# ✅ URL Firebase Anda (Langsung dimasukkan agar tidak error)
FIREBASE_DB_URL = "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app"

# Cookie Awal (Wajib diupdate lewat Firebase / Hardcode disini jika mau)
# Jika kosong, bot akan mencoba mengambil dari Firebase Config nanti
INITIAL_COOKIES = os.environ.get("ORION_COOKIES_JSON", '{"_ga": "GA1.1.1647406654.1766912547"}')

# Internal Config
ORION_API_URL = "https://orionterminal.com/api/screener"
CYCLE_ACTIVE_SEC = 270   
CYCLE_PAUSE_SEC = 30     
POLL_INTERVAL = 3        
DATA_LIMIT = 1000
MAX_SNAPSHOTS_TO_KEEP = 48
SCHEMA_VERSION = "5.6.0"
ROUNDING_PRECISION = 8  
MAX_AUTH_RETRIES = 5    
MAX_MEMORY_KEYS = 2000 
FREEZE_THRESHOLD_PCT = 0.2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Orion-v5.6")

# ==========================================
# 2. SCHEMA GUARD
# ==========================================
class SchemaGuard:
    def __init__(self):
        self.ref_keys: Set[str] = set()
        self.locked = False
        self.cycle_count = 0 
        self.warmup_cycles = 3
        
    def validate_batch(self, items: List[Tuple[str, Dict]]) -> bool:
        if not items: return True
        sample_pool = list(items)
        
        if not self.locked:
            self.cycle_count += 1
            current_batch_keys = set()
            for _, data in sample_pool:
                current_batch_keys.update(data.keys())
            
            if len(current_batch_keys) > len(self.ref_keys):
                self.ref_keys = current_batch_keys

            if self.cycle_count >= self.warmup_cycles:
                self.locked = True
                logger.info(f"🛡️ Schema Locked. Known fields: {list(self.ref_keys)[:5]}...")
            return True

        sample_size = min(len(sample_pool), 5)
        samples = random.sample(sample_pool, sample_size)
        valid_votes = 0
        possible_tickers = {'ticker', 'symbol', 'pair', 'name'}
        possible_prices = {'last_price', 'price', 'close', 'last', 'p'}
        
        for _, data in samples:
            keys = set(data.keys())
            has_ticker = bool(keys.intersection(possible_tickers))
            has_price = bool(keys.intersection(possible_prices))
            if has_ticker and has_price:
                valid_votes += 1
            else:
                if valid_votes == 0: logger.debug(f"🔍 Failed Keys: {list(keys)}")
        
        if valid_votes / sample_size < 0.5:
            logger.warning(f"⚠️ Partial Schema Drift! Valid: {valid_votes}/{sample_size}.")
            return True 
        return True

# ==========================================
# 3. STATE & INTEGRITY
# ==========================================
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
                    return last_data['integrity']['chain_hash']
        except: pass
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
            logger.error(f"📉 MARKET ANOMALY (Flags: {anomalies}/3)")
            return False
        return True

# ==========================================
# 4. PARSER
# ==========================================
class DataParser:
    SCHEMA_MAP = {
        'price':        ['11', 'last_price', 'close', 'price', 'p', 0.0],
        'volume_24h':   ['10', 'volume_24h', 'volume', 'v', 0.0],
        'change_24h':   ['6', 'change_24h', 'change', 'ch', 0.0],
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

# ==========================================
# 5. NETWORK CLIENT
# ==========================================
class RobustOrionClient:
    def __init__(self, firebase_url):
        self.session = requests.Session()
        self.firebase_url = firebase_url
        self._lock = threading.Lock()
        self.auth_fail_count = 0
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

    def _scavenge_cookies(self) -> bool:
        logger.info("♻️ Scavenging cookies from Firebase...")
        try:
            url = f"{self.firebase_url}/config/orion_cookies.json"
            res = requests.get(url, timeout=10)
            if res.status_code == 200 and res.json():
                with self._lock:
                    self.session.cookies.clear()
                    self.session.cookies.update(res.json())
                    logger.info("✅ Cookies Hot-Reloaded.")
                return True
        except: pass
        return False

    def fetch(self) -> Tuple[Any, str]:
        params = {"limit": DATA_LIMIT, "sort": "volume", "order": "desc"}
        backoff = 2
        
        if self.auth_fail_count > MAX_AUTH_RETRIES:
            logger.critical("🛑 TOO MANY AUTH FAILURES. Sleeping 5 mins...")
            time.sleep(300)
            self.auth_fail_count = 0 

        for _ in range(3):
            try:
                with self._lock:
                    res = self.session.get(ORION_API_URL, params=params, timeout=20)
                if res.status_code == 429:
                    time.sleep(30); continue
                if res.status_code in [403, 401]:
                    self.auth_fail_count += 1
                    logger.warning(f"⛔ Auth Failed ({self.auth_fail_count}).")
                    if self._scavenge_cookies():
                        time.sleep(2); continue
                    return {}, "AUTH_DEAD"
                if res.status_code == 200:
                    self.auth_fail_count = 0
                    return res.json(), "OK"
                if res.status_code >= 500:
                    time.sleep(backoff); backoff *= 2; continue
            except: time.sleep(backoff)
        return {}, "FAIL"

# ==========================================
# 6. FIREBASE CLIENT
# ==========================================
class SecureFirebaseClient:
    def __init__(self, db_url):
        self.db_url = db_url.rstrip('/') if db_url else ""

    def _recursive_round(self, obj):
        if isinstance(obj, float): return round(obj, ROUNDING_PRECISION)
        if isinstance(obj, dict): return {k: self._recursive_round(v) for k, v in obj.items()}
        if isinstance(obj, list): return [self._recursive_round(x) for x in obj]
        return obj

    def _compute_canonical_hash(self, market_data: Dict) -> str:
        rounded = self._recursive_round(market_data)
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
        
        for _ in range(2):
            try:
                res = requests.put(url, json=container, timeout=10)
                if res.status_code == 200:
                    logger.info(f"✅ Pushed: {ts_key} | Coins: {len(market_data)}")
                    self.cleanup()
                    return chain_hash
                time.sleep(1)
            except: pass
        return prev_hash

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
    client = RobustOrionClient(FIREBASE_DB_URL)
    fb = SecureFirebaseClient(FIREBASE_DB_URL)
    schema = SchemaGuard()
    stats = MarketStats()
    state = StateTracker(FIREBASE_DB_URL)
    
    logger.info(f"🚀 ENGINE v5.6 STARTED | Chain: {state.prev_hash[:8]}")

    while True:
        cycle_start = time.time()
        logger.info("🟢 CYCLE ACTIVE")

        while (time.time() - cycle_start) < CYCLE_ACTIVE_SEC:
            loop_s = time.time()
            raw, status = client.fetch()
            
            if status == "AUTH_DEAD":
                logger.critical("💀 AUTH DEAD. Waiting..."); time.sleep(15); continue
            if not raw:
                time.sleep(POLL_INTERVAL); continue

            items = []
            if isinstance(raw, dict):
                if 'data' in raw and isinstance(raw['data'], list):
                    items = [(x.get('ticker', 'UNK'), x) for x in raw['data']]
                else: items = raw.items()
            elif isinstance(raw, list):
                items = [(x.get('ticker', 'UNK'), x) for x in raw]

            schema.validate_batch(items)
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
                time.sleep(5); continue

            total = len(market_snapshot)
            freeze_ratio = frozen_count / total if total > 0 else 0
            
            if total > 50 and freeze_ratio < FREEZE_THRESHOLD_PCT:
                meta = {'total': total, 'frozen': frozen_count, 'ts': datetime.now().isoformat(), 'v': SCHEMA_VERSION}
                new_hash = fb.push(market_snapshot, meta, state.prev_hash)
                if new_hash != state.prev_hash: state.prev_hash = new_hash
            else:
                logger.warning(f"⚠️ Quality Gate: Coins={total}")

            elapsed = time.time() - loop_s
            time.sleep(max(0, POLL_INTERVAL - elapsed))

        logger.info(f"⏸️ COOLDOWN ({CYCLE_PAUSE_SEC}s)...")
        time.sleep(CYCLE_PAUSE_SEC)

if __name__ == "__main__":
    try: run()
    except KeyboardInterrupt: pass
    except Exception as e: logger.critical(f"🔥 FATAL: {e}"); sys.exit(1)
