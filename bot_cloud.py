import os
import sys
import time
import json
import logging
import hashlib
import random
from datetime import datetime
from typing import Dict, List, Any, Optional, Union

import requests

# ============================================================================
# CONFIGURATION (HARDCODED FOR STABILITY)
# ============================================================================

# Firebase URL - FILL THIS IN WITH YOUR PROJECT URL
DEFAULT_FIREBASE_URL = "https://your-project-default-rtdb.firebaseio.com"

# Environment variables override (GitHub Actions)
FIREBASE_DB_URL = os.environ.get("FIREBASE_DB_URL", DEFAULT_FIREBASE_URL)
ORION_COOKIES_JSON = os.environ.get("ORION_COOKIES_JSON", "{}")

# Timing constraints for GitHub Actions
RUNTIME_LIMIT_SECONDS = 270  # 4 minutes 30 seconds
COOLDOWN_SECONDS = 30
REQUEST_DELAY = 3

# Data validation
MIN_COINS_THRESHOLD = 50

# CORRECTED API ENDPOINT
ORION_API_URL = "https://orionterminal.com/api/screener"

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ============================================================================
# SCHEMA GUARD (RELAXED FOR NUMERIC KEYS)
# ============================================================================

class SchemaGuard:
    """
    Validates data structure but allows numeric keys.
    Downgraded from CRITICAL to WARNING for partial validity.
    """
    
    # Whitelisted numeric keys that Orion uses
    NUMERIC_KEY_WHITELIST = {'11', '10', '50', '6', '7', '8', '9', '12', '13', '14'}
    
    # Expected semantic keys
    SEMANTIC_KEYS = {'symbol', 'price', 'volume', 'change', 'rsi', 'ticker', 'pair'}
    
    @classmethod
    def validate_item(cls, item: Dict) -> bool:
        """
        Check if item has valid keys (either numeric whitelist or semantic).
        """
        if not isinstance(item, dict) or len(item) == 0:
            return False
        
        item_keys = set(item.keys())
        
        # Check if ANY whitelisted numeric key exists
        has_numeric = bool(item_keys & cls.NUMERIC_KEY_WHITELIST)
        
        # Check if ANY semantic key exists
        has_semantic = bool(item_keys & cls.SEMANTIC_KEYS)
        
        return has_numeric or has_semantic
    
    @classmethod
    def check_batch(cls, items: List[Dict], sample_size: int = 5) -> Dict:
        """
        Validate a random sample of items.
        Returns dict with validation results.
        """
        if not items:
            return {'valid': False, 'reason': 'Empty list'}
        
        # FIX: Convert to list before sampling to avoid deprecation warning
        sample = random.sample(list(items), min(sample_size, len(items)))
        
        valid_count = sum(1 for item in sample if cls.validate_item(item))
        
        return {
            'valid': valid_count > 0,  # At least 1 valid item (relaxed)
            'valid_count': valid_count,
            'sample_size': len(sample),
            'reason': f'{valid_count}/{len(sample)} items valid'
        }


# ============================================================================
# UNIVERSAL DATA PROCESSOR (FIXED FOR DICT-KEY-AS-TICKER)
# ============================================================================

class DataProcessor:
    """
    CRITICAL FIX: Handles Orion's structure where Ticker is the Dictionary Key.
    Example: {"BTC/USDT": {"11": 45000, "10": 123456}, "ETH/USDT": {...}}
    """
    
    # Fallback key mappings (numeric keys prioritized)
    PRICE_KEYS = ['11', 'last_price', 'close', 'price', 'p', 'lastPrice']
    VOLUME_KEYS = ['10', 'volume_24h', 'volume', 'v', 'volume24h', 'vol']
    CHANGE_KEYS = ['6', 'change_24h', 'change', 'ch', 'change24h', 'priceChange']
    RSI_KEYS = ['50', 'rsi_14', 'rsi', '14', 'rsi14']
    
    @staticmethod
    def normalize_structure(raw_data: Any) -> List[Dict]:
        """
        Convert ANY input structure into a List of Dict.
        
        FIXED: Now handles Dict-Key-as-Ticker structure.
        Example Input: {"BTC/USDT": {"11": 45000}, "ETH/USDT": {...}}
        """
        logger.info(f"🔍 Raw data type: {type(raw_data).__name__}")
        
        if isinstance(raw_data, list):
            logger.info(f"📦 Structure: Direct list with {len(raw_data)} items")
            return raw_data
        
        elif isinstance(raw_data, dict):
            # Check for nested 'data' key first
            if 'data' in raw_data and isinstance(raw_data['data'], (list, dict)):
                logger.info(f"📦 Structure: Nested 'data' key found")
                return DataProcessor.normalize_structure(raw_data['data'])
            
            # Check for other wrapper keys
            for wrapper_key in ['results', 'items', 'coins', 'markets']:
                if wrapper_key in raw_data and isinstance(raw_data[wrapper_key], (list, dict)):
                    logger.info(f"📦 Structure: Nested '{wrapper_key}' key found")
                    return DataProcessor.normalize_structure(raw_data[wrapper_key])
            
            # CRITICAL FIX: Ticker is the Key itself
            # Convert {"BTC/USDT": {...}, "ETH/USDT": {...}} 
            # to [{"_ticker": "BTC/USDT", ...}, {"_ticker": "ETH/USDT", ...}]
            items = []
            for ticker_key, value_dict in raw_data.items():
                if isinstance(value_dict, dict):
                    # Inject ticker into the item
                    item = value_dict.copy()
                    item['_ticker'] = ticker_key
                    items.append(item)
                else:
                    # Fallback: treat as simple item
                    items.append({'_ticker': ticker_key, 'value': value_dict})
            
            logger.info(f"📦 Structure: Dict-Key-as-Ticker, extracted {len(items)} tickers")
            return items
        
        else:
            logger.warning(f"⚠️ Unexpected data type: {type(raw_data)}")
            return []
    
    @staticmethod
    def extract_value(item: Dict, key_candidates: List[str], default: float = 0.0) -> float:
        """
        Try multiple keys in priority order, return first valid value.
        """
        for key in key_candidates:
            if key in item:
                try:
                    value = item[key]
                    if value is None or value == '':
                        continue
                    return float(value)
                except (ValueError, TypeError):
                    continue
        return default
    
    @staticmethod
    def extract_ticker(item: Dict) -> Optional[str]:
        """
        Extract ticker/symbol from item.
        Priority: _ticker (injected), symbol, pair, ticker
        """
        # Priority 1: Injected ticker from dict key
        if '_ticker' in item:
            raw_ticker = str(item['_ticker'])
            # Clean: "BTC/USDT" -> "BTC", "BTC-USDT" -> "BTC"
            if '/' in raw_ticker:
                return raw_ticker.split('/')[0]
            elif '-' in raw_ticker:
                return raw_ticker.split('-')[0]
            return raw_ticker
        
        # Priority 2: Semantic keys
        for key in ['symbol', 'ticker', 'pair', 's']:
            if key in item and item[key]:
                return str(item[key])
        
        return None
    
    @classmethod
    def parse_item(cls, item: Dict) -> Optional[Dict[str, Union[str, float]]]:
        """
        Extract normalized data from a single item.
        """
        if not isinstance(item, dict):
            return None
        
        # Extract ticker (CRITICAL)
        ticker = cls.extract_ticker(item)
        if not ticker:
            return None
        
        # Extract numerical fields with fallbacks
        price = cls.extract_value(item, cls.PRICE_KEYS)
        volume = cls.extract_value(item, cls.VOLUME_KEYS)
        change = cls.extract_value(item, cls.CHANGE_KEYS)
        rsi = cls.extract_value(item, cls.RSI_KEYS)
        
        return {
            'symbol': ticker,
            'price': price,
            'volume_24h': volume,
            'change_24h': change,
            'rsi': rsi,
            'raw_keys': list(item.keys())[:5]  # Debug sample
        }
    
    @classmethod
    def process(cls, raw_data: Any) -> List[Dict]:
        """
        Main entry point: Convert any structure to normalized coin list.
        """
        # Step 1: Normalize to list
        items = cls.normalize_structure(raw_data)
        
        if not items:
            logger.error("❌ Normalization returned empty list")
            return []
        
        # Step 2: Validate with SchemaGuard (relaxed)
        validation = SchemaGuard.check_batch(items)
        if not validation['valid']:
            logger.warning(f"⚠️ Schema validation: {validation['reason']}")
            # Don't fail, just warn
        else:
            logger.info(f"✅ Schema validation: {validation['reason']}")
        
        # Step 3: Parse each item
        parsed_coins = []
        failed_count = 0
        
        for i, item in enumerate(items):
            parsed = cls.parse_item(item)
            if parsed:
                parsed_coins.append(parsed)
            else:
                failed_count += 1
                if failed_count <= 3:  # Log first 3 failures
                    sample = str(item)[:150]
                    logger.warning(f"⚠️ Parsing gagal untuk item {i}: {sample}")
        
        logger.info(f"✅ Berhasil parsing {len(parsed_coins)} coins")
        if failed_count > 0:
            logger.info(f"⚠️ Gagal parsing {failed_count} items")
        
        # Final check
        if len(parsed_coins) == 0:
            logger.error("❌ HASIL PARSING NOL - Data mungkin tidak valid")
        
        return parsed_coins


# ============================================================================
# FIREBASE CLIENT
# ============================================================================

class FirebaseClient:
    """Handles all Firebase operations with integrity validation."""
    
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
        logger.info(f"🔥 Firebase initialized: {self.base_url}")
    
    @staticmethod
    def calculate_hash(data: Dict) -> str:
        """Calculate SHA256 hash for data integrity."""
        json_str = json.dumps(data, sort_keys=True)
        return hashlib.sha256(json_str.encode()).hexdigest()
    
    def push_snapshot(self, coins: List[Dict]) -> bool:
        """
        Push data snapshot to Firebase with atomic update.
        """
        timestamp = datetime.utcnow().isoformat() + 'Z'
        
        payload = {
            'timestamp': timestamp,
            'coin_count': len(coins),
            'coins': coins,
            '_metadata': {
                'harvester_version': '5.5',
                'integrity_hash': self.calculate_hash(coins)
            }
        }
        
        # Atomic PUT to overwrite snapshot
        safe_timestamp = timestamp.replace(':', '-').replace('.', '-')
        endpoint = f"{self.base_url}/orion_snapshots/{safe_timestamp}.json"
        
        try:
            response = requests.put(endpoint, json=payload, timeout=10)
            response.raise_for_status()
            logger.info(f"🚀 Berhasil push {len(coins)} coins ke Firebase")
            logger.info(f"📍 Endpoint: {endpoint}")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Firebase push gagal: {e}")
            return False


# ============================================================================
# MAIN HARVESTER
# ============================================================================

class OrionHarvester:
    """Main orchestrator for data collection."""
    
    def __init__(self):
        self.session = requests.Session()
        self.firebase = FirebaseClient(FIREBASE_DB_URL)
        self.start_time = time.time()
        self._setup_session()
    
    def _setup_session(self):
        """Configure session with cookies and headers."""
        # Parse cookies from environment
        try:
            cookies = json.loads(ORION_COOKIES_JSON)
            for name, value in cookies.items():
                self.session.cookies.set(name, value)
            logger.info(f"🍪 Loaded {len(cookies)} cookies")
        except json.JSONDecodeError:
            logger.warning("⚠️ Tidak ada cookies, melanjutkan tanpa autentikasi")
        
        # Set realistic headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://orionterminal.com/',
            'Origin': 'https://orionterminal.com'
        })
    
    def fetch_data(self) -> Optional[Any]:
        """Fetch raw data from Orion API."""
        try:
            logger.info(f"📡 Fetching data dari {ORION_API_URL}...")
            response = self.session.get(ORION_API_URL, timeout=15)
            
            # Check for auth failures
            if response.status_code == 401:
                logger.error("❌ Autentikasi gagal - cookies expired")
                return None
            
            if response.status_code == 404:
                logger.error("❌ Endpoint tidak ditemukan - URL mungkin berubah")
                return None
            
            response.raise_for_status()
            data = response.json()
            
            logger.info(f"✅ Response diterima: {len(str(data))} bytes")
            return data
            
        except requests.exceptions.ConnectionError as e:
            logger.error(f"❌ Koneksi gagal (DNS Error): {e}")
            logger.error("💡 Periksa URL endpoint atau koneksi internet")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Request gagal: {e}")
            return None
    
    def should_continue(self) -> bool:
        """Check if we should continue the loop."""
        elapsed = time.time() - self.start_time
        remaining = RUNTIME_LIMIT_SECONDS - elapsed
        
        if remaining <= 0:
            logger.info(f"⏰ Runtime limit tercapai ({RUNTIME_LIMIT_SECONDS}s)")
            return False
        
        logger.info(f"⏱️ Sisa waktu: {int(remaining)}s")
        return True
    
    def harvest_cycle(self) -> bool:
        """
        Single harvest cycle.
        Returns True if data was successfully pushed.
        """
        # Fetch raw data
        raw_data = self.fetch_data()
        if raw_data is None:
            logger.error("❌ Fetch gagal, skip cycle ini")
            return False
        
        # Parse with universal processor
        coins = DataProcessor.process(raw_data)
        
        # Validate
        if len(coins) < MIN_COINS_THRESHOLD:
            logger.warning(f"⚠️ Hanya {len(coins)} coins parsed (threshold: {MIN_COINS_THRESHOLD})")
            logger.warning("⚠️ Skip Firebase push - data tidak lengkap")
            return False
        
        # Push to Firebase
        success = self.firebase.push_snapshot(coins)
        return success
    
    def run(self):
        """Main execution loop with GitHub Actions lifecycle."""
        logger.info("=" * 70)
        logger.info("🚀 Orion Harvester v5.5 MASTER STABLE STARTING")
        logger.info(f"⏰ Runtime limit: {RUNTIME_LIMIT_SECONDS}s")
        logger.info(f"🔥 Firebase URL: {FIREBASE_DB_URL}")
        logger.info(f"🌐 Orion API: {ORION_API_URL}")
        logger.info("=" * 70)
        
        cycle_count = 0
        success_count = 0
        
        # Main watchdog loop
        while self.should_continue():
            cycle_count += 1
            logger.info(f"\n{'='*70}")
            logger.info(f"🔄 Cycle {cycle_count} dimulai...")
            logger.info(f"{'='*70}")
            
            success = self.harvest_cycle()
            if success:
                success_count += 1
            
            # Rate limiting
            if self.should_continue():
                logger.info(f"💤 Sleep {REQUEST_DELAY}s (rate limit)...")
                time.sleep(REQUEST_DELAY)
        
        # Graceful shutdown
        logger.info("\n" + "=" * 70)
        logger.info("🏁 GRACEFUL SHUTDOWN")
        logger.info(f"📊 Total cycles: {cycle_count}")
        logger.info(f"✅ Successful pushes: {success_count}")
        logger.info(f"💤 Cooldown: {COOLDOWN_SECONDS}s")
        logger.info("=" * 70)
        
        time.sleep(COOLDOWN_SECONDS)
        logger.info("👋 Harvester exit dengan bersih")
        sys.exit(0)


# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    """Entry point with error handling."""
    try:
        harvester = OrionHarvester()
        harvester.run()
    except KeyboardInterrupt:
        logger.info("\n⚠️ Interrupted oleh user")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"💥 Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
