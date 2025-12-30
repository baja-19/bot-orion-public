# ----- REPLACE OrionAPI.fetch with this -----
class OrionAPI:
    def __init__(self):
        self.session = requests.Session()
        self.rotate_headers()

    def rotate_headers(self):
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
            # letakkan cookie di session cookies (lebih reliable daripada header string)
            self.session.cookies.update(cookies)
            # juga set header cookie string (optional)
            self.session.headers["cookie"] = "; ".join(f"{k}={v}" for k, v in cookies.items())

    def fetch(self):
        """Ambil data JSON dari endpoint XHR Orion dengan timeout dan debug logging."""
        try:
            r = self.session.get(ORION_API_URL, timeout=TIMEOUT_REQUEST)
        except Exception as e:
            raise RuntimeError(f"HTTP request failed: {e}")

        # Debug: status + small header snippet
        lg(f"API response status: {r.status_code}")
        try:
            hdr_sample = {k: r.headers[k] for k in list(r.headers.keys())[:6]}
            lg(f"API headers sample: {hdr_sample}")
        except:
            pass

        # If 304 or 204 or 204-like, treat as empty
        if r.status_code in (204, 304):
            raise RuntimeError(f"API returned empty status {r.status_code}")

        # If response is HTML (likely blocked), log snapshot
        ctype = r.headers.get("content-type", "")
        if "html" in ctype.lower() or r.text.strip().startswith("<"):
            snippet = r.text[:1000].replace('\n', ' ')
            lg(f"API appears to return HTML or blocked page (snippet): {snippet!r}")
            raise RuntimeError(f"API returned HTML (content-type: {ctype})")

        # Try parse JSON
        try:
            parsed = r.json()
            # small debug snippet of keys/top-level
            if isinstance(parsed, dict):
                lg(f"API JSON top keys: {list(parsed.keys())[:10]}")
            return parsed
        except Exception as e:
            # fallback: log text (first 1000 chars) for debugging
            txt = r.text[:1000].replace('\n', ' ')
            lg(f"Failed JSON parse: {e}. Text snippet: {txt!r}")
            raise RuntimeError("Failed to parse JSON from API")

# ----- ADD helper to find rows ----- 
def find_rows_in_json(obj):
    """
    Recursively search for the first list of dicts (rows) in the JSON.
    Returns list or None.
    """
    # direct simple cases
    if isinstance(obj, list):
        # check if list of dicts
        if obj and isinstance(obj[0], dict):
            return obj
        # if empty list or not dicts, continue
    if isinstance(obj, dict):
        # common keys to try first
        common_keys = ["data", "rows", "result", "payload", "markets", "items"]
        for k in common_keys:
            if k in obj and isinstance(obj[k], list) and obj[k] and isinstance(obj[k][0], dict):
                return obj[k]
        # else deep search (BFS) but limit depth
        queue = list(obj.items())
        depth = 0
        while queue and depth < 3:
            depth += 1
            newq = []
            for k, v in queue:
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    return v
                if isinstance(v, dict):
                    for kk, vv in v.items():
                        newq.append((kk, vv))
            queue = newq
    return None

# ----- REPLACE normalize_data with improved version -----
def normalize_data(raw):
    """
    Mengambil semua field koin dari response JSON secara dinamis.
    Gunakan find_rows_in_json untuk menemukan list of dicts.
    """
    rows = find_rows_in_json(raw)
    coins = {}

    if not rows:
        lg("⚠️ find_rows_in_json gagal menemukan list of dicts di response JSON")
        # debug: log top-level JSON keys / small dump
        try:
            if isinstance(raw, dict):
                lg(f"Top-level JSON keys: {list(raw.keys())[:20]}")
            txt = json.dumps(raw)[:1000]
            lg(f"Response JSON snippet: {txt}")
        except Exception:
            lg("⚠️ Gagal meng-dump response JSON for debug")
        return coins

    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol_raw = row.get("symbol") or row.get("market") or row.get("pair") or row.get("name")
        if not symbol_raw:
            # Try to guess symbol from keys like 's' or 't'
            symbol_raw = row.get("s") or row.get("t") or None
            if not symbol_raw:
                continue
        symbol = (
            str(symbol_raw).replace("/", "_").replace("-", "_").replace(".", "_").upper()
        )
        if not SYMBOL_REGEX.match(symbol):
            continue
        coins[symbol] = {
            "symbol": symbol,
            "updated_utc": datetime.utcnow().isoformat(),
            **row
        }
    return coins

# ----- REPLACE fetch_and_push with robust retry/backoff and non-raising behavior -----
def fetch_and_push(api, root_ref, start_time):
    """
    Try fetch -> normalize -> push.
    If no coins found, try browser fallback + retry with exponential backoff (several attempts).
    On persistent failure, log and return False (do not raise to kill loop).
    """
    max_attempts = 4
    attempt = 0
    backoff_base = 1.5
    raw = None
    while attempt < max_attempts:
        attempt += 1
        try:
            lg(f"Attempt {attempt} to fetch API")
            raw = api.fetch()
            coins = normalize_data(raw)
            if coins:
                # push and return True
                ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
                root_ref.child("coins").update(coins)
                root_ref.child("metadata").update({
                    "last_update": ts,
                    "total_coins": len(coins),
                    "source": "orion_xhr_api"
                })
                root_ref.child("snapshots").child(ts).set(coins)
                lg(f"✅ {len(coins)} coins pushed @ {ts}")
                return True
            else:
                lg(f"⚠️ No coins parsed on attempt {attempt}")
                # if first attempt failed, try browser fallback to harvest cookie then retry
                if attempt == 1:
                    # ensure we have time left for fallback
                    if time.time() - start_time >= GLOBAL_TIMEOUT - 8:
                        lg("⏳ Waktu tersisa tidak cukup untuk fallback, skip fallback")
                        break
                    try:
                        cookies = browser_fallback(start_time)
                        api.rotate_headers()
                        api.set_cookies_header(cookies)
                    except Exception as e_fb:
                        lg(f"⚠️ Browser fallback failed: {e_fb}")
                # exponential backoff sleep small chunks
                sleep_for = min(8, (backoff_base ** attempt))
                lg(f"⏳ Backoff sleeping {sleep_for}s before next attempt")
                time.sleep(sleep_for)
                continue

        except Exception as e:
            lg(f"⚠️ Fetch attempt {attempt} raised: {e}")
            # if likely blocked/HTML, attempt fallback immediately (only once)
            if attempt == 1:
                if time.time() - start_time >= GLOBAL_TIMEOUT - 8:
                    lg("⏳ Tidak cukup waktu untuk fallback, abort fetch attempts")
                    break
                try:
                    cookies = browser_fallback(start_time)
                    api.rotate_headers()
                    api.set_cookies_header(cookies)
                except Exception as e_fb:
                    lg(f"⚠️ Browser fallback failed during exception path: {e_fb}")
            sleep_for = min(8, (backoff_base ** attempt))
            lg(f"⏳ Backoff sleeping {sleep_for}s after exception")
            time.sleep(sleep_for)
            continue

    # After attempts exhausted, log raw snippet for debugging and return False
    try:
        lg("❌ Semua upaya fetch gagal — mencatat snippet response untuk debugging")
        if raw is not None:
            try:
                snippet = json.dumps(raw)[:1500]
            except Exception:
                snippet = str(raw)[:1000]
            lg(f"Raw response snippet: {snippet}")
    except Exception:
        pass

    return False
