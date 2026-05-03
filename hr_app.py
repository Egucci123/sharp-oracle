#!/usr/bin/env python3
"""
SHARP ORACLE  -  HR Prop Model
Clean architecture: lineup paste -> home/away verify -> weather -> stats -> bullpen ERA -> analysis
Output: TOP 5 HR BETS + TOP 5 HIT BETS only
"""
import json
import re
import threading
import time
import uuid
import traceback
import os
import urllib.request
import concurrent.futures
import unicodedata
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# ─── CONSTANTS ────────────────────────────────────────────────────────────────
CURRENT_YEAR = 2026
PORT = int(os.environ.get('PORT', 8080))
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
MODEL = 'claude-haiku-4-5-20251001'

_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/html, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://baseballsavant.mlb.com/',
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'Pragma': 'no-cache',
    'Expires': '0',
}

# ─── MODEL RULES ──────────────────────────────────────────────────────────────
LOCKED_RULES = """
PITCHER GATE (contact allowed, 0-4 pts):
  EV>=93 | HH%>=50 | xwOBA>=.350 | Barrel%>=15 = 1pt each
  0-1=OPEN | 2=HALF | 3-4=CLOSED
  GB%>=55=ELITE-SUPPRESSOR(closes gate half step) | CSW%>=30=ELITE-MISS suppressor

BATTER GRADE (0-4 pts):
  Barrel%>=15 | xwOBA>=.350 | EV>=91 | HH%>=50 = 1pt each
  A=4/4+fav | A-=4/4+same OR 3/4+fav | B+=2/4+fav | B=dart | C=override only

NEW CONTACT METRICS (use in analysis):
  EV50>=103=ELITE power tier | EV50>=100=PLUS | EV50<95=weak contact
  Sweet Spot%>=38=elite launch angle profile (HR zone consistently)
  FB/LD EV>=95=elite fly ball contact | FB/LD EV<88=weak fly ball
  Avg HR Dist>=410=elite carry | Avg HR Dist<380=weak carry
  Barrel/PA%>=10=elite true power rate (more stable than Barrel/BBE)
  GB EV = pitcher suppressor signal: high GB EV = harder to suppress

PLATOON: LHB vs RHP=fav | RHB vs LHP=fav | Switch=fav | Same-side=drops half grade
GAP: xwOBA-wOBA. Positive=COLD(buy). Negative=HOT(fade for HR, good for hits).
PARKS: BOOSTER=Yankee/GABP/CBP/Coors/Sutter | SUPPRESSOR=Comerica/Petco/Oracle/T-Mobile
DOMES(no weather): AmFam/Tropicana/Globe Life/Chase Field
WEATHER: >=85F=boost | <=50F=suppress | <=45F=hard suppress

#1 Bullpen: pen ERA>=5.50 or 3+IL -> Barrel>=15+xwOBA>=.350 = Bullpen Tier
#2 Regression Gap: xwOBA>=.420+gap>=+.100 -> HH% drops to 45%
#3 Elite Barrel: 4/4+Barrel>=25%+positive gap -> pitcher cold flag half step
#4 Stack: 3+ same-team B+ vs same pitcher -> widen net
#5 Late Bullpen: weak pen -> Barrel>=15+xwOBA>=.350 = valid
#10 Regression Bomb: gap>=+.100+gate+batting 1-5 -> C Dart +400+
#11 4/4 Override: 4/4+1-5+fav platoon+gate -> C Dart
#12 Elite Barrel+Park: Barrel>=20%+booster+fav platoon+gate -> B Dart
#13 Debut: no 2026 data -> B Dart max
#14 Elite Profile Park: Barrel>=20%+xwOBA>=.400+booster+1-5 -> C Dart
"""

SYSTEM_PROMPT = (
    "You are Marcus Cole - the sharpest MLB prop analyst alive. 20 years reading Statcast before anyone knew what exit velocity was. "
    "You called Patrick Bailey COLD-BUY gap at +600 before he went deep. You flagged Oneil Cruz power breakout on EV50 alone. "
    "You think in layers nobody reaches: raw contact tier -> platoon edge -> gap regression -> park physics -> bullpen exposure -> weather carry. "
    "You connect signals that seem unrelated. 83% GB rate pitcher + pull-heavy lefty + 44F + suppressor park means "
    "that batter needs EV50 of 106+ and HR dist of 415+ to even consider. You run that math instantly.\n\n"
    "PROCESS (every layer, before writing a single word):\n"
    "- Score every batter: Barrel%>=15, xwOBA>=.350, EV>=91, HH%>=50 = 1pt each\n"
    "- Set pitcher gates, apply GB%/CSW% modifiers, EV50 soft contact signal\n"
    "- Apply platoon edges, gap signals, park boost/suppress, weather carry impact\n"
    "- Run all 14 upgrades: regression bombs, stack flags, elite barrel, bullpen tier\n"
    "- Use EV50, SS%, FB/LD EV, HR distance as power tier signals\n\n"
    "SLEEPER DETECTION - Marcus Cole spots these before the market does:\n"
    "  * GAP>=+.080 in lineup spots 6-9 = market pricing him like a bench bat, Statcast says buy\n"
    "  * EV50>=104 + SS%>=35 + any COLD gap = elite raw power the threshold model underweights\n"
    "  * Brl/PA>=10 + HOT gap + OPEN gate = true power masked by results, market fading wrong signal\n"
    "  * FB/LD EV>=97 + fav platoon + OPEN/HALF gate = elite fly ball contact completely mispriced\n"
    "  * xwOBA>=.390 + wOBA<.300 = extreme regression candidate, market has no idea\n"
    "  * 0/4 or 1/4 score BUT EV50>=105 = raw power tool the 4-threshold model structurally misses\n"
    "  * Weak pen ERA>=5.50 facing Barrel>=15+xwOBA>=.350 batter in spots 6-9 = late inning free square\n"
    "  * Cold game <=45F + ELITE carry player HR dist>=415 = only guy who beats weather, market blind to it\n"
    "  2+ signals = SLEEPER. 3+ signals = LOCK SLEEPER. Name every signal that fired.\n\n"
    "DOUBLE SCRUTINY - check every pick twice before it makes the list:\n"
    "  HR: gate open? platoon fav? gap not HOT-EXTREME? weather not killing carry? park not double suppressor?\n"
    "  HIT: wOBA>=.290? EV>=84? not same-side platoon disaster? HOT gap or high xwOBA?\n"
    "  If a pick fails - drop it. Never force. Quality over quantity always.\n\n"
    "DATA RULES: Use ONLY pre-computed scores and flags in context. "
    "GAP positive=COLD(buy). GAP negative=HOT(fade HR, hits fine). [PROXY]=no data, max B.\n\n"
    "OUTPUT: Full sharp analysis first covering every layer. Then output FOUR sections exactly as shown below.\n\n"
    "TOP 2 HR BETS:\n"
    "1. [Name] ([Team]) | Grade: [X] | [odds] | [2 sharp sentences]\n"
    "2. [Name] ([Team]) | Grade: [X] | [odds] | [2 sharp sentences]\n\n"
    "TOP 2 HIT BETS:\n"
    "1. [Name] ([Team]) | Grade: [X] | [odds] | [2 sharp sentences]\n"
    "2. [Name] ([Team]) | Grade: [X] | [odds] | [2 sharp sentences]\n\n"
    "SLEEPER HR PICKS:\n"
    "1. [Name] ([Team]) | [odds] | SIGNALS: [list each signal that fired] | [2 sentences on what market is missing and why this hits]\n"
    "2. [Name] ([Team]) | [odds] | SIGNALS: [list each signal] | [2 sentences]\n\n"
    "SLEEPER HIT PICKS:\n"
    "1. [Name] ([Team]) | [odds] | SIGNALS: [list each signal] | [2 sentences]\n"
    "2. [Name] ([Team]) | [odds] | SIGNALS: [list each signal] | [2 sentences]\n\n"
    "SLEEPER rules: if no real sleeper exists for a slot write exactly: NO SLEEPER - no mispriced edge on this slate. "
    "Never force a fake sleeper. Finding what nobody sees is the whole point.\n\n"
    + LOCKED_RULES
)

# ─── JOB STORE ────────────────────────────────────────────────────────────────
jobs = {}
store_lock = threading.Lock()

def new_job():
    jid = str(uuid.uuid4())[:8]
    with store_lock:
        jobs[jid] = {
            'status': 'pending', 'steps': [], 'result': None,
            'error': None, 'statcast': [], 'park_confirm': {},
            'bullpen': {}, 'created': time.time()
        }
    return jid

def step_set(jid, n, state, label=None):
    with store_lock:
        steps = jobs[jid].setdefault('steps', [])
        while len(steps) <= n:
            steps.append({'state': 'pending', 'label': ''})
        steps[n] = {'state': state, 'label': label or ''}

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def normalize_name(name):
    nfkd = unicodedata.normalize('NFKD', str(name))
    return ''.join(c for c in nfkd if not unicodedata.combining(c)).strip()

def safe_float(v):
    try:
        f = float(str(v).replace('%','').strip())
        return f if f == f else None
    except Exception:
        return None

def sane(stat, val):
    ranges = {
        'exit_velocity': (50.0, 125.0),
        'hard_hit_pct':  (0.0,  100.0),
        'barrel_pct':    (0.0,  100.0),
        'xwoba':         (0.05, 1.000),
        'woba':          (0.05, 1.000),
        'gb_pct':        (0.0,  100.0),
        'csw_pct':       (0.0,  100.0),
    }
    if val is None:
        return None
    lo, hi = ranges.get(stat, (None, None))
    if lo is None:
        return val
    return val if lo <= val <= hi else None

def savant_get(url, timeout=15, accept_json=False):
    headers = dict(_HEADERS)
    if accept_json:
        headers['Accept'] = 'application/json, text/javascript, */*'
    # Add timestamp cache-buster only if not already present
    if '_=' not in url:
        sep = '&' if '?' in url else '?'
        url = f"{url}{sep}_={int(time.time())}"
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode('utf-8', errors='replace')
    except Exception:
        return None

def call_claude(messages, system=None, max_tokens=4096):
    payload = {
        'model': MODEL,
        'max_tokens': max_tokens,
        'messages': messages,
    }
    if system:
        payload['system'] = system
    req = urllib.request.Request(
        'https://api.anthropic.com/v1/messages',
        data=json.dumps(payload).encode(),
        headers={
            'Content-Type': 'application/json',
            'x-api-key': ANTHROPIC_API_KEY,
            'anthropic-version': '2023-06-01',
        },
        method='POST'
    )
    try:
        with urllib.request.urlopen(req, timeout=90) as r:
            data = json.loads(r.read())
            return data['content'][0]['text']
    except Exception as e:
        return f"[Claude error: {e}]"

# ─── STATS CACHE (bulk leaderboard, loaded once per run) ─────────────────────
_stats_cache = {}
_stats_loaded = False
_stats_loading = False
_stats_lock = threading.Lock()
_csv_dir = os.path.dirname(os.path.abspath(__file__))

def _download_csvs():
    """Download Savant CSVs fresh. Called at startup and daily at midnight."""
    urls = [
        # Exit velocity CSV  -  has ev50, fbld, gb_ev, sweet_spot%, avg_hr_dist, max_hit_speed
        ('statcast_batters.csv',
         f'https://baseballsavant.mlb.com/leaderboard/statcast'
         f'?type=batter&year={CURRENT_YEAR}&position=&team=&min=1&csv=true'),
        ('statcast_pitchers.csv',
         f'https://baseballsavant.mlb.com/leaderboard/statcast'
         f'?type=pitcher&year={CURRENT_YEAR}&position=&team=&min=1&csv=true'),
        # Expected stats CSV  -  has xwOBA, wOBA, barrel_batted_rate, hard_hit_percent
        ('expected_batters.csv',
         f'https://baseballsavant.mlb.com/leaderboard/expected_statistics'
         f'?type=batter&year={CURRENT_YEAR}&position=&team=&min=1&csv=false'),
        ('expected_pitchers.csv',
         f'https://baseballsavant.mlb.com/leaderboard/expected_statistics'
         f'?type=pitcher&year={CURRENT_YEAR}&position=&team=&min=1&csv=false'),
    ]
    for filename, url in urls:
        try:
            path = os.path.join(_csv_dir, filename)
            raw = savant_get(url, accept_json=True, timeout=30)
            if raw and len(raw) > 1000 and not raw.strip().startswith('<'):
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(raw)
                print(f"[CSV DOWNLOAD] {filename}  -  {len(raw)} bytes")
            else:
                print(f"[CSV DOWNLOAD] {filename}  -  FAILED (got HTML or empty)")
        except Exception as e:
            print(f"[CSV DOWNLOAD] {filename}  -  ERROR: {e}")

def _daily_refresh_loop():
    """Background thread: refresh CSVs once at startup, then daily at midnight ET."""
    import datetime
    # Initial download at startup
    print("[CSV REFRESH] Initial download starting...")
    _download_csvs()
    print("[CSV REFRESH] Initial download complete")
    # Then reset cache and re-download at midnight every day
    while True:
        now = datetime.datetime.utcnow()
        # Midnight UTC = 8pm ET / 7pm CT  -  good time for next-day data
        tomorrow = (now + datetime.timedelta(days=1)).replace(
            hour=15, minute=0, second=0, microsecond=0)  # 11am ET = 15:00 UTC
        sleep_secs = (tomorrow - now).total_seconds()
        print(f"[CSV REFRESH] Next refresh in {sleep_secs/3600:.1f} hours")
        time.sleep(max(sleep_secs, 3600))
        print("[CSV REFRESH] Daily refresh starting...")
        _download_csvs()
        # Reset cache so next run picks up fresh data
        with _stats_lock:
            global _stats_cache, _stats_loaded, _stats_loading
            _stats_cache = {}
            _stats_loaded = False
            _stats_loading = False
        print("[CSV REFRESH] Daily refresh complete  -  cache cleared")

def load_stats_cache():
    """
    Load stats from auto-downloaded CSV files.
    Thread-safe: blocks until cache is fully loaded.
    Returns the global _stats_cache dict.
    """
    global _stats_cache, _stats_loaded, _stats_loading

    # Fast path
    with _stats_lock:
        if _stats_loaded:
            return _stats_cache

    # Need to load — claim or wait
    i_claimed = False
    with _stats_lock:
        if _stats_loaded:
            return _stats_cache  # check again inside lock
        if not _stats_loading:
            _stats_loading = True
            i_claimed = True
            # Only clear cache AFTER claiming, right before loading
            _stats_cache = {}

    if not i_claimed:
        # Another thread is loading — wait for it
        for _ in range(300):  # wait up to 60s
            time.sleep(0.2)
            with _stats_lock:
                if _stats_loaded:
                    return _stats_cache
        # Timed out — return whatever we have
        with _stats_lock:
            return _stats_cache

    # Only the thread that claimed i_claimed=True reaches here
    # Double-check we still need to load (another thread may have finished)
    with _stats_lock:
        if _stats_loaded:
            _stats_loading = False
            return _stats_cache

    # Load the CSVs and populate _stats_cache

    def parse_name(row):
        """Extract player name from CSV or JSON row."""
        lf = row.get('last_name, first_name', '')
        if lf:
            parts = lf.split(',', 1)
            if len(parts) == 2:
                return f"{parts[1].strip()} {parts[0].strip()}"
        return (row.get('name_display_first_last') or
                row.get('player_name') or row.get('name') or
                f"{row.get('first_name','')} {row.get('last_name','')}".strip()).strip(' ,')

    def pull_rows(rows):
        """Store/merge a list of player rows into the stats cache."""
        count = 0
        for row in rows:
            name = parse_name(row)
            if name and len(name) > 2:
                key = normalize_name(name).lower()
                if key in _stats_cache:
                    for k, v in row.items():
                        if v not in ('', None, 'null', 'None', 'NaN'):
                            _stats_cache[key][k] = v
                else:
                    _stats_cache[key] = dict(row)
                count += 1
        return count

    def parse_raw(raw):
        """Parse CSV or JSON text into list of dicts."""
        if not raw:
            return []
        raw = raw.strip().lstrip('\ufeff')
        if raw.startswith('"') or (not raw.startswith('{') and not raw.startswith('[')):
            try:
                import csv as _csv2, io as _io2
                reader = _csv2.DictReader(_io2.StringIO(raw))
                return [dict(r) for r in reader]
            except Exception as e:
                print(f"[CSV parse error] {e}")
                return []
        try:
            data = json.loads(raw)
            return data if isinstance(data, list) else data.get('data', [])
        except Exception:
            return []

    def pull_endpoint(url, player_type):
        raw = savant_get(url, accept_json=True, timeout=25)
        return pull_rows(parse_raw(raw))

    def pull_endpoint_raw(raw, player_type):
        return pull_rows(parse_raw(raw))

    import os as _os

    def load_local(filename):
        path = _os.path.join(_csv_dir, filename)
        if _os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8-sig') as f:
                    return f.read()
            except Exception:
                pass
        return None

    # PASS 1: statcast leaderboard  -  EV, HH%, Barrel%, GB% (auto-downloaded daily)
    for player_type, filename in [('batter', 'statcast_batters.csv'),
                                   ('pitcher', 'statcast_pitchers.csv')]:
        raw = load_local(filename)
        if raw:
            n = pull_endpoint_raw(raw, player_type)
            print(f"[STATS CACHE] {filename}={n} rows")
        else:
            # File not yet downloaded  -  try live
            url = (f'https://baseballsavant.mlb.com/leaderboard/statcast'
                   f'?type={player_type}&year={CURRENT_YEAR}&position=&team=&min=1&csv=true')
            n = pull_endpoint(url, player_type)
            print(f"[STATS CACHE] live statcast {player_type}={n}")

    # PASS 2: expected_statistics  -  xwOBA, wOBA (merges into existing entries)
    for player_type, filename in [('batter', 'expected_batters.csv'),
                                   ('pitcher', 'expected_pitchers.csv')]:
        raw = load_local(filename)
        if raw:
            n = pull_endpoint_raw(raw, player_type)
            print(f"[STATS CACHE] {filename}={n} rows")
        else:
            url = (f'https://baseballsavant.mlb.com/leaderboard/expected_statistics'
                   f'?type={player_type}&year={CURRENT_YEAR}&position=&team=&min=1&csv=false')
            n = pull_endpoint(url, player_type)
            print(f"[STATS CACHE] live expected_stats {player_type}={n}")

    print(f"[STATS CACHE] Total players={len(_stats_cache)}")
    with _stats_lock:
        _stats_loaded = True
        _stats_loading = False
    return _stats_cache

def clear_stats_cache():
    global _stats_cache, _stats_loaded, _stats_loading
    with _stats_lock:
        _stats_cache = {}
        _stats_loaded = False
        _stats_loading = False

def get_cached_stats(name, cache=None):
    """Look up player by name from bulk cache."""
    if cache is None:
        cache = load_stats_cache()
    key = normalize_name(name).lower()
    if key in cache:
        return cache[key]
    parts = key.split()
    if len(parts) >= 2:
        if f'{parts[0]} {parts[-1]}' in cache:
            return cache[f'{parts[0]} {parts[-1]}']
        if f'{parts[-1]}, {parts[0]}' in cache:
            return cache[f'{parts[-1]}, {parts[0]}']
    return None

# ─── SAVANT PAGE SCRAPE (last-resort fallback only) ─────────────────────────
def scrape_player_page(player_name):
    """
    Last resort fallback. Only used when player not found in CSV cache.
    Happens for very new call-ups or players with 0 PA in 2026.
    Returns partial stats dict or None.
    """
    slug = normalize_name(player_name).lower().replace(' ', '-')
    html = None
    for url in [
        f'https://baseballsavant.mlb.com/savant-player/{slug}?season={CURRENT_YEAR}',
        f'https://baseballsavant.mlb.com/savant-player/{slug}',
    ]:
        raw = savant_get(url)
        if raw and len(raw) > 10000:
            html = raw
            break
    if not html:
        return None

    # Scan page for known field names embedded in JS/JSON blobs
    stats = {}
    field_map = {
        'exit_velocity': [r'"avg_hit_speed"\s*:\s*"?([\d.]+)"?'],
        'hard_hit_pct':  [r'"ev95percent"\s*:\s*"?([\d.]+)"?'],
        'barrel_pct':    [r'"brl_percent"\s*:\s*"?([\d.]+)"?',
                          r'"brl_bip_percent"\s*:\s*"?([\d.]+)"?'],
        'xwoba':         [r'"est_woba"\s*:\s*"?([.\d]+)"?'],
        'woba':          [r'"woba"\s*:\s*"?([.\d]+)"?'],
        'ev50':          [r'"ev50"\s*:\s*"?([\d.]+)"?'],
    }
    for stat, patterns in field_map.items():
        for pat in patterns:
            m2 = re.search(pat, html)
            if m2:
                val = safe_float(m2.group(1))
                if val is not None:
                    stats[stat] = val
                    break

    return stats if stats.get('xwoba') is not None else None

# ─── FETCH ONE PLAYER ─────────────────────────────────────────────────────────
def fetch_one_player(info, cache=None):
    """
    Fetch stats for one player.
    1. Try bulk leaderboard cache (name lookup)
    2. Fall back to Savant page scrape
    cache param: pre-loaded cache dict to avoid repeated load_stats_cache() calls
    """
    name = info.get('name', '').strip()
    result = {
        **info,
        'exit_velocity': None, 'hard_hit_pct': None,
        'barrel_pct': None, 'barrel_pa': None,
        'xwoba': None, 'woba': None,
        'gb_pct': None, 'csw_pct': None,
        'ev50': None, 'sweet_spot_pct': None,
        'fbld_ev': None, 'gb_ev': None,
        'avg_hr_dist': None, 'max_hit_speed': None,
        'gap': None, 'fetch_status': 'not found', 'data_source': None,
    }
    if not name:
        result['fetch_status'] = 'no name'
        return result

    # SOURCE 1: bulk cache — use pre-loaded cache if provided
    row = get_cached_stats(name, cache=cache)
    if row:
        def g(row, *keys):
            for k in keys:
                v = row.get(k)
                if v not in (None, '', 'null', 'None', 'NaN', 'null'):
                    f = safe_float(v)
                    if f is not None and f > 0:
                        return f
            return None
        # Cover ALL possible field names across every Savant endpoint
        result['exit_velocity'] = sane('exit_velocity', g(row,
            'exit_velocity_avg',    # CSV expected_statistics
            'avg_hit_speed',        # CSV statcast / JSON custom leaderboard
            'exit_velocity'))
        result['hard_hit_pct']  = sane('hard_hit_pct', g(row,
            'hard_hit_percent',     # CSV expected_statistics
            'ev95percent',          # CSV statcast / JSON custom
            'hard_hit_bip_percent'))
        result['barrel_pct']    = sane('barrel_pct', g(row,
            'brl_percent',          # CSV statcast (brl_percent = per BBE)
            'barrel_batted_rate',   # CSV expected_statistics + JSON
            'brl_bip_percent'))
        result['barrel_pa']     = sane('barrel_pct', g(row,
            'brl_pa'))              # Barrel per PA  -  more stable
        result['xwoba']         = sane('xwoba', g(row,
            'est_woba',             # CSV expected_statistics
            'xwoba'))               # statcast / JSON
        result['woba']          = sane('woba', g(row, 'woba'))
        result['gb_pct']        = sane('gb_pct', g(row,
            'groundballs_percent', 'gb_percent', 'gb_pct', 'gb'))
        result['csw_pct']       = sane('csw_pct', g(row,
            'csw', 'csw_pct', 'called_strike_whiff_pct'))
        # New fields from exit_velocity CSV
        result['ev50']          = sane('exit_velocity', g(row, 'ev50'))
        result['sweet_spot_pct']= sane('hard_hit_pct', g(row,
            'anglesweetspotpercent', 'sweet_spot_percent', 'la_sweet_spot_percent'))
        result['fbld_ev']       = sane('exit_velocity', g(row, 'fbld'))
        result['gb_ev']         = sane('exit_velocity', g(row, 'gb'))
        result['avg_hr_dist']   = g(row, 'avg_hr_distance')
        result['max_hit_speed'] = sane('exit_velocity', g(row, 'max_hit_speed'))
        if result['xwoba'] is not None:
            result['data_source']  = 'leaderboard'
            result['fetch_status'] = 'ok'

    # SOURCE 2: page scrape if cache miss or missing key fields
    if result['fetch_status'] != 'ok' or result['xwoba'] is None:
        scraped = scrape_player_page(name)
        if scraped:
            for k, v in scraped.items():
                checked = sane(k, v)
                if checked is not None and result.get(k) is None:
                    result[k] = checked
            if result['xwoba'] is not None:
                result['fetch_status'] = 'ok'
                result['data_source'] = (result.get('data_source') or '') + '+page'

    if result['fetch_status'] != 'ok':
        result['fetch_status'] = 'found/no stats'
        return result

    # Compute GAP
    if result['xwoba'] is not None and result['woba'] is not None:
        result['gap'] = round(result['xwoba'] - result['woba'], 3)

    return result

def fetch_all_parallel(players, workers=12, cache=None):
    """Fetch all players in parallel. Cache loaded once and shared across all workers."""
    if cache is None:
        cache = load_stats_cache()
    print(f"[FETCH] {len(players)} players, cache={len(cache)}, workers={workers}")
    import functools
    fetch = functools.partial(fetch_one_player, cache=cache)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(fetch, players))
    ok = sum(1 for r in results if r.get('fetch_status') == 'ok')
    print(f"[FETCH] Done: {ok}/{len(results)} ok")
    return results

# ─── WEATHER ─────────────────────────────────────────────────────────────────
PARK_COORDS = {
    'Oracle Park':              (37.7786, -122.3893),
    'Nationals Park':           (38.8730, -77.0074),
    'Citizens Bank Park':       (39.9061, -75.1665),
    'Yankee Stadium':           (40.8296, -73.9262),
    'Coors Field':              (39.7559, -104.9942),
    'Fenway Park':              (42.3467, -71.0972),
    'Wrigley Field':            (41.9484, -87.6553),
    'Busch Stadium':            (38.6226, -90.1928),
    'Petco Park':               (32.7076, -117.1570),
    'Comerica Park':            (42.3390, -83.0485),
    'T-Mobile Park':            (47.5914, -122.3325),
    'Great American Ball Park': (39.0979, -84.5075),
    'Truist Park':              (33.8908, -84.4679),
    'PNC Park':                 (40.4469, -80.0057),
    'Minute Maid Park':         (29.7572, -95.3555),
    'Citi Field':               (40.7571, -73.8458),
    'Kauffman Stadium':         (39.0517, -94.4803),
    'American Family Field':    (43.0280, -87.9712),
    'Globe Life Field':         (32.7473, -97.0840),
    'Chase Field':              (33.4453, -112.0667),
    'Tropicana Field':          (27.7683, -82.6534),
    'Dodger Stadium':           (34.0739, -118.2400),
    'Angel Stadium':            (33.8003, -117.8827),
    'Target Field':             (44.9817, -93.2781),
    'Sutter Health Park':       (38.5802, -121.5000),
    'Rogers Centre':            (43.6414, -79.3894),
    'Progressive Field':        (41.4962, -81.6852),
    'LoanDepot Park':           (25.7781, -80.2196),
    'Camden Yards':             (39.2838, -76.6216),
    'Guaranteed Rate Field':    (41.8299, -87.6338),
    'Rate Field':               (41.8299, -87.6338),
    'Daikin Park':              (29.7572, -95.3555),
}

DOME_PARKS = {
    'American Family Field', 'Tropicana Field', 'Globe Life Field',
    'Rogers Centre', 'LoanDepot Park',
}

def fetch_weather(park_name):
    if park_name in DOME_PARKS:
        return {'temp_f': None, 'condition': 'Dome/Indoor', 'wind_mph': None,
                'flag': 'DOME', 'notes': 'Dome  -  weather not applicable'}

    coords = PARK_COORDS.get(park_name)
    if not coords:
        for k, v in PARK_COORDS.items():
            if any(w in park_name.lower() for w in k.lower().split() if len(w) > 4):
                coords = v
                break

    temp_f, wind_mph, condition = None, None, 'Unknown'

    if coords:
        lat, lon = coords
        try:
            pts_req = urllib.request.Request(
                f'https://api.weather.gov/points/{lat},{lon}',
                headers={'User-Agent': 'SharpOracle/1.0', 'Accept': 'application/json'}
            )
            with urllib.request.urlopen(pts_req, timeout=8) as r:
                pts = json.loads(r.read())
            fc_url = pts['properties']['forecastHourly']
            fc_req = urllib.request.Request(fc_url, headers={'User-Agent': 'SharpOracle/1.0', 'Accept': 'application/json'})
            with urllib.request.urlopen(fc_req, timeout=8) as r:
                fc = json.loads(r.read())
            periods = fc['properties']['periods']
            # Find the period closest to game time (1pm local = 18:00 UTC roughly)
            # Try to find a period between 11am-7pm today, else fall back to [0]
            import datetime
            now_utc = datetime.datetime.utcnow()
            best = periods[0]
            for p in periods[:12]:
                start = p.get('startTime', '')
                # Look for afternoon periods (11am-7pm)
                try:
                    dt = datetime.datetime.fromisoformat(start.replace('Z','+00:00'))
                    # Convert to rough local time (subtract 4-6h for ET/CT)
                    local_h = (dt.hour - 5) % 24
                    if 11 <= local_h <= 19:
                        best = p
                        break
                except Exception:
                    pass
            temp_f    = safe_float(best.get('temperature'))
            wind_str  = best.get('windSpeed', '0 mph')
            wind_mph  = safe_float(wind_str.split()[0]) if wind_str else None
            condition = best.get('shortForecast', 'Unknown')
        except Exception:
            pass

    if temp_f is None and coords:
        try:
            lat, lon = coords
            loc = f"{lat},{lon}"
            wttr_url = f'https://wttr.in/{loc}?format=j1'
            req = urllib.request.Request(wttr_url, headers={'User-Agent': 'curl/7.68.0'})
            with urllib.request.urlopen(req, timeout=8) as r:
                data = json.loads(r.read())
            cur = data['current_condition'][0]
            temp_f   = safe_float(cur.get('temp_F'))
            wind_mph = safe_float(cur.get('windspeedMiles'))
            condition = cur.get('weatherDesc', [{}])[0].get('value', 'Unknown')
        except Exception:
            pass

    flag = 'NEUTRAL'
    notes = []
    if temp_f is not None:
        if temp_f <= 45:
            flag = 'HARD_SUPPRESSOR'
            notes.append(f'{temp_f}F = hard suppress (<=45F)')
        elif temp_f <= 50:
            flag = 'SUPPRESSOR'
            notes.append(f'{temp_f}F = meaningful suppress (<=50F)')
        elif temp_f >= 85:
            flag = 'BOOSTER'
            notes.append(f'{temp_f}F = mild boost (>=85F)')
        else:
            notes.append(f'{temp_f}F = neutral')
    else:
        notes.append('Weather unavailable  -  neutral assumed')

    if wind_mph and wind_mph >= 15:
        notes.append(f'Wind {wind_mph}mph  -  check direction')

    return {
        'temp_f': temp_f, 'condition': condition,
        'wind_mph': wind_mph, 'flag': flag,
        'notes': ' | '.join(notes) if notes else 'No weather impact',
    }

# ─── PARK LOOKUP ─────────────────────────────────────────────────────────────
PARK_LOOKUP = {
    'yankees':      ('Yankee Stadium',           'BOOSTER'),
    'reds':         ('Great American Ball Park',  'BOOSTER'),
    'phillies':     ('Citizens Bank Park',        'BOOSTER'),
    'rockies':      ('Coors Field',               'BOOSTER'),
    'athletics':    ('Sutter Health Park',        'BOOSTER'),
    'tigers':       ('Comerica Park',             'SUPPRESSOR'),
    'padres':       ('Petco Park',                'SUPPRESSOR'),
    'giants':       ('Oracle Park',               'SUPPRESSOR'),
    'mariners':     ('T-Mobile Park',             'SUPPRESSOR'),
    'brewers':      ('American Family Field',      'DOME'),
    'rays':         ('Tropicana Field',           'DOME'),
    'rangers':      ('Globe Life Field',           'DOME'),
    'diamondbacks': ('Chase Field',               'DOME'),
    'astros':       ('Daikin Park',               'NEUTRAL'),
    'dodgers':      ('Dodger Stadium',            'NEUTRAL'),
    'red sox':      ('Fenway Park',               'NEUTRAL'),
    'cubs':         ('Wrigley Field',             'NEUTRAL'),
    'cardinals':    ('Busch Stadium',             'NEUTRAL'),
    'braves':       ('Truist Park',               'NEUTRAL'),
    'pirates':      ('PNC Park',                  'NEUTRAL'),
    'mets':         ('Citi Field',                'NEUTRAL'),
    'royals':       ('Kauffman Stadium',          'NEUTRAL'),
    'twins':        ('Target Field',              'NEUTRAL'),
    'angels':       ('Angel Stadium',             'NEUTRAL'),
    'blue jays':    ('Rogers Centre',             'DOME'),
    'guardians':    ('Progressive Field',         'NEUTRAL'),
    'marlins':      ('LoanDepot Park',            'DOME'),
    'orioles':      ('Camden Yards',              'NEUTRAL'),
    'white sox':    ('Rate Field',                'NEUTRAL'),
    'nationals':    ('Nationals Park',            'NEUTRAL'),
}

def resolve_park(home_team):
    key = normalize_name(home_team).lower()
    for k, v in PARK_LOOKUP.items():
        if k in key or key in k:
            return v[0], v[1]
    return f'{home_team} Park', 'NEUTRAL'

# ─── BULLPEN ERA ─────────────────────────────────────────────────────────────
TEAM_IDS = {
    'angels': 108, 'astros': 117, 'athletics': 133, 'blue jays': 141,
    'braves': 144, 'brewers': 158, 'cardinals': 138, 'cubs': 112,
    'diamondbacks': 109, 'dodgers': 119, 'giants': 137, 'guardians': 114,
    'mariners': 136, 'marlins': 146, 'mets': 121, 'nationals': 120,
    'orioles': 110, 'padres': 135, 'phillies': 143, 'pirates': 134,
    'rangers': 140, 'rays': 139, 'red sox': 111, 'reds': 113,
    'rockies': 115, 'royals': 118, 'tigers': 116, 'twins': 142,
    'white sox': 145, 'yankees': 147,
}

def fetch_bullpen_era(team_name):
    key = normalize_name(team_name).lower()
    team_id = None
    for k, v in TEAM_IDS.items():
        if k in key or key in k:
            team_id = v
            break
    if not team_id:
        return {'era': None, 'tier': 'UNKNOWN'}

    # Try covers.com bullpen stats page
    COVERS_ABBREVS = {
        'angels': 'LAA', 'astros': 'HOU', 'athletics': 'OAK', 'blue jays': 'TOR',
        'braves': 'ATL', 'brewers': 'MIL', 'cardinals': 'STL', 'cubs': 'CHC',
        'diamondbacks': 'ARI', 'dodgers': 'LAD', 'giants': 'SF', 'guardians': 'CLE',
        'mariners': 'SEA', 'marlins': 'MIA', 'mets': 'NYM', 'nationals': 'WSH',
        'orioles': 'BAL', 'padres': 'SD', 'phillies': 'PHI', 'pirates': 'PIT',
        'rangers': 'TEX', 'rays': 'TB', 'red sox': 'BOS', 'reds': 'CIN',
        'rockies': 'COL', 'royals': 'KC', 'tigers': 'DET', 'twins': 'MIN',
        'white sox': 'CWS', 'yankees': 'NYY',
    }
    abbrev = None
    for k, v in COVERS_ABBREVS.items():
        if k in key or key in k:
            abbrev = v
            break

    # Try covers.com
    if abbrev:
        try:
            url = f'https://www.covers.com/sport/baseball/mlb/statistics/team-bullpenera/{CURRENT_YEAR}'
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=10) as r:
                html = r.read().decode('utf-8', errors='replace')
            # Find the team row
            pattern = rf'{abbrev}[^<]*</[^>]+>[^<]*<[^>]+>([0-9]+\.[0-9]+)'
            m = re.search(pattern, html)
            if not m:
                # Try broader search near abbrev
                idx = html.find(f'>{abbrev}<')
                if idx == -1:
                    idx = html.find(f'">{abbrev}')
                if idx > 0:
                    snippet = html[idx:idx+200]
                    nm = re.search(r'([0-9]+\.[0-9]+)', snippet)
                    if nm:
                        era = safe_float(nm.group(1))
                        if era and 0 < era < 10:
                            tier = 'WEAK' if era >= 5.50 else 'AVERAGE' if era >= 4.50 else 'SOLID'
                            return {'era': round(era, 2), 'tier': tier}
        except Exception:
            pass

    # Fallback: MLB Stats API
    for url in [
        f'https://statsapi.mlb.com/api/v1/teams/{team_id}/stats?stats=season&group=pitching&season={CURRENT_YEAR}&gameType=R',
        f'https://statsapi.mlb.com/api/v1/teams/{team_id}/stats?stats=season&group=pitching&season={CURRENT_YEAR}',
    ]:
        try:
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())
            splits = data.get('stats', [{}])[0].get('splits', [])
            if splits:
                era = safe_float(splits[0].get('stat', {}).get('era'))
                if era and 0 < era < 20:
                    tier = 'WEAK' if era >= 5.50 else 'AVERAGE' if era >= 4.50 else 'SOLID'
                    return {'era': round(era, 2), 'tier': tier}
        except Exception:
            pass
    return {'era': None, 'tier': 'UNKNOWN'}

# ─── LINEUP PARSING ──────────────────────────────────────────────────────────
def parse_lineup(raw, game_date=None):
    """
    Parse raw lineup paste using Claude.
    Handles any format: MLB.com, Rotowire, FantasyPros, plain text, etc.
    Extracts: away_team, home_team, away_pitcher, home_pitcher,
              away_batters, home_batters (with hand, lineup_pos).
    """
    prompt = f"""You are parsing an MLB lineup. It could be pasted from ANY source  - 
MLB.com, Rotowire, FantasyPros, ESPN, a plain text list, or anything else.
Extract the information and return JSON only, no other text.

RULES:
- The game format is AwayTeam @ HomeTeam (team before @ is AWAY, team after @ is HOME)
- If no @ symbol, infer from context (stadium name, "home"/"away" labels, etc.)
- Away pitcher faces HOME batters. Home pitcher faces AWAY batters.
- Batter hand: R=right, L=left, S=switch. If not listed, guess from player name knowledge.
- Lineup position: batting order 1-9. If not listed, use the order they appear.
- Include ALL batters listed, even if hand is unknown (use "R" as default).
- If a field is truly unknown, use "?" not null.

Return this exact JSON structure:
{{
  "away_team": "team name",
  "home_team": "team name",
  "away_pitcher": {{"name": "First Last", "hand": "R/L"}},
  "home_pitcher": {{"name": "First Last", "hand": "R/L"}},
  "away_batters": [{{"name": "First Last", "hand": "R/L/S", "lineup_pos": 1}}],
  "home_batters": [{{"name": "First Last", "hand": "R/L/S", "lineup_pos": 1}}],
  "park_name": "stadium name or ?",
  "game_date": "{game_date or ''}"
}}

Lineup to parse:
{raw}"""

    resp = call_claude([{'role': 'user', 'content': prompt}], max_tokens=2000)
    try:
        m = re.search(r'\{.*\}', resp, re.DOTALL)
        if m:
            return json.loads(m.group())
    except Exception:
        pass

    # Hard fallback
    return {
        'away_team': '?', 'home_team': '?',
        'away_pitcher': {}, 'home_pitcher': {},
        'away_batters': [], 'home_batters': [],
        'park_name': '?', 'game_date': game_date or '',
    }

# ─── SCORING ─────────────────────────────────────────────────────────────────
def compute_pitcher_gate(p):
    ev   = p.get('exit_velocity')
    ev50 = p.get('ev50')
    hh   = p.get('hard_hit_pct')
    xw   = p.get('xwoba')
    brl  = p.get('barrel_pct')
    gb   = p.get('gb_pct')
    csw  = p.get('csw_pct')
    fbld = p.get('fbld_ev')

    score = sum([
        1 if (ev  is not None and ev  >= 93.0) else 0,
        1 if (hh  is not None and hh  >= 50.0) else 0,
        1 if (xw  is not None and xw  >= 0.350) else 0,
        1 if (brl is not None and brl >= 15.0) else 0,
    ])
    gate = 'OPEN' if score <= 1 else 'HALF' if score == 2 else 'CLOSED'

    # GB% modifier
    gb_flag = ''
    if gb is not None:
        if gb >= 55:
            gb_flag = f' GB%={gb}(ELITE-SUPPRESSOR->gate+0.5)'
            if gate == 'OPEN': gate = 'OPEN->HALF'
            elif gate == 'HALF': gate = 'HALF->CLOSED'
        elif gb >= 48:
            gb_flag = f' GB%={gb}(SOLID-GB)'
        else:
            gb_flag = f' GB%={gb}(fly-ball-prone->batter-boost)'

    # CSW% modifier
    csw_flag = ''
    if csw is not None:
        if csw >= 30:   csw_flag = f' CSW%={csw}(ELITE-MISS)'
        elif csw < 25:  csw_flag = f' CSW%={csw}(hittable)'

    # EV50  -  for pitchers, LOWER ev50 = better (softer contact allowed)
    ev50_flag = ''
    if ev50 is not None:
        if ev50 >= 103:   ev50_flag = f' EV50={ev50}(DANGER-batters-squaring-up)'
        elif ev50 <= 96:  ev50_flag = f' EV50={ev50}(ELITE-soft-contact)'

    # FB/LD EV for pitchers  -  lower = better
    fbld_flag = ''
    if fbld is not None:
        if fbld >= 95:   fbld_flag = f' FB/LD={fbld}(HARD-fly-balls->HR-risk)'
        elif fbld <= 88: fbld_flag = f' FB/LD={fbld}(SOFT-fly-balls->suppressor)'

    pts = [
        f"EV={ev or 'N/A'}{'✓' if ev and ev>=93 else '✗'}",
        f"HH%={hh or 'N/A'}{'✓' if hh and hh>=50 else '✗'}",
        f"xwOBA={xw or 'N/A'}{'✓' if xw and xw>=0.350 else '✗'}",
        f"Brl%={brl or 'N/A'}{'✓' if brl and brl>=15 else '✗'}",
    ]
    return score, gate, ' | '.join(pts) + gb_flag + csw_flag + ev50_flag + fbld_flag

def compute_batter_score(b):
    brl    = b.get('barrel_pct')
    brl_pa = b.get('barrel_pa')
    ev     = b.get('exit_velocity')
    ev50   = b.get('ev50')
    hh     = b.get('hard_hit_pct')
    xw     = b.get('xwoba')
    wo     = b.get('woba')
    gap    = b.get('gap')
    ss     = b.get('sweet_spot_pct')
    fbld   = b.get('fbld_ev')
    hr_dist= b.get('avg_hr_dist')

    # Upgrade #2: Regression Gap  -  xwOBA>=.420 + gap>=+.100 -> HH% threshold drops to 45%
    hh_threshold = 50.0
    upgrade2_flag = ''
    if xw is not None and xw >= 0.420 and gap is not None and gap >= 0.100:
        hh_threshold = 45.0
        upgrade2_flag = ' [#2-REG-GAP:HH%->45]'

    score = sum([
        1 if (brl is not None and brl >= 15.0) else 0,
        1 if (xw  is not None and xw  >= 0.350) else 0,
        1 if (ev  is not None and ev  >= 91.0)  else 0,
        1 if (hh  is not None and hh  >= hh_threshold) else 0,
    ])

    if gap is None:        gap_flag = 'N/A'
    elif gap >= 0.100:     gap_flag = 'COLD-BUY'
    elif gap > 0:          gap_flag = 'COLD'
    elif gap == 0:         gap_flag = 'NEUTRAL'
    elif gap > -0.060:     gap_flag = 'HOT'
    else:                  gap_flag = 'HOT-EXTREME'

    hr_cap = ''
    if gap is not None and gap < 0:
        hit_tag = ' HIT-PICK-YES' if (wo and wo >= 0.320) else ''
        hr_cap = f' HR-CAP-C{hit_tag}' if gap <= -0.060 else f' HR-CAP-B{hit_tag}'

    # EV50 tier flag
    ev50_flag = ''
    if ev50 is not None:
        if ev50 >= 103:   ev50_flag = f' EV50={ev50}(ELITE)'
        elif ev50 >= 100: ev50_flag = f' EV50={ev50}(PLUS)'
        elif ev50 < 95:   ev50_flag = f' EV50={ev50}(WEAK)'
        else:             ev50_flag = f' EV50={ev50}'

    # Sweet spot flag
    ss_flag = ''
    if ss is not None:
        if ss >= 38:   ss_flag = f' SS%={ss}(ELITE-LA)'
        elif ss >= 32: ss_flag = f' SS%={ss}(GOOD-LA)'
        else:          ss_flag = f' SS%={ss}(poor-LA)'

    # FB/LD EV flag  -  contact quality on actual fly balls
    fbld_flag = ''
    if fbld is not None:
        if fbld >= 95:   fbld_flag = f' FB/LD-EV={fbld}(ELITE)'
        elif fbld < 88:  fbld_flag = f' FB/LD-EV={fbld}(WEAK)'

    # HR distance flag
    hrd_flag = ''
    if hr_dist and hr_dist > 0:
        if hr_dist >= 410:   hrd_flag = f' HR-DIST={hr_dist}(ELITE-CARRY)'
        elif hr_dist >= 390: hrd_flag = f' HR-DIST={hr_dist}(avg)'
        elif hr_dist > 0:    hrd_flag = f' HR-DIST={hr_dist}(short)'

    # Barrel/PA  -  more stable power rate
    brl_pa_flag = ''
    if brl_pa is not None:
        if brl_pa >= 10: brl_pa_flag = f' Brl/PA={brl_pa}(ELITE)'
        elif brl_pa >= 6: brl_pa_flag = f' Brl/PA={brl_pa}'

    # Upgrade #3: Elite Barrel  -  4/4 + Barrel>=25% + positive gap
    upgrade3_flag = ''
    if score == 4 and brl is not None and brl >= 25.0 and gap is not None and gap > 0:
        upgrade3_flag = ' [#3-ELITE-BARREL:pitcher-cold-half-step]'

    # Upgrade #10: Regression Bomb  -  gap>=+.100 + batting 1-5
    upgrade10_flag = ''
    lineup_pos = b.get('lineup_pos', 99)
    if gap is not None and gap >= 0.100 and lineup_pos <= 5:
        upgrade10_flag = ' [#10-REG-BOMB:C-DART+400]'

    # Upgrade #14: Elite Profile Park  -  Barrel>=20% + xwOBA>=.400 + 1-5
    upgrade14_flag = ''
    if (brl is not None and brl >= 20.0 and
        xw is not None and xw >= 0.400 and
        lineup_pos <= 5):
        upgrade14_flag = ' [#14-ELITE-PROFILE:C-DART-if-booster]'

    pts = [
        f"Brl={brl or 'N/A'}{'✓' if brl and brl>=15 else '✗'}",
        f"xwOBA={xw or 'N/A'}{'✓' if xw and xw>=0.350 else '✗'}",
        f"EV={ev or 'N/A'}{'✓' if ev and ev>=91 else '✗'}",
        f"HH%={hh or 'N/A'}{'✓' if hh and hh>=hh_threshold else '✗'}",
    ]
    extra = ev50_flag + ss_flag + fbld_flag + hrd_flag + brl_pa_flag
    upgrade_flags = upgrade2_flag + upgrade3_flag + upgrade10_flag + upgrade14_flag
    return score, ' | '.join(pts) + extra + upgrade_flags, gap_flag, hr_cap

def compute_platoon(bh, ph):
    bh, ph = str(bh).upper(), str(ph).upper()
    if bh == 'S': return 'FAV(SW)'
    if (bh=='L' and ph=='R') or (bh=='R' and ph=='L'): return 'FAV'
    return 'SAME'

# ─── CONTEXT BUILDER ─────────────────────────────────────────────────────────
def build_context(parsed, all_statcast, weather, park_name, park_cat, pen_era):
    home = parsed.get('home_team', '?')
    away = parsed.get('away_team', '?')
    wx = weather
    temp_str = f"{wx['temp_f']}F" if wx.get('temp_f') else 'N/A'

    lines = [
        f"GAME: {away} @ {home}",
        f"PARK: {park_name} [{park_cat}]",
        f"WEATHER: {temp_str} | {wx.get('condition','N/A')} | Wind {wx.get('wind_mph','N/A')} mph | {wx.get('flag','NEUTRAL')}",
        f"WEATHER NOTE: {wx.get('notes','')}",
        '',
        'VERIFIED ASSIGNMENTS:',
        f"  HOME team: {home}  -  plays at {park_name}",
        f"  AWAY team: {away}",
    ]

    # Pitcher gates
    pitcher_gates = {}
    lines.append('')
    lines.append('=== PITCHERS (gate pre-computed) ===')
    for p in all_statcast:
        if p.get('role') != 'PITCHER':
            continue
        score, gate, breakdown = compute_pitcher_gate(p)
        faces = p.get('faces_team', '?')
        pitcher_gates[faces] = {'gate': gate, 'score': score, 'hand': p.get('hand','?'), 'name': p.get('name','?')}
        g = p.get('gap')
        gs = f"{g:+.3f}" if g is not None else 'N/A'
        proxy = '[PROXY] ' if 'no stat' in str(p.get('fetch_status','')) else ''
        lines.append(
            f"  {proxy}{p.get('name','?')} ({p.get('hand','?')}HP) "
            f"pitches for {p.get('team','?')}, FACES {faces} batters | "
            f"GATE={score}/4={gate} | gap={gs} | {breakdown}"
        )

    # Bullpen ERA
    lines.append('')
    lines.append('=== BULLPEN ERA ===')
    weak_pen_teams = []
    for team, data in pen_era.items():
        era = data.get('era')
        tier = data.get('tier', 'UNKNOWN')
        era_str = f"{era:.2f}" if era else 'N/A'
        flag = ''
        if tier == 'WEAK':
            flag = ' ⚠ WEAK PEN  -  #1/#5 ACTIVE: all Barrel>=15+xwOBA>=.350 batters facing this bullpen are live'
            weak_pen_teams.append(team)
        lines.append(f"  {team}: ERA={era_str} [{tier}]{flag}")
    if weak_pen_teams:
        lines.append(f"  BULLPEN TIER TEAMS: {weak_pen_teams}  -  flag ANY batter (Brl>=15+xwOBA>=.350) facing these pens")

    # Batters
    proxy_count = sum(1 for b in all_statcast
                      if b.get('role') == 'BATTER'
                      and 'no stat' in str(b.get('fetch_status','')))
    if proxy_count > 5:
        lines.append(f'\n!! DATA WARNING: {proxy_count} batters on PROXY !!')

    for team in [away, home]:
        opp = home if team == away else away
        gate_info = pitcher_gates.get(team, {})
        opp_gate = gate_info.get('gate', '?')
        opp_hand = gate_info.get('hand', '?')
        opp_pitcher = gate_info.get('name', '?')

        lines.append('')
        lines.append(f'=== {team.upper()} BATTERS vs {opp_pitcher} ({opp}, gate={opp_gate}) ===')
        batters = [b for b in all_statcast if b.get('role') == 'BATTER' and b.get('team') == team]
        batters.sort(key=lambda x: x.get('lineup_pos', 99))

        # Upgrade #4: Stack check  -  3+ batters B+ or better vs same pitcher
        team_scores = []
        for b in batters:
            sc, _, _, _ = compute_batter_score(b)
            pl = compute_platoon(b.get('hand','?'), opp_hand)
            team_scores.append((sc, pl))
        b_plus_count = sum(1 for sc, pl in team_scores
                          if (sc >= 3 and pl == 'FAV') or (sc >= 3.5 and pl == 'SAME'))
        stack_flag = f'  ⚡ STACK GAME  -  {b_plus_count} B+ batters vs {opp_pitcher} (widen net)' if b_plus_count >= 3 else ''
        if stack_flag:
            lines.append(stack_flag)

        for b in batters:
            score, breakdown, gap_flag, hr_cap = compute_batter_score(b)
            platoon = compute_platoon(b.get('hand','?'), opp_hand)
            g = b.get('gap')
            gs = f"{g:+.3f}" if g is not None else 'N/A'
            proxy = '[PROXY] ' if 'no stat' in str(b.get('fetch_status','')) else ''

            # Compute grade label (score is int 0-4)
            if platoon in ('FAV', 'FAV(SW)'):
                if score == 4:    grade = 'A'
                elif score == 3:  grade = 'A-'
                elif score == 2:  grade = 'B+'
                elif score == 1:  grade = 'B'
                else:             grade = 'C'
            else:  # SAME side  -  drops half grade
                if score == 4:    grade = 'A-'
                elif score == 3:  grade = 'B+'
                elif score == 2:  grade = 'B'
                else:             grade = 'C'

            # Upgrade #11: 4/4 + 1-5 + fav platoon -> flag C Dart
            u11 = ''
            if score == 4 and b.get('lineup_pos',99) <= 5 and platoon in ('FAV','FAV(SW)'):
                u11 = ' [#11:4/4-FAV-1-5->C-DART]'

            # Upgrade #12: Barrel>=20% + booster park + fav platoon
            u12 = ''
            brl = b.get('barrel_pct') or 0
            if brl >= 20 and park_cat == 'BOOSTER' and platoon in ('FAV','FAV(SW)'):
                u12 = ' [#12:ELITE-BARREL+BOOSTER->B-DART]'

            # Upgrade #14 already in breakdown string if applicable
            lines.append(
                f"  #{b.get('lineup_pos','?')} {proxy}{b.get('name','?')} ({b.get('hand','?')}HB) | "
                f"SCORE={score}/4 GRADE={grade} | plat={platoon} | gap={gs}({gap_flag}){hr_cap}{u11}{u12} | "
                f"wOBA={b.get('woba','N/A')} | {breakdown}"
            )

    lines.append('')
    lines.append('RULES: Use pre-computed SCORE, GATE, platoon exactly. Do not re-compute.')
    lines.append('HR-CAP-C = max C grade. HR-CAP-B = max B grade. HIT-PICK-YES = include in hits.')
    lines.append('HOT gap = fade HR only, does NOT suppress hit probability.')
    return '\n'.join(lines)

# ─── MAIN JOB ────────────────────────────────────────────────────────────────
def run_job(jid, sid, raw_lineup, game_date=None):
    with store_lock:
        jobs[jid]['status'] = 'running'
    try:
        # STEP 1: Parse lineup
        step_set(jid, 0, 'active', 'Parsing lineup...')
        # NOTE: Do NOT clear cache here  -  cache is managed by _daily_refresh_loop
        # Clearing on every run causes partial data when downloads are in progress
        parsed = parse_lineup(raw_lineup, game_date)
        home = parsed.get('home_team', '?')
        away = parsed.get('away_team', '?')
        print(f"[PARSE] away={away} @ home={home} | "
              f"home_pitcher={parsed.get('home_pitcher',{}).get('name','?')} faces {away} | "
              f"away_pitcher={parsed.get('away_pitcher',{}).get('name','?')} faces {home}")
        step_set(jid, 0, 'done', f'{away} @ {home}')

        # STEP 2: Park + weather + bullpen ERA (fast context first)
        step_set(jid, 1, 'active', 'Fetching park, weather & bullpen...')
        park_name, park_cat = resolve_park(home)
        weather = fetch_weather(park_name)

        # Fetch bullpen ERA in parallel with weather
        pen_era = {}
        for team in [home, away]:
            if team and team != '?':
                data = fetch_bullpen_era(team)
                pen_era[team] = data
                era_str = f"{data['era']:.2f}" if data.get('era') else 'N/A'
                print(f"[PEN ERA] {team}: ERA={era_str} [{data['tier']}]")

        with store_lock:
            jobs[jid]['park_confirm'] = {
                'park': park_name, 'category': park_cat,
                'temp_f': weather['temp_f'], 'condition': weather['condition'],
                'wind_mph': weather['wind_mph'], 'weather_flag': weather['flag'],
                'notes': weather.get('notes', ''),
            }
            jobs[jid]['bullpen'] = pen_era

        temp_str = f"{weather['temp_f']}F" if weather['temp_f'] else 'N/A'
        pen_summary = ' | '.join(f"{t}={d.get('era','N/A')}" for t,d in pen_era.items())
        step_set(jid, 1, 'done', f'{park_name} | {temp_str} {weather["flag"]} | Pen: {pen_summary}')

        # STEP 3: Statcast fetch
        step_set(jid, 2, 'active', 'Fetching Statcast...')

        # Wait until CSV download and cache load is fully complete
        # The background thread downloads CSVs at startup — if a run starts
        # before download finishes, we wait here rather than get partial data
        waited = 0
        while not _stats_loaded and waited < 60:
            time.sleep(0.5)
            waited += 0.5
            if int(waited) % 5 == 0 and waited == int(waited):
                print(f"[STATS] Waiting for cache to load... {int(waited)}s elapsed")

        cache = load_stats_cache()
        print(f"[STATS] Cache ready: {len(cache)} players (waited {waited:.1f}s)")

        hp = parsed.get('home_pitcher', {})
        ap = parsed.get('away_pitcher', {})
        pitcher_list = []
        if hp.get('name'):
            pitcher_list.append({**hp, 'role': 'PITCHER', 'team': home, 'faces_team': away, 'lineup_pos': 0})
        if ap.get('name'):
            pitcher_list.append({**ap, 'role': 'PITCHER', 'team': away, 'faces_team': home, 'lineup_pos': 0})

        batter_list = []
        for b in parsed.get('home_batters', []):
            batter_list.append({**b, 'role': 'BATTER', 'team': home})
        for b in parsed.get('away_batters', []):
            batter_list.append({**b, 'role': 'BATTER', 'team': away})

        # Pass cache directly — no re-loading in parallel workers
        # Keep workers low to avoid mobile connection issues
        pitcher_stats = fetch_all_parallel(pitcher_list, workers=2, cache=cache)
        batter_stats  = fetch_all_parallel(batter_list, workers=2, cache=cache)
        all_statcast  = pitcher_stats + batter_stats

        ok = sum(1 for x in all_statcast if x.get('fetch_status') == 'ok')
        print(f"[STATS] {ok}/{len(all_statcast)} ok")

        slim_statcast = []
        for p in all_statcast:
            slim_statcast.append({
                'name':           p.get('name'),
                'role':           p.get('role'),
                'team':           p.get('team'),
                'barrel_pct':     p.get('barrel_pct'),
                'exit_velocity':  p.get('exit_velocity'),
                'ev50':           p.get('ev50'),
                'hard_hit_pct':   p.get('hard_hit_pct'),
                'xwoba':          p.get('xwoba'),
                'woba':           p.get('woba'),
                'gap':            p.get('gap'),
                'sweet_spot_pct': p.get('sweet_spot_pct'),
                'fetch_status':   p.get('fetch_status'),
            })

        # Write statcast immediately so UI can show it while analysis runs
        with store_lock:
            jobs[jid]['statcast'] = slim_statcast
            jobs[jid]['statcast_total'] = len(slim_statcast)
        step_set(jid, 2, 'done', f'Stats: {ok}/{len(all_statcast)} fetched')

        # STEP 4: Analysis (runs in background, statcast already visible)
        step_set(jid, 3, 'active', 'Running model analysis...')
        ctx = build_context(parsed, all_statcast, weather, park_name, park_cat, pen_era)
        analysis = call_claude(
            [{'role': 'user', 'content': ctx}],
            system=SYSTEM_PROMPT,
            max_tokens=8000
        )
        with store_lock:
            jobs[jid]['result'] = analysis
            jobs[jid]['status'] = 'done'
        step_set(jid, 3, 'done', 'Analysis complete')

    except Exception as e:
        tb = traceback.format_exc()
        print(f"[RUN_JOB ERROR] {e}\n{tb}")
        with store_lock:
            jobs[jid]['status'] = 'error'
            jobs[jid]['error'] = str(e)

# ─── HTML UI ─────────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Sharp Oracle</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0a0e1a;color:#e2e8f0;font-family:'Segoe UI',sans-serif;min-height:100vh;display:flex;flex-direction:column}
.header{background:#060a14;border-bottom:2px solid #f7c948;padding:10px 16px;display:flex;align-items:center;gap:12px;flex-shrink:0}
.logo{font-size:1.2em;font-weight:900;letter-spacing:3px;color:#f7c948}
.logo span{color:#e2e8f0;font-weight:300}
.header-status{font-size:11px;color:#64748b;margin-left:auto;display:flex;align-items:center;gap:6px}
.status-dot{width:7px;height:7px;border-radius:50%;background:#22c55e;display:inline-block;box-shadow:0 0 6px #22c55e}
.topnav{background:#080d18;border-bottom:1px solid #1e3a5f;display:flex;overflow-x:auto;flex-shrink:0;-webkit-overflow-scrolling:touch}
.topnav::-webkit-scrollbar{height:0}
.nav-btn{flex-shrink:0;padding:10px 18px;font-size:12px;font-weight:700;color:#475569;background:none;border:none;cursor:pointer;border-bottom:2px solid transparent;letter-spacing:.5px;white-space:nowrap;transition:all .15s}
.nav-btn:hover{color:#94a3b8}
.nav-btn.active{color:#f7c948;border-bottom-color:#f7c948}
.panel{display:none;padding:12px;overflow-y:auto;height:calc(100vh - 90px);-webkit-overflow-scrolling:touch}
.panel.active{display:block}
.card{background:#111827;border:1px solid #1e3a5f;border-radius:8px;padding:14px;margin-bottom:10px}
.card-title{font-size:10px;font-weight:700;letter-spacing:1.5px;color:#64748b;text-transform:uppercase;margin-bottom:8px}
textarea{width:100%;background:#080d18;color:#e2e8f0;border:1px solid #1e3a5f;border-radius:6px;padding:12px;font-size:13px;resize:vertical;min-height:160px;font-family:'Courier New',monospace;line-height:1.5}
textarea:focus{outline:none;border-color:#f7c948}
textarea::placeholder{color:#1e3a5f}
.run-btn{background:#f7c948;color:#080d18;border:none;border-radius:6px;padding:12px;font-size:14px;font-weight:800;cursor:pointer;width:100%;margin-top:8px;text-transform:uppercase}
.run-btn:hover{background:#e6b800}
.run-btn:disabled{background:#1e293b;color:#475569;cursor:not-allowed}
.steps{display:flex;flex-direction:column;gap:5px}
.step{display:flex;align-items:center;gap:8px;padding:7px 10px;border-radius:6px;background:#080d18;border:1px solid #1e293b;font-size:12px;color:#475569}
.step.active{border-color:#f7c948;color:#f7c948}
.step.done{border-color:#22c55e;color:#22c55e}
.step.error{border-color:#ef4444;color:#ef4444}
.pill-row{display:flex;gap:8px;flex-wrap:wrap}
.pill{background:#0d1424;border:1px solid #1e3a5f;border-radius:16px;padding:4px 12px;font-size:11px;color:#64748b}
.pill b{color:#f7c948}.pill.bad b{color:#ef4444}.pill.warn b{color:#f97316}.pill.good b{color:#22c55e}
.tbl-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;width:100%}
table{width:100%;border-collapse:collapse;font-size:11px;min-width:520px}
th{background:#080d18;padding:6px 6px;text-align:left;color:#475569;font-weight:600;font-size:9px;letter-spacing:.8px;text-transform:uppercase;border-bottom:1px solid #1e3a5f;white-space:nowrap}
td{padding:5px 6px;border-bottom:1px solid #0d1424;white-space:nowrap;font-size:11px}
@media(max-width:600px){table{font-size:10px}th{font-size:8px;padding:5px 5px}td{padding:4px 5px}}
tr:hover td{background:#0a0f1a}
tr.pitcher-row td{background:#06090f}
.hit{color:#22c55e;font-weight:700}.miss{color:#334155}.na{color:#1e3a5f}.hot{color:#ef4444}
.result-box{background:#080d18;border:1px solid #1e3a5f;border-radius:6px;padding:16px;white-space:pre-wrap;font-family:'Courier New',monospace;font-size:13px;line-height:1.8;width:100%;min-height:200px}
.panel{-webkit-overflow-scrolling:touch}
@media(max-width:600px){.result-box{font-size:11.5px;padding:12px;line-height:1.7}.logo{font-size:1em;letter-spacing:2px}}
</style>
</head>
<body>
<div class="header">
  <div class="logo">⚡ SHARP<span> ORACLE</span></div>
  <div class="header-status"><span class="status-dot"></span><span id="cacheStatus">checking...</span></div>
</div>
<div class="topnav">
  <button class="nav-btn active" onclick="show('analyze',this)">ANALYZE</button>
  <button class="nav-btn" onclick="show('stats',this)">STATCAST</button>
  <button class="nav-btn" onclick="show('picks',this)">PICKS</button>
</div>

<div id="panel-analyze" class="panel active">
  <div class="card">
    <div class="card-title">Lineup Input</div>
    <textarea id="lineup" placeholder="Paste lineup here...&#10;&#10;AwayTeam @ HomeTeam&#10;Pitcher Name (Hand)&#10;1. Batter Name (Hand) POS"></textarea>
    <button class="run-btn" id="runBtn" onclick="runModel()">▶ RUN MODEL</button>
  </div>
  <div class="card" id="stepsCard" style="display:none">
    <div class="card-title">Progress</div>
    <div class="steps" id="steps"></div>
  </div>
  <div class="card" id="infoCard" style="display:none">
    <div class="card-title">Game Info</div>
    <div class="pill-row" id="pillRow"></div>
  </div>
</div>

<div id="panel-stats" class="panel">
  <div class="card">
    <div class="card-title">Statcast - 2026</div>
    <div style="font-size:10px;color:#334155;margin-bottom:4px;display:none" id="scrollHint">&lt; scroll &gt;</div>
  <div class="tbl-wrap" id="tblWrap">
      <table>
        <thead><tr>
          <th>Player</th><th>Role</th><th>Team</th>
          <th>BRL%</th><th>EV</th><th>EV50</th><th>HH%</th>
          <th>xwOBA</th><th>wOBA</th><th>GAP</th><th>SS%</th><th>OK</th>
        </tr></thead>
        <tbody id="statBody"><tr><td colspan="12" class="na" style="padding:20px;text-align:center">Run a lineup to populate</td></tr></tbody>
      </table>
    </div>
  </div>
</div>

<div id="panel-picks" class="panel">
  <div class="result-box" id="result">Run a lineup to see picks...</div>
</div>

<script>
let pollTimer=null, curJid=null, pollErrors=0;

fetch('/api/status').then(r=>r.json()).then(d=>{
  const n=d.cache_players||0;
  document.getElementById('cacheStatus').innerHTML=n>100?`<span style="color:#22c55e">${n} players</span>`:`<span style="color:#f97316">loading...</span>`;
}).catch(()=>{document.getElementById('cacheStatus').textContent='offline'});

function show(name,btn){
  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.nav-btn').forEach(b=>b.classList.remove('active'));
  document.getElementById('panel-'+name).classList.add('active');
  if(btn)btn.classList.add('active');
}

function runModel(){
  const txt=document.getElementById('lineup').value.trim();
  if(!txt)return;
  document.getElementById('runBtn').disabled=true;
  document.getElementById('result').textContent='Analyzing...';
  document.getElementById('stepsCard').style.display='';
  document.getElementById('infoCard').style.display='none';
  pollErrors=0;
  show('analyze',document.querySelector('.nav-btn'));
  fetch('/api/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({lineup:txt})})
  .then(r=>r.json())
  .then(d=>{curJid=d.jid; pollTimer=setInterval(poll,2000);})
  .catch(e=>{document.getElementById('runBtn').disabled=false; alert('Error: '+e);});
}

function poll(){
  if(!curJid)return;
  fetch('/api/poll?jid='+curJid+'&t='+Date.now())
  .then(r=>{
    if(!r.ok) throw new Error('poll '+r.status);
    return r.json();
  })
  .then(d=>{
    pollErrors=0;
    updateSteps(d.steps||[]);
    if(d.park_confirm&&Object.keys(d.park_confirm).length) showInfo(d.park_confirm,d.bullpen||{});

    // Fetch statcast separately when ready  -  small focused request
    if(d.has_statcast){
      fetch('/api/statcast?jid='+curJid+'&t='+Date.now())
      .then(r=>r.json())
      .then(s=>{
        if(s.statcast&&s.statcast.length>0){
          showStats(s.statcast);
          // Auto-switch to statcast tab when data arrives
          // but only if still on analyze tab (not if user manually switched)
          const active=document.querySelector('.panel.active');
          if(active&&active.id==='panel-analyze'){
            show('stats',document.querySelectorAll('.nav-btn')[1]);
          }
        }
      })
      .catch(()=>{});
    }

    if(d.status==='done'||d.status==='error'){
      clearInterval(pollTimer);
      document.getElementById('runBtn').disabled=false;

      if(d.status==='error'){
        document.getElementById('result').textContent='Error: '+(d.error||'unknown');
        show('picks',document.querySelectorAll('.nav-btn')[2]);
        return;
      }

      // Fetch result separately  -  can be large, isolated request
      fetch('/api/result?jid='+curJid+'&t='+Date.now())
      .then(r=>r.json())
      .then(res=>{
        document.getElementById('result').textContent=res.result||'No result';
        show('picks',document.querySelectorAll('.nav-btn')[2]);
      })
      .catch(()=>{
        document.getElementById('result').textContent='Error loading result';
        show('picks',document.querySelectorAll('.nav-btn')[2]);
      });

      fetch('/api/status').then(r=>r.json()).then(s=>{
        document.getElementById('cacheStatus').innerHTML=`<span style="color:#22c55e">${s.cache_players||0} players</span>`;
      });
    }
  })
  .catch(()=>{
    pollErrors++;
    // Retry up to 5 times before giving up
    if(pollErrors>5){
      clearInterval(pollTimer);
      document.getElementById('runBtn').disabled=false;
      document.getElementById('result').textContent='Connection error  -  try again';
    }
  });
}

function updateSteps(steps){
  document.getElementById('steps').innerHTML=steps.map(s=>{
    const icon=s.state==='done'?'OK':s.state==='active'?'*':s.state==='error'?'X':'o';
    return `<div class="step ${s.state}">${icon} ${s.label||''}</div>`;
  }).join('');
}

function showInfo(p,pen){
  const wc=(p.weather_flag||'').includes('SUPPRESSOR')||(p.weather_flag||'')==='DOME'?'warn':(p.weather_flag||'').includes('BOOST')?'good':'';
  const temp=p.temp_f?p.temp_f+'F':'N/A';
  const wind=p.wind_mph?p.wind_mph+' mph':'N/A';
  const penHtml=Object.entries(pen||{}).map(([t,dd])=>{
    const era=dd.era?dd.era.toFixed(2):'N/A';
    const pc=dd.tier==='WEAK'?'bad':dd.tier==='AVERAGE'?'warn':'good';
    return `<div class="pill ${pc}">${t}: <b>${era} [${dd.tier}]</b></div>`;
  }).join('');
  document.getElementById('pillRow').innerHTML=`
    <div class="pill">Park: <b>${p.park||'?'}</b></div>
    <div class="pill">Type: <b>${p.category||'?'}</b></div>
    <div class="pill ${wc}">Weather: <b>${temp} - ${p.weather_flag||'?'}</b></div>
    <div class="pill">Wind: <b>${wind}</b></div>${penHtml}`;
  document.getElementById('infoCard').style.display='';
}

function showStats(stats){
  try {
    const fv=(v,thr)=>{
      if(v==null||v===undefined||v==='')return`<span class="na">-</span>`;
      const n=parseFloat(v);
      if(isNaN(n))return`<span class="na">-</span>`;
      return`<span class="${n>=thr?'hit':'miss'}">${thr<1?n.toFixed(3):n.toFixed(1)}</span>`;
    };
    const fw=v=>{
      if(v==null||v===undefined||v==='')return`<span class="na">-</span>`;
      const n=parseFloat(v);
      return isNaN(n)?`<span class="na">-</span>`:n.toFixed(3);
    };
    const rows=stats.map(p=>{
      try{
        const gap=p.gap!=null&&!isNaN(p.gap)?(p.gap>=0?'+':'')+parseFloat(p.gap).toFixed(3):'-';
        const gc=p.gap==null?'na':p.gap>=0.060?'hit':p.gap<=-0.060?'hot':'';
        return`<tr class="${p.role==='PITCHER'?'pitcher-row':''}">
          <td><b>${p.name||'?'}</b></td>
          <td>${p.role||'?'}</td>
          <td>${p.team||'?'}</td>
          <td>${fv(p.barrel_pct,15)}</td>
          <td>${fv(p.exit_velocity,91)}</td>
          <td>${fv(p.ev50,100)}</td>
          <td>${fv(p.hard_hit_pct,50)}</td>
          <td>${fv(p.xwoba,0.350)}</td>
          <td>${fw(p.woba)}</td>
          <td class="${gc}">${gap}</td>
          <td>${fv(p.sweet_spot_pct,38)}</td>
          <td>${p.fetch_status==='ok'?'OK':'!'}</td>
        </tr>`;
      }catch(e){
        return`<tr><td colspan="12" style="color:#ef4444">${p.name||'?'} - render error</td></tr>`;
      }
    }).join('');
    document.getElementById('statBody').innerHTML=rows||'<tr><td colspan="12">No data</td></tr>';
  } catch(e) {
    document.getElementById('statBody').innerHTML=`<tr><td colspan="12" style="color:#ef4444">Error: ${e.message}</td></tr>`;
  }
  if(document.getElementById('panel-analyze').classList.contains('active'))
    show('stats',document.querySelectorAll('.nav-btn')[1]);
  if(window.innerWidth < 600) document.getElementById('scrollHint').style.display='block';
}
</script>
</body>
</html>"""

# ─── HTTP SERVER ─────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[{self.command}] {self.path} {args[1] if len(args)>1 else ''}")

    def _json(self, data, code=200):
        body = json.dumps(data, default=str).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(body))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate')
        self.send_header('Pragma', 'no-cache')
        self.end_headers()
        self.wfile.write(body)

    def _html(self, body):
        enc = body.encode()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', len(enc))
        self.end_headers()
        self.wfile.write(enc)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == '/':
            self._html(HTML)
        elif path == '/api/poll':
            qs = parse_qs(urlparse(self.path).query)
            jid = qs.get('jid', [None])[0]
            if not jid or jid not in jobs:
                self._json({'error': 'not found'}, 404)
                return
            with store_lock:
                job = jobs[jid]
                # Send lightweight poll  -  no statcast, no result text (those are fetched separately)
                snap = {
                    'status':       job['status'],
                    'steps':        job['steps'],
                    'park_confirm': job['park_confirm'],
                    'bullpen':      job['bullpen'],
                    'error':        job['error'],
                    'has_statcast': len(job.get('statcast', [])) > 0,
                    'has_result':   bool(job.get('result')),
                }
            self._json(snap)

        elif path == '/api/statcast':
            qs = parse_qs(urlparse(self.path).query)
            jid = qs.get('jid', [None])[0]
            if not jid or jid not in jobs:
                self._json({'error': 'not found'}, 404)
                return
            with store_lock:
                self._json({'statcast': jobs[jid].get('statcast', [])})

        elif path == '/api/result':
            qs = parse_qs(urlparse(self.path).query)
            jid = qs.get('jid', [None])[0]
            if not jid or jid not in jobs:
                self._json({'error': 'not found'}, 404)
                return
            with store_lock:
                self._json({'result': jobs[jid].get('result', '')})
        elif path == '/api/debug':
            qs = parse_qs(urlparse(self.path).query)
            name = qs.get('name', ['Elly De La Cruz'])[0]
            result = {'name_tested': name}

            # Test each leaderboard endpoint raw
            endpoints = [
                ('expected_stats_batter',
                 f'https://baseballsavant.mlb.com/leaderboard/expected_statistics?type=batter&year={CURRENT_YEAR}&position=&team=&min=1&csv=false'),
                ('statcast_batter_csv',
                 f'https://baseballsavant.mlb.com/leaderboard/statcast?type=batter&year={CURRENT_YEAR}&position=&team=&min=1&csv=true'),
                ('custom_batter_csv',
                 f'https://baseballsavant.mlb.com/leaderboard/custom?year={CURRENT_YEAR}&type=batter&filter=&min=1&selections=pa,avg_hit_speed,ev95percent,barrel_batted_rate,groundballs_percent,hard_hit_percent,csw&chart=false&csv=true'),
                ('expected_stats_pitcher',
                 f'https://baseballsavant.mlb.com/leaderboard/expected_statistics?type=pitcher&year={CURRENT_YEAR}&position=&team=&min=1&csv=false'),
            ]
            for ep_name, url in endpoints:
                raw = savant_get(url, accept_json=True, timeout=20)
                if not raw:
                    result[ep_name] = 'NO_RESPONSE'
                    continue
                raw2 = raw.strip().lstrip('\ufeff')
                result[f'{ep_name}_bytes'] = len(raw2)
                result[f'{ep_name}_starts_with'] = raw2[:80]

                # Try CSV
                import csv, io
                rows = []
                try:
                    reader = csv.DictReader(io.StringIO(raw2))
                    rows = [dict(r) for r in reader]
                except Exception:
                    pass

                if not rows:
                    # Try JSON
                    try:
                        data = json.loads(raw2)
                        rows = data if isinstance(data, list) else data.get('data', [])
                    except Exception as e:
                        result[f'{ep_name}_parse_err'] = str(e)

                result[f'{ep_name}_rows'] = len(rows)
                if rows:
                    result[f'{ep_name}_keys'] = list(rows[0].keys())[:25]
                    # Find target player
                    tgt = normalize_name(name).lower()
                    for row in rows:
                        lf = row.get('last_name, first_name', '')
                        if lf:
                            parts = lf.split(',', 1)
                            n = f"{parts[1].strip()} {parts[0].strip()}" if len(parts)==2 else lf
                        else:
                            n = (row.get('name_display_first_last') or
                                 row.get('player_name') or row.get('name') or '').strip()
                        if normalize_name(n).lower() == tgt:
                            result[f'{ep_name}_player'] = row
                            break

            # Test page scrape
            scraped = scrape_player_page(name)
            result['page_scraped'] = scraped

            self._json(result)

        elif path == '/api/status':
            import os as _os
            files = ['statcast_batters.csv','statcast_pitchers.csv',
                     'expected_batters.csv','expected_pitchers.csv']
            status = {}
            for f in files:
                path2 = _os.path.join(_csv_dir, f)
                if _os.path.exists(path2):
                    stat = _os.stat(path2)
                    import datetime
                    age_mins = (time.time() - stat.st_mtime) / 60
                    status[f] = {
                        'exists': True,
                        'bytes': stat.st_size,
                        'age_minutes': round(age_mins, 1),
                        'last_updated': datetime.datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S UTC'),
                    }
                else:
                    status[f] = {'exists': False}
            status['cache_players'] = len(_stats_cache)
            status['cache_loaded'] = _stats_loaded
            self._json(status)

        elif path == '/api/namecheck':
            # Test name lookup for a comma-separated list of players
            qs = parse_qs(urlparse(self.path).query)
            names = qs.get('names', [''])[0].split(',')
            result = {}
            cache = load_stats_cache()
            for raw_name in names:
                name = raw_name.strip()
                key = normalize_name(name).lower()
                parts = key.split()
                found = key in cache
                short = f'{parts[0]} {parts[-1]}' if len(parts)>=2 else key
                found_short = short in cache
                result[name] = {
                    'found': found or found_short,
                    'key_tried': key,
                    'short_key': short,
                    'xwoba': cache.get(key, cache.get(short, {})).get('est_woba') or
                             cache.get(key, cache.get(short, {})).get('xwoba'),
                }
            self._json({'cache_size': len(cache), 'results': result})

        elif path == '/api/rules':
            self._json({'rules': LOCKED_RULES, 'system': SYSTEM_PROMPT})
        else:
            self._json({'error': 'not found'}, 404)

    def do_POST(self):
        path = urlparse(self.path).path
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length)
        try:
            data = json.loads(body)
        except Exception:
            data = {}

        if path == '/api/start':
            raw = data.get('lineup', '').strip()
            if not raw:
                self._json({'error': 'no lineup'}, 400)
                return
            game_date = data.get('game_date')
            jid = new_job()
            sid = str(uuid.uuid4())[:8]
            t = threading.Thread(target=run_job, args=(jid, sid, raw, game_date), daemon=True)
            t.start()
            self._json({'jid': jid, 'sid': sid})
        else:
            self._json({'error': 'not found'}, 404)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

# ─── MAIN ────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print('╔════════════════════════════════════════════════╗')
    print('║     SHARP ORACLE  --  HR PROP MODEL  v3        ║')
    print('╚════════════════════════════════════════════════╝')
    print(f'  PC:    http://localhost:{PORT}')
    # Start daily CSV refresh thread
    t = threading.Thread(target=_daily_refresh_loop, daemon=True)
    t.start()
    server = HTTPServer(('0.0.0.0', PORT), Handler)
    server.serve_forever()
