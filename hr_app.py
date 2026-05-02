#!/usr/bin/env python3
"""
SHARP ORACLE — HR Prop Model
Clean architecture: lineup paste → home/away verify → weather → stats → bullpen ERA → analysis
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

BATTER GRADE (0-4 pts):
  Barrel%>=15 | xwOBA>=.350 | EV>=91 | HH%>=50 = 1pt each
  A=3.5-4/4+fav platoon | A-=4/4+same OR 3.5/4+fav | B+=situational | B=dart | C=override only

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
    "You are Marcus Cole — the sharpest MLB prop analyst in the game. 20 years breaking down Statcast before it was cool. "
    "You see things nobody else sees. You connect dots — a pitcher's GB rate against pull-heavy lefties at 48 degrees, "
    "a batter's xwOBA spike after a lineup change, a bullpen ERA that means the 6th inning is a free square. "
    "You don't chase chalk. You find the edges everyone else walks past.\n\n"
    "You think in layers: raw power metrics → platoon edge → gap signals → park physics → bullpen exposure → "
    "weather suppression → regression windows. Every pick has a reason that goes three levels deep.\n\n"
    "DATA: All stats are live-fetched 2026 season Statcast. Use ONLY the numbers provided — never substitute gut feel "
    "for actual data. GAP = xwOBA minus wOBA. Positive = COLD (regression coming, buy it). "
    "Negative = HOT (overperforming, fade HR, but hits are fine). [PROXY] = no 2026 data, handle conservatively.\n\n"
    "PROCESS: Before writing a single pick, run through the full model in your head:\n"
    "- Score every batter (Barrel%>=15, xwOBA>=.350, EV>=91, HH%>=50 = 1pt each)\n"
    "- Set pitcher gates (same thresholds, EV>=93 for pitchers)\n"
    "- Apply platoon edges, gap signals, park boost/suppress, weather\n"
    "- Check all 14 upgrades — bullpen tier, regression bombs, stack games, debut plays\n"
    "- Find the non-obvious angles: the guy batting 7th with a COLD-BUY gap nobody is pricing, "
    "the HOT-gap batter who's a monster hit play despite the HR fade, "
    "the weak bullpen that turns the 6th inning into a power slot\n\n"
    "OUTPUT FORMAT — exactly this, nothing else:\n\n"
    "TOP 5 HR BETS:\n"
    "1. [Name] ([Team]) | [odds estimate] | [2-3 sharp sentences — the real reason, not generic stats recitation]\n"
    "2-5. same format\n\n"
    "TOP 5 HIT BETS:\n"
    "1. [Name] ([Team]) | [odds estimate] | [2-3 sharp sentences]\n"
    "2-5. same format\n\n"
    "Be specific. Be sharp. Connect the dots. Find what the market is missing.\n\n"
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
        headers['Accept'] = 'application/json'
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
_stats_lock = threading.Lock()

def load_stats_cache():
    """Pull full Savant custom leaderboard (all 2026 players, min 1 PA)."""
    global _stats_cache, _stats_loaded
    with _stats_lock:
        if _stats_loaded:
            return _stats_cache
        _stats_cache = {}

    def pull(player_type):
        url = (
            f'https://baseballsavant.mlb.com/leaderboard/custom'
            f'?year={CURRENT_YEAR}&type={player_type}&filter=&min=1'
            f'&selections=pa,woba,xwoba,barrel_batted_rate,avg_hit_speed,'
            f'ev95percent,avg_best_speed,la_sweet_spot_percent,'
            f'groundballs_percent,csw'
            f'&chart=false'
        )
        raw = savant_get(url, accept_json=True, timeout=25)
        if not raw:
            return 0
        try:
            data = json.loads(raw)
            rows = data if isinstance(data, list) else data.get('data', [])
            count = 0
            for row in rows:
                name = (row.get('name_display_first_last') or
                        row.get('player_name') or row.get('name') or '').strip()
                if name:
                    key = normalize_name(name).lower()
                    _stats_cache[key] = row
                    count += 1
            return count
        except Exception:
            return 0

    b = pull('batter')
    p = pull('pitcher')
    print(f"[STATS CACHE] Batters={b} Pitchers={p} Total={len(_stats_cache)}")

    with _stats_lock:
        _stats_loaded = True
    return _stats_cache

def clear_stats_cache():
    global _stats_cache, _stats_loaded
    with _stats_lock:
        _stats_cache = {}
        _stats_loaded = False

def get_cached_stats(name):
    """Look up player by name from bulk cache."""
    cache = load_stats_cache()
    key = normalize_name(name).lower()
    if key in cache:
        return cache[key]
    # Try first+last only (handles middle names)
    parts = key.split()
    if len(parts) >= 2:
        short = f'{parts[0]} {parts[-1]}'
        if short in cache:
            return cache[short]
    return None

# ─── SAVANT PAGE SCRAPE (fallback) ────────────────────────────────────────────
def scrape_player_page(player_name):
    """
    Fallback: scrape Savant player page by name slug.
    Returns dict with EV, HH%, Barrel%, xwOBA, wOBA or None.
    """
    slug = normalize_name(player_name).lower().replace(' ', '-')
    urls = [
        f'https://baseballsavant.mlb.com/savant-player/{slug}?season={CURRENT_YEAR}',
        f'https://baseballsavant.mlb.com/savant-player/{slug}',
    ]
    html = None
    for url in urls:
        raw = savant_get(url)
        if raw and len(raw) > 5000:
            html = raw
            break
    if not html:
        return None

    m = re.search(
        r'\(2026\)\s*Avg\s*Exit\s*Vel[a-z]*:\s*([\d.]+)[,\s]*'
        r'Hard\s*Hit\s*%:\s*([\d.]+)[,\s]*'
        r'wOBA:\s*([.\d]+)[,\s]*'
        r'xwOBA:\s*([.\d]+)[,\s]*'
        r'Barrel\s*%:\s*([\d.]+)',
        html, re.I
    )
    if m:
        return {
            'exit_velocity': safe_float(m.group(1)),
            'hard_hit_pct':  safe_float(m.group(2)),
            'woba':          safe_float(m.group(3)),
            'xwoba':         safe_float(m.group(4)),
            'barrel_pct':    safe_float(m.group(5)),
        }
    return None

# ─── FETCH ONE PLAYER ─────────────────────────────────────────────────────────
def fetch_one_player(info):
    """
    Fetch stats for one player.
    1. Try bulk leaderboard cache (name lookup)
    2. Fall back to Savant page scrape
    """
    name = info.get('name', '').strip()
    result = {
        **info,
        'exit_velocity': None, 'hard_hit_pct': None,
        'barrel_pct': None, 'xwoba': None, 'woba': None,
        'gb_pct': None, 'csw_pct': None,
        'gap': None, 'fetch_status': 'not found', 'data_source': None,
    }
    if not name:
        result['fetch_status'] = 'no name'
        return result

    def g(row, *keys):
        for k in keys:
            v = row.get(k)
            if v not in (None, '', 'null', 'None', 'NaN'):
                f = safe_float(v)
                if f is not None:
                    return f
        return None

    # SOURCE 1: bulk cache
    row = get_cached_stats(name)
    if row:
        result['exit_velocity'] = sane('exit_velocity', g(row, 'avg_hit_speed'))
        result['hard_hit_pct']  = sane('hard_hit_pct',  g(row, 'ev95percent'))
        result['barrel_pct']    = sane('barrel_pct',    g(row, 'barrel_batted_rate'))
        result['xwoba']         = sane('xwoba',         g(row, 'est_woba', 'xwoba'))
        result['woba']          = sane('woba',           g(row, 'woba'))
        result['gb_pct']        = sane('gb_pct',         g(row, 'groundballs_percent', 'gb_percent'))
        result['csw_pct']       = sane('csw_pct',        g(row, 'csw', 'csw_pct'))
        result['data_source']   = 'leaderboard'
        result['fetch_status']  = 'ok'

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

def fetch_all_parallel(players, workers=12):
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(fetch_one_player, players))

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
                'flag': 'DOME', 'notes': 'Dome — weather not applicable'}

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
            period = fc['properties']['periods'][0]
            temp_f    = safe_float(period.get('temperature'))
            wind_str  = period.get('windSpeed', '0 mph')
            wind_mph  = safe_float(wind_str.split()[0]) if wind_str else None
            condition = period.get('shortForecast', 'Unknown')
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
        notes.append('Weather unavailable — neutral assumed')

    if wind_mph and wind_mph >= 15:
        notes.append(f'Wind {wind_mph}mph — check direction')

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
    Extracts: away_team, home_team, away_pitcher, home_pitcher,
              away_batters, home_batters (with hand, lineup_pos).
    Verifies home/away via MLB Stats API.
    """
    prompt = f"""Parse this MLB lineup paste. Return JSON only, no other text.

The format is: AwayTeam @ HomeTeam
The away team pitches at the away stadium, home team plays at HOME.
Away pitcher faces HOME batters. Home pitcher faces AWAY batters.

Return this exact JSON structure:
{{
  "away_team": "team name",
  "home_team": "team name",
  "away_pitcher": {{"name": "...", "hand": "R/L"}},
  "home_pitcher": {{"name": "...", "hand": "R/L"}},
  "away_batters": [{{"name": "...", "hand": "R/L/S", "lineup_pos": 1}}],
  "home_batters": [{{"name": "...", "hand": "R/L/S", "lineup_pos": 1}}],
  "park_name": "...",
  "game_date": "{game_date or ''}"
}}

Lineup:
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
    ev  = p.get('exit_velocity')
    hh  = p.get('hard_hit_pct')
    xw  = p.get('xwoba')
    brl = p.get('barrel_pct')
    gb  = p.get('gb_pct')
    csw = p.get('csw_pct')

    score = sum([
        1 if (ev  is not None and ev  >= 93.0) else 0,
        1 if (hh  is not None and hh  >= 50.0) else 0,
        1 if (xw  is not None and xw  >= 0.350) else 0,
        1 if (brl is not None and brl >= 15.0) else 0,
    ])
    gate = 'OPEN' if score <= 1 else 'HALF' if score == 2 else 'CLOSED'

    # GB% modifier — closes gate half step if elite grounder pitcher
    gb_flag = ''
    if gb is not None:
        if gb >= 55:
            gb_flag = f' GB%={gb}(ELITE-SUPPRESSOR→gate+0.5)'
            if gate == 'OPEN': gate = 'OPEN→HALF'
        elif gb >= 48:
            gb_flag = f' GB%={gb}(SOLID-GB)'
        else:
            gb_flag = f' GB%={gb}(fly-ball-prone)'

    # CSW% modifier
    csw_flag = ''
    if csw is not None:
        if csw >= 30:
            csw_flag = f' CSW%={csw}(ELITE-MISS)'
        elif csw < 25:
            csw_flag = f' CSW%={csw}(hittable)'

    pts = [
        f"EV={ev or 'N/A'}{'✓' if ev and ev>=93 else '✗'}",
        f"HH%={hh or 'N/A'}{'✓' if hh and hh>=50 else '✗'}",
        f"xwOBA={xw or 'N/A'}{'✓' if xw and xw>=0.350 else '✗'}",
        f"Brl%={brl or 'N/A'}{'✓' if brl and brl>=15 else '✗'}",
    ]
    return score, gate, ' | '.join(pts) + gb_flag + csw_flag

def compute_batter_score(b):
    brl = b.get('barrel_pct')
    ev  = b.get('exit_velocity')
    hh  = b.get('hard_hit_pct')
    xw  = b.get('xwoba')
    wo  = b.get('woba')
    gap = b.get('gap')

    # Upgrade #2: Regression Gap — xwOBA>=.420 + gap>=+.100 → HH% threshold drops to 45%
    hh_threshold = 50.0
    upgrade2_flag = ''
    if xw is not None and xw >= 0.420 and gap is not None and gap >= 0.100:
        hh_threshold = 45.0
        upgrade2_flag = ' [#2-REG-GAP:HH%→45]'

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

    # Upgrade #3: Elite Barrel — 4/4 + Barrel>=25% + positive gap
    upgrade3_flag = ''
    if score == 4 and brl is not None and brl >= 25.0 and gap is not None and gap > 0:
        upgrade3_flag = ' [#3-ELITE-BARREL:pitcher-cold-half-step]'

    # Upgrade #10: Regression Bomb — gap>=+.100 + batting 1-5
    upgrade10_flag = ''
    lineup_pos = b.get('lineup_pos', 99)
    if gap is not None and gap >= 0.100 and lineup_pos <= 5:
        upgrade10_flag = ' [#10-REG-BOMB:C-DART+400]'

    # Upgrade #11: 4/4 Override — 4/4 + 1-5 + fav platoon + gate (passed by caller)
    # Upgrade #12: Elite Barrel+Park — Barrel>=20% + booster park + fav platoon + gate
    # These require park/gate context — flagged in build_context instead

    # Upgrade #14: Elite Profile Park — Barrel>=20% + xwOBA>=.400 + booster + 1-5
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
    upgrade_flags = upgrade2_flag + upgrade3_flag + upgrade10_flag + upgrade14_flag
    return score, ' | '.join(pts) + upgrade_flags, gap_flag, hr_cap

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
        f"  HOME team: {home} — plays at {park_name}",
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
            flag = ' ⚠ WEAK PEN — #1/#5 ACTIVE: all Barrel>=15+xwOBA>=.350 batters facing this bullpen are live'
            weak_pen_teams.append(team)
        lines.append(f"  {team}: ERA={era_str} [{tier}]{flag}")
    if weak_pen_teams:
        lines.append(f"  BULLPEN TIER TEAMS: {weak_pen_teams} — flag ANY batter (Brl>=15+xwOBA>=.350) facing these pens")

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

        # Upgrade #4: Stack check — 3+ batters B+ or better vs same pitcher
        team_scores = []
        for b in batters:
            sc, _, _, _ = compute_batter_score(b)
            pl = compute_platoon(b.get('hand','?'), opp_hand)
            team_scores.append((sc, pl))
        b_plus_count = sum(1 for sc, pl in team_scores
                          if (sc >= 3 and pl == 'FAV') or (sc >= 3.5 and pl == 'SAME'))
        stack_flag = f'  ⚡ STACK GAME — {b_plus_count} B+ batters vs {opp_pitcher} (widen net)' if b_plus_count >= 3 else ''
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
            else:  # SAME side — drops half grade
                if score == 4:    grade = 'A-'
                elif score == 3:  grade = 'B+'
                elif score == 2:  grade = 'B'
                else:             grade = 'C'

            # Upgrade #11: 4/4 + 1-5 + fav platoon → flag C Dart
            u11 = ''
            if score == 4 and b.get('lineup_pos',99) <= 5 and platoon in ('FAV','FAV(SW)'):
                u11 = ' [#11:4/4-FAV-1-5→C-DART]'

            # Upgrade #12: Barrel>=20% + booster park + fav platoon
            u12 = ''
            brl = b.get('barrel_pct') or 0
            if brl >= 20 and park_cat == 'BOOSTER' and platoon in ('FAV','FAV(SW)'):
                u12 = ' [#12:ELITE-BARREL+BOOSTER→B-DART]'

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
        clear_stats_cache()  # fresh data every run
        parsed = parse_lineup(raw_lineup, game_date)
        home = parsed.get('home_team', '?')
        away = parsed.get('away_team', '?')
        print(f"[PARSE] away={away} @ home={home} | "
              f"home_pitcher={parsed.get('home_pitcher',{}).get('name','?')} faces {away} | "
              f"away_pitcher={parsed.get('away_pitcher',{}).get('name','?')} faces {home}")
        step_set(jid, 0, 'done', f'{away} @ {home}')

        # STEP 2: Park + weather
        step_set(jid, 1, 'active', 'Fetching park and weather...')
        park_name, park_cat = resolve_park(home)
        weather = fetch_weather(park_name)
        with store_lock:
            jobs[jid]['park_confirm'] = {
                'park': park_name, 'category': park_cat,
                'temp_f': weather['temp_f'], 'condition': weather['condition'],
                'wind_mph': weather['wind_mph'], 'weather_flag': weather['flag'],
                'notes': weather.get('notes', ''),
            }
        temp_str = f"{weather['temp_f']}F" if weather['temp_f'] else 'N/A'
        step_set(jid, 1, 'done', f'{park_name} [{park_cat}] | {temp_str} {weather["condition"]} | {weather["flag"]}')

        # STEP 3: Build player lists with correct team/pitcher assignment
        step_set(jid, 2, 'active', 'Fetching Statcast...')
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

        # Fetch stats
        pitcher_stats = fetch_all_parallel(pitcher_list, workers=2)
        batter_stats  = fetch_all_parallel(batter_list, workers=12)
        all_statcast  = pitcher_stats + batter_stats

        ok = sum(1 for x in all_statcast if x.get('fetch_status') == 'ok')
        cache_hits = sum(1 for x in all_statcast if 'leaderboard' in str(x.get('data_source','')))
        print(f"[STATS] {ok}/{len(all_statcast)} ok | {cache_hits} from leaderboard")

        with store_lock:
            jobs[jid]['statcast'] = all_statcast
        step_set(jid, 2, 'done', f'Stats: {ok}/{len(all_statcast)} fetched')

        # STEP 4: Bullpen ERA
        step_set(jid, 3, 'active', 'Fetching bullpen ERA...')
        pen_era = {}
        for team in [home, away]:
            if team and team != '?':
                data = fetch_bullpen_era(team)
                pen_era[team] = data
                era_str = f"{data['era']:.2f}" if data.get('era') else 'N/A'
                print(f"[PEN ERA] {team}: ERA={era_str} [{data['tier']}]")
        with store_lock:
            jobs[jid]['bullpen'] = pen_era
        pen_summary = ' | '.join(f"{t}={d.get('era','N/A')}" for t,d in pen_era.items())
        step_set(jid, 3, 'done', f"Pen ERA: {pen_summary}")

        # STEP 5: Analysis
        step_set(jid, 4, 'active', 'Running model analysis...')
        ctx = build_context(parsed, all_statcast, weather, park_name, park_cat, pen_era)
        analysis = call_claude(
            [{'role': 'user', 'content': ctx}],
            system=SYSTEM_PROMPT,
            max_tokens=1500
        )
        with store_lock:
            jobs[jid]['result'] = analysis
            jobs[jid]['status'] = 'done'
        step_set(jid, 4, 'done', 'Analysis complete')

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
body{background:#0a0e1a;color:#e2e8f0;font-family:'Segoe UI',sans-serif;min-height:100vh;padding:20px}
h1{text-align:center;font-size:1.8em;font-weight:800;letter-spacing:2px;color:#f7c948;margin-bottom:20px}
.card{background:#111827;border:1px solid #1e3a5f;border-radius:12px;padding:20px;margin-bottom:16px}
textarea{width:100%;background:#0d1424;color:#e2e8f0;border:1px solid #1e3a5f;border-radius:8px;
  padding:12px;font-size:14px;resize:vertical;min-height:200px;font-family:monospace}
button{background:#f7c948;color:#0a0e1a;border:none;border-radius:8px;padding:12px 24px;
  font-size:15px;font-weight:700;cursor:pointer;width:100%;margin-top:10px}
button:hover{background:#e6b800}
button:disabled{background:#333;color:#666;cursor:not-allowed}
.steps{display:flex;flex-direction:column;gap:8px;margin-top:12px}
.step{display:flex;align-items:center;gap:10px;padding:8px 12px;border-radius:8px;
  background:#0d1424;border:1px solid #1e3a5f;font-size:13px}
.step.active{border-color:#f7c948;color:#f7c948}
.step.done{border-color:#22c55e;color:#22c55e}
.step.error{border-color:#ef4444;color:#ef4444}
.dot{width:10px;height:10px;border-radius:50%;background:currentColor;flex-shrink:0}
.stat-table{width:100%;border-collapse:collapse;font-size:13px;margin-top:10px}
.stat-table th{background:#1e3a5f;padding:6px 10px;text-align:left;color:#94a3b8;font-weight:600}
.stat-table td{padding:6px 10px;border-bottom:1px solid #1e293b}
.stat-table tr:hover td{background:#1a2744}
.ok{color:#22c55e}.na{color:#64748b}
.result{background:#0d1424;border:1px solid #22c55e;border-radius:8px;padding:16px;
  white-space:pre-wrap;font-family:monospace;font-size:14px;line-height:1.6;margin-top:12px}
.park-bar{display:flex;gap:12px;flex-wrap:wrap;margin-top:8px;font-size:13px}
.park-pill{background:#1e3a5f;border-radius:20px;padding:4px 12px;color:#94a3b8}
.park-pill span{color:#f7c948;font-weight:700}
</style>
</head>
<body>
<h1>⚡ SHARP ORACLE</h1>

<div class="card">
  <textarea id="lineup" placeholder="Paste lineup here...&#10;&#10;Format: Team1 @ Team2&#10;Pitcher name...&#10;Batter list..."></textarea>
  <button id="runBtn" onclick="runModel()">RUN MODEL</button>
</div>

<div class="card" id="parkCard" style="display:none">
  <div class="park-bar" id="parkBar"></div>
</div>

<div class="card" id="stepsCard" style="display:none">
  <div class="steps" id="steps"></div>
</div>

<div class="card" id="statsCard" style="display:none">
  <table class="stat-table">
    <thead><tr>
      <th>Player</th><th>Role</th><th>BRL%</th><th>EV</th><th>HH%</th>
      <th>xwOBA</th><th>wOBA</th><th>GAP</th><th>STATUS</th>
    </tr></thead>
    <tbody id="statBody"></tbody>
  </table>
</div>

<div class="card" id="resultCard" style="display:none">
  <div class="result" id="result"></div>
</div>

<script>
let pollTimer = null;
let curJid = null;

function runModel() {
  const txt = document.getElementById('lineup').value.trim();
  if (!txt) return;
  document.getElementById('runBtn').disabled = true;
  document.getElementById('resultCard').style.display = 'none';
  document.getElementById('statsCard').style.display = 'none';
  document.getElementById('parkCard').style.display = 'none';
  document.getElementById('stepsCard').style.display = 'none';

  fetch('/api/start', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({lineup: txt})
  })
  .then(r => r.json())
  .then(d => {
    curJid = d.jid;
    document.getElementById('stepsCard').style.display = '';
    pollTimer = setInterval(poll, 1500);
  })
  .catch(e => {
    document.getElementById('runBtn').disabled = false;
    alert('Error: ' + e);
  });
}

function poll() {
  if (!curJid) return;
  fetch('/api/poll?jid=' + curJid)
  .then(r => r.json())
  .then(d => {
    updateSteps(d.steps || []);
    if (d.park_confirm && Object.keys(d.park_confirm).length > 0) {
      showPark(d.park_confirm, d.bullpen || {});
    }
    if (d.statcast && d.statcast.length > 0) {
      showStats(d.statcast);
    }
    if (d.status === 'done') {
      clearInterval(pollTimer);
      document.getElementById('result').textContent = d.result || '';
      document.getElementById('resultCard').style.display = '';
      document.getElementById('runBtn').disabled = false;
    } else if (d.status === 'error') {
      clearInterval(pollTimer);
      document.getElementById('result').textContent = 'Error: ' + (d.error || 'unknown');
      document.getElementById('resultCard').style.display = '';
      document.getElementById('runBtn').disabled = false;
    }
  })
  .catch(() => {});
}

function updateSteps(steps) {
  const c = document.getElementById('steps');
  document.getElementById('stepsCard').style.display = '';
  c.innerHTML = steps.map(s =>
    `<div class="step ${s.state}"><div class="dot"></div>${s.label || ''}</div>`
  ).join('');
}

function showPark(p, pen) {
  const bar = document.getElementById('parkBar');
  const temp = p.temp_f ? p.temp_f + '°F' : 'N/A';
  const wind = p.wind_mph ? p.wind_mph + ' mph' : 'N/A';
  let penStr = '';
  if (pen && Object.keys(pen).length > 0) {
    penStr = Object.entries(pen).map(([t,d]) => {
      const era = d.era ? d.era.toFixed(2) : 'N/A';
      return `<div class="park-pill">${t} Pen: <span>${era} [${d.tier}]</span></div>`;
    }).join('');
  }
  bar.innerHTML = `
    <div class="park-pill">Park: <span>${p.park}</span></div>
    <div class="park-pill">Cat: <span>${p.category}</span></div>
    <div class="park-pill">Temp: <span>${temp}</span></div>
    <div class="park-pill">Wind: <span>${wind}</span></div>
    <div class="park-pill">Weather: <span>${p.weather_flag}</span></div>
    ${penStr}
  `;
  document.getElementById('parkCard').style.display = '';
}

function showStats(stats) {
  const f = (v, threshold, inverse) => {
    if (v == null) return `<span class="na">N/A</span>`;
    const hi = inverse ? v <= threshold : v >= threshold;
    return `<span class="${hi ? 'ok' : ''}">${v}</span>`;
  };
  document.getElementById('statBody').innerHTML = stats.map(p => {
    const st = p.fetch_status === 'ok' ? '✓' : `⚠ ${p.fetch_status}`;
    const stCls = p.fetch_status === 'ok' ? 'ok' : 'na';
    const gap = p.gap != null ? (p.gap >= 0 ? '+' : '') + p.gap : 'N/A';
    return `<tr>
      <td>${p.name || '?'}</td>
      <td>${p.role || '?'}</td>
      <td>${f(p.barrel_pct, 15)}</td>
      <td>${f(p.exit_velocity, 91)}</td>
      <td>${f(p.hard_hit_pct, 50)}</td>
      <td>${f(p.xwoba, 0.350)}</td>
      <td>${p.woba != null ? p.woba : '<span class="na">N/A</span>'}</td>
      <td>${gap}</td>
      <td class="${stCls}">${st}</td>
    </tr>`;
  }).join('');
  document.getElementById('statsCard').style.display = '';
}
</script>
</body>
</html>"""

# ─── HTTP SERVER ─────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[{self.command}] {self.path} {args[1] if len(args)>1 else ''}")

    def _json(self, data, code=200):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(body))
        self.send_header('Access-Control-Allow-Origin', '*')
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
                snap = dict(jobs[jid])
            self._json(snap)
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
    server = HTTPServer(('0.0.0.0', PORT), Handler)
    server.serve_forever()
