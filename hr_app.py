#!/usr/bin/env python3
"""
Sharp Oracle HR Model App — v2
Run: python hr_app.py
Opens: http://localhost:5555
Phone: http://[YOUR_LOCAL_IP]:5555 (same WiFi)
"""

import json
import re
import unicodedata
import concurrent.futures
import threading
import time
import uuid
import traceback
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
import urllib.request
import urllib.error

# ─── CONFIG ───────────────────────────────────────────────────────────────────
import os
PORT   = int(os.environ.get('PORT', 5555))
APIKEY = os.environ.get('ANTHROPIC_API_KEY', '')
MODEL  = "claude-haiku-4-5-20251001"

# ─── LOCKED MODEL RULES ───────────────────────────────────────────────────────
LOCKED_RULES = """
PITCHER GATE (contact allowed, 0-4 pts):
  EV>=93 | HH%>=50 | xwOBA>=.350 | Barrel%>=15 = 1pt each
  0-1=OPEN | 2=HALF | 3-4=CLOSED

BATTER GRADE (0-4 pts):
  Barrel%>=15 | xwOBA>=.350 | EV>=93 | HH%>=50 = 1pt each
  A=3.5-4/4+fav platoon | A-=4/4+same OR 3.5/4+fav | B+=situational | B=dart | C=override only

PLATOON: LHB vs RHP=fav | RHB vs LHP=fav | Switch=fav | Same-side=drops half grade
EV THRESHOLD: 91 mph (lowered from 93 — research shows 91+ is meaningfully above MLB avg ~90mph)
EV50 FLAGS: EV50=ELITE(>=103mph) | EV50=PLUS(>=100mph) — hardest 50% EV, better power predictor
FB% FLAGS: FB%=HIGH(>=45%) strong fly ball hitter | FB%=SOLID(>=38%) | FB%=LOW-GROUNDER(<25%) HR suppressor
  EV50=ELITE + FB%=HIGH = upgrade HR grade half step (raw power converting to air)
  FB%=LOW-GROUNDER = hard HR suppressor regardless of EV/barrel metrics
xSLG FLAGS: xSLG=ELITE(>=.600)=power tier, adds to HR grade | xSLG=POWER(>=.500)=strong HR candidate
SWEET SPOT FLAGS: SS%=ELITE(>=38%)=elite launch consistency | SS%=SOLID(>=30%)=good plane
  xSLG>=.600 + 3+/4 thresholds = upgrade half step on HR grade
  SS%<20% = hard HR suppressor (hits ball on wrong plane even with power)
GAP: xwOBA-wOBA. Positive=COLD(buy). Negative=HOT(fade for HR, good for hits).
PARKS: BOOSTER=Yankee/GABP/CBP/Coors/Sutter | SUPPRESSOR=Comerica/Petco/Oracle/T-Mobile
DOMES(no weather): AmFam/Tropicana/Globe Life/Chase Field
WEATHER: >=85F=boost | <=50F=suppress | <=45F=hard suppress

#1 Bullpen: pen ERA>=5.50 or 3+IL -> Barrel>=15+xwOBA>=.350 = Bullpen Tier regardless of starter
#2 Regression Gap: xwOBA>=.420+gap>=+.100 -> HH% drops to 45%
#3 Elite Barrel: 4/4+Barrel>=25%+positive gap -> pitcher cold flag half step only
#4 Stack: 3+ same-team B+ vs same pitcher -> STACK GAME, widen net
#5 Late Bullpen: even suppressors get pen check; weak pen -> Barrel>=15+xwOBA>=.350 = valid
#10 Regression Bomb: gap>=+.100+gate+batting 1-5 -> C Dart 0.25u +400+
#11 4/4 Override: 4/4+1-5+fav platoon+gate -> C Dart
#12 Elite Barrel+Park: Barrel>=20%+booster+fav platoon+gate -> B Dart 0.5u
#13 Debut: no 2026 data -> B Dart max
#14 Elite Profile Park: Barrel>=20%+xwOBA>=.400+booster+1-5 -> C Dart, no platoon required

GB% RULES (live fetched):
  GB%>=55 = ELITE-SUPPRESSOR: close gate half step regardless of contact score
  GB%>=48 = SOLID-GB: mild suppressor, note in analysis
  GB%<40  = fly-ball-prone: mild booster for batter HR grades

CSW% RULES (live fetched):
  CSW%>=30 = ELITE-SWING-MISS: misses bats at elite rate, suppresses contact quality
  CSW%<25  = hittable: batters make contact freely, slight batter boost

DEAD ENDS: bat speed, swing length, BABIP, sprint speed, HR/FB <50 PA
NOISE RULE: below-threshold batters going deep vs elite = variance, never adjust

OUTPUT (always all 9):
1. Pitcher grades 2. Full 18-batter table 3. Park+weather 4. Upgrades #1-#14
5. Formal picks 6. Gun-to-head TOP 2 HR 7. Gun-to-head TOP 2 hits
8. Holy Grail HR parlay (3-leg, only if 2+ A/A- exist) 9. Holy Grail hit parlay (5-leg, no fillers)
"""

SYSTEM_PROMPT = (
    "Sharp Oracle. 20-year baseball scout. Direct, no filler, peer-level. "
    "Use ONLY the Statcast numbers in the data block -- never substitute training knowledge. "
    "gap=xwOBA-wOBA. Positive=cold(buy). Negative=hot(fade HR, good hits). "
    "Missing data: mark [PROXY], grade from career knowledge.\n"
    "STRICT TOKEN BUDGET -- follow exactly:\n"
    "S1 pitchers: name | gate | gap | GB%/CSW% flags | one-line note. NO letter grades.\n"
    "S2 batter table: one row per batter, NO notes column, NO explanations in table.\n"
    "S4 upgrades: one line per upgrade. Format: #1 BULLPEN: n/a | #2 REG GAP: n/a | etc.\n"
    "S5: formal picks with full reasoning.\n"
    "S6: GUN-TO-HEAD TOP 2 HR ONLY. The 2 best. No 3rd pick, no honorable mentions.\n"
    "S7: GUN-TO-HEAD TOP 2 HITS ONLY. The 2 best. No 3rd pick, no honorable mentions.\n"
    "S8: HOLY GRAIL HR PARLAY — 3 legs. ONLY build if 2+ batters qualify A or A- grade. "
    "If fewer than 2 A/A- batters exist, write: SKIP — insufficient A-grade legs.\n"
    "S9: HOLY GRAIL HIT PARLAY — 5 legs. Only cold-gap batters OR hot-gap with HIT-PICK-YES. "
    "No fillers. If fewer than 5 qualify, write fewer legs and note it.\n"
    "Each pick S6-S9: 2 sentences max. No tables.\n\n"
    + LOCKED_RULES
)

# ─── IN-MEMORY JOB STORE ──────────────────────────────────────────────────────
jobs = {}
sessions = {}
store_lock = threading.Lock()


def new_job():
    jid = str(uuid.uuid4())
    with store_lock:
        jobs[jid] = {
            'status': 'pending',
            'steps': [
                {'n': 1, 'label': 'Parse lineup with Claude', 'state': 'wait'},
                {'n': 2, 'label': 'Confirm park + fetch weather', 'state': 'wait'},
                {'n': 3, 'label': 'Fetch pitcher Statcast', 'state': 'wait'},
                {'n': 4, 'label': 'Fetch 18 batter Statcast (parallel)', 'state': 'wait'},
                {'n': 5, 'label': 'Run full model (all upgrades)', 'state': 'wait'},
            ],
            'statcast': [],
            'result': '',
            'error': '',
        }
    return jid


def step_set(jid, n, state, label=None):
    with store_lock:
        j = jobs.get(jid)
        if not j:
            return
        for s in j['steps']:
            if s['n'] == n:
                s['state'] = state
                if label:
                    s['label'] = label
                break


def get_job_snapshot(jid):
    with store_lock:
        j = jobs.get(jid)
        if not j:
            return {}
        return {
            'status': j['status'],
            'steps': [dict(s) for s in j['steps']],
            'statcast': list(j['statcast']),
            'result': j['result'],
            'error': j['error'],
            'park_confirm': j.get('park_confirm', {}),
        }


def get_session(sid):
    with store_lock:
        if sid not in sessions:
            sessions[sid] = {'messages': [], 'game_data': None, 'statcast': []}
        return sessions[sid]


# ─── UTILS ────────────────────────────────────────────────────────────────────
def normalize_name(name):
    nfkd = unicodedata.normalize('NFKD', str(name))
    return ''.join(c for c in nfkd if not unicodedata.combining(c)).strip()


def safe_float(v):
    if v is None:
        return None
    s = str(v).strip().rstrip('.')
    try:
        return float(s)
    except Exception:
        return None


# ─── STATCAST FETCH ───────────────────────────────────────────────────────────
# Strategy:
#   1. Search Savant for player_id
#   2. Pull 2026 stats from the Savant expected-stats leaderboard CSV endpoint
#      (returns clean JSON with real season-to-date numbers, not scraped HTML)
#   3. Sanity-check every number against MLB-realistic ranges before accepting it
#   4. If leaderboard fails, fall back to savant-player page with tight parsing

CURRENT_YEAR = 2026

_HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'),
    'Accept': 'application/json, text/html, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://baseballsavant.mlb.com/',
}


# ─── LEADERBOARD CACHE ────────────────────────────────────────────────────────
# Pull the full 2026 leaderboard once and cache it for the session.
# This avoids per-player search requests that Savant rate-limits aggressively.
_leaderboard_cache = {'batter': None, 'pitcher': None}
_statcast_cache = None       # batter xSLG + SS% data
_batted_ball_cache = None    # pitcher GB% data
_arsenal_cache = None        # pitcher CSW% data
_cache_lock = threading.Lock()


def clear_leaderboard_cache():
    """Clear all caches so next run pulls fresh data."""
    global _batted_ball_cache, _arsenal_cache, _statcast_cache
    with _cache_lock:
        _leaderboard_cache['batter'] = None
        _leaderboard_cache['pitcher'] = None
        _batted_ball_cache = None
        _arsenal_cache = None
        _statcast_cache = None


def _load_leaderboard(player_type='batter'):
    """Pull full 2026 leaderboard for batters or pitchers. Cached per job run."""
    with _cache_lock:
        if _leaderboard_cache[player_type] is not None:
            return _leaderboard_cache[player_type]

    url = (
        f'https://baseballsavant.mlb.com/leaderboard/expected_statistics'
        f'?type={player_type}&year={CURRENT_YEAR}&position=&team=&min=0'
    )
    raw = savant_get(url, accept_json=True)
    if not raw:
        return []

    try:
        data = json.loads(raw)
        rows = data if isinstance(data, list) else data.get('data', [])
        with _cache_lock:
            _leaderboard_cache[player_type] = rows
        return rows
    except Exception:
        return []


def _load_batted_ball_cache():
    """Pull full 2026 pitcher batted-ball leaderboard (GB%) once and cache."""
    global _batted_ball_cache
    with _cache_lock:
        if _batted_ball_cache is not None:
            return _batted_ball_cache
    url = (
        f'https://baseballsavant.mlb.com/leaderboard/batted-ball'
        f'?type=pitcher&year={CURRENT_YEAR}'
    )
    raw = savant_get(url, accept_json=True)
    if not raw:
        return []
    try:
        data = json.loads(raw)
        rows = data if isinstance(data, list) else data.get('data', [])
        with _cache_lock:
            _batted_ball_cache = rows
        return rows
    except Exception:
        return []


def _load_arsenal_cache():
    """Pull full 2026 pitcher arsenal leaderboard (CSW%) once and cache."""
    global _arsenal_cache
    with _cache_lock:
        if _arsenal_cache is not None:
            return _arsenal_cache
    url = (
        f'https://baseballsavant.mlb.com/leaderboard/pitch-arsenals'
        f'?type=pitcher&year={CURRENT_YEAR}'
    )
    raw = savant_get(url, accept_json=True)
    if not raw:
        return []
    try:
        data = json.loads(raw)
        rows = data if isinstance(data, list) else data.get('data', [])
        with _cache_lock:
            _arsenal_cache = rows
        return rows
    except Exception:
        return []


def _load_statcast_cache():
    """Pull full 2026 Statcast leaderboard (xSLG, SS%) once and cache."""
    global _statcast_cache
    with _cache_lock:
        if _statcast_cache is not None:
            return _statcast_cache
    url = (
        f'https://baseballsavant.mlb.com/leaderboard/statcast'
        f'?type=batter&year={CURRENT_YEAR}&position=&team=&min=0'
    )
    raw = savant_get(url, accept_json=True)
    if not raw:
        return []
    try:
        data = json.loads(raw)
        rows = data if isinstance(data, list) else data.get('data', [])
        with _cache_lock:
            _statcast_cache = rows
        return rows
    except Exception:
        return []


def _name_match_score(row, target):
    """Score a leaderboard row against a target name. Higher = better match."""
    target = normalize_name(target).lower()
    # Savant format varies: last_name+first_name fields OR combined name field
    first = normalize_name(str(row.get('first_name', ''))).lower()
    last  = normalize_name(str(row.get('last_name', ''))).lower()
    # Also handle combined name field
    combined = normalize_name(str(row.get('name', '') or row.get('player_name', ''))).lower()

    full1 = f"{first} {last}".strip()
    full2 = f"{last} {first}".strip()
    full3 = f"{last}, {first}".strip()

    candidates = [c for c in [full1, full2, full3, combined] if c.strip()]

    if target in candidates:
        return 100  # exact

    # Check combined name field exact match
    if combined and target == combined:
        return 100

    # Partial: count matching words against best candidate
    parts = target.split()
    best = 0
    for cand in candidates:
        score = sum(2 if p == last else 1 for p in parts if p in cand)
        best = max(best, score)
    return best


# Known player IDs for players that commonly fail name matching
KNOWN_PLAYER_IDS = {
    'jose ramirez': '608070',
    'jose ramírez': '608070',
    'christian vazquez': '477132',
    'christian vázquez': '477132',
    'j.d. martinez': '502110',
    'jd martinez': '502110',
    'michael a. taylor': '534606',
}

def lookup_player_in_leaderboard(name, player_type='batter'):
    """Find player stats directly from leaderboard by name matching.
    Also enriches with xSLG and SS% from the statcast leaderboard."""
    # Check known IDs first
    name_key = normalize_name(name).lower()
    forced_pid = KNOWN_PLAYER_IDS.get(name_key)

    rows = _load_leaderboard(player_type)
    if not rows:
        return None, None

    best_score = 0
    best_row = None
    for row in rows:
        row_pid = str(row.get('player_id') or row.get('batter') or row.get('pitcher') or '')
        # If we have a forced pid, match by ID first
        if forced_pid and row_pid == forced_pid:
            best_row = row
            best_score = 100
            break
        score = _name_match_score(row, name)
        if score > best_score:
            best_score = score
            best_row = row

    if best_score < 2 or best_row is None:
        # Last resort: use forced_pid and fetch directly
        if forced_pid:
            return forced_pid, None
        return None, None

    pid = str(best_row.get('player_id') or best_row.get('batter') or best_row.get('pitcher') or '') or forced_pid or ''
    stats = _extract_leaderboard_row(best_row)

    # Enrich with xSLG and SS% from statcast leaderboard (different endpoint)
    if player_type == 'batter' and pid:
        sc_rows = _load_statcast_cache()
        for sc_row in sc_rows:
            sc_pid = str(sc_row.get('player_id') or sc_row.get('batter') or '')
            if sc_pid == pid:
                def g(*keys):
                    for k in keys:
                        v = sc_row.get(k)
                        if v not in (None, '', 'null', 'None'):
                            f = safe_float(v)
                            if f is not None:
                                return f
                    return None
                xslg = g('xslg', 'est_slg', 'xslg_percent')
                ssp  = g('la_sweet_spot_percent', 'sweet_spot_percent', 'sweet_spot_pct')
                ev50 = g('ev50', 'ev_50', 'best_speed')
                fb   = g('fb_percent', 'flyball_percent', 'fb_pct', 'fly_ball_percent')
                if xslg is not None: stats['xslg'] = xslg
                if ssp  is not None: stats['sweet_spot_pct'] = ssp
                if ev50 is not None: stats['ev50'] = ev50
                if fb   is not None: stats['fb_pct'] = fb
                break

    return pid, stats


# Sanity ranges — if a parsed value is outside these it's a bad parse, discard it
SANE = {
    'exit_velocity':  (50.0, 120.0),   # mph
    'hard_hit_pct':   (0.0, 100.0),    # percent
    'woba':           (0.050, 0.700),   # stat value
    'xwoba':          (0.050, 0.700),
    'barrel_pct':     (0.0, 60.0),     # percent
    'xslg':           (0.050, 1.500),   # expected slugging
    'sweet_spot_pct': (0.0, 100.0),    # launch angle sweet spot %
    'gb_pct':         (0.0, 100.0),    # ground ball % (pitchers)
    'csw_pct':        (0.0, 60.0),     # called strike + whiff %
    'ev50':           (70.0, 125.0),   # hardest 50% EV — better power predictor than avg EV
    'fb_pct':         (0.0, 100.0),    # flyball rate (batters)
}


def sane(stat, val):
    """Return val if within MLB-realistic range, else None."""
    if val is None:
        return None
    lo, hi = SANE.get(stat, (None, None))
    if lo is None:
        return val
    return val if lo <= val <= hi else None


# Park → (lat, lon) for weather lookup
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
}


DOME_PARKS = {
    'American Family Field', 'Tropicana Field', 'Globe Life Field',
    'Rogers Centre', 'LoanDepot Park',
    # Chase Field, Minute Maid, Dodger Stadium all have retractable roofs
    # — fetch real weather so model can apply boost/suppress correctly
}

def fetch_weather(park_name, park_category=None):
    """
    Fetch current weather for the given park.
    Skips fetch for domes — returns DOME flag.
    Primary: NWS (api.weather.gov) — no key, US only, 2-step lookup.
    Fallback: wttr.in JSON — no key, global.
    Returns dict: {temp_f, condition, wind_mph, flag, notes}
    """
    if park_category == 'DOME' or park_name in DOME_PARKS:
        return {
            'temp_f': None, 'condition': 'Dome/Indoor', 'wind_mph': None,
            'flag': 'DOME', 'notes': 'Dome — weather not applicable'
        }

    coords = PARK_COORDS.get(park_name)
    if not coords:
        for k, v in PARK_COORDS.items():
            if any(w in park_name.lower() for w in k.lower().split() if len(w) > 4):
                coords = v
                break

    temp_f, wind_mph, condition = None, None, 'Unknown'

    # ── Attempt 1: NWS ────────────────────────────────────────────────────────
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
            period = next((p for p in fc['properties']['periods'] if p.get('isDaytime', True)),
                          fc['properties']['periods'][0])
            temp_f    = safe_float(period.get('temperature'))
            wind_str  = period.get('windSpeed', '0 mph')
            wind_mph  = safe_float(wind_str.split()[0]) if wind_str else None
            condition = period.get('shortForecast', 'Unknown')
        except Exception:
            pass  # fall through to wttr.in

    # ── Attempt 2: wttr.in ────────────────────────────────────────────────────
    if temp_f is None:
        try:
            # Use park name as location query
            loc = urllib.request.quote(park_name.replace(' ', '+'))
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

    # ── Model flag ────────────────────────────────────────────────────────────
    flag = 'NEUTRAL'
    notes = []
    if temp_f is not None:
        if temp_f <= 45:
            flag = 'HARD_SUPPRESSOR'
            notes.append(f'{temp_f}F = hard suppress (model rule: <=45F)')
        elif temp_f <= 50:
            flag = 'SUPPRESSOR'
            notes.append(f'{temp_f}F = meaningful suppress (model rule: <=50F)')
        elif temp_f >= 85:
            flag = 'BOOSTER'
            notes.append(f'{temp_f}F = mild boost (model rule: >=85F)')
        else:
            notes.append(f'{temp_f}F = neutral temp range (no model impact)')
    else:
        notes.append('Weather fetch failed — assume neutral')

    if wind_mph and wind_mph >= 15:
        notes.append(f'Wind {wind_mph} mph — note direction for HR factor')

    return {
        'temp_f':    temp_f,
        'condition': condition,
        'wind_mph':  wind_mph,
        'flag':      flag,
        'notes':     ' | '.join(notes) if notes else 'No weather impact',
    }




def savant_get(url, timeout=15, accept_json=False):
    headers = dict(_HEADERS)
    if accept_json:
        headers['Accept'] = 'application/json'
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return raw.decode('utf-8', errors='replace')
    except Exception:
        return None


def search_player_id(name):
    """Return Savant player_id for a given name, or None."""
    clean = normalize_name(name)

    def try_search(q):
        encoded = urllib.request.quote(q)
        raw = savant_get(
            f'https://baseballsavant.mlb.com/player/search-all?search={encoded}',
            accept_json=True
        )
        if not raw:
            return None
        try:
            data = json.loads(raw)
            if not isinstance(data, list) or not data:
                return None
            # Score each result by name similarity
            ql = q.lower()
            best_pid = None
            best_score = -1
            for entry in data:
                full = (entry.get('name') or
                        entry.get('name_display_first_last') or
                        entry.get('first_name', '') + ' ' + entry.get('last_name', '')).strip().lower()
                # exact match wins immediately
                if full == ql:
                    pid = entry.get('id') or entry.get('player_id') or entry.get('xba_id')
                    if pid:
                        return str(pid)
                # partial score
                score = sum(1 for part in ql.split() if part in full)
                if score > best_score:
                    pid = entry.get('id') or entry.get('player_id') or entry.get('xba_id')
                    if pid:
                        best_score = score
                        best_pid = str(pid)
            return best_pid
        except Exception:
            return None

    pid = try_search(clean)
    if pid:
        return pid

    # fallback: last name only
    parts = clean.split()
    if len(parts) > 1:
        pid = try_search(parts[-1])
    return pid


def fetch_from_leaderboard(player_id, player_type='batter'):
    """
    Pull 2026 season stats from Savant's expected-stats leaderboard endpoint.
    This is a proper data API that returns clean JSON — much more reliable than
    scraping the JS-heavy player page.
    player_type: 'batter' or 'pitcher'
    """
    # Savant leaderboard endpoint accepts player_id filter
    url = (
        f'https://baseballsavant.mlb.com/leaderboard/expected_statistics'
        f'?type={player_type}&year={CURRENT_YEAR}&position=&team=&min=1'
        f'&player_id={player_id}'
    )
    raw = savant_get(url, accept_json=True)
    if not raw:
        return None

    # Try JSON parse first (API sometimes returns JSON directly)
    try:
        data = json.loads(raw)
        if isinstance(data, list) and data:
            return _extract_leaderboard_row(data[0])
        if isinstance(data, dict) and 'data' in data:
            rows = data['data']
            if rows:
                return _extract_leaderboard_row(rows[0])
    except Exception:
        pass

    # Sometimes returns HTML — try to find embedded JSON
    m = re.search(r'var\s+\w+\s*=\s*(\[.*?\]);', raw, re.DOTALL)
    if m:
        try:
            rows = json.loads(m.group(1))
            if rows:
                return _extract_leaderboard_row(rows[0])
        except Exception:
            pass

    return None


def _extract_leaderboard_row(row):
    """Pull our 5 stats from a leaderboard JSON row."""
    def g(*keys):
        for k in keys:
            v = row.get(k)
            if v not in (None, '', 'null', 'None'):
                f = safe_float(v)
                if f is not None:
                    return f
        return None

    return {
        'exit_velocity':  g('avg_hit_speed', 'exit_velocity', 'avg_exit_velocity', 'launch_speed'),
        'hard_hit_pct':   g('hard_hit_percent', 'hard_hit_pct', 'hh_pct'),
        'woba':           g('woba', 'w_oba'),
        'xwoba':          g('xwoba', 'xw_oba', 'est_woba'),
        'barrel_pct':     g('barrel_batted_rate', 'barrel_pct', 'barrels_per_bbe_percent', 'brl_percent'),
        'xslg':           g('xslg', 'est_slg', 'xslg_percent'),
        'sweet_spot_pct': g('la_sweet_spot_percent', 'sweet_spot_percent', 'sweet_spot_pct', 'solidcontact_percent'),
        'ev50':           g('ev50', 'ev_50', 'best_speed'),
        'fb_pct':         g('fb_percent', 'flyball_percent', 'fb_pct', 'fly_ball_percent'),
    }


def fetch_from_player_page(player_id):
    """
    Fallback: scrape the savant player page.
    Looks for the stat summary line that Savant displays at the top:
      (2026) Avg Exit Velocity: X, Hard Hit %: X, wOBA: X, xwOBA: X, Barrel %: X
    This is the most reliable text pattern on the page.
    """
    html = savant_get(f'https://baseballsavant.mlb.com/savant-player/{player_id}')
    if not html:
        return None

    stats = {'exit_velocity': None, 'hard_hit_pct': None,
             'woba': None, 'xwoba': None, 'barrel_pct': None}

    # PRIMARY: the "(2026) Avg Exit Velocity: X, Hard Hit %: X, ..." summary line
    # This is what shows in the search snippet and at top of page
    year_block = re.search(
        r'\((?:2026|2025)\)\s*Avg\s*Exit\s*Vel[a-z]*:\s*([\d.]+)[,\s]*'
        r'Hard\s*Hit\s*%:\s*([\d.]+)[,\s]*'
        r'wOBA:\s*([.\d]+)[,\s]*'
        r'xwOBA:\s*([.\d]+)[,\s]*'
        r'Barrel\s*%:\s*([\d.]+)',
        html, re.I
    )
    if year_block:
        stats['exit_velocity'] = safe_float(year_block.group(1))
        stats['hard_hit_pct']  = safe_float(year_block.group(2))
        stats['woba']          = safe_float(year_block.group(3))
        stats['xwoba']         = safe_float(year_block.group(4))
        stats['barrel_pct']    = safe_float(year_block.group(5))
        if any(v is not None for v in stats.values()):
            return stats

    # SECONDARY: look for JSON blobs with the right year marker
    # Only trust blobs that contain a year:2026 or season:2026 field
    for blob in re.findall(r'(\[{.*?}\])', html, re.DOTALL):
        try:
            arr = json.loads(blob)
            if not (isinstance(arr, list) and arr and isinstance(arr[0], dict)):
                continue
            row = arr[0]
            # Verify this is 2026 data
            year_val = str(row.get('year', row.get('season', row.get('game_year', ''))))
            if year_val and year_val not in ('2026', ''):
                continue  # skip — this is old season data

            extracted = _extract_leaderboard_row(row)
            if extracted and any(v is not None for v in extracted.values()):
                return extracted
        except Exception:
            pass

    # TERTIARY: key:value JSON patterns — only if we find a 2026 anchor nearby
    has_2026 = '2026' in html
    if has_2026:
        kv_patterns = [
            ('exit_velocity', [r'"avg_hit_speed"\s*:\s*"?([\d.]+)"?']),
            ('hard_hit_pct',  [r'"hard_hit_percent"\s*:\s*"?([\d.]+)"?']),
            ('xwoba',         [r'"xwoba"\s*:\s*"?([\d.]+)"?']),
            ('woba',          [r'"woba"\s*:\s*"?([\d.]+)"?']),
            ('barrel_pct',    [r'"barrel_batted_rate"\s*:\s*"?([\d.]+)"?',
                               r'"barrels_per_bbe_percent"\s*:\s*"?([\d.]+)"?']),
        ]
        for stat, pats in kv_patterns:
            for pat in pats:
                m = re.search(pat, html, re.I)
                if m:
                    stats[stat] = safe_float(m.group(1))
                    break

    return stats if any(v is not None for v in stats.values()) else None



def fetch_pitcher_extras(player_id):
    """
    Fetch GB% and CSW% for pitchers using cached full leaderboards.
    Avoids per-pitcher requests that get rate-limited.
    """
    result = {'gb_pct': None, 'csw_pct': None}

    def g(row, *keys):
        for k in keys:
            v = row.get(k)
            if v not in (None, '', 'null', 'None'):
                f = safe_float(v)
                if f is not None:
                    return f
        return None

    # GB% from cached batted-ball leaderboard
    bb_rows = _load_batted_ball_cache()
    for row in bb_rows:
        pid = str(row.get('player_id') or row.get('pitcher') or '')
        if pid == str(player_id):
            result['gb_pct'] = g(row, 'gb_percent', 'groundball_percent', 'gb_pct', 'gb')
            break

    # CSW% from cached arsenal leaderboard
    csw_rows = _load_arsenal_cache()
    for row in csw_rows:
        pid = str(row.get('player_id') or row.get('pitcher') or '')
        if pid == str(player_id):
            result['csw_pct'] = g(row, 'csw', 'csw_pct', 'csw_percent', 'called_strike_whiff_pct')
            break

    return result

def fetch_one_player(info):
    """Full pipeline: search → leaderboard API → page fallback → sanity check."""
    result = {
        **info,
        'exit_velocity': None, 'hard_hit_pct': None,
        'woba': None, 'xwoba': None, 'barrel_pct': None,
        'xslg': None, 'sweet_spot_pct': None, 'ev50': None, 'fb_pct': None,
        'gb_pct': None, 'csw_pct': None,
        'gap': None, 'player_id': None,
        'fetch_status': 'not found',
        'data_source': None,
    }
    name = info.get('name', '')
    if not name.strip():
        result['fetch_status'] = 'no name'
        return result

    ptype = 'pitcher' if info.get('role') == 'PITCHER' else 'batter'

    # Strategy 1: Full leaderboard name match (avoids per-player search rate limits)
    lb_pid, raw_stats = lookup_player_in_leaderboard(name, ptype)
    source = 'leaderboard-cache'
    pid = None

    if lb_pid:
        result['player_id'] = lb_pid
        pid = lb_pid
    else:
        # Strategy 2: Per-player search fallback
        pid = search_player_id(name)
        if pid:
            result['player_id'] = pid
            raw_stats = fetch_from_leaderboard(pid, ptype)
            source = 'leaderboard'

    # Strategy 3: Player page fallback
    if not raw_stats or not any(v is not None for v in (raw_stats or {}).values()):
        if pid:
            raw_stats = fetch_from_player_page(pid)
            source = 'player-page'
        else:
            result['fetch_status'] = 'id not found'
            return result

    if not raw_stats:
        result['fetch_status'] = 'found/no stats'
        return result

    # Sanity-check every stat — discard anything outside MLB-realistic range
    bad = []
    for stat, val in raw_stats.items():
        checked = sane(stat, val)
        if val is not None and checked is None:
            bad.append(f'{stat}={val}(INSANE)')
        result[stat] = checked

    has_data = any(result[k] is not None for k in ['exit_velocity', 'xwoba', 'barrel_pct'])
    if has_data:
        result['fetch_status'] = 'ok'
        result['data_source'] = source + (f' [DISCARDED: {",".join(bad)}]' if bad else '')
    else:
        result['fetch_status'] = 'found/no valid stats'
        result['data_source'] = source

    if result['xwoba'] is not None and result['woba'] is not None:
        result['gap'] = round(result['xwoba'] - result['woba'], 3)

    # For pitchers, fetch GB% and CSW% from separate endpoints
    if info.get('role') == 'PITCHER' and pid:
        extras = fetch_pitcher_extras(pid)
        for k, v in extras.items():
            checked = sane(k, v)
            if checked is not None:
                result[k] = checked

    return result


def fetch_all_parallel(players, workers=12):
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        fmap = {ex.submit(fetch_one_player, p): p for p in players}
        out = []
        for f in concurrent.futures.as_completed(fmap):
            try:
                out.append(f.result())
            except Exception as exc:
                p = fmap[f]
                out.append({
                    **p,
                    'fetch_status': f'error: {exc}',
                    'exit_velocity': None, 'hard_hit_pct': None,
                    'woba': None, 'xwoba': None, 'barrel_pct': None,
                    'gap': None, 'data_source': 'error',
                })
    return out


# ─── CLAUDE API ───────────────────────────────────────────────────────────────
def call_claude(messages, system=None, max_tokens=4096):
    payload = {'model': MODEL, 'max_tokens': max_tokens, 'messages': messages}
    if system:
        payload['system'] = system
    body = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        'https://api.anthropic.com/v1/messages',
        data=body,
        headers={
            'x-api-key': APIKEY,
            'anthropic-version': '2023-06-01',
            'content-type': 'application/json',
        },
        method='POST',
    )
    with urllib.request.urlopen(req, timeout=180) as r:
        data = json.loads(r.read().decode('utf-8'))
    return data['content'][0]['text']



# ─── PARK LOOKUP TABLE ────────────────────────────────────────────────────────
# Definitive map: team keyword -> (park_name, category)
# category: BOOSTER / SUPPRESSOR / DOME / NEUTRAL
PARK_LOOKUP = {
    # Boosters
    'yankees':    ('Yankee Stadium', 'BOOSTER'),
    'yankee':     ('Yankee Stadium', 'BOOSTER'),
    'reds':       ('Great American Ball Park', 'BOOSTER'),
    'great american': ('Great American Ball Park', 'BOOSTER'),
    'phillies':   ('Citizens Bank Park', 'BOOSTER'),
    'citizens bank': ('Citizens Bank Park', 'BOOSTER'),
    'rockies':    ('Coors Field', 'BOOSTER'),
    'coors':      ('Coors Field', 'BOOSTER'),
    'athletics':  ('Sutter Health Park', 'BOOSTER'),
    'sutter':     ('Sutter Health Park', 'BOOSTER'),
    # Suppressors
    'tigers':     ('Comerica Park', 'SUPPRESSOR'),
    'comerica':   ('Comerica Park', 'SUPPRESSOR'),
    'padres':     ('Petco Park', 'SUPPRESSOR'),
    'petco':      ('Petco Park', 'SUPPRESSOR'),
    'giants':     ('Oracle Park', 'SUPPRESSOR'),
    'oracle':     ('Oracle Park', 'SUPPRESSOR'),
    'mariners':   ('T-Mobile Park', 'SUPPRESSOR'),
    't-mobile':   ('T-Mobile Park', 'SUPPRESSOR'),
    # Domes
    'dodgers':    ('Dodger Stadium', 'NEUTRAL'),  # retractable roof — fetch real weather
    'brewers':    ('American Family Field', 'DOME'),
    'american family': ('American Family Field', 'DOME'),
    'rays':       ('Tropicana Field', 'DOME'),
    'tropicana':  ('Tropicana Field', 'DOME'),
    'rangers':    ('Globe Life Field', 'DOME'),
    'globe life': ('Globe Life Field', 'DOME'),
    'diamondbacks': ('Chase Field', 'NEUTRAL'),  # retractable roof — fetch real weather
    'chase field': ('Chase Field', 'NEUTRAL'),  # retractable roof
    # Neutral (all others get NEUTRAL if not matched)
    'nationals':  ('Nationals Park', 'NEUTRAL'),
    'nationals park': ('Nationals Park', 'NEUTRAL'),
    'mets':       ('Citi Field', 'NEUTRAL'),
    'citi':       ('Citi Field', 'NEUTRAL'),
    'cardinals':  ('Busch Stadium', 'NEUTRAL'),
    'busch':      ('Busch Stadium', 'NEUTRAL'),
    'cubs':       ('Wrigley Field', 'NEUTRAL'),
    'wrigley':    ('Wrigley Field', 'NEUTRAL'),
    'white sox':  ('Guaranteed Rate Field', 'NEUTRAL'),
    'guaranteed rate': ('Guaranteed Rate Field', 'NEUTRAL'),
    'braves':     ('Truist Park', 'NEUTRAL'),
    'truist':     ('Truist Park', 'NEUTRAL'),
    'astros':     ('Minute Maid Park', 'NEUTRAL'),
    'minute maid': ('Minute Maid Park', 'NEUTRAL'),
    'red sox':    ('Fenway Park', 'NEUTRAL'),
    'fenway':     ('Fenway Park', 'NEUTRAL'),
    'orioles':    ('Camden Yards', 'NEUTRAL'),
    'camden':     ('Camden Yards', 'NEUTRAL'),
    'twins':      ('Target Field', 'NEUTRAL'),
    'target field': ('Target Field', 'NEUTRAL'),
    'pirates':    ('PNC Park', 'NEUTRAL'),
    'pnc':        ('PNC Park', 'NEUTRAL'),
    'royals':     ('Kauffman Stadium', 'NEUTRAL'),
    'kauffman':   ('Kauffman Stadium', 'NEUTRAL'),
    'angels':     ('Angel Stadium', 'NEUTRAL'),
    'angel':      ('Angel Stadium', 'NEUTRAL'),
    'blue jays':  ('Rogers Centre', 'DOME'),
    'rogers':     ('Rogers Centre', 'DOME'),
    'guardians':  ('Progressive Field', 'NEUTRAL'),
    'progressive': ('Progressive Field', 'NEUTRAL'),
    'marlins':    ('LoanDepot Park', 'NEUTRAL'),
    'loandepot':  ('LoanDepot Park', 'NEUTRAL'),
    'tigers':     ('Comerica Park', 'SUPPRESSOR'),
    'sf ':        ('Oracle Park', 'SUPPRESSOR'),
    'san francisco': ('Oracle Park', 'SUPPRESSOR'),
    'washington': ('Nationals Park', 'NEUTRAL'),
    'new york mets': ('Citi Field', 'NEUTRAL'),
    'new york yankees': ('Yankee Stadium', 'BOOSTER'),
}


def resolve_park(home_team_str):
    """
    Given a home team string, return (park_name, category).
    Checks the lookup table first; falls back to NEUTRAL if no match.
    """
    key = home_team_str.lower().strip()
    # direct match
    if key in PARK_LOOKUP:
        return PARK_LOOKUP[key]
    # partial match — find longest matching key
    best_k, best_v = None, None
    for k, v in PARK_LOOKUP.items():
        if k in key or key in k:
            if best_k is None or len(k) > len(best_k):
                best_k, best_v = k, v
    if best_v:
        return best_v
    return (f'{home_team_str} Park', 'NEUTRAL')


def fetch_mlb_game_today(team1_hint, team2_hint, game_date=None):
    """
    Hit the MLB Stats API to find the game on game_date between two teams.
    game_date: 'YYYY-MM-DD' string, or None for today.
    Returns dict with confirmed home_team, away_team, venue_name, or None if not found.
    """
    import datetime
    if not game_date:
        game_date = datetime.date.today().isoformat()

    url = f'https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={game_date}&hydrate=team,venue'
    req = urllib.request.Request(url, headers={'User-Agent': 'SharpOracle/1.0', 'Accept': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
    except Exception as e:
        return None

    t1 = team1_hint.lower()
    t2 = team2_hint.lower()

    for date_entry in data.get('dates', []):
        for game in date_entry.get('games', []):
            teams = game.get('teams', {})
            home = teams.get('home', {}).get('team', {})
            away = teams.get('away', {}).get('team', {})
            home_name = home.get('name', '').lower()
            away_name = away.get('name', '').lower()
            venue = game.get('venue', {}).get('name', '')

            # Check if both teams appear in this game
            h_match = any(w in home_name for w in t1.split() if len(w) > 3) or                       any(w in home_name for w in t2.split() if len(w) > 3)
            a_match = any(w in away_name for w in t1.split() if len(w) > 3) or                       any(w in away_name for w in t2.split() if len(w) > 3)

            if h_match and a_match and home_name != away_name:
                return {
                    'home_team': home.get('name', ''),
                    'away_team': away.get('name', ''),
                    'venue_name': venue,
                    'game_pk': game.get('gamePk'),
                }
    return None


def parse_lineup_claude(raw, game_date=None):
    """
    1. Claude extracts players, pitchers, hands from the raw paste.
    2. MLB Stats API confirms which team is home on game_date + the real venue.
    3. Park lookup table sets the category (BOOSTER/SUPPRESSOR/DOME/NEUTRAL).
    The paste direction doesn't matter — MLB API is ground truth.
    game_date: 'YYYY-MM-DD', or None for today.
    """
    # Step A: Claude parses players/pitchers (we don't trust its home/away call)
    prompt = (
        "Parse this baseball lineup paste. Extract ONLY the players and pitchers — "
        "do NOT worry about which team is home or away, I will verify that separately.\n\n"
        "Return a JSON object with:\n"
        "- team1: name of first team mentioned\n"
        "- team2: name of second team mentioned\n"
        "- team1_pitcher: {name, hand} (L or R)\n"
        "- team2_pitcher: {name, hand}\n"
        "- team1_batters: [{name, lineup_pos (1-9), hand (L/R/S)}]\n"
        "- team2_batters: [{name, lineup_pos (1-9), hand (L/R/S)}]\n\n"
        "Use MLB common knowledge for batter handedness. Mark '?' if truly unknown.\n"
        "Return ONLY raw JSON. No markdown fences. No explanation.\n\n"
        f"LINEUP:\n{raw}"
    )
    resp = call_claude([{'role': 'user', 'content': prompt}])
    clean = resp.strip()
    clean = re.sub(r'^```(?:json)?\s*', '', clean, flags=re.M)
    clean = re.sub(r'\s*```\s*$', '', clean, flags=re.M)
    clean = clean.strip()
    start = clean.find('{')
    end   = clean.rfind('}')
    if start != -1 and end != -1:
        clean = clean[start:end+1]
    raw_parsed = json.loads(clean)

    team1 = raw_parsed.get('team1', '')
    team2 = raw_parsed.get('team2', '')

    # Step B: MLB Stats API — ground truth for home/away and venue
    mlb_game = fetch_mlb_game_today(team1, team2, game_date=game_date)

    if mlb_game:
        home_name  = mlb_game['home_team']
        away_name  = mlb_game['away_team']
        venue_name = mlb_game['venue_name']
        # Figure out which of team1/team2 is home
        t1_lower = team1.lower()
        t2_lower = team2.lower()
        home_lower = home_name.lower()
        # Match by shared words
        t1_is_home = any(w in home_lower for w in t1_lower.split() if len(w) > 3)
        if t1_is_home:
            home_team  = team1
            away_team  = team2
            home_p     = raw_parsed.get('team1_pitcher', {})
            away_p     = raw_parsed.get('team2_pitcher', {})
            home_bats  = raw_parsed.get('team1_batters', [])
            away_bats  = raw_parsed.get('team2_batters', [])
        else:
            home_team  = team2
            away_team  = team1
            home_p     = raw_parsed.get('team2_pitcher', {})
            away_p     = raw_parsed.get('team1_pitcher', {})
            home_bats  = raw_parsed.get('team2_batters', [])
            away_bats  = raw_parsed.get('team1_batters', [])

        park_name = venue_name
        park_source = 'MLB API (confirmed)'
    else:
        # Fallback: use paste order, treat @ direction or first-mentioned as away
        # Try to detect @ direction in raw paste
        at_match = re.search(r'([A-Za-z ]+?)\s*@\s*([A-Za-z ]+?)(?:\n|$|vs)', raw)
        if at_match:
            away_team = team1
            home_team = team2
        else:
            away_team = team1
            home_team = team2
        home_p    = raw_parsed.get('team1_pitcher', {})
        away_p    = raw_parsed.get('team2_pitcher', {})
        home_bats = raw_parsed.get('team1_batters', [])
        away_bats = raw_parsed.get('team2_batters', [])
        park_name = ''
        park_source = 'FALLBACK (MLB API unavailable — verify park)'

    # Step C: Park lookup always overrides venue name for category
    park_lookup_name, category = resolve_park(home_team)
    # If MLB API gave us a venue, use it as display name but get category from lookup
    if park_name:
        # Verify the MLB venue matches the lookup — if close enough, trust MLB name
        shared = any(w in park_name.lower() for w in park_lookup_name.lower().split() if len(w) > 3)
        if not shared:
            # Mismatch — lookup wins for category, use MLB name for display but flag it
            park_source += f' [WARNING: MLB venue={park_name}, lookup={park_lookup_name}]'
    else:
        park_name = park_lookup_name

    return {
        'home_team':    home_team,
        'away_team':    away_team,
        'park_name':    park_name,
        'park_category': category,
        'park_source':  park_source,
        'home_pitcher': home_p,
        'away_pitcher': away_p,
        'home_batters': home_bats,
        'away_batters': away_bats,
    }



def compute_pitcher_gate(p):
    """Pre-compute pitcher gate score — thresholds are hard-coded, no ambiguity."""
    score = 0
    pts = []
    ev  = p.get('exit_velocity')
    hh  = p.get('hard_hit_pct')
    xw  = p.get('xwoba')
    brl = p.get('barrel_pct')

    if ev  is not None and ev  >= 93.0: score += 1; pts.append(f'EV={ev}✓')
    else: pts.append(f'EV={ev if ev is not None else "N/A"}✗')

    if hh  is not None and hh  >= 50.0: score += 1; pts.append(f'HH%={hh}✓')
    else: pts.append(f'HH%={hh if hh is not None else "N/A"}✗')

    if xw  is not None and xw  >= 0.350: score += 1; pts.append(f'xwOBA={xw}✓')
    else: pts.append(f'xwOBA={xw if xw is not None else "N/A"}✗')

    if brl is not None and brl >= 15.0: score += 1; pts.append(f'Brl%={brl}✓')
    else: pts.append(f'Brl%={brl if brl is not None else "N/A"}✗')

    if score <= 1:   gate = 'OPEN'
    elif score == 2: gate = 'HALF'
    else:            gate = 'CLOSED'

    # GB% and CSW% bonus flags — fetched live, inform gate adjustment
    gb  = p.get('gb_pct')
    csw = p.get('csw_pct')
    bonus = []
    if gb is not None:
        if gb >= 55:   bonus.append(f'GB%={gb}(ELITE-SUPPRESSOR)')
        elif gb >= 48: bonus.append(f'GB%={gb}(SOLID-GB)')
        else:          bonus.append(f'GB%={gb}(fly-ball-prone)')
    if csw is not None:
        if csw >= 30:   bonus.append(f'CSW%={csw}(ELITE-SWING-MISS)')
        elif csw >= 27: bonus.append(f'CSW%={csw}(avg-CSW)')
        else:           bonus.append(f'CSW%={csw}(hittable)')
    bonus_str = ' | '.join(bonus)

    return score, gate, ' | '.join(pts), bonus_str


def compute_batter_score(b):
    """Pre-compute batter threshold score and all flags."""
    score = 0
    pts = []
    brl = b.get('barrel_pct')
    ev  = b.get('exit_velocity')
    hh  = b.get('hard_hit_pct')
    xw  = b.get('xwoba')
    wo  = b.get('woba')
    gap = b.get('gap')

    if brl is not None and brl >= 15.0: score += 1; pts.append(f'Brl={brl}✓')
    else: pts.append(f'Brl={brl if brl is not None else "N/A"}✗')

    if xw  is not None and xw  >= 0.350: score += 1; pts.append(f'xwOBA={xw}✓')
    else: pts.append(f'xwOBA={xw if xw is not None else "N/A"}✗')

    # EV threshold lowered from 93 to 91 — research shows 91+ is meaningfully above avg
    if ev  is not None and ev  >= 91.0: score += 1; pts.append(f'EV={ev}✓')
    else: pts.append(f'EV={ev if ev is not None else "N/A"}✗')

    if hh  is not None and hh  >= 50.0: score += 1; pts.append(f'HH%={hh}✓')
    else: pts.append(f'HH%={hh if hh is not None else "N/A"}✗')

    # Gap direction — any positive = COLD, any negative = HOT
    if gap is not None:
        if gap >= 0.100:  gap_flag = 'COLD-BUY'
        elif gap > 0:     gap_flag = 'COLD'
        elif gap == 0:    gap_flag = 'NEUTRAL'
        elif gap > -0.060: gap_flag = 'HOT'
        else:             gap_flag = 'HOT-EXTREME'
    else:
        gap_flag = 'N/A'

    # xSLG power flag
    xslg = b.get('xslg')
    xslg_flag = ''
    if xslg is not None:
        if xslg >= 0.600:   xslg_flag = ' xSLG=ELITE'
        elif xslg >= 0.500: xslg_flag = ' xSLG=POWER'
        elif xslg >= 0.400: xslg_flag = ' xSLG=AVG'

    # Sweet Spot % — launch consistency
    ssp = b.get('sweet_spot_pct')
    ssp_flag = ''
    if ssp is not None:
        if ssp >= 38:   ssp_flag = ' SS%=ELITE'
        elif ssp >= 30: ssp_flag = ' SS%=SOLID'

    # EV50 — hardest 50% of batted balls, better power predictor than avg EV
    ev50 = b.get('ev50')
    ev50_flag = ''
    if ev50 is not None:
        if ev50 >= 103:  ev50_flag = ' EV50=ELITE'
        elif ev50 >= 100: ev50_flag = ' EV50=PLUS'

    # FB% — flyball rate, needed to convert hard contact into HRs
    fb = b.get('fb_pct')
    fb_flag = ''
    if fb is not None:
        if fb >= 45:   fb_flag = ' FB%=HIGH'
        elif fb >= 38: fb_flag = ' FB%=SOLID'
        elif fb < 25:  fb_flag = ' FB%=LOW-GROUNDER'

    hr_cap = ''
    if gap is not None and gap < 0:
        wo = b.get('woba')
        if gap <= -0.060:
            hit_tag = ' HIT-PICK-YES' if (wo is not None and wo >= 0.380) else ' HIT-PICK-MAYBE'
            hr_cap = f' HR-CAP-C{hit_tag}'
        else:
            hit_tag = ' HIT-PICK-YES' if (wo is not None and wo >= 0.320) else ''
            hr_cap = f' HR-CAP-B{hit_tag}'

    extra_flags = xslg_flag + ssp_flag + ev50_flag + fb_flag
    return score, ' | '.join(pts), gap_flag, hr_cap, extra_flags


def compute_platoon(batter_hand, pitcher_hand):
    """Pre-compute platoon matchup."""
    bh = str(batter_hand).upper()
    ph = str(pitcher_hand).upper()
    if bh == 'S':
        return 'FAV(switch)'
    if (bh == 'L' and ph == 'R') or (bh == 'R' and ph == 'L'):
        return 'FAV'
    return 'SAME'


def build_context_str(parsed, all_statcast):
    home = parsed.get('home_team', 'HOME')
    away = parsed.get('away_team', 'AWAY')
    park = parsed.get('park_name', '?')
    category = parsed.get('park_category', 'NEUTRAL')
    wx = parsed.get('weather', {})
    temp_str = f"{wx.get('temp_f')}F" if wx.get('temp_f') is not None else 'N/A'
    wx_flag  = wx.get('flag', 'UNKNOWN')
    wx_notes = wx.get('notes', 'No weather data')

    lines = [
        f"GAME: {away} @ {home}",
        f"PARK: {park} [{category}]",
        f"WEATHER: {temp_str} | {wx.get('condition','N/A')} | Wind {wx.get('wind_mph','N/A')} mph | FLAG: {wx_flag}",
        f"NOTE: {wx_notes}",
        '',
        'THRESHOLDS: Pitcher gate = EV>=93 | HH%>=50 | xwOBA>=.350 | Barrel%>=15 (each=1pt)',
        'THRESHOLDS: Batter score = same 4 thresholds | 0-1=OPEN 2=HALF 3-4=CLOSED',
        'GAP: xwOBA-wOBA. Positive=COLD(buy). Negative=HOT(fade HR, hit pick only).',
        '',
    ]

    # ── PITCHERS (pre-computed gate) ────────────────────────────────────────
    lines.append('=== PITCHERS (gate pre-computed by Python — use these scores exactly) ===')
    pitcher_gates = {}
    for p in all_statcast:
        if p.get('role') != 'PITCHER':
            continue
        score, gate, breakdown, bonus = compute_pitcher_gate(p)
        # Key by faces_team: this pitcher will face these batters
        faces = p.get('faces_team') or p.get('team','')
        pitcher_gates[faces] = {
            'gate': gate, 'score': score, 'hand': p.get('hand','?'),
            'name': p.get('name','?'), 'pitcher_team': p.get('team','?')
        }
        g = p.get('gap')
        gs = f"{g:+.3f}" if g is not None else 'N/A'
        gap_dir = 'COLD' if (g is not None and g > 0) else ('HOT' if (g is not None and g < 0) else 'NEUTRAL')
        proxy = '[PROXY] ' if 'not found' in str(p.get('fetch_status','')) or 'no stat' in str(p.get('fetch_status','')) else ''
        faces = p.get('faces_team') or '?'
        bonus_display = f' | {bonus}' if bonus else ''
        lines.append(
            f"  {proxy}{p.get('name','?')} ({p.get('hand','?')}HP) "
            f"[pitches for {p.get('team','?')}, FACES {faces} batters] | "
            f"GATE={score}/4={gate} | gap={gs}({gap_dir}) | {breakdown}{bonus_display}"
        )
    lines.append('')

    # ── BATTERS (pre-computed score, platoon, gap flag, HR cap) ─────────────
    proxy_count = sum(1 for b in all_statcast
                      if b.get('role') == 'BATTER'
                      and ('not found' in str(b.get('fetch_status','')) or 'no stat' in str(b.get('fetch_status',''))))
    if proxy_count > 6:
        lines.append(f"!! DATA WARNING: {proxy_count} batters on PROXY !!")
        lines.append('')

    for team in [away, home]:
        # find opposing pitcher hand
        opp_team = home if team == away else away
        # pitcher_gates is keyed by the team the pitcher FACES
        # so to find the pitcher facing 'team', look up pitcher_gates[team]
        opp_gate_info = pitcher_gates.get(team, {})
        opp_gate = opp_gate_info.get('gate', '?')
        opp_hand = opp_gate_info.get('hand', '?')
        opp_pitcher_name = opp_gate_info.get('name', '?')
        opp_pitcher_team = opp_gate_info.get('pitcher_team', opp_team)

        lines.append(f'=== {team.upper()} BATTERS vs {opp_pitcher_name} ({opp_pitcher_team}, gate={opp_gate}) ===')
        batters = [b for b in all_statcast if b.get('role') == 'BATTER' and b.get('team') == team]
        batters.sort(key=lambda x: x.get('lineup_pos', 99))

        for b in batters:
            score, breakdown, gap_flag, hr_cap, extra_flags = compute_batter_score(b)
            platoon = compute_platoon(b.get('hand','?'), opp_hand)
            g = b.get('gap')
            gs = f"{g:+.3f}" if g is not None else 'N/A'
            proxy = '[PROXY] ' if 'not found' in str(b.get('fetch_status','')) or 'no stat' in str(b.get('fetch_status','')) else ''
            xslg_str = f" xSLG={b.get('xslg','N/A')}" if b.get('xslg') is not None else ''
            ssp_str  = f" SS%={b.get('sweet_spot_pct','N/A')}" if b.get('sweet_spot_pct') is not None else ''
            ev50_str = f" EV50={b.get('ev50','N/A')}" if b.get('ev50') is not None else ''
            fb_str   = f" FB%={b.get('fb_pct','N/A')}" if b.get('fb_pct') is not None else ''
            lines.append(
                f"  #{b.get('lineup_pos','?')} {proxy}{b.get('name','?')} ({b.get('hand','?')}HB) | "
                f"SCORE={score}/4 | plat={platoon} | gap={gs}({gap_flag}){hr_cap}{extra_flags} | "
                f"wOBA={b.get('woba','N/A')}{xslg_str}{ssp_str}{ev50_str}{fb_str} | {breakdown}"
            )
        lines.append('')

    lines.append('INSTRUCTION: Use pre-computed GATE, SCORE, platoon, gap flags exactly. Do not re-compute.')
    lines.append('HR-CAP-C = max HR grade C. HR-CAP-B = max HR grade B. These are hard ceilings.')
    lines.append('HIT-PICK-YES = strong hit candidate (high wOBA, running hot). Include in hit picks and hit parlay.')
    lines.append('HIT-PICK-MAYBE = moderate hit candidate. HOT gap batters with high wOBA BELONG in hit picks.')
    lines.append('Running HOT (negative gap) = FADE for HR only. It does NOT suppress hit probability.')
    lines.append('Top hit picks should include the highest wOBA batters regardless of gap direction.')

    return '\n'.join(lines)



# ─── BACKGROUND JOB ───────────────────────────────────────────────────────────
def run_job(jid, sid, raw_lineup, game_date=None):
    with store_lock:
        jobs[jid]['status'] = 'running'
    # Clear leaderboard cache so every run gets fresh daily data
    clear_leaderboard_cache()
    try:
        # S1
        step_set(jid, 1, 'active', 'Parsing lineup with Claude...')
        parsed = parse_lineup_claude(raw_lineup, game_date=game_date)
        home = parsed.get('home_team', '?')
        away = parsed.get('away_team', '?')
        step_set(jid, 1, 'done', f'Parsed: {away} @ {home}')

        # S2 — Park confirm + weather
        park_name = parsed.get('park_name', '?')
        park_cat  = parsed.get('park_category', 'NEUTRAL')
        step_set(jid, 2, 'active', f'Confirming park: {park_name} [{park_cat}] — fetching weather...')
        weather = fetch_weather(park_name, park_category=park_cat)
        temp_str = f"{weather['temp_f']}F" if weather['temp_f'] is not None else 'N/A'
        parsed['weather'] = weather
        with store_lock:
            jobs[jid]['park_confirm'] = {
                'park': park_name,
                'category': park_cat,
                'park_source': parsed.get('park_source', ''),
                'temp_f': weather['temp_f'],
                'condition': weather['condition'],
                'wind_mph': weather['wind_mph'],
                'weather_flag': weather['flag'],
                'notes': weather.get('notes', ''),
            }
        step_set(jid, 2, 'done',
                 f'{park_name} [{park_cat}] | {temp_str} {weather["condition"]} | {weather["flag"]}')

        # S3 — pitchers
        step_set(jid, 3, 'active', 'Fetching pitcher Statcast...')
        pitcher_list = []
        hp = parsed.get('home_pitcher', {})
        ap = parsed.get('away_pitcher', {})
        if hp.get('name'):
            # home pitcher faces the AWAY batters
            pitcher_list.append({**hp, 'role': 'PITCHER', 'team': home,
                                  'faces_team': away, 'lineup_pos': 0})
        if ap.get('name'):
            # away pitcher faces the HOME batters
            pitcher_list.append({**ap, 'role': 'PITCHER', 'team': away,
                                  'faces_team': home, 'lineup_pos': 0})
        pitcher_stats = fetch_all_parallel(pitcher_list, workers=2)
        step_set(jid, 3, 'done', f'Pitchers: {len(pitcher_stats)} fetched')

        # S4 — batters
        step_set(jid, 4, 'active', 'Fetching batter Statcast in parallel...')
        batter_list = []
        for b in parsed.get('home_batters', []):
            batter_list.append({**b, 'role': 'BATTER', 'team': home})
        for b in parsed.get('away_batters', []):
            batter_list.append({**b, 'role': 'BATTER', 'team': away})
        batter_stats = fetch_all_parallel(batter_list, workers=12)
        all_statcast = pitcher_stats + batter_stats
        ok = sum(1 for x in all_statcast if x.get('fetch_status') == 'ok')
        with store_lock:
            jobs[jid]['statcast'] = all_statcast
        step_set(jid, 4, 'done', f'Statcast: {ok}/{len(all_statcast)} found')

        sess = get_session(sid)
        sess['game_data'] = parsed
        sess['statcast']  = all_statcast

        # S5 — full model
        step_set(jid, 5, 'active', 'Running model (all 14 upgrades)...')
        ctx = build_context_str(parsed, all_statcast)
        user_msg = (
            "Run the full Sharp Oracle HR model on this game.\n"
            "Follow mandatory S1-S9 order. Check every upgrade (#1-#5, #10-#14) against every batter.\n"
            "Produce all 9 required output sections.\n\n"
            + ctx
        )
        msgs = [{'role': 'user', 'content': user_msg}]
        analysis = call_claude(msgs, system=SYSTEM_PROMPT, max_tokens=4096)
        msgs.append({'role': 'assistant', 'content': analysis})
        sess['messages'] = msgs

        step_set(jid, 5, 'done', 'Analysis complete')
        with store_lock:
            jobs[jid]['result'] = analysis
            jobs[jid]['status'] = 'done'

    except Exception as exc:
        err = f"{exc}\n{traceback.format_exc()}"
        with store_lock:
            jobs[jid]['error']  = err
            jobs[jid]['status'] = 'error'
        # mark any active step as error
        for s in jobs[jid]['steps']:
            if s['state'] == 'active':
                s['state'] = 'error'


# ─── HTML ─────────────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>Sharp Oracle · HR Model</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=IBM+Plex+Mono:wght@300;400;500&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#07090c;--s1:#0d1118;--s2:#141c28;--bdr:#1c2333;
  --acc:#f0a500;--grn:#3fb950;--red:#f85149;--blu:#58a6ff;
  --mut:#4a5568;--txt:#dce6f0;--txt2:#8892a4;
  --mono:'IBM Plex Mono',monospace;
  --sans:'IBM Plex Sans',sans-serif;
  --disp:'Bebas Neue',sans-serif;
}
html,body{height:100%;background:var(--bg);color:var(--txt);font-family:var(--sans)}
body{display:flex;flex-direction:column;overflow:hidden}

/* header */
header{
  display:flex;align-items:center;gap:12px;flex-wrap:wrap;
  padding:14px 18px 12px;border-bottom:1px solid var(--bdr);flex-shrink:0;
  background:linear-gradient(180deg,rgba(240,165,0,.05) 0%,transparent 100%);
}
.logo{font-family:var(--disp);font-size:clamp(1.7rem,6vw,2.6rem);color:var(--acc);letter-spacing:3px;line-height:1}
.logo-sub{font-family:var(--mono);font-size:.58rem;color:var(--mut);letter-spacing:2px;margin-top:2px}
.live{width:7px;height:7px;background:var(--grn);border-radius:50%;animation:blink 2s infinite;margin-left:auto}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}

/* tabs */
.tabs{display:flex;border-bottom:1px solid var(--bdr);flex-shrink:0;overflow-x:auto}
.tab{
  font-family:var(--mono);font-size:.65rem;letter-spacing:1.5px;text-transform:uppercase;
  padding:10px 16px;background:none;border:none;border-bottom:2px solid transparent;
  color:var(--txt2);cursor:pointer;transition:all .15s;white-space:nowrap;
}
.tab:hover{color:var(--txt)}
.tab.on{color:var(--acc);border-bottom-color:var(--acc)}

/* main content area */
.content{flex:1;overflow:hidden;display:flex;flex-direction:column}
.pane{display:none;flex:1;overflow-y:auto;padding:14px 18px 80px;flex-direction:column;gap:12px}
.pane.on{display:flex}

/* cards */
.card{background:var(--s1);border:1px solid var(--bdr);border-radius:7px;padding:14px}
.clabel{
  font-family:var(--mono);font-size:.58rem;letter-spacing:2px;text-transform:uppercase;
  color:var(--acc);margin-bottom:9px;display:flex;align-items:center;gap:8px;
}
.clabel::after{content:'';flex:1;height:1px;background:var(--bdr)}

/* inputs */
textarea{
  width:100%;background:var(--s2);border:1px solid var(--bdr);border-radius:5px;
  color:var(--txt);font-family:var(--mono);font-size:.76rem;padding:10px 11px;
  transition:border-color .15s;line-height:1.55;
}
textarea:focus{outline:none;border-color:var(--acc)}
textarea::placeholder{color:var(--mut)}
#lineupInput{min-height:130px;resize:vertical}
#chatInput{min-height:40px;height:40px;resize:none;flex:1}

/* buttons */
.btn{
  font-family:var(--mono);font-size:.68rem;letter-spacing:1px;text-transform:uppercase;
  padding:9px 18px;border-radius:5px;border:none;cursor:pointer;transition:all .15s;
}
.btn-p{background:var(--acc);color:#000;font-weight:600}
.btn-p:hover{background:#ffb800;transform:translateY(-1px)}
.btn-p:disabled{opacity:.35;cursor:not-allowed;transform:none}
.btn-g{background:var(--s2);color:var(--txt2);border:1px solid var(--bdr)}
.btn-g:hover{border-color:var(--acc);color:var(--acc)}
.brow{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}

/* progress steps */
.steps{display:flex;flex-direction:column;gap:8px}
.step{display:flex;align-items:center;gap:9px;font-family:var(--mono);font-size:.7rem;color:var(--txt2)}
.step.done{color:var(--grn)}.step.active{color:var(--acc)}.step.error{color:var(--red)}
.sicon{
  width:20px;height:20px;border-radius:50%;background:var(--bdr);
  display:flex;align-items:center;justify-content:center;font-size:.6rem;flex-shrink:0;
}
.step.done .sicon{background:var(--grn);color:#000}
.step.active .sicon{background:var(--acc);color:#000;animation:blink .8s infinite}
.step.error .sicon{background:var(--red);color:#fff}

/* statcast table */
.twrap{overflow-x:auto;margin-top:8px}
table{width:100%;border-collapse:collapse;font-family:var(--mono);font-size:.66rem;min-width:540px}
th{color:var(--acc);text-align:left;padding:5px 8px;border-bottom:1px solid var(--bdr);letter-spacing:1px;white-space:nowrap}
td{padding:4px 8px;border-bottom:1px solid rgba(28,35,51,.6);color:var(--txt2)}
tr:hover td{background:var(--s2)}
.g{color:var(--grn);font-weight:500}.w{color:var(--acc)}.na{color:var(--mut);font-style:italic}
.ok{color:var(--grn)}.nf{color:var(--red)}

/* chat */
#chatPane{padding-bottom:0}
.chatmsgs{flex:1;overflow-y:auto;display:flex;flex-direction:column;gap:12px;padding-bottom:6px}
.msg{animation:fu .22s ease}
@keyframes fu{from{opacity:0;transform:translateY(5px)}to{opacity:1;transform:translateY(0)}}
.mlabel{font-family:var(--mono);font-size:.58rem;letter-spacing:2px;text-transform:uppercase;margin-bottom:5px}
.msg.scout .mlabel{color:var(--acc)}.msg.user .mlabel{color:var(--blu)}.msg.sys .mlabel{color:var(--mut)}
.mbody{
  background:var(--s1);border:1px solid var(--bdr);border-radius:6px;
  padding:12px 14px;font-size:.82rem;line-height:1.75;white-space:pre-wrap;word-break:break-word;
}
.msg.scout .mbody{border-left:3px solid var(--acc)}
.msg.user .mbody{border-left:3px solid var(--blu);background:rgba(88,166,255,.04);font-family:var(--mono);font-size:.74rem}
.msg.sys .mbody{color:var(--mut);font-family:var(--mono);font-size:.7rem}

/* quick actions */
.qa{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:9px}
.qb{
  font-family:var(--mono);font-size:.6rem;padding:5px 10px;background:var(--s2);
  border:1px solid var(--bdr);border-radius:4px;color:var(--txt2);cursor:pointer;transition:all .15s;
}
.qb:hover{border-color:var(--acc);color:var(--acc)}

/* chat footer */
.cfooter{
  display:none;flex;gap:8px;padding:10px 18px;border-top:1px solid var(--bdr);
  background:var(--bg);flex-shrink:0;
}
.cfooter.on{display:flex}

/* rules */
#rulesContent{
  font-family:var(--mono);font-size:.7rem;line-height:1.9;white-space:pre-wrap;
  color:var(--txt2);background:var(--s1);border:1px solid var(--bdr);
  border-radius:7px;padding:16px;
}

/* error */
.ebox{
  background:rgba(248,81,73,.1);border:1px solid rgba(248,81,73,.3);
  border-radius:6px;padding:11px;color:var(--red);font-family:var(--mono);font-size:.7rem;
}

::-webkit-scrollbar{width:4px;height:4px}
::-webkit-scrollbar-track{background:var(--s1)}
::-webkit-scrollbar-thumb{background:var(--bdr);border-radius:2px}

@media(max-width:480px){
  .btn{font-size:.62rem;padding:8px 13px}
  .pane{padding:12px 12px 70px}
  header{padding:12px}
}
</style>
</head>
<body>

<header>
  <div>
    <div class="logo">SHARP ORACLE</div>
    <div class="logo-sub">// MLB HR PROP MODEL · ALL UPGRADES LOCKED</div>
  </div>
  <div class="live"></div>
</header>

<div class="tabs">
  <button class="tab on" onclick="goTab('analyze',this)">ANALYZE</button>
  <button class="tab"    onclick="goTab('chat',this)">CHAT</button>
  <button class="tab"    onclick="goTab('rules',this)">MODEL RULES</button>
</div>

<div class="content">

  <!-- ANALYZE -->
  <div id="analyzePane" class="pane on">
    <div class="card">
      <div class="clabel">LINEUP PASTE</div>
      <textarea id="lineupInput" placeholder="Paste any lineup format -- messy, clean, app export. Include team names, pitchers, batting orders.&#10;&#10;Example:&#10;Phillies vs Mets at Citizens Bank Park&#10;PHI SP: Zack Wheeler (R) | NYM SP: Sean Manaea (L)&#10;PHI: 1. Schwarber L  2. Turner R  3. Harper L ..."></textarea>
      <div style="display:flex;align-items:center;gap:10px;margin-top:10px;flex-wrap:wrap">
        <div style="display:flex;flex-direction:column;gap:4px">
          <label style="font-family:var(--mono);font-size:.58rem;letter-spacing:1.5px;color:var(--mut);text-transform:uppercase">GAME DATE</label>
          <input type="date" id="gameDate" style="background:var(--s2);border:1px solid var(--bdr);border-radius:5px;color:var(--txt);font-family:var(--mono);font-size:.76rem;padding:7px 10px;width:160px">
        </div>
        <div style="display:flex;gap:8px;align-items:flex-end;flex:1;flex-wrap:wrap">
          <button class="btn btn-p" id="runBtn" onclick="startAnalysis()">&#9654; RUN FULL MODEL</button>
          <button class="btn btn-g" onclick="clearAll()">CLEAR</button>
        </div>
      </div>
    </div>

    <div class="card" id="progressCard" style="display:none">
      <div class="clabel">PIPELINE</div>
      <div class="steps">
        <div class="step" id="s1"><div class="sicon">1</div><span>Parse lineup with Claude</span></div>
        <div class="step" id="s2"><div class="sicon">2</div><span>Confirm park + fetch weather</span></div>
        <div class="step" id="s3"><div class="sicon">3</div><span>Fetch pitcher Statcast</span></div>
        <div class="step" id="s4"><div class="sicon">4</div><span>Fetch 18 batter Statcast (parallel)</span></div>
        <div class="step" id="s5"><div class="sicon">5</div><span>Run full model (all upgrades)</span></div>
      </div>
    </div>

    <div class="card" id="parkCard" style="display:none">
      <div class="clabel">PARK &amp; WEATHER CONFIRMED</div>
      <div id="parkInfo" style="font-family:var(--mono);font-size:.75rem;line-height:1.9;color:var(--txt2)"></div>
    </div>

    <div class="card" id="statcastCard" style="display:none">
      <div class="clabel">STATCAST PULL</div>
      <div class="twrap">
        <table>
          <thead><tr>
            <th>PLAYER</th><th>ROLE</th><th>BRL%</th><th>EV</th><th>HH%</th>
            <th>xwOBA</th><th>xSLG</th><th>SS%</th><th>EV50</th><th>FB%</th><th>wOBA</th><th>GAP</th><th>STATUS</th>
          </tr></thead>
          <tbody id="tblBody"></tbody>
        </table>
      </div>
    </div>

    <div id="errBox" class="ebox" style="display:none"></div>
  </div>

  <!-- CHAT -->
  <div id="chatPane" class="pane">
    <div class="qa">
      <button class="qb" onclick="qa('gun to head top 5 HR picks rank them')">TOP 5 HR</button>
      <button class="qb" onclick="qa('gun to head top 5 hit picks rank them')">TOP 5 HITS</button>
      <button class="qb" onclick="qa('best 3-leg HR parlay and 5-leg hit parlay')">PARLAYS</button>
      <button class="qb" onclick="qa('who has the highest HR ceiling on this slate and why')">LONGEST HR</button>
      <button class="qb" onclick="qa('re-check all upgrades 1 through 14 against every batter')">RE-CHECK UPGRADES</button>
      <button class="qb" onclick="qa('stack game check - any team with 3+ B+ batters vs same pitcher')">STACK CHECK</button>
      <button class="qb" onclick="qa('bullpen tier picks - who qualifies if pen ERA over 5.50')">BULLPEN TIER</button>
      <button class="qb" onclick="qa('regression bombs - xwOBA minus wOBA over .100 batting 1-5')">REG BOMBS</button>
    </div>
    <div class="chatmsgs" id="chatMsgs">
      <div class="msg sys">
        <div class="mlabel">SYSTEM</div>
        <div class="mbody">Run a lineup analysis first (ANALYZE tab). Then come back here for follow-ups.</div>
      </div>
    </div>
  </div>

  <!-- RULES -->
  <div id="rulesPane" class="pane">
    <div id="rulesContent">Loading...</div>
  </div>

</div>

<!-- Chat footer -->
<div class="cfooter" id="chatFooter">
  <textarea id="chatInput" placeholder="Ask a follow-up..." onkeydown="chatKey(event)" rows="1"></textarea>
  <button class="btn btn-p" id="sendBtn" onclick="sendChat()">SEND</button>
</div>

<script>
/* -- state -- */
let sessionId   = 'sess_' + Date.now();
let jobId       = null;
let pollTimer   = null;
let ready       = false;

/* -- tabs -- */
function goTab(name, el) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('on'));
  document.querySelectorAll('.pane').forEach(p => p.classList.remove('on'));
  el.classList.add('on');
  document.getElementById(name + 'Pane').classList.add('on');
  const footer = document.getElementById('chatFooter');
  footer.classList.toggle('on', name === 'chat');
}

/* -- rules -- */
// Default date picker to today
(function(){
  const d = new Date();
  const pad = n => String(n).padStart(2,'0');
  const iso = d.getFullYear() + '-' + pad(d.getMonth()+1) + '-' + pad(d.getDate());
  document.getElementById('gameDate').value = iso;
})();

fetch('/api/rules').then(r => r.json()).then(d => {
  document.getElementById('rulesContent').textContent = d.rules || 'Error loading rules.';
}).catch(() => {});

/* -- steps -- */
function setStep(n, state, label) {
  const el = document.getElementById('s' + n);
  if (!el) return;
  el.className = 'step' + (state ? ' ' + state : '');
  if (label) el.querySelector('span').textContent = label;
  const ic = el.querySelector('.sicon');
  if (state === 'done')  ic.textContent = '\u2713';
  else if (state === 'error') ic.textContent = '\u2717';
  else ic.textContent = n;
}

/* -- start -- */
async function startAnalysis() {
  const lineup = document.getElementById('lineupInput').value.trim();
  if (!lineup) { alert('Paste a lineup first.'); return; }

  document.getElementById('runBtn').disabled = true;
  document.getElementById('progressCard').style.display = 'block';
  document.getElementById('statcastCard').style.display = 'none';
  document.getElementById('errBox').style.display = 'none';
  [1,2,3,4].forEach(n => setStep(n, '', ''));

  try {
    const r = await fetch('/api/start', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({lineup, session_id: sessionId, game_date: document.getElementById('gameDate').value || ''})
    });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const d = await r.json();
    if (d.error) { showErr(d.error); document.getElementById('runBtn').disabled = false; return; }
    jobId = d.job_id;
    pollTimer = setInterval(poll, 1200);
  } catch(e) {
    showErr('Start failed: ' + e.message);
    document.getElementById('runBtn').disabled = false;
  }
}

/* -- poll -- */
async function poll() {
  if (!jobId) return;
  try {
    const r = await fetch('/api/poll', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({job_id: jobId})
    });
    if (!r.ok) return; // blip, retry
    const d = await r.json();
    if (d.error && !d.status) return;

    (d.steps || []).forEach(s => setStep(s.n, s.state, s.label));

    if (d.statcast && d.statcast.length) renderTable(d.statcast);

    // Park confirm card
    if (d.park_confirm && d.park_confirm.park) {
      const pc = d.park_confirm;
      const wxFlag = pc.weather_flag || 'UNKNOWN';
      const flagColor = wxFlag === 'BOOSTER' ? 'var(--grn)' :
                        wxFlag.includes('SUPPRESSOR') ? 'var(--red)' : 'var(--txt2)';
      const tempStr = pc.temp_f != null ? pc.temp_f + 'F' : 'N/A';
      const windStr = pc.wind_mph != null ? pc.wind_mph + ' mph wind' : '';
      document.getElementById('parkCard').style.display = 'block';
      document.getElementById('parkInfo').innerHTML =
        '<span style="color:var(--acc)">' + pc.park + '</span>' +
        ' <span style="color:var(--txt2);font-size:.65rem">[' + (pc.category||'') + ']</span>' +
        '<span style="color:var(--mut);font-size:.6rem"> · ' + (pc.park_source||'') + '</span><br>' +
        tempStr + ' · ' + (pc.condition||'') + (windStr ? ' · ' + windStr : '') + '<br>' +
        '<span style="color:' + flagColor + '">WEATHER MODEL FLAG: ' + wxFlag + '</span>' +
        (pc.notes ? '<br><span style="color:var(--mut);font-size:.65rem">' + pc.notes + '</span>' : '');
    }

    if (d.status === 'done') {
      clearInterval(pollTimer);
      document.getElementById('runBtn').disabled = false;
      ready = true;
      addMsg('scout', d.result);
      document.querySelectorAll('.tab')[1].click();
    } else if (d.status === 'error') {
      clearInterval(pollTimer);
      document.getElementById('runBtn').disabled = false;
      showErr(d.error || 'Unknown error');
    }
  } catch(e) { /* network blip */ }
}

/* -- table -- */
function renderTable(data) {
  document.getElementById('statcastCard').style.display = 'block';
  const tbody = document.getElementById('tblBody');
  tbody.innerHTML = '';
  data.forEach(p => {
    const gap  = p.gap != null ? (p.gap > 0 ? '+' + p.gap.toFixed(3) : p.gap.toFixed(3)) : '\u2014';
    const gapc = p.gap > 0.05 ? 'g' : p.gap < -0.05 ? 'w' : '';
    const proxy = (p.fetch_status || '').includes('not found') || (p.fetch_status || '').includes('no stat');
    const f = (v, thr) => v != null
      ? `<span class="${parseFloat(v) >= thr ? 'g' : ''}">${v}</span>`
      : `<span class="na">N/A</span>`;
    const src = (p.data_source || p.fetch_status || '\u2014').replace('leaderboard','API').replace('player-page','pg');
    const tr = document.createElement('tr');
    tr.innerHTML =
      `<td>${proxy ? '<span class="warn">\u26A0 </span>' : ''}${p.name||'\u2014'}</td>` +
      `<td>${p.role||'\u2014'}</td>` +
      `<td>${f(p.barrel_pct, 15)}</td>` +
      `<td>${f(p.exit_velocity, 93)}</td>` +
      `<td>${f(p.hard_hit_pct, 50)}</td>` +
      `<td>${f(p.xwoba, .350)}</td>` +
      `<td>${f(p.xslg, .500)}</td>` +
      `<td>${f(p.sweet_spot_pct, 30)}</td>` +
      `<td>${f(p.ev50, 100)}</td>` +
      `<td>${f(p.fb_pct, 38)}</td>` +
      `<td>${p.woba != null ? p.woba : '<span class="na">N/A</span>'}</td>` +
      `<td class="${gapc}">${gap}</td>` +
      `<td class="${p.fetch_status === 'ok' ? 'ok' : 'nf'}">${p.fetch_status||'\u2014'}</td>`;
    tbody.appendChild(tr);
  });
}

/* -- chat -- */
function addMsg(role, text) {
  const c = document.getElementById('chatMsgs');
  const ph = c.querySelector('.msg.sys');
  if (ph && ph.querySelector('.mbody').textContent.includes('Run a lineup')) ph.remove();
  const labels = {scout:'SHARP ORACLE', user:'YOU', sys:'SYSTEM'};
  const div = document.createElement('div');
  div.className = 'msg ' + role;
  div.innerHTML = `<div class="mlabel">${labels[role]||role.toUpperCase()}</div>`
                + `<div class="mbody">${esc(text)}</div>`;
  c.appendChild(div);
  div.scrollIntoView({behavior:'smooth', block:'end'});
}

function esc(s) {
  return String(s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function qa(text) {
  document.getElementById('chatInput').value = text;
  sendChat();
}

function chatKey(e) {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendChat(); }
}

async function sendChat() {
  if (!ready) { alert('Run a lineup analysis first.'); return; }
  const inp  = document.getElementById('chatInput');
  const text = inp.value.trim();
  if (!text) return;
  addMsg('user', text);
  inp.value = '';
  const btn = document.getElementById('sendBtn');
  btn.disabled = true; btn.textContent = '...';
  try {
    const r = await fetch('/api/chat', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({message: text, session_id: sessionId})
    });
    const d = await r.json();
    addMsg(d.response ? 'scout' : 'sys', d.response || ('Error: ' + (d.error||'unknown')));
  } catch(e) {
    addMsg('sys', 'Network error: ' + e.message);
  }
  btn.disabled = false; btn.textContent = 'SEND';
}

function showErr(msg) {
  const b = document.getElementById('errBox');
  b.style.display = 'block';
  b.textContent = '\u274C ' + msg;
}

function clearAll() {
  clearInterval(pollTimer);
  jobId = null; ready = false;
  sessionId = 'sess_' + Date.now();
  document.getElementById('lineupInput').value = '';
  document.getElementById('progressCard').style.display = 'none';
  document.getElementById('statcastCard').style.display = 'none';
  document.getElementById('errBox').style.display = 'none';
  document.getElementById('runBtn').disabled = false;
  document.getElementById('chatMsgs').innerHTML =
    '<div class="msg sys"><div class="mlabel">SYSTEM</div>'
    + '<div class="mbody">Run a lineup analysis first (ANALYZE tab). Then come back here for follow-ups.</div></div>';
  document.getElementById('parkCard').style.display='none';
  [1,2,3,4,5].forEach(n => setStep(n,'',''));
}
</script>
</body>
</html>
"""


# ─── HTTP HANDLER ─────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        ts = time.strftime('%H:%M:%S')
        print(f'  [{ts}] {self.path} {args[1]}')

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def _cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def do_GET(self):
        path = urlparse(self.path).path
        if path in ('/', '/index.html'):
            body = HTML.encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self._cors()
            self.end_headers()
            self.wfile.write(body)
        elif path == '/api/rules':
            self._json({'rules': LOCKED_RULES})
        else:
            self.send_response(404)
            self.end_headers()

    def _json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        n = int(self.headers.get('Content-Length', 0))
        raw = self.rfile.read(n) if n else b''
        try:
            return json.loads(raw.decode('utf-8'))
        except Exception:
            return {}

    def do_POST(self):
        path = urlparse(self.path).path
        body = self._body()
        if path == '/api/start':
            self._start(body)
        elif path == '/api/poll':
            self._poll(body)
        elif path == '/api/chat':
            self._chat(body)
        else:
            self.send_response(404)
            self.end_headers()

    def _start(self, body):
        lineup    = body.get('lineup', '').strip()
        sid       = body.get('session_id', 'default')
        game_date = body.get('game_date', '').strip() or None
        if not lineup:
            self._json({'error': 'No lineup provided.'}, 400)
            return
        jid = new_job()
        threading.Thread(target=run_job, args=(jid, sid, lineup, game_date), daemon=True).start()
        self._json({'job_id': jid})

    def _poll(self, body):
        jid = body.get('job_id', '')
        snap = get_job_snapshot(jid)
        if not snap:
            self._json({'error': 'Job not found'}, 404)
            return
        self._json(snap)

    def _chat(self, body):
        sid  = body.get('session_id', 'default')
        msg  = body.get('message', '').strip()
        sess = get_session(sid)
        if not sess.get('messages'):
            self._json({'error': 'No analysis loaded. Run a lineup first.'})
            return
        msgs = sess['messages'].copy()
        msgs.append({'role': 'user', 'content': msg})
        try:
            reply = call_claude(msgs, system=SYSTEM_PROMPT, max_tokens=1500)
            msgs.append({'role': 'assistant', 'content': reply})
            sess['messages'] = msgs
            self._json({'response': reply})
        except Exception as exc:
            self._json({'error': str(exc)}, 500)


# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        local_ip = s.getsockname()[0]
        s.close()
    except Exception:
        local_ip = 'localhost'

    print()
    print('  \u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557')
    print('  \u2551     SHARP ORACLE  --  HR PROP MODEL  v2           \u2551')
    print('  \u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d')
    print()
    print(f'  PC:    http://localhost:{PORT}')
    print(f'  Phone: http://{local_ip}:{PORT}')
    print()
    print('  Architecture: background thread + polling (zero SSE).')
    print('  Statcast: ThreadPoolExecutor 12 workers, all batters parallel.')
    print('  Press Ctrl+C to stop.')
    print()

    server = HTTPServer(('0.0.0.0', PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\n  Stopped.')
