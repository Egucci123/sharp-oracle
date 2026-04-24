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

# ─── PYBASEBALL SETUP ─────────────────────────────────────────────────────────
try:
    import pandas as pd
    from pybaseball import (
        statcast_batter_exitvelo_barrels,
        statcast_batter_expected_stats,
        statcast_pitcher_exitvelo_barrels,
        statcast_pitcher_expected_stats,
        playerid_lookup,
    )
    import pybaseball
    pybaseball.cache.enable()
    PYBASEBALL_OK = True
except ImportError:
    PYBASEBALL_OK = False

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


# ─── PYBASEBALL DATA LAYER ───────────────────────────────────────────────────
# Pulls full 2026 season stats via pybaseball once per run.
# Covers ALL players — no rate limiting, no blocking, correct data every time.
# Falls back to Savant scrape if pybaseball unavailable.

_pyb_batter_cache = None   # merged batter DataFrame (exitvelo + expected)
_pyb_pitcher_cache = None  # merged pitcher DataFrame
_pyb_lock = threading.Lock()


def _pull_pybaseball_data():
    """Pull all 2026 batter and pitcher data via pybaseball. Store separately, no merge."""
    global _pyb_batter_cache, _pyb_pitcher_cache
    if not PYBASEBALL_OK:
        return False
    try:
        # Store as dict of DataFrames keyed by type — no merging, look up per stat
        b_ev = statcast_batter_exitvelo_barrels(2026, minBBE=1)
        b_ev['player_id'] = b_ev['player_id'].astype(str)

        b_ex = statcast_batter_expected_stats(2026, minPA=1)
        b_ex['player_id'] = b_ex['player_id'].astype(str)

        with _pyb_lock:
            _pyb_batter_cache = {'ev': b_ev, 'ex': b_ex}

        p_ev = statcast_pitcher_exitvelo_barrels(2026, minBBE=1)
        p_ev['player_id'] = p_ev['player_id'].astype(str)

        p_ex = statcast_pitcher_expected_stats(2026, minPA=1)
        p_ex['player_id'] = p_ex['player_id'].astype(str)

        with _pyb_lock:
            _pyb_pitcher_cache = {'ev': p_ev, 'ex': p_ex}

        return True
    except Exception as e:
        print(f"[pyb] ERROR: {e}")
        import traceback; traceback.print_exc()
        return False


def _get_pyb_row(player_id, player_type='batter'):
    """Get rows from both pybaseball DataFrames for a player."""
    with _pyb_lock:
        cache = _pyb_batter_cache if player_type == 'batter' else _pyb_pitcher_cache
    if cache is None:
        return None
    pid = str(player_id)
    ev_row = None
    ex_row = None
    ev_df = cache.get('ev')
    ex_df = cache.get('ex')
    if ev_df is not None and not ev_df.empty:
        rows = ev_df[ev_df['player_id'] == pid]
        if not rows.empty:
            ev_row = rows.iloc[0]
    if ex_df is not None and not ex_df.empty:
        rows = ex_df[ex_df['player_id'] == pid]
        if not rows.empty:
            ex_row = rows.iloc[0]
    if ev_row is None and ex_row is None:
        return None
    return {'ev': ev_row, 'ex': ex_row}


def _extract_pyb_stats(rows, player_type='batter'):
    """Extract stats from separate ev/ex pybaseball rows."""
    if rows is None:
        return None

    def f(row, *cols):
        if row is None:
            return None
        for c in cols:
            if c in row.index:
                v = row[c]
                try:
                    if pd.notna(v):
                        fv = float(v)
                        if fv == fv:  # not NaN
                            return fv
                except Exception:
                    pass
        return None

    ev = rows.get('ev')
    ex = rows.get('ex')

    # CONFIRMED column names from pybaseball (verified via /api/pyb-debug):
    # exitvelo_barrels: avg_hit_speed, ev50, anglesweetspotpercent, brl_percent,
    #                   ev95percent (closest to HH%), fbld, gb
    # expected_stats:   est_woba (=xwOBA), est_slg (=xSLG), woba

    if player_type == 'batter':
        return {
            'exit_velocity':  f(ev, 'avg_hit_speed'),
            'hard_hit_pct':   f(ev, 'ev95percent'),       # balls hit 95mph+ %
            'barrel_pct':     f(ev, 'brl_percent'),
            'xwoba':          f(ex, 'est_woba'),
            'woba':           f(ex, 'woba'),
            'xslg':           f(ex, 'est_slg'),
            'sweet_spot_pct': f(ev, 'anglesweetspotpercent'),
            'ev50':           f(ev, 'ev50'),
            'fb_pct':         None,  # not in pybaseball
        }
    else:  # pitcher
        return {
            'exit_velocity':  f(ev, 'avg_hit_speed'),
            'hard_hit_pct':   f(ev, 'ev95percent'),
            'barrel_pct':     f(ev, 'brl_percent'),
            'xwoba':          f(ex, 'est_woba'),
            'woba':           f(ex, 'woba'),
            'gb_pct':         None,  # Savant fallback
            'csw_pct':        None,  # Savant fallback
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
    global _batted_ball_cache, _arsenal_cache, _statcast_cache, _pyb_batter_cache, _pyb_pitcher_cache
    with _cache_lock:
        _leaderboard_cache['batter'] = None
        _leaderboard_cache['pitcher'] = None
        _batted_ball_cache = None
        _arsenal_cache = None
        _statcast_cache = None
    with _pyb_lock:
        _pyb_batter_cache = None
        _pyb_pitcher_cache = None


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


def _load_batter_batted_ball_cache():
    """Pull full 2026 batter batted-ball leaderboard (FB%) once and cache."""
    global _batter_batted_ball_cache
    with _cache_lock:
        if _batter_batted_ball_cache is not None:
            return _batter_batted_ball_cache
    url = (
        f'https://baseballsavant.mlb.com/leaderboard/batted-ball'
        f'?type=batter&year={CURRENT_YEAR}'
    )
    raw = savant_get(url, accept_json=True)
    if not raw:
        return []
    try:
        data = json.loads(raw)
        rows = data if isinstance(data, list) else data.get('data', [])
        with _cache_lock:
            _batter_batted_ball_cache = rows
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
    # Common accent/spelling variants
    'jose ramirez': '608070',
    'jose ramírez': '608070',
    'christian vazquez': '477132',
    'christian vázquez': '477132',
    'j.d. martinez': '502110',
    'jd martinez': '502110',
    'michael a. taylor': '534606',
    # Players confirmed returning wrong data from bulk leaderboard
    'dane myers': '667472',
    'jahmai jones': '663330',
    'spencer steer': '668715',
    # Common players with name collision risk
    'elly de la cruz': '682829',   # confirmed from savant URL
    'ke bryan hayes': '663647',
    "ke'bryan hayes": '663647',
    'sal stewart': '701398',   # confirmed from savant URL
    'dillon dingler': '693307',   # confirmed from savant URL
    'tyler stephenson': '661397',
    'eugenio suarez': '553993',
    'eugenio suárez': '553993',
    'matt mclain': '680574',   # corrected ID
    'riley greene': '682985',
    'spencer torkelson': '679529',
    'gleyber torres': '650402',
    'javier baez': '595879',
    'javier báez': '595879',
    'kevin mcgonigle': '805808',   # corrected ID
    'tyler stephenson': '663886',
    'brice turang': '671218',
    'william contreras': '661388',
    'jake bauers': '664353',
    'gary sanchez': '425794',
    'gary sánchez': '425794',
    'garrett mitchell': '669060',
    'kerry carpenter': '681481',
    'wenceel perez': '676080',
    'wenceel pérez': '676080',
    'jahmai jones': '663330',
    # ── VERIFIED FROM SAVANT URLS 2026-04-24 ─────────────────────────────────
    # Confirmed from baseballsavant.mlb.com/savant-player/name-ID URLs
    'royce lewis': '668904',        # savant-player/royce-lewis-668904
    'jonny deluca': '676356',       # savant-player/jonny-deluca-676356
    'victor caratini': '605170',    # savant-player/victor-caratini-605170
    'trevor larnach': '663616',     # savant-player/trevor-larnach-663616
    'cedric mullins': '656775',     # savant-player/cedric-mullins-656775
    'matt wallner': '670242',       # savant-player/matt-wallner-670242
    'jonathan aranda': '666018',    # savant-player/jonathan-aranda-666018
    'yandy diaz': '650490',         # savant-player/yandy-diaz-650490
    'yandy díaz': '650490',
    'brooks lee': '686797',         # savant-player/brooks-lee-686797
    'drew rasmussen': '656876',     # savant-player/drew-rasmussen-656876
    'taj bradley': '671737',        # savant-player/taj-bradley-671737
    'byron buxton': '621439',       # savant-player/byron-buxton-621439
    'junior caminero': '691406',    # savant-player/junior-caminero-691406
    'brayan bello': '678394',       # savant-player/brayan-bello-678394
    'adley rutschman': '668939',    # savant-player/adley-rutschman-668939
    'wilyer abreu': '677800',       # savant-player/wilyer-abreu-677800
    'pete alonso': '624413',        # savant-player/pete-alonso-624413
    'masataka yoshida': '807799',   # savant-player/masataka-yoshida-807799
    'jarren duran': '680776',       # savant-player/jarren-duran-680776
    'ceddanne rafaela': '678882',   # savant-player/ceddanne-rafaela-678882
    'junior caminero': '691406',    # savant-player/junior-caminero-691406
    'gunnar henderson': '683002',   # savant-player/gunnar-henderson-683002
    'taylor ward': '621493',        # savant-player/taylor-ward-621493
    'leody taveras': '665750',      # savant-player/leody-taveras-665750
    'gavin williams': '668909',     # savant-player/gavin-williams-668909
    'max scherzer': '453286',       # savant-player/max-scherzer-453286
    'andres gimenez': '665926',     # savant-player/andres-gimenez-665926
    'andrés giménez': '665926',
    'vladimir guerrero jr': '665489',  # savant-player/vladimir-guerrero-jr-665489
    'vladimir guerrero jr.': '665489',
    'chase delauter': '800050',     # savant-player/chase-delauter-800050
    'jesus sanchez': '660821',      # savant-player/jesus-sanchez-660821
    'jesús sánchez': '660821',
    'bo naylor': '666310',          # savant-player/bo-naylor-666310
    'bo naylor': '666310',
    'josh naylor': '647304',        # savant-player/josh-naylor-647304
    'rhys hoskins': '543333',       # savant-player/rhys-hoskins-543333
    'willson contreras': '575929',  # savant-player/willson-contreras-575929 (BOS)
    'jonathan aranda': '666018',    # savant-player/jonathan-aranda-666018
    'taylor walls': '657757',       # confirmed from Baseball Cube
    'royce lewis': '668904',
    'trevor story': '596115',       # long-tenured player
    'pete alonso': '624413',
}


# ─── MLB PLAYER ID CACHE ──────────────────────────────────────────────────────
# Populated at first use from MLB Stats API — covers ALL active 2026 players.
# MLBAM IDs are identical to Baseball Savant player IDs.
_mlb_player_id_cache = {}
_mlb_cache_loaded = False
_mlb_cache_lock = threading.Lock()


def _load_mlb_player_cache():
    """Pull all active 2026 MLB players from Stats API and cache name→ID."""
    global _mlb_player_id_cache, _mlb_cache_loaded
    with _mlb_cache_lock:
        if _mlb_cache_loaded:
            return _mlb_player_id_cache
    url = 'https://statsapi.mlb.com/api/v1/sports/1/players?season=2026'
    req = urllib.request.Request(url, headers=_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
        people = data.get('people', [])
        cache = {}
        for p in people:
            pid = str(p.get('id', ''))
            if not pid:
                continue
            full  = normalize_name(p.get('fullName', '')).lower()
            first = normalize_name(p.get('firstName', '')).lower()
            last  = normalize_name(p.get('lastName', '')).lower()
            for key in [full, f'{first} {last}', f'{last} {first}', f'{last}, {first}']:
                if key.strip():
                    cache[key.strip()] = pid
            # Also map first-word + last for nicknames (e.g. "ken" for "kenneth")
            parts = first.split()
            if len(parts) > 1 and last:
                cache[f'{parts[0]} {last}'] = pid
        with _mlb_cache_lock:
            _mlb_player_id_cache = cache
            _mlb_cache_loaded = True
        return cache
    except Exception:
        with _mlb_cache_lock:
            _mlb_cache_loaded = True
        return {}


def get_player_id(name):
    """Return MLBAM/Savant player ID for a name.
    Priority: KNOWN_PLAYER_IDS → pybaseball playerid_lookup → MLB Stats API cache.
    """
    key = normalize_name(name).lower()

    # 1. Hardcoded overrides (highest confidence, corrected IDs)
    if key in KNOWN_PLAYER_IDS:
        return KNOWN_PLAYER_IDS[key]

    # 2. pybaseball playerid_lookup — covers every MLB player by name
    if PYBASEBALL_OK:
        try:
            parts = key.split()
            if len(parts) >= 2:
                last = parts[-1]
                first = parts[0]
                result = playerid_lookup(last, first)
                if result is not None and not result.empty:
                    # Filter to players who actually played in MLB
                    played = result[result['mlb_played_first'].notna()]
                    if played.empty:
                        played = result
                    # Pick most recent player if multiple
                    pid = str(int(played.sort_values('mlb_played_last', ascending=False).iloc[0]['key_mlbam']))
                    if pid and pid != 'nan':
                        return pid
        except Exception:
            pass

    # 3. MLB Stats API cache fallback
    cache = _load_mlb_player_cache()
    pid = cache.get(key)
    if pid:
        return pid
    parts = key.split()
    if len(parts) >= 2:
        short = f'{parts[0]} {parts[-1]}'
        pid = cache.get(short)
        if pid:
            return pid

    return None


def lookup_player_in_leaderboard(name, player_type='batter'):
    """Find player stats directly from leaderboard by name matching.
    Also enriches with xSLG and SS% from the statcast leaderboard."""
    # Use get_player_id() — checks KNOWN_PLAYER_IDS then full MLB Stats API cache
    forced_pid = get_player_id(name)

    rows = _load_leaderboard(player_type)

    best_score = 0
    best_row = None

    for row in rows:
        row_pid = str(row.get('player_id') or row.get('batter') or row.get('pitcher') or '')
        # If we have a known ID, ONLY match that exact row — ignore all name matching
        if forced_pid:
            if row_pid == forced_pid:
                best_row = row
                best_score = 100
                break
            continue
        score = _name_match_score(row, name)
        if score > best_score:
            best_score = score
            best_row = row

    if best_score < 2 or best_row is None:
        # Bulk leaderboard failed — use known PID and let fetch_one_player try individual fetch
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
                ssp  = g('sweet_spot_percent', 'la_sweet_spot_percent', 'sweet_spot_pct', 'ideal_angle_rate')
                ev50 = g('avg_best_speed', 'ev50', 'ev_50', 'best_speed')
                fb   = g('fb_percent', 'flyball_percent', 'fb_pct', 'fly_ball_percent')
                if xslg is not None: stats['xslg'] = xslg
                if ssp  is not None: stats['sweet_spot_pct'] = ssp
                if ev50 is not None: stats['ev50'] = ev50
                if fb   is not None: stats['fb_pct'] = fb
                break
        # Also pull FB% from batter batted-ball leaderboard if not found yet
        if stats.get('fb_pct') is None and pid:
            bb_rows = _load_batter_batted_ball_cache()
            for bb_row in bb_rows:
                bb_pid = str(bb_row.get('player_id') or bb_row.get('batter') or '')
                if bb_pid == pid:
                    def g3(*keys):
                        for k in keys:
                            v = bb_row.get(k)
                            if v not in (None, '', 'null', 'None'):
                                f = safe_float(v)
                                if f is not None:
                                    return f
                        return None
                    fb = g3('fb_percent', 'flyball_percent', 'fly_ball_percent', 'fb_pct')
                    if fb is not None:
                        stats['fb_pct'] = fb
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
    """Return Savant player_id for a given name, or None.
    Checks local MLB Stats API cache before hitting Savant search endpoint."""
    # Try local cache first — avoids rate-limited Savant search
    pid = get_player_id(name)
    if pid:
        return pid
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
        'xslg':           g('xslg', 'est_slg', 'xslg_percent', 'est_slugging'),
        'sweet_spot_pct': g('sweet_spot_percent', 'la_sweet_spot_percent', 'sweet_spot_pct', 'solidcontact_percent', 'ideal_angle_rate'),
        'ev50':           g('avg_best_speed', 'ev50', 'ev_50', 'best_speed', 'avg_hyper_speed', 'hyper_speed'),
        'fb_pct':         g('fb_percent', 'flyball_percent', 'fb_pct', 'fly_ball_percent', 'fly_ball_rate', 'flyballs_percent'),
    }


def fetch_from_player_page(player_id, player_name=None):
    """
    Fallback: scrape the savant player page.
    Tries name-slug URL first, then numeric ID URL.
    Looks for the stat summary line that Savant displays at the top:
      (2026) Avg Exit Velocity: X, Hard Hit %: X, wOBA: X, xwOBA: X, Barrel %: X
    This is the most reliable text pattern on the page.
    """
    # Use numeric ID only — slug URLs can resolve to wrong cached pages
    # Also try the stats=statcast URL which forces the statcast summary line
    urls = [
        f'https://baseballsavant.mlb.com/savant-player/{player_id}?stats=statcast-r-hitting-mlb&season={CURRENT_YEAR}',
        f'https://baseballsavant.mlb.com/savant-player/{player_id}',
    ]

    html = None
    for url in urls:
        html = savant_get(url)
        if html:
            break
    if not html:
        return None
    if not html:
        return None

    stats = {
        'exit_velocity': None, 'hard_hit_pct': None,
        'woba': None, 'xwoba': None, 'barrel_pct': None,
        'xslg': None, 'sweet_spot_pct': None, 'ev50': None, 'fb_pct': None,
    }

    # PRIMARY: the "(2026) Avg Exit Velocity: X, Hard Hit %: X, ..." summary line
    year_block = re.search(
        r'\(2026\)\s*Avg\s*Exit\s*Vel[a-z]*:\s*([\d.]+)[,\s]*'
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

    # SECONDARY: scan ALL JSON blobs on the page for extra stats
    # Don't filter by year — just merge any plausible stat keys found
    # The page always loads current year data first
    for blob in re.findall(r'(\[{.+?}\])', html, re.DOTALL):
        try:
            arr = json.loads(blob)
            if not (isinstance(arr, list) and arr and isinstance(arr[0], dict)):
                continue
            row = arr[0]
            # Skip if this looks like old season data
            year_val = str(row.get('year', row.get('season', row.get('game_year', ''))))
            if year_val and year_val not in ('2026', ''):
                continue
            extracted = _extract_leaderboard_row(row)
            if not extracted:
                continue
            # Merge — only fill in fields still missing
            for k, v in extracted.items():
                if v is not None and stats.get(k) is None:
                    stats[k] = v
        except Exception:
            pass

    # TERTIARY: regex key:value scan for any remaining missing fields
    kv_map = [
        ('exit_velocity',   [r'"avg_hit_speed"\s*:\s*"?([\d.]+)"?']),
        ('hard_hit_pct',    [r'"hard_hit_percent"\s*:\s*"?([\d.]+)"?']),
        ('xwoba',           [r'"xwoba"\s*:\s*"?([\d.]+)"?']),
        ('woba',            [r'"woba"\s*:\s*"?([\d.]+)"?']),
        ('barrel_pct',      [r'"barrel_batted_rate"\s*:\s*"?([\d.]+)"?',
                             r'"barrels_per_bbe_percent"\s*:\s*"?([\d.]+)"?']),
        ('xslg',            [r'"xslg"\s*:\s*"?([\d.]+)"?',
                             r'"est_slg"\s*:\s*"?([\d.]+)"?']),
        ('sweet_spot_pct',  [r'"sweet_spot_percent"\s*:\s*"?([\d.]+)"?',
                             r'"la_sweet_spot_percent"\s*:\s*"?([\d.]+)"?']),
        ('ev50',            [r'"avg_best_speed"\s*:\s*"?([\d.]+)"?']),
        ('fb_pct',          [r'"fb_percent"\s*:\s*"?([\d.]+)"?',
                             r'"flyball_percent"\s*:\s*"?([\d.]+)"?']),
    ]
    for stat, pats in kv_map:
        if stats.get(stat) is not None:
            continue
        for pat in pats:
            m = re.search(pat, html, re.I)
            if m:
                stats[stat] = safe_float(m.group(1))
                break

    return stats if any(v is not None for v in stats.values()) else None



def fetch_extended_batter_stats(player_id):
    """
    Fetch xSLG, SS%, EV50, FB% for a batter via individual Savant endpoints.
    Called when bulk leaderboard is blocked — uses per-player API calls.
    """
    result = {'xslg': None, 'sweet_spot_pct': None, 'ev50': None, 'fb_pct': None}

    def g(row, *keys):
        for k in keys:
            v = row.get(k)
            if v not in (None, '', 'null', 'None'):
                f = safe_float(v)
                if f is not None:
                    return f
        return None

    # xSLG + SS% + EV50 from expected_statistics individual endpoint
    url = (f'https://baseballsavant.mlb.com/leaderboard/expected_statistics'
           f'?type=batter&year={CURRENT_YEAR}&player_id={player_id}&min=0')
    raw = savant_get(url, accept_json=True)
    if raw:
        try:
            data = json.loads(raw)
            rows = data if isinstance(data, list) else data.get('data', [])
            if rows:
                row = rows[0]
                result['xslg']          = g(row, 'xslg', 'est_slg', 'est_slugging')
                result['sweet_spot_pct']= g(row, 'sweet_spot_percent', 'la_sweet_spot_percent', 'sweet_spot_pct')
                result['ev50']          = g(row, 'avg_best_speed', 'ev50', 'ev_50')
        except Exception:
            pass

    # FB% from batted-ball individual endpoint
    url2 = (f'https://baseballsavant.mlb.com/leaderboard/batted-ball'
            f'?type=batter&year={CURRENT_YEAR}&player_id={player_id}')
    raw2 = savant_get(url2, accept_json=True)
    if raw2:
        try:
            data2 = json.loads(raw2)
            rows2 = data2 if isinstance(data2, list) else data2.get('data', [])
            if rows2:
                row2 = rows2[0]
                result['fb_pct'] = g(row2, 'fb_percent', 'flyball_percent', 'fly_ball_percent', 'fb_pct')
        except Exception:
            pass

    return result


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
    """
    Option 2 pipeline:
    - EV, HH%, Barrel%  → Savant player page scrape (confirmed working)
    - xwOBA, xSLG, wOBA → pybaseball expected_stats (confirmed correct by MLBAM ID)
    - EV50, SS%         → pybaseball expected_stats (best effort, may be N/A)
    - FB%               → N/A (no reliable source)
    """
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
    pid = None

    # Get player ID — KNOWN_PLAYER_IDS → MLB Stats API cache → Savant search
    pid = get_player_id(name)
    if not pid:
        pid = search_player_id(name)
    if not pid:
        result['fetch_status'] = 'id not found'
        return result
    result['player_id'] = pid

    sources = []

    # ── SOURCE A: Savant player page → EV, HH%, Barrel% ─────────────────────
    # This scrapes the confirmed summary line:
    # "(2026) Avg Exit Velocity: X, Hard Hit %: X, wOBA: X, xwOBA: X, Barrel %: X"
    page_stats = fetch_from_player_page(pid, player_name=name)
    if page_stats:
        for k in ['exit_velocity', 'hard_hit_pct', 'barrel_pct']:
            v = sane(k, page_stats.get(k))
            if v is not None:
                result[k] = v
        # Also grab wOBA/xwOBA from page as fallback
        for k in ['woba', 'xwoba']:
            v = sane(k, page_stats.get(k))
            if v is not None and result[k] is None:
                result[k] = v
        sources.append('savant-page')

    # ── SOURCE B: pybaseball expected_stats → xwOBA, xSLG, wOBA, EV50, SS% ──
    # Only use expected_stats (ex) — NOT exitvelo_barrels which has wrong IDs
    if PYBASEBALL_OK:
        with _pyb_lock:
            cache = _pyb_batter_cache if ptype == 'batter' else _pyb_pitcher_cache
        if cache is not None:
            ex_df = cache.get('ex')
            if ex_df is not None and not ex_df.empty:
                rows = ex_df[ex_df['player_id'] == str(pid)]
                if not rows.empty:
                    row = rows.iloc[0]
                    def pf(*cols):
                        for c in cols:
                            if c in row.index:
                                try:
                                    v = float(row[c])
                                    if v == v:  # not NaN
                                        return v
                                except Exception:
                                    pass
                        return None
                    # xwOBA — pybaseball confirmed correct
                    xw = sane('xwoba', pf('est_woba'))
                    if xw is not None:
                        result['xwoba'] = xw
                    # wOBA — use pybaseball if page didn't get it
                    wo = sane('woba', pf('woba'))
                    if wo is not None and result['woba'] is None:
                        result['woba'] = wo
                    # xSLG
                    xs = sane('xslg', pf('est_slg'))
                    if xs is not None:
                        result['xslg'] = xs
                    sources.append('pybaseball-expected')

    if not any(result[k] is not None for k in ['exit_velocity', 'xwoba', 'barrel_pct']):
        result['fetch_status'] = 'found/no stats'
        result['data_source'] = '+'.join(sources) or 'none'
        return result

    # All stats already sanity-checked during extraction above
    result['fetch_status'] = 'ok'
    result['data_source'] = '+'.join(sources)

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
        # Match team1 to home — check words length > 2 (catches MIL, DET, etc)
        # Also try full string containment
        t1_words_match = any(w in home_lower for w in t1_lower.split() if len(w) > 2)
        t2_words_match = any(w in home_lower for w in t2_lower.split() if len(w) > 2)
        t1_is_home = t1_words_match and not t2_words_match or (
            t1_words_match and t2_words_match and
            sum(1 for w in t1_lower.split() if w in home_lower and len(w) > 2) >
            sum(1 for w in t2_lower.split() if w in home_lower and len(w) > 2)
        )
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
    # Clear all caches so every run gets fresh daily data
    clear_leaderboard_cache()
    # Pull fresh pybaseball data (all 2026 players in one shot)
    if PYBASEBALL_OK:
        _pull_pybaseball_data()
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
        elif path == '/api/pyb-debug':
            # Dumps actual pybaseball column names and a sample row
            # Hit this ONCE after deploy to see exact field names
            try:
                if not PYBASEBALL_OK:
                    self._json({'error': 'pybaseball not available'})
                    return
                result = {}
                # Pull fresh
                b_ev = statcast_batter_exitvelo_barrels(2026, minBBE=1)
                b_ex = statcast_batter_expected_stats(2026, minPA=1)
                result['exitvelo_cols'] = list(b_ev.columns)
                result['expected_cols'] = list(b_ex.columns)
                # Sample: Jose Ramirez (608070)
                b_ev['player_id'] = b_ev['player_id'].astype(str)
                b_ex['player_id'] = b_ex['player_id'].astype(str)
                ev_row = b_ev[b_ev['player_id'] == '608070']
                ex_row = b_ex[b_ex['player_id'] == '608070']
                result['exitvelo_sample'] = ev_row.iloc[0].to_dict() if not ev_row.empty else {}
                result['expected_sample'] = ex_row.iloc[0].to_dict() if not ex_row.empty else {}
                # Also check Elly De La Cruz (682829) and show first 3 rows of exitvelo
                elly_ev = b_ev[b_ev['player_id'] == '682829']
                elly_ex = b_ex[b_ex['player_id'] == '682829']
                result['elly_exitvelo'] = elly_ev.iloc[0].to_dict() if not elly_ev.empty else 'NOT FOUND'
                result['elly_expected'] = elly_ex.iloc[0].to_dict() if not elly_ex.empty else 'NOT FOUND'
                result['exitvelo_first3_ids'] = b_ev['player_id'].head(3).tolist()
                result['expected_first3_ids'] = b_ex['player_id'].head(3).tolist()
                # Check Myers (667472) and Jones (663330) specifically
                for test_pid, test_name in [('667472','Dane Myers'),('663330','Jahmai Jones')]:
                    ev_r = b_ev[b_ev['player_id'] == test_pid]
                    ex_r = b_ex[b_ex['player_id'] == test_pid]
                    result[f'{test_name}_ev'] = ev_r.iloc[0].to_dict() if not ev_r.empty else 'NOT FOUND'
                    result[f'{test_name}_ex'] = ex_r.iloc[0].to_dict() if not ex_r.empty else 'NOT FOUND'
                # Convert any non-serializable types
                import math
                def clean(d):
                    out = {}
                    for k,v in d.items():
                        try:
                            if v is None or (isinstance(v, float) and math.isnan(v)):
                                out[k] = None
                            else:
                                out[k] = float(v) if hasattr(v,'__float__') else str(v)
                        except:
                            out[k] = str(v)
                    return out
                result['exitvelo_sample'] = clean(result['exitvelo_sample'])
                result['expected_sample'] = clean(result['expected_sample'])
                self._json(result)
            except Exception as ex:
                import traceback
                self._json({'error': str(ex), 'trace': traceback.format_exc()})

        elif path == '/api/id-test':
            try:
                from urllib.parse import parse_qs
                qs = parse_qs(urlparse(self.path).query)
                name = qs.get('name', ['Elly De La Cruz'])[0]
                pid_final = get_player_id(name)
                # Also test a few known players
                tests = {
                    'Elly De La Cruz': ('682829', get_player_id('Elly De La Cruz')),
                    'Jahmai Jones':    ('663330', get_player_id('Jahmai Jones')),
                    'Dane Myers':      ('667472', get_player_id('Dane Myers')),
                    'Spencer Steer':   ('668715', get_player_id('Spencer Steer')),
                    name:              ('?',      pid_final),
                }
                self._json({
                    'pybaseball_ok': PYBASEBALL_OK,
                    'requested': {'name': name, 'pid': pid_final},
                    'known_player_tests': {k: {'expected': v[0], 'got': v[1], 'match': v[0]==v[1] or v[0]=='?'} for k,v in tests.items()},
                })
            except Exception as ex:
                import traceback
                self._json({'error': str(ex), 'trace': traceback.format_exc()})

        elif path == '/api/debug':
            try:
                from urllib.parse import parse_qs
                qs = parse_qs(urlparse(self.path).query)
                name = qs.get('name', [''])[0]
                if not name:
                    self._json({'error': 'Pass ?name=PlayerName'})
                    return
                clear_leaderboard_cache()
                # Test individual player page fetch
                pid = search_player_id(name)
                lb_pid, lb_stats = lookup_player_in_leaderboard(name, 'batter')
                # Also test bulk
                rows = _load_leaderboard('batter')
                best = None
                for row in rows:
                    if _name_match_score(row, name) >= 2:
                        best = row
                        break
                if not best and not pid:
                    self._json({'error': f'Not found: {name}', 'bulk_rows': len(rows), 'search_pid': pid, 'lb_pid': lb_pid})
                    return
                if not best:
                    # Individual fetch worked but bulk blocked
                    page_stats = fetch_from_player_page(pid) if pid else None
                    self._json({
                        'name': name, 'pid': pid, 'lb_pid': lb_pid,
                        'bulk_rows': len(rows),
                        'lb_stats': lb_stats,
                        'page_stats': page_stats,
                        'note': 'Bulk leaderboard blocked — using individual fetch'
                    })
                    return
                pid = str(best.get('player_id') or best.get('batter') or '')
                try:
                    sc_rows = _load_statcast_cache()
                    sc_best = next((r for r in sc_rows if str(r.get('player_id') or r.get('batter','')) == pid), None)
                except Exception:
                    sc_best = None
                try:
                    bb_rows = _load_batter_batted_ball_cache()
                    bb_best = next((r for r in bb_rows if str(r.get('player_id') or r.get('batter','')) == pid), None)
                except Exception:
                    bb_best = None
                self._json({
                    'name': name, 'pid': pid,
                    'expected_keys': sorted(best.keys()),
                    'statcast_keys': sorted(sc_best.keys()) if sc_best else [],
                    'batted_ball_keys': sorted(bb_best.keys()) if bb_best else [],
                    'expected_sample': {k:v for k,v in best.items() if any(x in k for x in ['speed','hit','slg','woba','barrel','sweet','fly','fb','ev','xslg'])},
                    'statcast_sample': {k:v for k,v in sc_best.items() if any(x in k for x in ['speed','hit','slg','woba','barrel','sweet','fly','fb','ev','xslg'])} if sc_best else {},
                    'batted_ball_sample': {k:v for k,v in bb_best.items() if any(x in k for x in ['fb','fly','gb','barrel','speed'])} if bb_best else {},
                })
            except Exception as ex:
                import traceback
                self._json({'error': str(ex), 'trace': traceback.format_exc()})
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
