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

# Pybaseball removed — caused server crashes with parallel threads

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
    "Sharp Oracle. 20-year MLB scout. Statcast HR prop model. Peer-level, zero filler.\n"
    "\n"
    "DATA: All stats are live-fetched. Use ONLY the numbers in the data block. "
    "Never substitute training knowledge. [PROXY] = grade conservatively.\n"
    "\n"
    "OUTPUT — 9 sections, always all 9, in order:\n"
    "S1: Pitchers — name | team | gate | gap | GB%/CSW% | one-line note. NO letter grades.\n"
    "S2: Batter table — one row each, no notes column.\n"
    "S3: Park + weather card.\n"
    "S4: Upgrades #1-#5, #10-#14 — one line each.\n"
    "S5: Formal picks with reasoning. HR picks separate from hit picks.\n"
    "S6: TOP 2 HR. Exactly 2. 2 sentences max each.\n"
    "S7: TOP 2 HITS. Exactly 2. 2 sentences max each.\n"
    "S8: 3-leg HR parlay. ONLY if 2+ A/A- grades exist. Otherwise SKIP.\n"
    "S9: 5-leg hit parlay. No fillers. Cold-gap or hot-gap HIT-PICK-YES only.\n"
    "\n"
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
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'Pragma': 'no-cache',
    'Expires': '0',
}



def clear_leaderboard_cache():
    """No-op — leaderboard caches removed."""
    pass


PYBASEBALL_OK = False
_pyb_batter_cache = None
_pyb_pitcher_cache = None
_pyb_lock = __import__('threading').Lock()


def _pull_pybaseball_data():
    """No-op — pybaseball removed."""
    return False


# ─── KNOWN PLAYER IDS ────────────────────────────────────────────────────────
# Hardcoded MLBAM IDs verified from baseballsavant.mlb.com/savant-player/name-ID URLs
# These take priority over MLB Stats API cache — use when page scrape returns wrong player
KNOWN_PLAYER_IDS = {
    # Accent/spelling variants
    'jose ramirez': '608070', 'jose ramírez': '608070',
    'christian vazquez': '477132', 'christian vázquez': '477132',
    'jd martinez': '502110', 'j.d. martinez': '502110',
    'michael a. taylor': '534606',
    # Confirmed wrong from bulk leaderboard
    'dane myers': '667472', 'jahmai jones': '663330',
    'spencer steer': '668715', 'elly de la cruz': '682829',
    'matt mclain': '680574', 'kevin mcgonigle': '805808',
    'tyler stephenson': '663886',
    # Verified from Savant URLs — recurring slates
    'royce lewis': '668904', 'jonny deluca': '676356',
    'victor caratini': '605170', 'trevor larnach': '663616',
    'cedric mullins': '656775', 'matt wallner': '670242',
    'jonathan aranda': '666018', 'yandy diaz': '650490',
    'yandy díaz': '650490', 'brooks lee': '686797',
    'drew rasmussen': '656876', 'taj bradley': '671737',
    'byron buxton': '621439', 'junior caminero': '691406',
    'brayan bello': '678394', 'adley rutschman': '668939',
    'wilyer abreu': '677800', 'pete alonso': '624413',
    'masataka yoshida': '807799', 'jarren duran': '680776',
    'ceddanne rafaela': '678882', 'gunnar henderson': '683002',
    'taylor ward': '621493', 'leody taveras': '665750',
    'gavin williams': '668909', 'max scherzer': '453286',
    'andres gimenez': '665926', 'andrés giménez': '665926',
    'vladimir guerrero jr': '665489', 'vladimir guerrero jr.': '665489',
    'chase delauter': '800050', 'jesus sanchez': '660821',
    'jesús sánchez': '660821', 'bo naylor': '666310',
    'josh naylor': '647304', 'rhys hoskins': '543333',
    'willson contreras': '575929', 'trevor story': '596115',
    'brice turang': '671218', 'william contreras': '661388',
    'jake bauers': '664353', 'gary sanchez': '425794',
    'gary sánchez': '425794', 'garrett mitchell': '669060',
    'kerry carpenter': '681481', 'wenceel perez': '676080',
    'wenceel pérez': '676080', 'mitch garver': '641598',
    'will wilson': '683737', 'jordan walker': '691023',
    'alec burleson': '676475', 'ivan herrera': '671056',
    'julio rodriguez': '677594', 'julio rodríguez': '677594',
    'bryan woo': '693433', 'randy arozarena': '668227',
    'matthew liberatore': '669461', 'cal raleigh': '663728',
    'masyn winn': '691026', 'jp crawford': '641487',
    'j.p. crawford': '641487', 'victor scott ii': '687363',
    'jj wetherholt': '802139', 'ryan jeffers': '680777',
    'shane mcclanahan': '663556', 'bailey ober': '641927',
    'jake fraley': '642378', 'josh bell': '605137',
    'richie palacios': '676939', 'luke keaschall': '701444',
    'esteury ruiz': '665923', 'kyle stowers': '669065',
    'xavier edwards': '669364', 'otto lopez': '672640',
    'robbie ray': '594798', 'eury perez': '677542',
    'eury pérez': '677542', 'drew gilbert': '694109',
    'luis arraez': '650333', 'rafael devers': '646240',
    'jung hoo lee': '808967', 'heliot ramos': '671218',
    'matt chapman': '656305', 'casey schmitt': '676693',
    'willy adames': '642715', 'patrick bailey': '672389',
    'agustin ramirez': '701217', 'agustín ramírez': '701217',
    'leo jimenez': '677800', 'leo jiménez': '677800',
    'heriberto hernandez': '671296', 'javier sanoja': '694521',
    'connor norby': '681962', 'rhys hoskins': '543333',
    'kazuma okamoto': '672960', 'kevin gausman': '592332',
    'joey cantillo': '676282', 'myles straw': '668678',
    'davis schneider': '676896', 'eloy jimenez': '650391',
    'eloy jiménez': '650391', 'daulton varsho': '662139',
    'david fry': '681867', 'ernie clement': '676801',
    'steven kwan': '680757', 'angel martinez': '677651',
    'ángel martínez': '677651', 'brayan rocchio': '672523',
    # 2026-04-25 verified
    'chris sale': '519242', 'aaron nola': '605400',
    'jorge mateo': '622761', 'ronald acuna jr': '660670',
    'ronald acuña jr.': '660670', 'ronald acuña jr': '660670',
    'ronald acuna jr.': '660670',
    'bryce harper': '547180', 'matt olson': '621566',
    'michael harris ii': '671739', 'drake baldwin': '686948',
    'trea turner': '607208', 'austin riley': '663586',
    'ozzie albies': '645277', 'kyle schwarber': '656941',
    'adolis garcia': '666969', 'adolis garcía': '666969',
    'alec bohm': '664761', 'edmundo sosa': '624641',       # fixed: was 660688
    'mauricio dubon': '643289', 'mauricio dubón': '643289',
    'dylan moore': '664238',       # savant-player/dylan-moore-664238
    'rafael marchan': '660688',    # savant-player/rafael-marchan-660688
    'rafael marchán': '660688',
    'eli white': '656774',         # savant-player/eli-white-656774
    # 2026-04-26 full slate IDs
    'connelly early': '813349',
    'kyle bradish': '680694',
    'roman anthony': '701350',
    'marcelo mayer': '694785',
    'samuel basallo': '694212',
    'dylan beavers': '687637',
    'blaze alexander': '677942',
    'slade cecconi': '677944',
    'patrick corbin': '548389',
    'austin hedges': '595978',
    'keider montero': '672456',
    'rhett lowder': '695076',
    'colt keith': '683002',
    'tj friedl': '663804',
    'nathaniel lowe': '663993',
    'jj bleday': '668709',
    'jose trevino': '650402',
    'ke bryan hayes': '663647',
    "ke'bryan hayes": '663647',
    'jose quintana': '500779',
    'nolan mclean': '808487',
    'bo bichette': '666182',
    'juan soto': '665742',
    'luis robert jr': '673357',
    'luis robert jr.': '673357',
    'mark vientos': '683734',
    'marcus semien': '572138',
    'brett baty': '672724',
    'tyrone taylor': '621011',
    'tommy pham': '502054',
    'ezequiel tovar': '678662',
    'jake mccarthy': '661331',
    'mickey moniak': '666917',
    'kyle karros': '808274',
    'jordan beck': '686780',
    'luis gil': '661563',
    'spencer arrighetti': '681293',
    'trent grisham': '663738',
    'ben rice': '682620',
    'aaron judge': '592450',
    'jazz chisholm jr': '665862',
    'jazz chisholm jr.': '665862',
    'jc escarra': '808267',
    'jose caballero': '664728',
    'yordan alvarez': '670541',
    'christian walker': '572233',
    'dustin harris': '683154',
    'daniel johnson': '669455',
    'braden shewmake': '676092',
    'isaac paredes': '670623',
    'cam smith': '681481',
    'christian vazquez': '477132',
    'christian v\u00e1zquez': '477132',
    'ryan mcmahon': '641857',
    'paul goldschmidt': '502671',
    'cody bellinger': '641355',
    'carmen mlodzinski': '669387',
    'kyle harrison': '690986',
    'jake mangum': '669680',
    'nick gonzales': '676801',
    'bryan reynolds': '668804',
    'ryan o\'hearn': '621485',
    'konnor griffin': '804606',
    'joey bart': '641940',
    'billy cook': '808286',
    'nick yorke': '683737',
    'luis rengifo': '660821',
    'david hamilton': '683154',
    'brandon lockridge': '663527',
    'emerson hancock': '676106',
    'michael mcgreevy': '700241',
    'leo rivas': '681910',
    'dominic canzone': '671277',
    'connor joe': '642232',
    'jt ginn': '669372',
    'j.t. ginn': '669372',
    'kumar rocker': '694655',
    'simeon woods richardson': '680573',
    'griffin jax': '657808',
    'james outman': '656555',
    'hunter feduccia': '808291',
    'taylor walls': '657757',
    'foster griffin': '656061',
    'bryan hudson': '657756',
    'james wood': '694192',
    'daylen lile': '808271',
    'cj abrams': '682928',
    'c.j. abrams': '682928',
    'jacob young': '672851',
    'nasim nunez': '694497',
    'nasim nu\u00f1ez': '694497',
    'drew millas': '673951',
    'curtis mead': '681756',
    'brady house': '694175',
    'miguel vargas': '671277',
    'everson pereira': '676475',
    'colson montgomery': '694174',
    'derek hill': '650895',
    'drew romo': '694162',
    'luisangel acuna': '694496',
    'luisangel acu\u00f1a': '694496',
    'chase meidroth': '808272',
    'munetaka murakami': '807800',
    'nick kurtz': '808264',
    'shea langeliers': '669127',
    'tyler soderstrom': '694209',
    'brent rooker': '667670',
    'carlos cortes': '680120',
    'jacob wilson': '808269',
    'jeff mcneil': '657537',
    'lawrence butler': '677649',
    'darell hernaiz': '672695',
    'brandon nimmo': '607043',
    'joc pederson': '592626',
    'corey seager': '608369',
    'josh jung': '673490',
    'evan carter': '694497',
    'jake burger': '669394',
    'josh smith': '669713',
    'danny jansen': '657136',
    'alejandro osuna': '808290',
    'paul skenes': '694973',
    'patrick corbin': '548389',
    # ID overrides for players with slug collision issues
    'blaze alexander': '677942',   # force ID — slug resolves to wrong player
    'dylan beavers': '687637',     # force ID — slug resolves to wrong player
    'marcelo mayer': '694785',     # savant-player/marcelo-mayer-694785
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

    # 2. pybaseball playerid_lookup DISABLED
    # Downloads a CSV on every unique name lookup — crashes server when 18 players
    # are fetched in parallel. Rely on KNOWN_PLAYER_IDS + MLB Stats API cache instead.

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
                if xslg is not None: stats['xslg'] = xslg
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
}  # end SANE


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
    # Add timestamp cache-buster to every Savant URL
    sep = '&' if '?' in url else '?'
    url = f"{url}{sep}_={int(time.time())}"
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



def fetch_from_player_page(player_id, player_name=None):
    """
    Fetch EV, HH%, Barrel% from Savant player page HTML summary line.
    This is the only Savant endpoint that works from Railway for per-player data.
    xwOBA/xSLG/wOBA come from pybaseball expected_stats (handled in fetch_one_player).
    """
    pid = str(player_id)
    stats = {
        'exit_velocity': None, 'hard_hit_pct': None, 'barrel_pct': None,
        'xwoba': None, 'woba': None,
    }

    # Try numeric ID URL with year param — forces 2026 stats
    urls = [
        f'https://baseballsavant.mlb.com/savant-player/{pid}?stats=statcast-r-hitting-mlb&season={CURRENT_YEAR}',
        f'https://baseballsavant.mlb.com/savant-player/{pid}',
    ]
    if player_name:
        slug = normalize_name(player_name).lower().replace(' ', '-')
        urls.insert(0, f'https://baseballsavant.mlb.com/savant-player/{slug}-{pid}?season={CURRENT_YEAR}')

    html = None
    for url in urls:
        html = savant_get(url)
        if html and len(html) > 5000:
            break

    if not html:
        return None

    # PRIMARY: extract from "(2026) Avg Exit Velocity: X, Hard Hit %: X..." summary line
    # This is server-side rendered and most reliable
    m = re.search(
        r'\(2026\)\s*Avg\s*Exit\s*Vel[a-z]*:\s*([\d.]+)[,\s]*'
        r'Hard\s*Hit\s*%:\s*([\d.]+)[,\s]*'
        r'wOBA:\s*([.\d]+)[,\s]*'
        r'xwOBA:\s*([.\d]+)[,\s]*'
        r'Barrel\s*%:\s*([\d.]+)',
        html, re.I
    )
    if m:
        stats['exit_velocity'] = safe_float(m.group(1))
        stats['hard_hit_pct']  = safe_float(m.group(2))
        stats['woba']          = safe_float(m.group(3))
        stats['xwoba']         = safe_float(m.group(4))
        stats['barrel_pct']    = safe_float(m.group(5))

    # SECONDARY/TERTIARY: blob scanning and regex are unreliable —
    # the page blobs contain split/zone data not season totals.
    # Summary line is the only reliable source from the page.
    # xSLG is filled by pybaseball in fetch_one_player.
    # FB%, EV50, SS% are not accessible from Railway (leaderboard endpoints blocked).

    return stats if any(v is not None for v in stats.values()) else None


def fetch_extended_batter_stats(player_id):
    """Deprecated — FB%, EV50, SS% not reliably accessible from Railway. No-op."""""
    return {}


def fetch_pitcher_extras(player_id):
    """
    Fetch GB% and CSW% for pitchers via direct Savant JSON endpoints.
    Uses individual player_id filter — no bulk cache, always fresh.
    """
    result = {'gb_pct': None, 'csw_pct': None}
    pid = str(player_id)

    def g(row, *keys):
        for k in keys:
            v = row.get(k)
            if v not in (None, '', 'null', 'None'):
                f = safe_float(v)
                if f is not None:
                    return f
        return None

    def fetch_json(url):
        raw = savant_get(url, accept_json=True)
        if not raw:
            return None
        try:
            data = json.loads(raw)
            rows = data if isinstance(data, list) else data.get('data', [])
            if not rows:
                return None
            for row in rows:
                rid = str(row.get('player_id') or row.get('pitcher') or '')
                if rid == pid:
                    return row
            if len(rows) == 1:
                return rows[0]
            return None
        except Exception:
            return None

    # GB% from pitcher batted-ball endpoint
    row_bb = fetch_json(
        f'https://baseballsavant.mlb.com/leaderboard/batted-ball'
        f'?type=pitcher&year={CURRENT_YEAR}&player_id={pid}'
    )
    if row_bb:
        result['gb_pct'] = g(row_bb, 'gb_percent', 'groundball_percent', 'gb_pct', 'gb')

    # CSW% from pitch arsenal endpoint
    row_csw = fetch_json(
        f'https://baseballsavant.mlb.com/leaderboard/pitch-arsenals'
        f'?type=pitcher&year={CURRENT_YEAR}&player_id={pid}'
    )
    if row_csw:
        result['csw_pct'] = g(row_csw, 'csw', 'csw_pct', 'csw_percent', 'called_strike_whiff_pct')

    # Fallback: bulk caches removed — individual endpoints only
    # If individual endpoints are blocked, GB%/CSW% stay None (shown as PROXY in output)

    return result

def fetch_one_player(info):
    """
    Clean data pipeline:
    1. Use pre-resolved player ID (set by resolve_all_ids before parallel fetch)
    2. Hit Savant player page — extract summary line for EV, HH%, Barrel%, xwOBA, wOBA
    3. xSLG filled from page JSON if available
    4. Pitcher extras: GB%, CSW% via individual Savant endpoints
    """
    result = {
        **info,
        'exit_velocity': None, 'hard_hit_pct': None, 'barrel_pct': None,
        'xwoba': None, 'woba': None,
        'gb_pct': None, 'csw_pct': None,
        'gap': None, 'player_id': None,
        'fetch_status': 'not found',
        'data_source': None,
    }
    name = info.get('name', '').strip()
    if not name:
        result['fetch_status'] = 'no name'
        return result

    ptype = 'pitcher' if info.get('role') == 'PITCHER' else 'batter'

    # Get player ID if available — used as fallback in page fetch
    # Name-slug fetch works without ID for most players
    pid = info.get('resolved_player_id') or get_player_id(name)
    if pid:
        result['player_id'] = pid

    # Fetch from Savant player page — name-slug first, ID as fallback
    page_stats = fetch_from_player_page(player_id=pid, player_name=name)
    if not page_stats or not any(v is not None for v in page_stats.values()):
        result['fetch_status'] = 'found/no stats' if pid else 'id not found'
        result['data_source'] = 'page-empty'
        return result

    # Apply sanity checks and copy stats
    for k, v in page_stats.items():
        checked = sane(k, v)
        if checked is not None:
            result[k] = checked

    result['fetch_status'] = 'ok'
    result['data_source'] = 'savant-page'

    # Compute GAP
    if result['xwoba'] is not None and result['woba'] is not None:
        result['gap'] = round(result['xwoba'] - result['woba'], 3)

    # Pitcher extras: GB% and CSW%
    if ptype == 'pitcher':
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

    xslg_flag = ''



    hr_cap = ''
    if gap is not None and gap < 0:
        wo = b.get('woba')
        if gap <= -0.060:
            hit_tag = ' HIT-PICK-YES' if (wo is not None and wo >= 0.380) else ' HIT-PICK-MAYBE'
            hr_cap = f' HR-CAP-C{hit_tag}'
        else:
            hit_tag = ' HIT-PICK-YES' if (wo is not None and wo >= 0.320) else ''
            hr_cap = f' HR-CAP-B{hit_tag}'

    extra_flags = xslg_flag
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


def build_context_str(parsed, all_statcast, pen_era=None):
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
            lines.append(
                f"  #{b.get('lineup_pos','?')} {proxy}{b.get('name','?')} ({b.get('hand','?')}HB) | "
                f"SCORE={score}/4 | plat={platoon} | gap={gs}({gap_flag}){hr_cap}{extra_flags} | "
                f"wOBA={b.get('woba','N/A')} | {breakdown}"
            )
        lines.append('')

    lines.append('INSTRUCTION: Use pre-computed GATE, SCORE, platoon, gap flags exactly. Do not re-compute.')
    lines.append('HR-CAP-C = max HR grade C. HR-CAP-B = max HR grade B. These are hard ceilings.')
    lines.append('HIT-PICK-YES = strong hit candidate (high wOBA, running hot). Include in hit picks and hit parlay.')
    lines.append('HIT-PICK-MAYBE = moderate hit candidate. HOT gap batters with high wOBA BELONG in hit picks.')
    lines.append('Running HOT (negative gap) = FADE for HR only. It does NOT suppress hit probability.')
    lines.append('Top hit picks should include the highest wOBA batters regardless of gap direction.')

    # Add bullpen ERA data
    if pen_era:
        lines.append('')
        lines.append('BULLPEN ERA (current season):')
        for team, data in pen_era.items():
            era = data.get('era')
            tier = data.get('tier', 'UNKNOWN')
            era_str = f"{era:.2f}" if era is not None else "N/A"
            flag = " ⚠ WEAK PEN (ERA>=5.50 — upgrades #1 and #5 may apply)" if tier == 'WEAK' else ""
            lines.append(f"  {team.upper()}: ERA={era_str} [{tier}]{flag}")

    return '\n'.join(lines)



# ─── BACKGROUND JOB ───────────────────────────────────────────────────────────

# ─── BULLPEN ERA FETCH ────────────────────────────────────────────────────────
# Uses MLB Stats API to get current season bullpen ERA per team
# Called during run_job to populate pen ERA for upgrades #1 and #5

_TEAM_NAME_TO_ID = {
    'angels': 108, 'astros': 117, 'athletics': 133, 'blue jays': 141,
    'braves': 144, 'brewers': 158, 'cardinals': 138, 'cubs': 112,
    'diamondbacks': 109, 'dodgers': 119, 'giants': 137, 'guardians': 114,
    'mariners': 136, 'marlins': 146, 'mets': 121, 'nationals': 120,
    'orioles': 110, 'padres': 135, 'phillies': 143, 'pirates': 134,
    'rangers': 140, 'rays': 139, 'red sox': 111, 'reds': 113,
    'rockies': 115, 'royals': 118, 'tigers': 116, 'twins': 142,
    'white sox': 145, 'yankees': 147, 'cubs': 112, 'athletics': 133,
}


def get_team_id(team_name):
    """Get MLB team ID from team name."""
    name = normalize_name(team_name).lower()
    for k, v in _TEAM_NAME_TO_ID.items():
        if k in name or name in k:
            return v
    return None


def fetch_bullpen_era(team_name):
    """
    Fetch current season team pitching ERA via MLB Stats API.
    Uses the team totals endpoint — returns overall pitching ERA as proxy for pen quality.
    Returns dict: {era, tier}
    """
    result = {'era': None, 'il_count': 0, 'tier': 'UNKNOWN'}
    team_id = get_team_id(team_name)
    if not team_id:
        return result

    # MLB Stats API team stats — pitching group, season totals
    urls = [
        (f'https://statsapi.mlb.com/api/v1/teams/{team_id}/stats'
         f'?stats=season&group=pitching&season={CURRENT_YEAR}&gameType=R'),
        (f'https://statsapi.mlb.com/api/v1/teams/{team_id}/stats'
         f'?stats=season&group=pitching&season={CURRENT_YEAR}'),
    ]

    for url in urls:
        try:
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())
            # MLB Stats API returns stats[0].splits[0].stat.era for team totals
            stats = data.get('stats', [])
            for stat_group in stats:
                splits = stat_group.get('splits', [])
                if splits:
                    era_str = splits[0].get('stat', {}).get('era', '')
                    era = safe_float(era_str)
                    if era is not None and 0 < era < 20:
                        result['era'] = round(era, 2)
                        break
            if result['era'] is not None:
                break
        except Exception:
            continue

    if result['era'] is not None:
        if result['era'] >= 5.50:
            result['tier'] = 'WEAK'
        elif result['era'] >= 4.50:
            result['tier'] = 'AVERAGE'
        else:
            result['tier'] = 'SOLID'

    return result


def resolve_all_ids(player_list):
    """
    Step 2 of the new pipeline: resolve MLBAM IDs for all players sequentially
    before any parallel fetching starts.
    Returns dict: {normalized_name: player_id or None}
    Logs results so Railway logs show exactly which IDs were found/missing.
    """
    id_map = {}
    missing = []
    for info in player_list:
        name = info.get('name', '').strip()
        if not name:
            continue
        pid = get_player_id(name)
        if not pid:
            pid = search_player_id(name)
        id_map[name] = pid
        if not pid:
            missing.append(name)

    found = len(id_map) - len(missing)
    print(f"[ID RESOLUTION] {found}/{len(id_map)} resolved | "
          f"Missing: {missing if missing else 'none'}")
    return id_map


def run_job(jid, sid, raw_lineup, game_date=None):
    with store_lock:
        jobs[jid]['status'] = 'running'
    # Clear all caches so every run gets fresh daily data
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

        # S3 — Build full player list and resolve ALL IDs first (sequential, before any fetching)
        step_set(jid, 3, 'active', 'Resolving player IDs...')
        pitcher_list = []
        hp = parsed.get('home_pitcher', {})
        ap = parsed.get('away_pitcher', {})
        if hp.get('name'):
            pitcher_list.append({**hp, 'role': 'PITCHER', 'team': home,
                                  'faces_team': away, 'lineup_pos': 0})
        if ap.get('name'):
            pitcher_list.append({**ap, 'role': 'PITCHER', 'team': away,
                                  'faces_team': home, 'lineup_pos': 0})
        batter_list = []
        for b in parsed.get('home_batters', []):
            batter_list.append({**b, 'role': 'BATTER', 'team': home})
        for b in parsed.get('away_batters', []):
            batter_list.append({**b, 'role': 'BATTER', 'team': away})

        # Resolve all IDs sequentially — this is the single source of truth
        all_players = pitcher_list + batter_list
        id_map = resolve_all_ids(all_players)

        # Inject confirmed IDs into player dicts before fetching
        for p in all_players:
            name = p.get('name', '')
            pid = id_map.get(name)
            if pid:
                p['resolved_player_id'] = pid

        step_set(jid, 3, 'done', f'IDs resolved: {sum(1 for v in id_map.values() if v)}/{len(id_map)}')

        # S4 — Now fetch stats using confirmed IDs (parallel safe — no ID resolution in workers)
        step_set(jid, 4, 'active', 'Fetching Statcast in parallel...')
        pitcher_stats = fetch_all_parallel(pitcher_list, workers=2)
        batter_stats = fetch_all_parallel(batter_list, workers=12)
        all_statcast = pitcher_stats + batter_stats
        ok = sum(1 for x in all_statcast if x.get('fetch_status') == 'ok')
        with store_lock:
            jobs[jid]['statcast'] = all_statcast
        step_set(jid, 4, 'done', f'Statcast: {ok}/{len(all_statcast)} found')

        sess = get_session(sid)
        sess['game_data'] = parsed
        sess['statcast']  = all_statcast

        # Fetch bullpen ERA for both teams
        pen_era = {}
        for team_name in [parsed.get('team1',''), parsed.get('team2','')]:
            if team_name:
                pen_data = fetch_bullpen_era(team_name)
                if pen_data.get('era') is not None:
                    pen_era[normalize_name(team_name).lower()] = pen_data

        # S5 — full model
        step_set(jid, 5, 'active', 'Running model (all 14 upgrades)...')
        ctx = build_context_str(parsed, all_statcast, pen_era=pen_era)
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
            <th>xwOBA</th><th>wOBA</th><th>GAP</th><th>STATUS</th>
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

        elif path == '/api/page-debug':
            # Dumps what the page scrape actually finds for a player
            try:
                from urllib.parse import parse_qs
                qs = parse_qs(urlparse(self.path).query)
                pid = qs.get('pid', ['545361'])[0]  # default: Mike Trout
                html = savant_get(
                    f'https://baseballsavant.mlb.com/savant-player/{pid}?stats=statcast-r-hitting-mlb&season={CURRENT_YEAR}'
                )
                if not html:
                    self._json({'error': 'no html returned'})
                    return
                # Find all JSON blobs and extract keys
                import re
                blobs_found = []
                for blob in re.findall(r'(\[{.+?}\])', html, re.DOTALL):
                    try:
                        arr = json.loads(blob)
                        if isinstance(arr, list) and arr and isinstance(arr[0], dict):
                            row = arr[0]
                            year_val = str(row.get('year', row.get('season', row.get('game_year', ''))))
                            # Find row matching player_id
                            target_row = None
                            for r in arr:
                                if str(r.get('player_id', '')) == pid:
                                    target_row = r
                                    break
                            if target_row is None:
                                target_row = row
                            blobs_found.append({
                                'year': year_val,
                                'row_count': len(arr),
                                'player_id_match': str(target_row.get('player_id','')) == pid,
                                'keys': sorted(target_row.keys()),
                                'avg_ev': target_row.get('avg_ev'),
                                'hard_hit_bip_percent': target_row.get('hard_hit_bip_percent'),
                                'brl_bip_percent': target_row.get('brl_bip_percent'),
                                'fb_percent': target_row.get('fb_percent'),
                                'xwoba': target_row.get('xwoba'),
                                'xslg': target_row.get('xslg'),
                                'woba': target_row.get('woba'),
                                'player_id': target_row.get('player_id'),
                            })
                    except Exception:
                        pass
                # Also check summary line
                m = re.search(
                    r'\(2026\)\s*Avg\s*Exit\s*Vel[a-z]*:\s*([\d.]+)[,\s]*'
                    r'Hard\s*Hit\s*%:\s*([\d.]+)[,\s]*'
                    r'wOBA:\s*([.\d]+)[,\s]*'
                    r'xwOBA:\s*([.\d]+)[,\s]*'
                    r'Barrel\s*%:\s*([\d.]+)',
                    html, re.I
                )
                self._json({
                    'pid': pid,
                    'html_length': len(html),
                    'summary_line_found': bool(m),
                    'summary_values': {
                        'ev': m.group(1), 'hh': m.group(2),
                        'woba': m.group(3), 'xwoba': m.group(4), 'brl': m.group(5)
                    } if m else {},
                    'blobs_with_2026': [b for b in blobs_found if b['year'] in ('2026', '')],
                    'all_blob_years': [b['year'] for b in blobs_found],
                })
            except Exception as ex:
                import traceback
                self._json({'error': str(ex), 'trace': traceback.format_exc()})

        elif path == '/api/pen-debug':
            # Test bullpen ERA fetch for any team
            from urllib.parse import parse_qs
            qs = parse_qs(urlparse(self.path).query)
            team = qs.get('team', ['Red Sox'])[0]
            team_id = get_team_id(team)
            result = {'team': team, 'team_id': team_id}
            if team_id:
                url = (f'https://statsapi.mlb.com/api/v1/teams/{team_id}/stats'
                       f'?stats=season&group=pitching&season={CURRENT_YEAR}&gameType=R')
                try:
                    req = urllib.request.Request(url, headers=_HEADERS)
                    with urllib.request.urlopen(req, timeout=10) as r:
                        data = json.loads(r.read())
                    result['raw_stats'] = data.get('stats', [])
                    result['era_fetch'] = fetch_bullpen_era(team)
                except Exception as ex:
                    result['error'] = str(ex)
            self._json(result)

        elif path == '/api/lb-test':
            # Test if Savant leaderboard endpoints are accessible from this Railway region
            from urllib.parse import parse_qs
            qs = parse_qs(urlparse(self.path).query)
            pid = qs.get('pid', ['545361'])[0]
            results = {}
            endpoints = {
                'expected_stats': f'https://baseballsavant.mlb.com/leaderboard/expected_statistics?type=batter&year={CURRENT_YEAR}&player_id={pid}&min=0',
                'statcast': f'https://baseballsavant.mlb.com/leaderboard/statcast?type=batter&year={CURRENT_YEAR}&player_id={pid}&min=0',
                'batted_ball': f'https://baseballsavant.mlb.com/leaderboard/batted-ball?type=batter&year={CURRENT_YEAR}&player_id={pid}',
                'arsenal': f'https://baseballsavant.mlb.com/leaderboard/pitch-arsenals?type=pitcher&year={CURRENT_YEAR}&player_id={pid}',
            }
            for name, url in endpoints.items():
                try:
                    raw = savant_get(url, accept_json=True)
                    if raw:
                        data = json.loads(raw)
                        rows = data if isinstance(data, list) else data.get('data', [])
                        results[name] = {'status': 'OK', 'rows': len(rows), 'sample': rows[0] if rows else None}
                    else:
                        results[name] = {'status': 'BLOCKED/EMPTY'}
                except Exception as ex:
                    results[name] = {'status': f'ERROR: {ex}'}
            self._json(results)

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
