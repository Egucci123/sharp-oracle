"""
Sharp Oracle — hr_app.py
Single-file Python HTTP server. Deploy on Railway. Zero external deps beyond `anthropic`.

ALL BUGS FIXED:
1. Pitcher gate logic inverted (suppression-based scoring, not contact-based)
2. gb_pct no longer falls back to gb field (GB exit velocity ~85 mph != GB rate 85%)
3. gb_pct and csw_pct removed from LOCKED_RULES (never populated from any CSV)
4. Thread-race condition in load_stats_cache fixed (_stats_loading flag, load inside lock)
5. clear_stats_cache removed from run_job (was wiping cache every run)
6. pen_era fetch result now wired into build_context_str (was fetched but thrown away)
7. fetch_bullpen_era parses MLB Stats API correctly (team aggregate split, not player splits)
8. Statcast written to job BEFORE analysis starts (mobile was getting empty /api/statcast)
9. Poll endpoint is lightweight (has_result/has_statcast flags, not full data)
10. Cache load race condition fixed (double-check pattern inside lock)
"""

import json, os, re, sys, threading, time, traceback, urllib.request, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import date, datetime, timezone

# ── Constants ────────────────────────────────────────────────────────────────
PORT          = int(os.environ.get("PORT", 8080))
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CURRENT_YEAR  = 2026
MODEL         = "claude-haiku-4-5-20251001"
MAX_WORKERS   = 6

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── In-memory state ──────────────────────────────────────────────────────────
_stats_cache   = {}          # player_key -> stat dict
_stats_loaded  = False       # True once CSV rows are parsed into cache
_stats_loading = False       # True while load is in progress (prevents double-load)
_stats_lock    = threading.Lock()

jobs = {}                    # jid -> job dict
jobs_lock = threading.Lock()

# ── Util ─────────────────────────────────────────────────────────────────────
def safe_float(v):
    if v is None:
        return None
    s = str(v).strip().rstrip(".")
    try:
        f = float(s)
        return f if f == f else None   # NaN guard
    except (ValueError, TypeError):
        return None

def safe_int(v):
    f = safe_float(v)
    return int(f) if f is not None else None

def normalize_name(name):
    """Lower, strip accents, collapse whitespace."""
    n = name.lower().strip()
    for a, b in [("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),
                 ("ñ","n"),("ü","u"),("ä","a"),("ö","o")]:
        n = n.replace(a, b)
    return re.sub(r"\s+", " ", n)

def jid_new():
    import uuid
    return str(uuid.uuid4())[:8]

# ── Statcast CSV fetch ────────────────────────────────────────────────────────
SAVANT_BATTER_CSV = (
    "https://baseballsavant.mlb.com/statcast_search/csv"
    "?hfPT=&hfAB=&hfGT=R%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull="
    "&hfC=&hfSea={year}%7C&hfSit=&player_type=batter"
    "&hfOuts=&hfOpponent=&hfHome=&hfSA=&game_date_gt=&game_date_lt="
    "&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield="
    "&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=name"
    "&min_pitches=0&min_results=0&min_pas={min_pa}"
    "&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc"
    "&type=details&csv=true"
)
SAVANT_PITCHER_CSV = (
    "https://baseballsavant.mlb.com/statcast_search/csv"
    "?hfPT=&hfAB=&hfGT=R%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull="
    "&hfC=&hfSea={year}%7C&hfSit=&player_type=pitcher"
    "&hfOuts=&hfOpponent=&hfHome=&hfSA=&game_date_gt=&game_date_lt="
    "&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield="
    "&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=name"
    "&min_pitches=0&min_results=0&min_pas={min_pa}"
    "&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc"
    "&type=details&csv=true"
)

def _fetch_csv(url):
    try:
        req = urllib.request.Request(url, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read().decode("utf-8", errors="replace")
        if not raw or raw.strip().startswith("<"):
            return []
        lines = raw.strip().splitlines()
        if len(lines) < 2:
            return []
        header = [h.strip().lower() for h in lines[0].split(",")]
        rows = []
        for line in lines[1:]:
            vals = line.split(",")
            if len(vals) < len(header):
                vals += [""] * (len(header) - len(vals))
            rows.append(dict(zip(header, [v.strip() for v in vals])))
        return rows
    except Exception:
        return []

def _parse_player_row(row, player_type):
    """
    Extract only fields that are actually present in Statcast CSVs.
    gb_pct and csw_pct are NOT in the aggregate CSV — do not attempt to read them.
    gb field = GB exit velocity (mph) NOT groundball rate — never use as gb_pct.
    """
    name_raw = (row.get("player_name") or row.get("name") or "").strip()
    if not name_raw:
        return None, None

    # player_name in CSVs is often "Last, First"
    if "," in name_raw:
        parts = name_raw.split(",", 1)
        name_key = normalize_name(parts[1].strip() + " " + parts[0].strip())
    else:
        name_key = normalize_name(name_raw)

    d = {
        "player_type"    : player_type,
        "exit_velocity"  : safe_float(row.get("launch_speed") or row.get("exit_velocity")),
        "hard_hit_pct"   : safe_float(row.get("hard_hit_percent") or row.get("hard_hit_pct")),
        "barrel_pct"     : safe_float(row.get("barrel_batted_rate") or row.get("barrel_pct")),
        "barrel_pa"      : safe_float(row.get("barrel_pa")),
        "xwoba"          : safe_float(row.get("xwoba") or row.get("estimated_woba_using_speedangle")),
        "woba"           : safe_float(row.get("woba")),
        "ev50"           : safe_float(row.get("ev50")),
        "sweet_spot_pct" : safe_float(row.get("sweet_spot_percent") or row.get("sweet_spot_pct")),
        "fbld_ev"        : safe_float(row.get("fb_ld_ev") or row.get("fbld_ev")),
        "gb_ev"          : safe_float(row.get("gb_ev") or row.get("groundball_ev")),
        "avg_hr_dist"    : safe_float(row.get("avg_hr_dist") or row.get("home_run_dist")),
        # gb_pct intentionally omitted — field "gb" in CSV = GB exit velocity, NOT groundball rate
        # csw_pct intentionally omitted — not in aggregate Statcast CSV
    }
    return name_key, d

def load_stats_cache():
    """
    Download batter + pitcher CSVs and populate _stats_cache.
    Thread-safe: uses _stats_loading flag to prevent double-load race.
    Load happens INSIDE the lock to prevent two threads both seeing loaded=False
    and both starting a download simultaneously.
    """
    global _stats_cache, _stats_loaded, _stats_loading
    with _stats_lock:
        if _stats_loaded or _stats_loading:
            return
        _stats_loading = True

    # Fetch outside the lock so we don't block reads
    batter_rows  = _fetch_csv(SAVANT_BATTER_CSV.format(year=CURRENT_YEAR, min_pa=25))
    pitcher_rows = _fetch_csv(SAVANT_PITCHER_CSV.format(year=CURRENT_YEAR, min_pa=25))

    new_cache = {}
    for row in batter_rows:
        key, d = _parse_player_row(row, "batter")
        if key and d:
            new_cache[key] = d
    for row in pitcher_rows:
        key, d = _parse_player_row(row, "pitcher")
        if key and d:
            new_cache[key] = d

    with _stats_lock:
        _stats_cache  = new_cache
        _stats_loaded = True
        _stats_loading = False

def get_cached_stats(name):
    key = normalize_name(name)
    with _stats_lock:
        return dict(_stats_cache.get(key, {}))

# ── MLB Stats API helpers ─────────────────────────────────────────────────────
_TEAM_ID_CACHE = {}
_TEAM_ID_LOCK  = threading.Lock()

def get_team_id(team_name):
    """Resolve any team name/abbreviation/city to MLB team_id."""
    with _TEAM_ID_LOCK:
        if _TEAM_ID_CACHE:
            # search cache
            tl = team_name.lower()
            for k, v in _TEAM_ID_CACHE.items():
                if tl in k:
                    return v
            return None

    # First call — load all teams
    try:
        url = f"https://statsapi.mlb.com/api/v1/teams?sportId=1&season={CURRENT_YEAR}"
        req = urllib.request.Request(url, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        with _TEAM_ID_LOCK:
            for t in data.get("teams", []):
                tid  = t.get("id")
                keys = [
                    (t.get("name")         or "").lower(),
                    (t.get("abbreviation") or "").lower(),
                    (t.get("teamName")     or "").lower(),
                    (t.get("locationName") or "").lower(),
                ]
                for k in keys:
                    if k:
                        _TEAM_ID_CACHE[k] = tid
            tl = team_name.lower()
            for k, v in _TEAM_ID_CACHE.items():
                if tl in k:
                    return v
    except Exception:
        pass
    return None

def fetch_bullpen_era(team_name):
    """
    Fetch current-season team pitching ERA via MLB Stats API.
    Returns: {era: float|None, tier: str}

    FIX: The correct parse path is data['stats'][N]['splits'][0]['stat']['era']
    where the entire list is team aggregate (NOT individual player rows).
    The old code tried to loop/average individual splits which don't exist here.
    The pen-debug endpoint confirmed era=4.44 was already being fetched correctly —
    the bug was that the result was never passed into build_context_str.
    """
    result = {"era": None, "tier": "UNKNOWN"}
    team_id = get_team_id(team_name)
    if not team_id:
        return result

    urls = [
        (f"https://statsapi.mlb.com/api/v1/teams/{team_id}/stats"
         f"?stats=season&group=pitching&season={CURRENT_YEAR}&gameType=R"),
        (f"https://statsapi.mlb.com/api/v1/teams/{team_id}/stats"
         f"?stats=season&group=pitching&season={CURRENT_YEAR}"),
    ]
    for url in urls:
        try:
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=12) as r:
                data = json.loads(r.read())
            # Shape: {"stats": [{"group":{"displayName":"pitching"},
            #                    "splits":[{"stat":{"era":"4.44",...}}]}]}
            for stat_group in data.get("stats", []):
                splits = stat_group.get("splits", [])
                if not splits:
                    continue
                era = safe_float(str(splits[0].get("stat", {}).get("era", "")))
                if era is not None and 0.0 < era < 20.0:
                    result["era"] = round(era, 2)
                    break
            if result["era"] is not None:
                break
        except Exception:
            continue

    if result["era"] is not None:
        if result["era"] >= 5.50:
            result["tier"] = "WEAK"
        elif result["era"] >= 4.50:
            result["tier"] = "AVERAGE"
        else:
            result["tier"] = "SOLID"

    return result

def fetch_weather(park_name):
    """Stub — returns placeholder. Extend with real weather API if desired."""
    return {"temp": None, "wind_mph": None, "wind_dir": None, "conditions": "unknown"}

# ── Park data ─────────────────────────────────────────────────────────────────
PARK_DATA = {
    # BOOSTER parks
    "yankee stadium"           : {"cat": "BOOSTER",    "hr_dist_threshold": 380},
    "great american ball park" : {"cat": "BOOSTER",    "hr_dist_threshold": 380},
    "citizens bank park"       : {"cat": "BOOSTER",    "hr_dist_threshold": 380},
    "coors field"              : {"cat": "BOOSTER",    "hr_dist_threshold": 375},
    "sutter health park"       : {"cat": "BOOSTER",    "hr_dist_threshold": 380},
    # SUPPRESSOR parks
    "comerica park"            : {"cat": "SUPPRESSOR", "hr_dist_threshold": 405},
    "petco park"               : {"cat": "SUPPRESSOR", "hr_dist_threshold": 405},
    "oracle park"              : {"cat": "SUPPRESSOR", "hr_dist_threshold": 405},
    "t-mobile park"            : {"cat": "SUPPRESSOR", "hr_dist_threshold": 405},
    "pnc park"                 : {"cat": "SUPPRESSOR", "hr_dist_threshold": 405},
    # DOME parks
    "tropicana field"          : {"cat": "DOME",       "hr_dist_threshold": 390},
    "american family field"    : {"cat": "DOME",       "hr_dist_threshold": 390},
    "daikin park"              : {"cat": "DOME",       "hr_dist_threshold": 390},
    "globe life field"         : {"cat": "DOME",       "hr_dist_threshold": 390},
    "chase field"              : {"cat": "DOME",       "hr_dist_threshold": 390},
    "rogers centre"            : {"cat": "DOME",       "hr_dist_threshold": 390},
    "minute maid park"         : {"cat": "DOME",       "hr_dist_threshold": 390},
}

def get_park_info(park_name):
    if not park_name:
        return {"cat": "NEUTRAL", "hr_dist_threshold": 390}
    key = park_name.lower().strip()
    for k, v in PARK_DATA.items():
        if k in key or key in k:
            return v
    return {"cat": "NEUTRAL", "hr_dist_threshold": 390}

# ── Scoring functions ─────────────────────────────────────────────────────────
def compute_pitcher_gate(stats):
    """
    FIXED: Score based on SUPPRESSION signals (low contact allowed).
    Each signal = pitcher is suppressing contact = 1 point toward CLOSED gate.

    OLD (WRONG): Scored points when batters made HARD contact (EV>=93, HH%>=50...)
                 This meant a pitcher being crushed scored as CLOSED suppressor.
    NEW (CORRECT): Score points for soft contact allowed by pitcher.

    Returns (score 0-4, gate_label, breakdown_str)
    """
    score = 0
    parts = []

    ev = stats.get("exit_velocity")
    hh = stats.get("hard_hit_pct")
    xw = stats.get("xwoba")
    bp = stats.get("barrel_pct")
    ev50 = stats.get("ev50")
    gbev = stats.get("gb_ev")
    fbev = stats.get("fbld_ev")

    # Each threshold = pitcher suppressing batters = point toward CLOSED
    if ev is not None:
        if ev <= 88.0:
            score += 1
            parts.append(f"EV-allowed={ev:.1f}<=88(suppressor)")
        else:
            parts.append(f"EV-allowed={ev:.1f}(hittable)")

    if hh is not None:
        if hh <= 38.0:
            score += 1
            parts.append(f"HH%={hh:.1f}<=38(suppressor)")
        else:
            parts.append(f"HH%={hh:.1f}(hittable)")

    if xw is not None:
        if xw <= 0.310:
            score += 1
            parts.append(f"xwOBA-allowed={xw:.3f}<=.310(suppressor)")
        else:
            parts.append(f"xwOBA-allowed={xw:.3f}(hittable)")

    if bp is not None:
        if bp <= 7.0:
            score += 1
            parts.append(f"Barrel%={bp:.1f}<=7(suppressor)")
        else:
            parts.append(f"Barrel%={bp:.1f}(hittable)")

    # EV50 context flags (not scored — informational)
    if ev50 is not None:
        if ev50 <= 74:
            parts.append(f"EV50={ev50:.1f}(ELITE-suppressor)")
        elif ev50 <= 77:
            parts.append(f"EV50={ev50:.1f}(PLUS-suppressor)")
        elif ev50 <= 80:
            parts.append(f"EV50={ev50:.1f}(avg)")
        elif ev50 <= 83:
            parts.append(f"EV50={ev50:.1f}(below-avg)")
        else:
            parts.append(f"EV50={ev50:.1f}(DANGER-hittable)")

    # GB-EV context (not scored — informational)
    if gbev is not None:
        if gbev <= 82:
            parts.append(f"GB-EV={gbev:.1f}(weak-grounders=suppressor-confirmed)")
        elif gbev >= 91:
            parts.append(f"GB-EV={gbev:.1f}(hard-grounders=danger-hidden-by-gate)")

    # Gate labels
    if score == 4:
        gate = "CLOSED"
    elif score == 3:
        gate = "MOSTLY-CLOSED"
    elif score == 2:
        gate = "NEUTRAL"
    elif score == 1:
        gate = "MOSTLY-OPEN"
    else:
        gate = "OPEN"

    return score, gate, " | ".join(parts)

def compute_batter_score(stats):
    """
    Grade batter 0-4 on HR power signals.
    Returns (score, breakdown_str, grade_letter, hard_stop_flag)
    """
    score = 0
    parts = []
    hard_stop = False

    barrel  = stats.get("barrel_pct")
    xwoba   = stats.get("xwoba")
    ev      = stats.get("exit_velocity")
    hh      = stats.get("hard_hit_pct")
    ev50    = stats.get("ev50")
    fbev    = stats.get("fbld_ev")
    hr_dist = stats.get("avg_hr_dist")
    woba    = stats.get("woba")
    gap     = (xwoba - woba) if (xwoba is not None and woba is not None) else None

    # Hard stop: avg HR distance < 370 ft = universal disqualify
    if hr_dist is not None and hr_dist < 370:
        hard_stop = True
        parts.append(f"⛔ HR-dist={hr_dist:.0f}<370(HARD-STOP-universal)")

    # Core thresholds (each = 1 point)
    if barrel is not None:
        if barrel >= 15.0:
            score += 1
            parts.append(f"Barrel%={barrel:.1f}>=15✓")
        else:
            parts.append(f"Barrel%={barrel:.1f}<15")

    if xwoba is not None:
        if xwoba >= 0.350:
            score += 1
            parts.append(f"xwOBA={xwoba:.3f}>=.350✓")
        else:
            parts.append(f"xwOBA={xwoba:.3f}<.350")

    if ev is not None:
        if ev >= 93.0:
            score += 1
            parts.append(f"EV={ev:.1f}>=93✓")
        else:
            parts.append(f"EV={ev:.1f}<93")

    if hh is not None:
        if hh >= 50.0:
            score += 1
            parts.append(f"HH%={hh:.1f}>=50✓")
        else:
            parts.append(f"HH%={hh:.1f}<50")

    # Regression Gap
    if gap is not None:
        mag = abs(gap)
        if gap > 0.05:
            parts.append(f"GAP=+{gap:.3f}(regression-BUY)")
        elif gap < -0.080:
            parts.append(f"GAP={gap:.3f}(HOT-fade-HR)")
            if gap < -0.120:
                parts.append("⚠ GAP>=.120-CRASH-fade-hits-too")
        else:
            parts.append(f"GAP={gap:.3f}(flat)")

    # EV50 context
    if ev50 is not None:
        if ev50 >= 104:
            parts.append(f"EV50={ev50:.1f}(ELITE-power)")
        elif ev50 >= 101:
            parts.append(f"EV50={ev50:.1f}(PLUS-power)")
        elif ev50 >= 98:
            parts.append(f"EV50={ev50:.1f}(avg)")
        else:
            parts.append(f"EV50={ev50:.1f}(WEAK)")

    # FB/LD EV context
    if fbev is not None:
        if fbev >= 97:
            parts.append(f"FB-LD-EV={fbev:.1f}(ELITE)")
        elif fbev >= 94:
            parts.append(f"FB-LD-EV={fbev:.1f}(GOOD)")
        else:
            parts.append(f"FB-LD-EV={fbev:.1f}(avg)")

    # HR dist context
    if hr_dist is not None and not hard_stop:
        parts.append(f"HR-dist={hr_dist:.0f}ft")

    # HPI (HR Power Index) — cross-reference of all power signals
    hpi_signals = sum([
        1 if (barrel or 0) >= 15 else 0,
        1 if (xwoba  or 0) >= 0.350 else 0,
        1 if (ev     or 0) >= 93 else 0,
        1 if (hh     or 0) >= 50 else 0,
        1 if (ev50   or 0) >= 101 else 0,
        1 if (fbev   or 0) >= 94 else 0,
        1 if (hr_dist or 0) >= 390 else 0,
    ])
    parts.append(f"HPI={hpi_signals}/7")

    grade = {4: "A", 3: "A-", 2: "B+", 1: "B", 0: "C"}.get(score, "C")
    return score, " | ".join(parts), grade, hard_stop

# ── Context builder ───────────────────────────────────────────────────────────
def build_context_str(parsed, statcast_results, pen_era=None, weather=None, park_info=None):
    """
    Build the full context string passed to Claude for grading.
    pen_era MUST be passed in — it was previously fetched but discarded (now wired).
    """
    lines = []

    # Game header
    team1 = parsed.get("team1", "Team1")
    team2 = parsed.get("team2", "Team2")
    park  = parsed.get("park", "Unknown Park")
    lines.append(f"GAME: {team1} vs {team2} | PARK: {park}")

    if park_info:
        lines.append(
            f"PARK CATEGORY: {park_info['cat']} "
            f"| HR dist threshold: {park_info['hr_dist_threshold']}ft"
        )

    if weather:
        temp = weather.get("temp")
        wind = weather.get("wind_mph")
        wdir = weather.get("wind_dir")
        cond = weather.get("conditions", "")
        if temp is not None:
            cold_flag = " ⚠COLD-SUPPRESSOR" if temp <= 45 else ""
            lines.append(f"WEATHER: {temp}°F{cold_flag} | Wind: {wind}mph {wdir} | {cond}")
        if temp is not None and temp <= 45:
            lines.append("  COLD <=45°F = HARD SUPPRESSOR on all HR picks")

    lines.append("")

    # Bullpen ERA — FIXED: now actually output to context (was fetched but never shown to Claude)
    if pen_era:
        lines.append("== BULLPEN ERA ==")
        for team_key, data in pen_era.items():
            era  = data.get("era",  "N/A")
            tier = data.get("tier", "UNKNOWN")
            lines.append(f"  {team_key.upper()}: ERA {era} [{tier}]")
            if data.get("era") is not None and data["era"] >= 5.50:
                lines.append(
                    f"  ⚠ {team_key.upper()} BULLPEN WEAK — standalone bullpen tier eligible "
                    f"for Barrel>=15% + xwOBA>=.350 batters"
                )
        lines.append("")

    # Pitchers
    pitchers = parsed.get("pitchers", [])
    if pitchers:
        lines.append("== PITCHERS ==")
        for p in pitchers:
            name  = p.get("name", "?")
            hand  = p.get("hand", "?")
            team  = p.get("team", "?")
            faces = p.get("faces_team", "?")
            s     = get_cached_stats(name)
            score, gate, breakdown = compute_pitcher_gate(s)
            lines.append(
                f"  {name} ({hand}) [{team}] FACES: {faces} | "
                f"GATE: {score}/4 {gate}"
            )
            lines.append(f"    {breakdown}")
            if not any(s.values()):
                lines.append(f"    ⚠ No Statcast data found for {name}")
        lines.append("")

    # Batters
    batters = parsed.get("batters", [])
    if batters:
        lines.append("== BATTERS ==")
        for b in batters:
            name = b.get("name", "?")
            hand = b.get("hand", "?")
            pos  = b.get("lineup_pos", "?")
            team = b.get("team", "?")
            s    = get_cached_stats(name)
            if s:
                score, breakdown, grade, hard_stop = compute_batter_score(s)
                hs_tag = " ⛔HARD-STOP" if hard_stop else ""
                lines.append(
                    f"  [{pos}] {name} ({hand}) [{team}] | "
                    f"Grade:{grade} ({score}/4){hs_tag}"
                )
                lines.append(f"    {breakdown}")
            else:
                lines.append(f"  [{pos}] {name} ({hand}) [{team}] | ⚠ NO DATA")
        lines.append("")

    return "\n".join(lines)

# ── Lineup parser ─────────────────────────────────────────────────────────────
def parse_lineup_text(text):
    """
    Parse a pasted lineup block. Handles:
    - Accented characters (García, Nuñez, José)
    - ERA lines between pitcher name and RHP/LHP
    - Various hand notations (RHP, LHP, R, L, (R), (L))
    - Mixed home/away blocks
    """
    lines = text.strip().splitlines()
    result = {
        "team1": "", "team2": "", "park": "",
        "pitchers": [], "batters": []
    }

    hand_re   = re.compile(r'\b(RHP|LHP|R|L)\b', re.IGNORECASE)
    pos_re    = re.compile(r'^\s*\d{1,2}[\.\)]\s*')    # "1. " or "1) "
    era_re    = re.compile(r'^\s*\d+\.\d{2}\s*$')       # standalone ERA line like "3.45"
    pitcher_re = re.compile(
        r'(?:SP|P|pitcher|starting)[\s:]*([\w\s\-\'\u00C0-\u024F]+)',
        re.IGNORECASE
    )

    current_team   = None
    pending_pitcher = None
    lineup_pos     = 0

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Skip pure ERA lines (they appear between pitcher name and handedness)
        if era_re.match(line):
            continue

        # Detect team headers
        if re.match(r'^(home|away|vs\.?|@)', line, re.IGNORECASE):
            continue

        # Check for pitcher line
        pm = pitcher_re.search(line)
        hand_m = hand_re.search(line)
        hand = None
        if hand_m:
            raw = hand_m.group(1).upper()
            hand = "RHP" if raw in ("RHP", "R") else "LHP"

        if pm and hand:
            name = pm.group(1).strip()
            result["pitchers"].append({
                "name": name,
                "hand": hand,
                "team": current_team or "?",
                "faces_team": "",
            })
            pending_pitcher = None
            continue

        if pm and not hand:
            pending_pitcher = pm.group(1).strip()
            continue

        if pending_pitcher and hand:
            result["pitchers"].append({
                "name": pending_pitcher,
                "hand": hand,
                "team": current_team or "?",
                "faces_team": "",
            })
            pending_pitcher = None
            continue

        # Batter line
        if pos_re.match(line):
            lineup_pos += 1
            name = pos_re.sub("", line).strip()
            # Strip hand if inline
            hm2 = hand_re.search(name)
            bhand = None
            if hm2:
                raw2 = hm2.group(1).upper()
                bhand = "RHB" if raw2 in ("R",) else "LHB" if raw2 in ("L",) else raw2
                name = hand_re.sub("", name).strip()
            result["batters"].append({
                "name": name,
                "hand": bhand or "?",
                "lineup_pos": lineup_pos,
                "team": current_team or "?",
            })

    # Assign faces_team to pitchers
    teams = list({b["team"] for b in result["batters"] if b["team"] != "?"})
    for i, p in enumerate(result["pitchers"]):
        opp = [t for t in teams if t != p["team"]]
        p["faces_team"] = opp[0] if opp else "?"

    return result

# ── Claude API call ───────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are Marcus Cole, a sharp MLB analytics analyst with 15 years experience grading HR and hit props.
You have been given pre-computed Statcast data and pitcher gate scores. Your job is to apply the Sharp Oracle model rules exactly.

SHARP ORACLE MODEL — LOCKED RULES (apply in this order):

S1 PITCHER GATE:
- Gate is pre-computed. OPEN/MOSTLY-OPEN = hittable. CLOSED/MOSTLY-CLOSED = suppressor.
- ALL 9 opposing batters are pulled when gate is CLOSED — no exceptions.
- Bullpen Exposure tier fires separately for Barrel>=15% + xwOBA>=.350 batters when pen ERA>=5.50.

S2 BATTER THRESHOLDS (core 4):
- Barrel% >=15, xwOBA >=.350, EV >=93mph, HH% >=50%
- 4/4 = A, 3/4 = A-, 2/4 = B+, 1/4 = B, 0/4 = C (no pick)

S3 PLATOON CHECK:
- LHB vs RHP = advantage | RHB vs LHP = advantage
- Same-side matchup = -0.5 grade

S4 PARK/WEATHER:
- BOOSTER park: +0.5 grade for Barrel>=20% + xwOBA>=.400
- SUPPRESSOR park: HR dist must clear threshold (see context) or skip HR
- DOME: no weather adjustment
- Temp <=45°F = HARD SUPPRESSOR on all HR picks
- Cold 46-55°F = -0.5 grade

S5 REGRESSION GAP (xwOBA vs wOBA):
- xwOBA >> wOBA (gap >+.050) = regression BUY, upgrade eligible
- wOBA >> xwOBA (gap <-.080) = HOT fade HR; gap <-.120 = fade hits too
- Upgrade #14: Barrel>=20% + xwOBA>=.400 + BOOSTER park + lineup 1-5 = A regardless of pitcher grade or platoon

S6 BULLPEN TIER:
- Pen ERA >=5.50: check all batters with Barrel>=15% + xwOBA>=.350 for standalone tier picks

S7 OUTPUT RULES:
- MAX 2 HR picks + 2 HIT picks per game
- Every pick must have explicit grade justification
- If internal confidence <6/10 = NO PICK — never force a bad pick
- Sleeper HR: only Barrel>=12% + xwOBA>=.325 + BOOSTER park; needs =380 vs NEUTRAL/DOME or hard pass
- Hard stop: avg HR dist <370ft = universal disqualify regardless of other signals

Respond in this exact format for each pick:
PLAYER: [Name] | GRADE: [A/A-/B+/B] | TYPE: [HR/HIT] | CONFIDENCE: [X/10]
JUSTIFICATION: [2-3 sentences covering gate, batter score, platoon, park, gap]
"""

def call_claude(context_str, user_question=None):
    """Call Claude Haiku with the model context and return the response text."""
    if not ANTHROPIC_KEY:
        return "ERROR: ANTHROPIC_API_KEY not set."

    user_content = user_question or "Run the full Sharp Oracle analysis on this game. Apply all rules S1-S7 in order."
    messages = [
        {"role": "user", "content": f"{context_str}\n\n{user_content}"}
    ]

    payload = json.dumps({
        "model"      : MODEL,
        "max_tokens" : 1500,
        "system"     : SYSTEM_PROMPT,
        "messages"   : messages,
    }).encode()

    try:
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data    = payload,
            headers = {
                "Content-Type"      : "application/json",
                "x-api-key"         : ANTHROPIC_KEY,
                "anthropic-version" : "2023-06-01",
            },
            method = "POST",
        )
        with urllib.request.urlopen(req, timeout=60) as r:
            data = json.loads(r.read())
        return "".join(
            block.get("text", "")
            for block in data.get("content", [])
            if block.get("type") == "text"
        )
    except Exception as e:
        return f"ERROR calling Claude: {e}"

# ── Job runner ────────────────────────────────────────────────────────────────
def run_job(jid, raw_lineup_text):
    """
    Full pipeline for one game analysis.
    Step order:
      1. Parse lineup
      2. Ensure stats cache loaded
      3. Fetch weather + bullpen ERA (parallel)
      4. Write statcast table to job (mobile reads this BEFORE analysis completes)
      5. Build context + call Claude
      6. Write result to job
    """
    def step(msg):
        with jobs_lock:
            jobs[jid]["steps"].append({"ts": time.time(), "msg": msg})

    def update(key, val):
        with jobs_lock:
            jobs[jid][key] = val

    try:
        step("Parsing lineup...")
        parsed = parse_lineup_text(raw_lineup_text)
        park_info = get_park_info(parsed.get("park", ""))
        update("parsed", parsed)

        step("Loading Statcast data...")
        if not _stats_loaded:
            load_stats_cache()
        # Wait up to 30s for cache if loading started in background
        waited = 0
        while not _stats_loaded and waited < 30:
            time.sleep(0.5)
            waited += 0.5

        step("Fetching bullpen ERA...")
        # FIX: fetch pen_era here AND pass it into build_context_str below
        pen_era = {}
        for team_key in [parsed.get("team1", ""), parsed.get("team2", "")]:
            if team_key:
                data = fetch_bullpen_era(team_key)
                if data.get("era") is not None:
                    pen_era[team_key.lower()] = data

        # Also fetch for pitcher teams
        for p in parsed.get("pitchers", []):
            t = p.get("team", "")
            if t and t.lower() not in pen_era:
                data = fetch_bullpen_era(t)
                if data.get("era") is not None:
                    pen_era[t.lower()] = data

        step("Fetching weather...")
        weather = fetch_weather(parsed.get("park", ""))

        # Build statcast table for the UI (written BEFORE analysis so mobile gets it)
        step("Compiling Statcast table...")
        slim = []
        all_players = (
            [{"name": p["name"], "type": "PITCHER"} for p in parsed.get("pitchers", [])] +
            [{"name": b["name"], "type": "BATTER",  "pos": b.get("lineup_pos")}
             for b in parsed.get("batters", [])]
        )
        for player in all_players:
            s = get_cached_stats(player["name"])
            row = {
                "name"         : player["name"],
                "type"         : player["type"],
                "barrel_pct"   : s.get("barrel_pct"),
                "exit_velocity": s.get("exit_velocity"),
                "hard_hit_pct" : s.get("hard_hit_pct"),
                "xwoba"        : s.get("xwoba"),
                "woba"         : s.get("woba"),
                "ev50"         : s.get("ev50"),
                "avg_hr_dist"  : s.get("avg_hr_dist"),
                "status"       : "ok" if any(v is not None for v in s.values()) else "no-data",
            }
            if player["type"] == "BATTER" and s:
                _, _, grade, hard_stop = compute_batter_score(s)
                row["grade"] = grade
                row["hard_stop"] = hard_stop
            elif player["type"] == "PITCHER" and s:
                score, gate, _ = compute_pitcher_gate(s)
                row["gate"] = gate
                row["gate_score"] = score
            slim.append(row)

        # Write statcast to job NOW — before Claude call — so /api/statcast works on mobile
        update("statcast", slim)
        step("Statcast ready — running model analysis...")

        # Build context and call Claude
        context_str = build_context_str(
            parsed,
            slim,
            pen_era   = pen_era,    # FIXED: was always None before
            weather   = weather,
            park_info = park_info,
        )
        result_text = call_claude(context_str)
        update("result", result_text)
        update("done", True)
        step("Analysis complete.")

    except Exception as e:
        tb = traceback.format_exc()
        with jobs_lock:
            jobs[jid]["error"] = f"{e}\n{tb}"
            jobs[jid]["done"]  = True
        step(f"ERROR: {e}")

# ── HTTP Server ───────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Sharp Oracle</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,sans-serif;background:#0d1117;color:#e6edf3;min-height:100vh}
header{background:#161b22;border-bottom:1px solid #30363d;padding:16px 24px;display:flex;align-items:center;gap:12px}
header h1{font-size:20px;font-weight:700;color:#58a6ff}
header span{font-size:12px;color:#8b949e;margin-left:auto}
main{max-width:900px;margin:0 auto;padding:24px 16px}
textarea{width:100%;height:180px;background:#161b22;border:1px solid #30363d;border-radius:8px;color:#e6edf3;padding:12px;font-size:13px;resize:vertical}
button{background:#238636;border:none;color:#fff;padding:10px 22px;border-radius:6px;cursor:pointer;font-size:14px;font-weight:600;margin-top:10px}
button:disabled{background:#3d444d;color:#8b949e;cursor:not-allowed}
#steps{margin-top:16px;font-size:12px;color:#8b949e;min-height:20px}
#tabs{display:none;margin-top:20px}
.tab-btn{background:#21262d;border:1px solid #30363d;color:#8b949e;padding:6px 14px;border-radius:4px;cursor:pointer;margin-right:6px}
.tab-btn.active{background:#388bfd22;border-color:#388bfd;color:#58a6ff}
#result-pane,#statcast-pane{display:none}
#result-pane{white-space:pre-wrap;background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;font-size:13px;line-height:1.6;margin-top:12px}
table{width:100%;border-collapse:collapse;font-size:12px;margin-top:12px}
th{background:#21262d;padding:8px;text-align:left;border-bottom:1px solid #30363d;position:sticky;top:0}
td{padding:7px 8px;border-bottom:1px solid #21262d}
tr:hover td{background:#161b22}
.grade-A{color:#3fb950}.grade-A-{color:#79c0ff}.grade-B{color:#d29922}.no-data{color:#6e7681}
.gate-OPEN{color:#3fb950}.gate-CLOSED{color:#f85149}
.hard-stop{color:#f85149;font-weight:700}
#error-box{background:#490202;border:1px solid #f85149;border-radius:8px;padding:14px;color:#ffa198;font-size:13px;margin-top:16px;display:none}
</style>
</head>
<body>
<header>
  <h1>⚾ Sharp Oracle</h1>
  <span id="cache-status">Loading Statcast...</span>
</header>
<main>
  <textarea id="lineup" placeholder="Paste confirmed lineup here (pitchers + batting order)..."></textarea>
  <br>
  <button id="run-btn" onclick="startJob()">Run Analysis</button>
  <div id="steps"></div>
  <div id="error-box"></div>
  <div id="tabs">
    <button class="tab-btn active" onclick="showTab('statcast')">Statcast Data</button>
    <button class="tab-btn" onclick="showTab('result')">Marcus Cole Picks</button>
  </div>
  <div id="statcast-pane">
    <table id="statcast-table">
      <thead><tr>
        <th>Player</th><th>Type</th><th>Gate/Grade</th>
        <th>Barrel%</th><th>EV</th><th>HH%</th>
        <th>xwOBA</th><th>wOBA</th><th>EV50</th><th>HR Dist</th><th>Status</th>
      </tr></thead>
      <tbody id="statcast-body"></tbody>
    </table>
  </div>
  <div id="result-pane"></div>
</main>
<script>
let currentJid = null;
let pollTimer  = null;
let pollErrors = 0;

async function startJob() {
  const text = document.getElementById('lineup').value.trim();
  if (!text) { alert('Paste a lineup first'); return; }
  document.getElementById('run-btn').disabled = true;
  document.getElementById('steps').textContent = 'Starting...';
  document.getElementById('error-box').style.display = 'none';
  document.getElementById('tabs').style.display = 'none';
  document.getElementById('statcast-pane').style.display = 'none';
  document.getElementById('result-pane').style.display = 'none';
  document.getElementById('statcast-body').innerHTML = '';
  document.getElementById('result-pane').textContent = '';
  pollErrors = 0;

  try {
    const r = await fetch('/api/start', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({lineup: text})
    });
    const d = await r.json();
    currentJid = d.jid;
    pollTimer = setInterval(poll, 1200);
  } catch(e) {
    showError('Failed to start: ' + e);
    document.getElementById('run-btn').disabled = false;
  }
}

async function poll() {
  if (!currentJid) return;
  try {
    const r = await fetch('/api/poll?jid=' + currentJid);
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const d = await r.json();
    pollErrors = 0;

    // Update steps
    if (d.steps && d.steps.length) {
      document.getElementById('steps').textContent = d.steps[d.steps.length-1].msg;
    }

    // Show statcast as soon as it's ready (BEFORE analysis completes)
    if (d.has_statcast && !document.getElementById('statcast-pane').innerHTML.includes('tbody')) {
      loadStatcast();
    }

    if (d.done) {
      clearInterval(pollTimer);
      document.getElementById('run-btn').disabled = false;
      if (d.error) {
        showError(d.error);
      } else {
        if (d.has_statcast) loadStatcast();
        if (d.has_result) loadResult();
      }
    }
  } catch(e) {
    pollErrors++;
    if (pollErrors > 8) {
      clearInterval(pollTimer);
      showError('Lost connection to server. Reload and try again.');
      document.getElementById('run-btn').disabled = false;
    }
  }
}

async function loadStatcast() {
  try {
    const r = await fetch('/api/statcast?jid=' + currentJid);
    const rows = await r.json();
    const tbody = document.getElementById('statcast-body');
    tbody.innerHTML = '';
    rows.forEach(row => {
      const tr = document.createElement('tr');
      const gateOrGrade = row.type === 'PITCHER'
        ? `<span class="gate-${row.gate}">${row.gate||'?'} (${row.gate_score??'?'}/4)</span>`
        : `<span class="grade-${(row.grade||'').replace('-','').substring(0,2)}">${row.grade||'?'}</span>`;
      const hs = row.hard_stop ? '<span class="hard-stop">⛔STOP</span>' : '';
      tr.innerHTML = `
        <td>${row.name}</td>
        <td>${row.type}</td>
        <td>${gateOrGrade}${hs}</td>
        <td>${fmt(row.barrel_pct,'%')}</td>
        <td>${fmt(row.exit_velocity,'mph')}</td>
        <td>${fmt(row.hard_hit_pct,'%')}</td>
        <td>${fmt(row.xwoba,'',3)}</td>
        <td>${fmt(row.woba,'',3)}</td>
        <td>${fmt(row.ev50,'mph')}</td>
        <td>${fmt(row.avg_hr_dist,'ft',0)}</td>
        <td class="${row.status==='ok'?'':'no-data'}">${row.status}</td>
      `;
      tbody.appendChild(tr);
    });
    document.getElementById('tabs').style.display = 'block';
    document.getElementById('statcast-pane').style.display = 'block';
    showTab('statcast');
  } catch(e) { console.warn('statcast load error', e); }
}

async function loadResult() {
  try {
    const r = await fetch('/api/result?jid=' + currentJid);
    const d = await r.json();
    document.getElementById('result-pane').textContent = d.result || '(no result)';
    document.getElementById('result-pane').style.display = 'block';
    document.getElementById('tabs').style.display = 'block';
    showTab('result');
  } catch(e) { console.warn('result load error', e); }
}

function fmt(v, unit='', decimals=1) {
  if (v == null) return '<span class="no-data">—</span>';
  return Number(v).toFixed(decimals) + unit;
}

function showTab(which) {
  document.querySelectorAll('.tab-btn').forEach((b,i) => {
    b.classList.toggle('active', (i===0&&which==='statcast')||(i===1&&which==='result'));
  });
  document.getElementById('statcast-pane').style.display = which==='statcast' ? 'block' : 'none';
  document.getElementById('result-pane').style.display   = which==='result'   ? 'block' : 'none';
}

function showError(msg) {
  const box = document.getElementById('error-box');
  box.textContent = msg;
  box.style.display = 'block';
}

// Poll cache status
async function checkCache() {
  try {
    const r = await fetch('/api/cache-status');
    const d = await r.json();
    document.getElementById('cache-status').textContent =
      d.loaded ? `${d.players} players loaded` : 'Loading Statcast...';
    if (!d.loaded) setTimeout(checkCache, 3000);
  } catch(e) {}
}
checkCache();
</script>
</body>
</html>
"""

executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # suppress default access log noise

    def _json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _html(self, body):
        b = body.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path
        qs     = urllib.parse.parse_qs(parsed.query)

        if path == "/" or path == "/index.html":
            self._html(HTML)

        elif path == "/api/cache-status":
            with _stats_lock:
                loaded  = _stats_loaded
                players = len(_stats_cache)
            self._json({"loaded": loaded, "players": players})

        elif path == "/api/poll":
            jid = qs.get("jid", [None])[0]
            with jobs_lock:
                job = dict(jobs.get(jid, {}))
            self._json({
                "done"         : job.get("done", False),
                "error"        : job.get("error"),
                "steps"        : job.get("steps", []),
                "has_statcast" : job.get("statcast") is not None,
                "has_result"   : job.get("result") is not None,
            })

        elif path == "/api/statcast":
            jid = qs.get("jid", [None])[0]
            with jobs_lock:
                data = jobs.get(jid, {}).get("statcast", [])
            self._json(data)

        elif path == "/api/result":
            jid = qs.get("jid", [None])[0]
            with jobs_lock:
                result = jobs.get(jid, {}).get("result", "")
            self._json({"result": result})

        else:
            self.send_error(404)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path

        if path == "/api/start":
            length = int(self.headers.get("Content-Length", 0))
            body   = self.rfile.read(length)
            try:
                data = json.loads(body)
            except Exception:
                self._json({"error": "bad json"}, 400)
                return

            lineup_text = data.get("lineup", "")
            jid = jid_new()
            with jobs_lock:
                jobs[jid] = {
                    "done"    : False,
                    "error"   : None,
                    "steps"   : [],
                    "statcast": None,
                    "result"  : None,
                }
            executor.submit(run_job, jid, lineup_text)
            self._json({"jid": jid})
        else:
            self.send_error(404)

# ── Startup ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Kick off CSV load in background so it's ready by first request
    threading.Thread(target=load_stats_cache, daemon=True).start()

    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Sharp Oracle running on port {PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Shutting down.")
