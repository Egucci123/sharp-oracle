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
MODEL = 'claude-sonnet-4-5'
MODEL_FAST = 'claude-haiku-4-5'  # For mechanical tasks: parsing, extraction, JSON

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
PITCHER GATE (suppression score, 0-4 pts):
  Score 1pt for each metric showing SUPPRESSED contact:
  EV<=88 | HH%<=38 | xwOBA<=.310 | Barrel%<=7 = 1pt each (pitcher suppressing)
  0-1=OPEN(hittable,bet batters) | 2=HALF | 3-4=CLOSED(elite suppressor,fade HR)
  DANGER signals (pitcher being crushed): EV>=92 | HH%>=52 | Brl%>=16 | xwOBA>=.370
  GB-EV<=81=soft-grounders-suppressor(closes gate half step)
  GB-EV>=90=hard-grounders-danger(batters squaring up)
  Pitcher EV50<=77=PLUS-soft-contact | EV50<=74=ELITE | EV50>=84=below-avg | EV50>=87=DANGER
  PITCHER HR VULNERABILITY (when provided — use this to adjust gate):
    HR/9>=1.5=HIGH-HR-RISK(opens gate) | HR/9>=1.8=ELITE-HR-RISK(bet HR hard)
    HR/9<=0.8=HR-SUPPRESSOR(adds half-step gate close)
    FlyBall%>=40%=FLY-BALL-PITCHER(extreme HR vulnerable, keeps gate open even if contact ok)
    GroundBall%>=55%=GROUNDER-PITCHER(suppresses HR regardless of gate score)
    HR/FB>15%=above-average HR rate | HR/FB<7%=LUCKY(regression due, more HRs coming)

BATTER GRADE (0-4 pts):
  Barrel%>=15 | xwOBA>=.350 | EV>=91 | HH%>=50 = 1pt each
  4/4=elite | 3/4=strong | 2/4=moderate | 1/4=weak | 0/4=fade

HPI THRESHOLDS (use Adj-HPI for final grade label):
  Adj-HPI>=7.0=A | 5.5-6.9=A- | 4.0-5.4=B+ | 3.0-3.9=B | <3.0=fade
  Adj-HPI = base HPI - platoon penalty - gate penalty +/- gap/park/pitcher bonuses
  HALF gate (2/4) = -0.5 HPI | CLOSED gate (3/4) = -1.0 HPI
  SAME platoon = -0.5 HPI (-0.3 if Barrel%>=15) | FAV platoon = 0 (no HPI added, just avoids penalty)
  Pitcher HR/9>=1.8 = +0.75 HPI to batters | HR/9>=1.5 = +0.5 | HR/9>=1.2 = +0.25

CONTACT METRICS (all from statcast CSV, calibrated to real 2026 distributions):
  EV50>=103=ELITE(top-10%) | EV50>=100=PLUS(top-50%) | EV50<97=WEAK(bot-25%)
  Sweet Spot%>=38=ELITE-LA(top-25%) | SS%>=32=GOOD-LA(top-40%)
  FB/LD EV>=96=ELITE(top-12%) | FB/LD EV>=94=GOOD(top-25%) | FB/LD EV<90=WEAK(bot-10%)
  Avg HR Dist>=410=ELITE-carry(top-10%) | HR Dist<380=weak-carry(bot-15%)
  Barrel/PA%>=10=ELITE true power rate(top-10%)
  GB-EV = pitcher soft contact signal (lower = softer grounders = real suppressor)

PLATOON: LHB vs RHP=fav | RHB vs LHP=fav | Switch=fav | Same-side=-0.5 HPI (NOT grade drop, NOT veto)
GAP: xwOBA-wOBA. Positive=COLD(buy). Negative=HOT(fade for HR, good for hits).
PARKS (2026 HR park factors — use these over generic BOOSTER/SUPPRESSOR labels):
  ELITE BOOSTER >1.20: GABP-Cincinnati=1.35 | Coors=1.30 | Yankee=1.28 | CBP-Philly=1.22
  BOOSTER 1.10-1.20: Camden-Yards=1.20 | Fenway=1.12 | Dodger=1.10
  SLIGHT BOOST 1.00-1.10: Kauffman=1.05(walls moved in 2026-was suppressor) | Wrigley=1.03
  NEUTRAL 0.90-1.00: AmFam | Globe-Life | Chase-Field | Truist | Busch
  SUPPRESSOR 0.75-0.90: Comerica=0.88 | Tropicana=0.87 | T-Mobile=0.85
  ELITE SUPPRESSOR <0.75: PNC-Pittsburgh=0.66 | Petco=0.78 | Oracle-SF=0.72
  NOTE: Kauffman moved walls in for 2026 — no longer a suppressor. Market may not know.
DOMES(no weather): AmFam/Tropicana/Globe Life/Chase Field
WEATHER: >=85F=boost | <=50F=suppress | <=45F=hard suppress

#1 Bullpen: pen ERA>=5.50 -> Barrel>=15+xwOBA>=.350 = Bullpen Tier
#2 Regression Buy: xwOBA>=.420+gap>=+.100 = STRONG BUY signal, elite hitter underperforming results
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
    "You are Marcus Cole - the sharpest MLB prop analyst alive. 20 years reading Statcast "
    "before anyone knew what exit velocity was. You called Patrick Bailey COLD-BUY gap at +600 "
    "before he went deep. You flagged Oneil Cruz on EV50 alone when everyone else saw a strikeout machine. "
    "You built your edge by understanding what each metric actually predicts — "
    "and what the market assumes it predicts that it doesn't.\n\n"

    "PROCESS - every layer before writing a word:\n"
    "1. Score every batter (Barrel%>=15, xwOBA>=.350, EV>=91, HH%>=50 = 1pt each)\n"
    "2. Set pitcher gates with GB%/CSW%/EV50 modifiers\n"
    "3. Apply platoon, GAP quality, park+weather combined, bullpen tier\n"
    "4. Run all 14 upgrades\n"
    "5. Apply the EDGE MATRIX to every candidate — this is where you find what nobody else sees\n\n"

    "EDGE MATRIX — data-driven edges the market systematically misprices:\n\n"

    "POWER PROFILE:\n"
    "  Every batter has a pre-computed HR POWER INDEX (HPI, 0-10) cross-referencing ALL power signals.\n"
    "  HPI>=7.0 = A-grade HR candidate. HPI 5.0-6.9 = A- candidate. HPI 4.0-4.9 = B+ pick.\n"
    "  Adj-HPI>=7.0=A | 5.5-6.9=A- | 4.0-5.4=B+ | 3.0-3.9=B | <3.0=fade.\n"
    "  Adjusted HPI>=4.0 = list it. Adjusted HPI 3.0-3.9 = sleeper only. <3.0 = fade.\n"
    "  GRADE LABEL must match Adj-HPI: don't call a 4.0 HPI batter 'B+' — that's B+.\n"
    "    4.0 = B+, 3.5 = B, 6.5 = A. Be precise. Grade determines bet sizing.\n\n"
    "  EV50 is the single best HR predictor in Statcast. It removes grounders/weak contact.\n"
    "  FB/LD EV is the EV that matters for HRs — only elevated contact becomes home runs.\n"
    "  FB/LD MISMATCH vs pitcher: Batter FB/LD 97 vs pitcher FB/LD allowed 90 = direct carry edge.\n"
    "  Barrel/PA>=10 on high-K batter = true elite power rate the market undervalues.\n\n"

    "PITCHER READS:\n"
    "  GATE LOGIC: Score 1pt per SUPPRESSION signal. OPEN=hittable=bet batters. CLOSED=suppressor=fade HR.\n"
    "  A CLOSED gate means the pitcher is ELITE and hard to hit — not that he's being crushed.\n"
    "  DANGER signals (pitcher being hit hard): EV>=92, HH%>=52, Brl%>=16, xwOBA>=.370.\n"
    "  DANGER signals mean bet batters HARDER — this pitcher is getting destroyed.\n"
    "  GB-EV<=81: Elite soft grounders — real suppressor, gate closes half step.\n"
    "  GB-EV>=90: Batters squaring him up on grounders — mistake pitch danger.\n"
    "  Pitcher EV50<=77: PLUS soft contact, genuine suppressor.\n"
    "  Pitcher EV50>=84: Batters making hard contact — more hittable than gate suggests.\n"
    "  FB/LD EV>=95: Hard fly balls allowed — HR risk when batters elevate.\n"
    "  FB/LD EV<=90: Soft fly balls — suppressor on elevated contact.\n\n"

    "GAP QUALITY — not all gaps are equal:\n"
    "  CRITICAL: COLD gap (positive) = xwOBA > wOBA = batter hitting ball BETTER than results show = BUY\n"
    "  HOT gap (negative) = wOBA > xwOBA = batter LUCKY, results better than contact = FADE HR always.\n"
    "    Magnitude determines strength: -.010 = weak fade, -.050 = moderate fade, -.080+ = extreme fade.\n"
    "    HOT gap NEVER = 'minimal HR fade' — it always fades HR. Only magnitude varies.\n"
    "  HOT gap (-.000 to -.079): Fades HR only. Hits remain live.\n"
    "  HOT-EXTREME gap (magnitude >=.120, meaning gap <= -.120): Fades HR hard AND suppresses hits. Hard fade both.\n"
    "  HOT-EXTREME gap (magnitude >=.120, meaning gap <= -.120): FADE BOTH HR AND HITS.\n"
    "    A gap of -.159 has magnitude .159 which is >=.120 = FADE HITS TOO.\n"
    "    A gap of -.085 has magnitude .085 which is >=.080 but <.120 = fade HR, hits marginal.\n"
    "    Example: wOBA .433 + xwOBA .274 = gap -.159 = magnitude .159 >=.120 = crash incoming on hits.\n"
    "  HOT gap + wOBA>=.380: Genuinely elite hitter. Hits are real. Only fade HR.\n"
    "  HOT gap + wOBA<.280: Lucky hitter about to crash. Fade HR AND hits.\n"
    "  COLD gap + wOBA<.250: Strong regression buy — xwOBA is the truth.\n"
    "  COLD gap + wOBA .250-.310: Good HR buy — market prices wOBA, you price xwOBA+EV50.\n"
    "  COLD gap>=+.060: Overrides wOBA floor for HR — if xwOBA>=.310 + EV50>=100, it's a pick.\n\n"

    "WEATHER + PARK MATH:\n"
    "  Every 10F below 70F = ~3-4 feet lost carry. Apply to HR distance:\n"
    "  50F = subtract 6-8ft. 45F = subtract 9-12ft. 40F = subtract 12-16ft.\n"
    "  Then check if temp-adjusted HR dist still clears the park.\n"
    "  BOOSTER park: HR dist>=380 = live carry. HR dist<380 = marginal.\n"
    "  NEUTRAL park: HR dist>=390 = live carry. HR dist<390 = marginal, lower confidence.\n"
    "  SUPPRESSOR park: HR dist>=405 = live. HR dist<405 = fade.\n"
    "    HR dist unknown/missing at SUPPRESSOR park = SKIP HR pick, HIT only.\n"
    "  DOME parks (Chase Field, Globe Life, Tropicana, AmFam): no weather adjustment BUT\n"
    "    neutral carry environment. Treat as NEUTRAL park for HR dist thresholds.\n"
    "    Chase Field specifically has 374ft alleys — HR dist<390 is genuine warning track risk.\n"
    "  SUPPRESSOR park + cold weather = only HR dist>=415 batters are live.\n"
    "  BOOSTER park + warm weather = downgrade required EV50 by 2pts.\n\n"

    "SLEEPER DETECTION — 2+ signals = SLEEPER, 3+ = LOCK:\n"
    "  * EV50>=104 + avg EV<90 = power hidden by contact issues, market prices the wrong metric\n"
    "  * FB/LD EV>=97 + avg EV<89 = elite fly ball contact, market sees avg EV and passes\n"
    "  * SS%>=40 + HR dist>=410 = elite HR profile buried under other metrics\n"
    "  * Barrel/PA>=10 + high-K batter = true power understated by Barrel/BBE\n"
    "  * COLD gap>=+.100 + lineup spots 6-9 = market completely ignores him\n"
    "  * Pitcher EV50>=83 + elite power batter (EV50>=103) = hard contact danger, gate undersells HR risk\n"
    "  * HOT gap + wOBA>=.380 = genuine elite hitter, hit props are real value\n"
    "  SLEEPER HR requires 2+ signals AND HR dist>=380. EV>=86. HPI>=3.0 after adjustments.\n"
    "    SAME platoon is -0.5 HPI only — does not block sleeper status.\n"
    "    3+ signals = LOCK SLEEPER regardless of platoon.\n\n"

    "DOUBLE SCRUTINY — every pick checked twice:\n"
    "  HR HARD STOPS — very few true disqualifications:\n"
    "    HR dist<370 = DISQUALIFIED for ANY HR pick. ABSOLUTE HARD STOP. ZERO EXCEPTIONS.\n"
    "    3+ signals do NOT override HR dist<370. Pitcher HR/9 does NOT override. NOTHING overrides.\n"
    "    Wind-adjusted dist<370 = STILL DISQUALIFIED. If wind-adj dist is 365ft = skip, full stop.\n"
    "    Gabriel Arias wind-adj 365ft = disqualified. No exceptions. Ever. List him for hits only.\n"
    "    CONTEXT TAG [HR-DISQUALIFIED-<370] = that batter is DEAD for ALL HR picks.\n"
    "      Not HR #1, not HR #2, not SLEEPER HR, not C-DART. Dead. List for hits only.\n"
    "      If you see [HR-DISQUALIFIED-<370] in a batter's context line = skip for HR entirely.\n"
    "      Raley 368ft [HR-DISQUALIFIED-<370] = no HR pick. Raleigh 373ft same = no HR pick.\n"
    "    SMALL SAMPLE: Brl%>=25 OR ISO>=0.400 OR EV>=99 with SS%>=50 AND HH%>=50 = check attempts.\n"
    "      If these elite numbers come from <20 BBE (tiny sample), DISQUALIFY — noise not signal.\n"
    "      John Rave (Brl%=40, ISO=0.857, 5 BIP) = auto-disqualified. Always check sample size.\n"
    "    HR dist<380 = DISQUALIFIED for SLEEPER HR only. Core picks live above 370.\n"
    "    HOT-EXTREME gap (magnitude>=.120) = FADE BOTH HR AND HITS always.\n"
    "    HOT-EXTREME gap (magnitude .080-.119) + HR dist<park_threshold = disqualify HR.\n"
    "    CLOSED gate (3/4) = -1.0 HPI on HR. HALF gate (2/4) = -0.5 HPI on HR. NOT grade letter drop.\n"
    "  Park-specific HOT-EXTREME disqualification thresholds:\n"
    "    BOOSTER park: HOT-EXTREME + HR dist<375 = disqualify.\n"
    "    NEUTRAL/DOME: HOT-EXTREME + HR dist<385 = disqualify.\n"
    "    SUPPRESSOR: HOT-EXTREME + HR dist<395 = disqualify.\n"
    "  HR SOFT CHECKS: Is HPI>=3.5 after adjustments? GAP direction? Carry clears park?\n"
    "  HIT HARD STOPS: wOBA<.270 = disqualify for hits. EV<80 = disqualify for hits.\n"
    "    EXCEPTION: COLD gap>=+.060 + xwOBA>=.310 overrides wOBA floor — wOBA .240+ is ok.\n"
    "    COLD gap +.030-.059 does NOT override wOBA floor. Needs >=+.060 to activate. Check gap magnitude first.\n"
    "  CONFIDENCE: Use adjusted HPI ONLY — no subjective confidence scoring.\n"
    "    Adj HPI>=5.5 = TOP 2 pick. Adj HPI 4.0-5.4 = B-grade/sleeper. Adj HPI<4.0 = fade.\n"
    "  Fail a HARD STOP = drop it. Everything else = list it with honest grade.\n\n"

    "RECENT FORM — last 14 games (shown as FORM14 in context):\n"
    "  HOT streak (avg>=.310 in last 14 games): upgrade hit prop 1 tier. Market uses season stats, you use form.\n"
    "  COLD streak (avg<=.185 in last 14): downgrade hit prop even if season xwOBA looks good.\n"
    "  FORM14 2+ HRs: batter is locked in on power — upgrade HR confidence regardless of HPI tier.\n"
    "  FORM14 OPS>=.950: elite recent contact — strong hit prop regardless of season numbers.\n"
    "  Pitcher STRUGGLING (ERA>=6.00 last 14 days): gate opens one step. Market prices season ERA.\n"
    "  Pitcher HOT-STRETCH (ERA<=2.50 last 14 days): gate closes one step — he's dealing right now.\n"
    "  ALWAYS check FORM14 before finalizing. HOT batter vs STRUGGLING pitcher = automatic upgrade.\n\n"

    "TIMES-THROUGH-ORDER — mispriced late AB value:\n"
    "  Pitchers allow 15-20% more damage each time through the lineup (research-confirmed).\n"
    "  3rd time through (typically 6th-7th inning for spots 1-4): +.040-.060 wOBA vs 1st time through.\n"
    "  Batters in spots 1-4 face starter 3 times. Spots 5-9 often see bullpen by 3rd PA.\n"
    "  FOR HITS: Spot 1-4 batter with moderate stats who faces tiring starter in 3rd AB = edge.\n"
    "  FOR HRs: 3rd time through + OPEN gate + warm park = upgrade even B-grade power profiles.\n"
    "  This edge is systematic — market prices at-bats equally. You price the 3rd AB higher.\n\n"

    "BULLPEN EXPOSURE FOR HITS:\n"
    "  WEAK pen (ERA>5.00): batters 5-9 frequently get 2nd PA against inferior relievers.\n"
    "  Market prices hit props based on starter matchup. You price reliever exposure too.\n"
    "  Upgrade ANY batter (even low xwOBA .280+) in spots 5-9 facing WEAK pen.\n"
    "  Bottom-of-lineup hit props at BOOSTER parks vs WEAK pens are chronically underpriced.\n\n"

    "LINEUP POSITION = PA VOLUME:\n"
    "  Spot 1-2: ~4.5 PA (most starter exposure, most total plate appearances)\n"
    "  Spot 3-5: ~4.0 PA (3 starter ABs + likely reliever in 4th PA)\n"
    "  Spot 6-9: ~3.3 PA (often see bullpen in 2nd or 3rd PA)\n"
    "  Hit prop value scales with PA count. Spot 1-2 hitters at same odds = better value.\n"
    "  For HR props: spot 1-4 gets 3rd AB vs starter (6th-7th inning) = times-through bonus.\n\n"

    "ML JUICE CHECK:\n"
    "  -200 ML = implied 66.7% win probability. Only bet if you have genuine edge ABOVE that.\n"
    "  -185 ML = 64.9%. If you have 3-4 factors but odds are -185, note juice is steep.\n"
    "  Sweet spot for ML value: -130 to -160 with 3+ factors = real edge.\n"
    "  Always note implied probability vs your confidence. Don't just pick direction — price matters.\n\n"

    "COLD gap +.000-.010 = NEUTRAL, not a buy signal. Only gaps >=+.020 are meaningful COLD.\n\n"



    "DART PICKS — C-DART and B-DART flags in context mean:\n"
    "  C-DART: Long-shot HR pick (+400 or better) the data supports despite low grade.\n"
    "  B-DART: Mid-range HR pick (+300 or better) with real power signal hidden by metrics.\n"
    "  When you see [#11:4/4-FAV-1-5->C-DART] = include as SLEEPER HR if HPI>=3.0 after adj.\n"
    "  When you see [#12:ELITE-BARREL+BOOSTER->B-DART] = include as SLEEPER HR pick.\n"
    "  When you see [#5:LATE-BULLPEN-ERA5.xx->HIT-LIVE] = include as SLEEPER HIT regardless of grade.\n\n"

    "PITCHER GAP NOTE: Pitcher gap direction is OPPOSITE to batters.\n"
    "  Pitcher positive gap (xwOBA > wOBA) = pitcher is BETTER than results show = tighten gate.\n"
    "  Pitcher negative gap (wOBA > xwOBA) = pitcher has been LUCKY = expect more hits coming.\n"
    "  PITCHER-LUCKY flag in context = opens gate half step even if score says HALF/CLOSED.\n\n"

    "MAX HIT SPEED — ceiling power signal:\n"
    "  MAX-SPEED>=115 = top-1% ceiling contact. Even if EV50 is average, he can go nuclear.\n"
    "  Market prices averages. You price ceiling. A batter with MAX-SPEED=116 and EV50=100\n"
    "  has more HR upside than a batter with EV50=103 and MAX-SPEED=108.\n"
    "  Combine MAX-SPEED>=115 with OPEN gate + FAV platoon = automatic sleeper candidate.\n\n"

    "LINEUP SPOT = PA EDGE:\n"
    "  Spot #1-2 with wOBA>=.330 vs HITTABLE pitcher (gate 0-1) = near-lock hit anchor.\n"
    "  Spot #1-2 always call out the ~4.5PA advantage explicitly in picks.\n"
    "  Spot #6-9 with [#5:LATE-BULLPEN] flag = bullpen exposure edge for hits.\n\n"

    "GAP=xwOBA-wOBA. Positive=COLD. Negative=HOT. [PROXY]=no 2026 data, max B.\n\n"

    "OUTPUT FORMAT — PICKS FIRST, ANALYSIS AFTER:\n"
    "Write in this exact order. Do not deviate. Picks come FIRST so they are never cut off.\n\n"

    "## PICKS\n\n"
    "**HR #1:** [Name] ([Team]) | Grade: [A/A-/B+/B] | Adj-HPI: [X] (base=[X] SAME=[0/-0.3/-0.5] gate=[0/-0.5/-1.0] hr9=[0/+0.25/+0.5/+0.75]) | [odds]\n"
    "[FAV platoon = 0 adjustment. Show math inline so it's auditable.]\n"
    "[3 sentences: key metrics + pitcher vulnerability + specific edge.]\n\n"
    "**HR #2:** [Name] ([Team]) | Grade: [X] | Adj-HPI: [X] | [odds]\n"
    "[3 sentences]\n"
    "OR: **NO HR #2** — [one-line reason]\n\n"
    "**HIT #1:** [Name] ([Team]) | Grade: [X] | [odds]\n"
    "[2 sentences: wOBA/xwOBA + pitcher gate + edge]\n\n"
    "**HIT #2:** [Name] ([Team]) | Grade: [X] | [odds]\n"
    "[2 sentences]\n"
    "OR: **NO HIT #2** — [one-line reason]\n\n"
    "**SLEEPER HR #1:** [Name] ([Team]) | [odds] | SIGNALS: [list] | [2 sentences]\n"
    "**SLEEPER HR #2:** [Name] ([Team]) | [odds] | SIGNALS: [list] | [2 sentences]\n"
    "OR: **NO SLEEPER HR** — no mispriced edge.\n\n"
    "**SLEEPER HIT #1:** [Name] ([Team]) | [odds] | [2 sentences]\n"
    "**SLEEPER HIT #2:** [Name] ([Team]) | [odds] | [2 sentences]\n"
    "OR: **NO SLEEPER HIT** — no mispriced edge.\n\n"
    "**ML:** [Team] | [odds] | [2 sentences] OR: **NO ML EDGE**\n\n"
    "**TOTALS:** OVER/UNDER [line] | [odds] | [2 sentences] OR: **NO TOTALS EDGE**\n\n"
    "---\n\n"
    "## GAME READS\n"
    "[Pitching gates, environment math, key batter analysis — concise bullets, no more than 400 words total]\n\n"

    "PICK RULES:\n"
    "Core HR = adjusted HPI>=4.5 after gate/platoon/gap adjustments.\n"
    "Sleeper HR = 3+ independent signals + HR dist>=380 + HPI>=3.0.\n"
    "Pitcher HR/9>=1.5 = +0.5 to every qualified batter's adjusted HPI vs that pitcher.\n"
    "Wind OUT 8mph+: add carry boost to HR dist before checking thresholds.\n"
    "Wind-adjusted dist still <370ft = STILL DISQUALIFIED. Wind can't save a 355ft hitter.\n"
    "SAME platoon = -0.3 HPI for power bats (Barrel%>=15), -0.5 for others.\n"
    "FORM14 2+ HRs in 14 games = +0.5 HPI boost (locked-in power stretch).\n"
    "HOT gap fades HR only. HOT-EXTREME (>=.120) fades HR AND hits.\n"
    "wOBA>=.370 with HOT gap = real hitter, hits remain live, HR faded.\n\n"

    "ML/TOTALS RULES:\n"
    "ML: need 3+ — xwOBA gap>0.050 | bullpen tier edge | run diff>20 | W4+ streak (W2/W3 = NOT meaningful) | home field.\n"
    "  If juice is -145 or better AND 3 factors align = take it. If juice is -185+ = need 4+ factors.\n"
    "  Think about what the market is mispricing: a team's FORM14 hot stretch + starter on STRUGGLING\n"
    "  stretch = real ML edge even if season stats say otherwise. Check FORM14 data in context.\n"
    "OVER: both pitchers hittable (gate 0-1) + wind OUT 8mph+ + temp>80F + weak pen >5.00 ERA.\n"
    "  OVER lean at 2 factors when wind OUT is very strong (12mph+) — wind alone changes run environment.\n"
    "UNDER: both pitchers elite (gate 2+) + wind IN 8mph+ + cold <55F + strong pens <3.50 ERA.\n"
    "  UNDER lean at 2 factors when both starters are CLOSED (3-4/4) + DOME environment.\n"
    "  Grounder pitcher (GB%>50) + wind IN = UNDER lean regardless of other factors.\n"
    "Sharp OVER plays: STRUGGLING pitcher (ERA>6 last 14 days) with OPEN gate in warm outdoor park.\n"
    "Sharp UNDER plays: DOME + two CLOSED gates + combined pens ERA<4.0 = score under 7.\n\n"
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
            'bullpen': {}, 'created': time.time(),
            'is_slate': False, 'parlay_result': None,
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

def call_claude(messages, system=None, max_tokens=4096, model=None):
    payload = {
        'model': model or MODEL,
        'max_tokens': max_tokens,
        'stream': True,
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
        with urllib.request.urlopen(req, timeout=240) as r:
            full_text = []
            for line in r:
                line = line.decode('utf-8').strip()
                if line.startswith('data: '):
                    data_str = line[6:]
                    if data_str == '[DONE]':
                        break
                    try:
                        chunk = json.loads(data_str)
                        if chunk.get('type') == 'content_block_delta':
                            delta = chunk.get('delta', {})
                            if delta.get('type') == 'text_delta':
                                full_text.append(delta.get('text', ''))
                    except Exception:
                        pass
            return ''.join(full_text)
    except Exception as e:
        return f"[Claude error: {e}]"

# ─── STATS CACHE (bulk leaderboard, loaded once per run) ─────────────────────
_stats_cache = {}
_stats_loaded = False
_stats_loading = False
_stats_lock = threading.Lock()

# Recent form cache — populated at startup/daily refresh, read-only during requests
_recent_form_cache = {}
_recent_form_lock  = threading.Lock()

def _fetch_all_recent_form():
    """
    Bulk-fetch last-14-game stats for all players in cache.
    Runs during startup/daily refresh — no user is waiting.
    Uses player_id from statcast CSVs to hit MLB Stats API.
    All failures silently skipped.
    """
    global _recent_form_cache
    if not _stats_cache:
        return

    # Collect unique player IDs
    players_to_fetch = []
    seen_pids = set()
    for key, row in _stats_cache.items():
        pid = row.get('player_id')
        if pid and pid not in seen_pids:
            seen_pids.add(pid)
            # Determine role from primary type
            ptype = row.get('__primary_type__', 'batter')
            # Get name from cache
            lf = row.get('last_name, first_name', '')
            if lf and ',' in lf:
                parts = lf.split(',', 1)
                name = f"{parts[1].strip()} {parts[0].strip()}"
            else:
                name = key  # fallback
            players_to_fetch.append((name, pid, ptype))

    print(f"[RECENT FORM] Fetching last-14 stats for {len(players_to_fetch)} players...")

    def _one_player(args):
        name, pid, ptype = args
        try:
            result = fetch_recent_form(pid, role=ptype, days=14)
            if result:
                return (name, result)
        except Exception:
            pass
        return None

    new_cache = {}
    try:
        # Batch in groups of 50 with small delays to avoid rate limiting
        import time as _time
        batch_size = 50
        for i in range(0, len(players_to_fetch), batch_size):
            batch = players_to_fetch[i:i+batch_size]
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
                futures = list(ex.map(_one_player, batch, timeout=30))
            for r in futures:
                if r:
                    name, form = r
                    new_cache[name] = form
            if i + batch_size < len(players_to_fetch):
                _time.sleep(2)  # be gentle on the API

        with _recent_form_lock:
            _recent_form_cache = new_cache
        print(f"[RECENT FORM] Cached {len(new_cache)}/{len(players_to_fetch)} players")
    except Exception as e:
        print(f"[RECENT FORM] Bulk fetch error: {e}")
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
         f'?type=batter&year={CURRENT_YEAR}&position=&team=&min=1&csv=true'),
        ('expected_pitchers.csv',
         f'https://baseballsavant.mlb.com/leaderboard/expected_statistics'
         f'?type=pitcher&year={CURRENT_YEAR}&position=&team=&min=1&csv=true'),
        # Custom pitcher stats  -  ground ball %, fly ball % (using confirmed Savant field names)
        ('custom_pitchers.csv',
         f'https://baseballsavant.mlb.com/leaderboard/custom'
         f'?year={CURRENT_YEAR}&type=pitcher&filter=&min=1'
         f'&selections=groundballs_percent,flyballs_percent,linedrives_percent,popups_percent'
         f'&chart=false&csv=true'),
        # Custom batter stats  -  ground ball %, fly ball %, ISO (confirmed Savant field names)
        ('custom_batters.csv',
         f'https://baseballsavant.mlb.com/leaderboard/custom'
         f'?year={CURRENT_YEAR}&type=batter&filter=&min=1'
         f'&selections=groundballs_percent,flyballs_percent,linedrives_percent,b_iso'
         f'&chart=false&csv=true'),
    ]
    for filename, url in urls:
        try:
            path = os.path.join(_csv_dir, filename)
            raw = savant_get(url, accept_json=True, timeout=30)
            if raw and len(raw) > 1000 and not raw.strip().startswith('<'):
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(raw)
                first_line = raw.split('\n')[0][:80]
                print(f"[CSV DOWNLOAD] {filename} OK - {len(raw)} bytes | {first_line}")
            else:
                preview = (raw or '')[:100].replace('\n',' ')
                print(f"[CSV DOWNLOAD] {filename} FAILED - got: {preview}")
        except Exception as e:
            print(f"[CSV DOWNLOAD] {filename} ERROR: {e}")

def _daily_refresh_loop():
    """Background thread: refresh CSVs once at startup, then daily at 11am ET."""
    global _stats_cache, _stats_loaded, _stats_loading
    import datetime
    print("[CSV REFRESH] Initial download starting...")
    _download_csvs()
    print("[CSV REFRESH] Initial download complete — loading cache...")
    load_stats_cache()
    print(f"[CSV REFRESH] Cache ready: {len(_stats_cache)} players")

    # Fetch recent form in background after cache is ready
    threading.Thread(target=_fetch_all_recent_form, daemon=True, name="recent-form-init").start()

    while True:
        now = datetime.datetime.utcnow()
        tomorrow = (now + datetime.timedelta(days=1)).replace(
            hour=15, minute=0, second=0, microsecond=0)  # 11am ET = 15:00 UTC
        sleep_secs = (tomorrow - now).total_seconds()
        print(f"[CSV REFRESH] Next refresh in {sleep_secs/3600:.1f} hours")
        time.sleep(max(sleep_secs, 3600))
        print("[CSV REFRESH] Daily refresh starting...")
        _download_csvs()
        with _stats_lock:
            _stats_cache = {}
            _stats_loaded = False
            _stats_loading = False
        load_stats_cache()
        print(f"[CSV REFRESH] Cache refreshed: {len(_stats_cache)} players")
        # Refresh recent form too
        threading.Thread(target=_fetch_all_recent_form, daemon=True, name="recent-form-daily").start()

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

    def pull_rows(rows, player_type='batter'):
        """Store/merge a list of player rows into the stats cache.
        Uses attempts count as tiebreaker — higher attempts = more reliable data.
        Tracks __batter_attempts__ and __pitcher_attempts__ separately to prevent
        pitcher stats overwriting batter stats for two-way players."""
        count = 0
        for row in rows:
            name = parse_name(row)
            if name and len(name) > 2:
                key = normalize_name(name).lower()
                new_attempts = safe_float(row.get('attempts', 0)) or 0
                if key in _stats_cache:
                    existing = _stats_cache[key]
                    # Track attempts by player_type separately
                    type_key = f'__{player_type}_attempts__'
                    existing_type_attempts = existing.get(type_key, 0) or 0
                    # Only merge new data if same player_type OR new has more attempts for this type
                    existing_type = existing.get('__primary_type__', player_type)
                    if player_type == existing_type or new_attempts >= existing_type_attempts:
                        for k, v in row.items():
                            if v not in ('', None, 'null', 'None', 'NaN'):
                                existing[k] = v
                        existing[type_key] = max(new_attempts, existing_type_attempts)
                        if new_attempts >= existing_type_attempts:
                            existing['__primary_type__'] = player_type
                else:
                    _stats_cache[key] = dict(row)
                    _stats_cache[key]['__primary_type__'] = player_type
                    _stats_cache[key][f'__{player_type}_attempts__'] = new_attempts
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
        return pull_rows(parse_raw(raw), player_type)

    def pull_endpoint_raw(raw, player_type):
        return pull_rows(parse_raw(raw), player_type)

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
                   f'?type={player_type}&year={CURRENT_YEAR}&position=&team=&min=1&csv=true')
            n = pull_endpoint(url, player_type)
            print(f"[STATS CACHE] live expected_stats {player_type}={n}")

    # PASS 3: custom Savant CSVs - fly ball %, ground ball %, ISO components
    for player_type, filename, fallback_url in [
        ('pitcher', 'custom_pitchers.csv',
         f'https://baseballsavant.mlb.com/leaderboard/custom'
         f'?year={CURRENT_YEAR}&type=pitcher&filter=&min=1'
         f'&selections=groundballs_percent,flyballs_percent,linedrives_percent,popups_percent'
         f'&chart=false&csv=true'),
        ('batter', 'custom_batters.csv',
         f'https://baseballsavant.mlb.com/leaderboard/custom'
         f'?year={CURRENT_YEAR}&type=batter&filter=&min=1'
         f'&selections=groundballs_percent,flyballs_percent,linedrives_percent,b_iso'
         f'&chart=false&csv=true'),
    ]:
        try:
            raw = load_local(filename)
            if raw:
                n = pull_endpoint_raw(raw, player_type)
                print(f"[STATS CACHE] {filename}={n} rows")
            else:
                n = pull_endpoint(fallback_url, player_type)
                print(f"[STATS CACHE] live custom {player_type}={n}")
        except Exception as e:
            print(f"[STATS CACHE] custom {player_type} SKIP: {e}")

    # PASS 4: Compute estimated HR/9 from data already in cache
    # Uses: slg_allowed - ba_allowed = ISO_allowed (from expected_stats pitcher CSV)
    # and brl_percent (from statcast pitcher CSV)
    # ISO_allowed maps to HR/9: ~(ISO_allowed - 0.050) * 8.5 (calibrated to MLB avg)
    # brl_percent for pitchers cross-checks: brl_pct * 0.15 ≈ HR/9
    try:
        computed = 0
        for key, row in _stats_cache.items():
            # Only process pitcher cache entries (have era field from expected_stats)
            era = row.get('era')
            if era is None:
                continue  # skip batters
            try:
                slg = float(row.get('slg', 0) or 0)
                ba  = float(row.get('ba',  0) or 0)
                iso_allowed = round(slg - ba, 3) if slg > 0 and ba > 0 else None

                brl_pct = float(row.get('brl_percent', 0) or 0)

                # Primary estimate from ISO_allowed (most direct power signal)
                if iso_allowed is not None and iso_allowed > 0:
                    # Anchored at MLB avg: ISO_allowed 0.165 = HR/9 1.15
                    # Slope 11.8 per unit ISO (calibrated against 2026 pitcher data)
                    hr9_from_iso = round(max(0.3, min(3.5, 1.15 + (iso_allowed - 0.165) * 11.8)), 2)
                    row['hr_per_9'] = hr9_from_iso
                    computed += 1
                elif brl_pct > 0:
                    # Fallback: barrel rate proxy (brl_pct * 0.15 ≈ HR/9 roughly)
                    hr9_from_brl = round(max(0.3, min(3.0, brl_pct * 0.15)), 2)
                    row['hr_per_9'] = hr9_from_brl
                    computed += 1
            except Exception:
                pass
        print(f"[STATS CACHE] HR/9 estimated from ISO_allowed for {computed} pitchers")
    except Exception as e:
        print(f"[STATS CACHE] HR/9 estimation SKIP: {e}")

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

def get_cached_stats(name, cache=None, player_type='batter'):
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
        'fly_ball_pct': None, 'ground_ball_pct': None,
        'hr_per_9': None, 'batter_fb_pct': None,
        'iso': None,
        'gap': None, 'fetch_status': 'not found', 'data_source': None,
    }
    if not name:
        result['fetch_status'] = 'no name'
        return result

    # SOURCE 1: bulk cache — use pre-loaded cache if provided
    row = get_cached_stats(name, cache=cache)

    # Validate: if cache row's primary type doesn't match requested role, check attempts
    if row is not None:
        cache_primary = row.get('__primary_type__', 'batter')
        requested_type = 'pitcher' if info.get('role') == 'PITCHER' else 'batter'
        if cache_primary != requested_type:
            # Check if the other-type has many more attempts (bad merge)
            other_attempts = row.get(f'__{cache_primary}_attempts__', 0) or 0
            my_attempts = row.get(f'__{requested_type}_attempts__', 0) or 0
            if other_attempts > my_attempts * 3:
                # Cache dominated by wrong player type - use cautiously
                print(f"[CACHE WARN] {name}: primary={cache_primary} requested={requested_type} "
                      f"({other_attempts} vs {my_attempts} attempts)")
                # For batters requested but pitcher data dominant: mark as proxy
                if requested_type == 'batter':
                    result['fetch_status'] = 'found/no stats'
                    return result
    if row:
        def g(row, *keys):
            """Returns first positive non-null value. Use for EV, HH% etc where 0 means missing."""
            for k in keys:
                v = row.get(k)
                if v not in (None, '', 'null', 'None', 'NaN'):
                    f = safe_float(v)
                    if f is not None and f > 0:
                        return f
            return None
        def gz(row, *keys):
            """Returns first non-null value including zero. Use for barrel% where 0 is valid."""
            for k in keys:
                v = row.get(k)
                if v not in (None, '', 'null', 'None', 'NaN'):
                    f = safe_float(v)
                    if f is not None:
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
        result['barrel_pct']    = sane('barrel_pct', gz(row,
            'brl_percent',          # CSV statcast (brl_percent = per BBE)
            'barrel_batted_rate',   # CSV expected_statistics + JSON
            'brl_bip_percent'))
        result['barrel_pa']     = sane('barrel_pct', gz(row,
            'brl_pa'))              # Barrel per PA  -  more stable
        result['xwoba']         = sane('xwoba', g(row,
            'est_woba',             # CSV expected_statistics
            'xwoba'))               # statcast / JSON
        result['woba']          = sane('woba', g(row, 'woba'))
        result['gb_pct']        = sane('gb_pct', g(row,
            'groundballs_percent', 'gb_percent', 'gb_pct'))
        result['csw_pct']       = sane('csw_pct', g(row,
            'csw', 'csw_pct', 'called_strike_whiff_pct'))
        # New fields from exit_velocity CSV
        result['ev50']          = sane('exit_velocity', g(row, 'ev50'))
        result['sweet_spot_pct']= sane('hard_hit_pct', g(row,
            'anglesweetspotpercent', 'sweet_spot_percent', 'la_sweet_spot_percent'))
        result['fbld_ev']       = sane('exit_velocity', g(row, 'fbld'))
        result['gb_ev']         = sane('exit_velocity', g(row, 'gb'))  # GB exit velocity (separate from GB rate)
        result['avg_hr_dist']   = g(row, 'avg_hr_distance')
        result['max_hit_speed'] = sane('exit_velocity', g(row, 'max_hit_speed'))
        # Fly ball % and ground ball % — try all known field name formats
        # confirmed working Savant names: flyballs_percent, groundballs_percent
        result['fly_ball_pct'] = g(row,
            'flyballs_percent', 'flyball_percent', 'fly_ball_percent',
            'p_flyball', 'b_flyball', 'flyBalls')
        result['ground_ball_pct'] = g(row,
            'groundballs_percent', 'groundball_percent', 'ground_ball_percent',
            'p_groundball', 'b_groundball', 'groundBalls')
        # If GB% known but FB% missing, estimate FB = 100 - GB - 28 (avg LD+popup)
        if result['fly_ball_pct'] is None and result['ground_ball_pct'] and result['ground_ball_pct'] > 0:
            est = round(100 - result['ground_ball_pct'] - 28, 1)
            if est > 5:
                result['fly_ball_pct'] = est
        # MLB Stats API fallbacks (merged in PASS 4)
        if result['fly_ball_pct'] is None and row.get('mlb_fb_pct'):
            result['fly_ball_pct'] = row.get('mlb_fb_pct')
        if result['ground_ball_pct'] is None and row.get('mlb_gb_pct'):
            result['ground_ball_pct'] = row.get('mlb_gb_pct')
        # batter_fb_pct = fly_ball_pct for batters (used in HPI)
        if result['fly_ball_pct'] is not None:
            result['batter_fb_pct'] = result['fly_ball_pct']
        # hr_per_9 from MLB Stats API merge
        result['hr_per_9'] = g(row, 'hr_per_9', 'homeRunsPer9')
        # ISO directly from Savant custom CSV (b_iso field)
        result['iso'] = g(row, 'b_iso', 'iso')
        if result['iso'] is None:
            slg = g(row, 'b_slg_percent', 'slg_percent', 'slg')
            avg = g(row, 'b_batting_average', 'batting_average', 'ba')
            if slg and avg:
                result['iso'] = round(slg - avg, 3)

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
    import functools
    fetch = functools.partial(fetch_one_player, cache=cache)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(fetch, players))
    ok = sum(1 for r in results if r.get('fetch_status') == 'ok')
    no_data = [r.get('name','?') for r in results if r.get('fetch_status') == 'found/no stats']
    if no_data:
        print(f"[STATS] No 2026 data (PROXY): {', '.join(no_data)}")

    # Pre-compute HPI for batters so it appears in the statcast table
    hpi_computed = 0
    hpi_errors = []
    for r in results:
        if r.get('role') == 'BATTER':
            try:
                _, breakdown, _, _ = compute_batter_score(r)
                if 'HPI=' in breakdown:
                    r['hpi'] = float(breakdown.split('HPI=')[1].split('/')[0])
                    hpi_computed += 1
                else:
                    r['hpi'] = 0.0
            except Exception as e:
                hpi_errors.append(f"{r.get('name','?')}:{e}")
                r['hpi'] = 0.0
    print(f"[HPI] Pre-computed for {hpi_computed}/{sum(1 for r in results if r.get('role')=='BATTER')} batters"
          + (f" | errors: {hpi_errors[:3]}" if hpi_errors else ""))

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

# Center field compass bearing = direction you look FROM home plate TO center field
# Wind blowing OUT = wind coming from BEHIND home plate, blowing toward CF
# e.g. Wrigley CF is to the NE (about 45deg). Famous "blowing out" = SW wind (225) blows toward NE
PARK_CF_BEARING = {
    'Yankee Stadium':           45,   # CF is to NE from home plate
    'Fenway Park':              340,  # CF is to NNW (center field gap)
    'Wrigley Field':            45,   # CF is to NE; E/NE winds blow OUT
    'Citizens Bank Park':       10,   # CF is to N/NNE
    'Coors Field':              350,  # CF is to N/NNW
    'Oracle Park':              310,  # CF is to NW (winds off bay = crosswind)
    'Petco Park':               340,  # CF is to NNW
    'Dodger Stadium':           330,  # CF is to NNW
    'Comerica Park':            350,  # CF is to N
    'Great American Ball Park': 340,  # CF is to NNW
    'Nationals Park':           340,  # CF is to NNW
    'Busch Stadium':            345,  # CF is to NNW
    'Truist Park':              350,  # CF is to N
    'PNC Park':                 330,  # CF is to NNW
    'Citi Field':               350,  # CF is to N
    'Kauffman Stadium':         340,  # CF is to NNW
    'Target Field':             335,  # CF is to NNW
    'T-Mobile Park':            320,  # CF is to NW
    'Angel Stadium':            330,  # CF is to NNW
    'Progressive Field':        340,  # CF is to NNW
    'Camden Yards':             340,  # CF is to NNW
    'Rate Field':               340,  # CF is to NNW
    'Guaranteed Rate Field':    340,
    'Daikin Park':              330,
    'Sutter Health Park':       330,  # CF to NNW; famous gusty W wind = crosswind
    'Minute Maid Park':         340,
}

def compute_wind_impact(wind_degree, wind_mph, park_name):
    """
    Returns dict with wind_dir_label, impact ('OUT'/'IN'/'CROSS'/'CALM'), carry_boost.
    OUT = wind helping carry toward CF = HR boost
    IN  = wind against CF = HR suppressor
    CROSS = wind across field = minimal impact
    """
    if wind_mph is None or wind_mph < 3:
        return {'impact': 'CALM', 'label': 'Calm', 'carry_boost': 0}
    if wind_degree is None:
        return {'impact': 'UNKNOWN', 'label': f'{wind_mph}mph unknown direction', 'carry_boost': 0}

    cf_bearing = PARK_CF_BEARING.get(park_name)
    if cf_bearing is None:
        return {'impact': 'UNKNOWN', 'label': f'{wind_mph}mph (park bearing unknown)', 'carry_boost': 0}

    # Meteorological convention: wind_degree = direction wind is COMING FROM
    # Wind blows TOWARD (wind_degree + 180) % 360
    # "Blowing out" = wind blowing TOWARD outfield = wind coming FROM home plate side
    # At Wrigley (CF=270/W): wind FROM E (90deg) blows toward W = blows OUT
    wind_toward = (wind_degree + 180) % 360

    # diff = angle between wind_toward and CF bearing
    diff = abs(wind_toward - cf_bearing)
    if diff > 180:
        diff = 360 - diff

    # diff < 45 = blowing out toward CF, diff > 135 = blowing in from CF
    if diff <= 45:
        impact = 'OUT'
        strength = 'hard' if wind_mph >= 15 else ('moderate' if wind_mph >= 8 else 'light')
        carry_boost = round(wind_mph * 1.5, 1)  # ~1.5ft per mph blowing out
        label = f'{wind_mph}mph BLOWING OUT ({strength}) +{carry_boost}ft carry est.'
    elif diff >= 135:
        impact = 'IN'
        strength = 'hard' if wind_mph >= 15 else ('moderate' if wind_mph >= 8 else 'light')
        carry_boost = round(-wind_mph * 1.2, 1)
        label = f'{wind_mph}mph BLOWING IN ({strength}) {carry_boost}ft carry est.'
    else:
        impact = 'CROSS'
        carry_boost = 0
        label = f'{wind_mph}mph crosswind (minimal HR impact)'

    return {'impact': impact, 'label': label, 'carry_boost': carry_boost}

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

    temp_f, wind_mph, wind_degree, wind_dir_raw, condition = None, None, None, None, 'Unknown'

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
            temp_f        = safe_float(best.get('temperature'))
            wind_str      = best.get('windSpeed', '0 mph')
            wind_mph      = safe_float(wind_str.split()[0]) if wind_str else None
            wind_dir_raw  = best.get('windDirection', None)  # e.g. "W", "NNW"
            condition     = best.get('shortForecast', 'Unknown')
            # NWS gives compass direction string, convert to degrees
            COMPASS_TO_DEG = {
                'N':0,'NNE':22,'NE':45,'ENE':67,'E':90,'ESE':112,'SE':135,'SSE':157,
                'S':180,'SSW':202,'SW':225,'WSW':247,'W':270,'WNW':292,'NW':315,'NNW':337
            }
            if wind_dir_raw in COMPASS_TO_DEG:
                wind_degree = COMPASS_TO_DEG[wind_dir_raw]
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
            temp_f       = safe_float(cur.get('temp_F'))
            wind_mph     = safe_float(cur.get('windspeedMiles'))
            wind_degree  = safe_float(cur.get('winddirDegree'))  # wttr.in provides this
            wind_dir_raw = cur.get('winddir16Point', None)
            condition    = cur.get('weatherDesc', [{}])[0].get('value', 'Unknown')
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

    # Compute wind impact (blowing out/in/cross)
    wind_impact = compute_wind_impact(wind_degree, wind_mph, park_name)
    if wind_impact['impact'] != 'CALM' and wind_mph and wind_mph >= 5:
        notes.append(f'WIND: {wind_impact["label"]}')

    return {
        'temp_f': temp_f, 'condition': condition,
        'wind_mph': wind_mph, 'wind_degree': wind_degree,
        'wind_dir': wind_dir_raw, 'wind_impact': wind_impact,
        'flag': flag,
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


def fetch_recent_form(player_id, role='batter', days=14):
    """Fetch last 14 game stats from MLB Stats API. Returns dict or None on failure."""
    if not player_id:
        return None
    try:
        group = 'pitching' if role == 'pitcher' else 'hitting'
        url = (f'https://statsapi.mlb.com/api/v1/people/{player_id}/stats'
               f'?stats=lastXGames&group={group}&gameType=R&lastXGames={days}&season={CURRENT_YEAR}')
        req = urllib.request.Request(url,
              headers={'User-Agent': 'SharpOracle/1.0', 'Accept': 'application/json'})
        with urllib.request.urlopen(req, timeout=3) as r:
            data = json.loads(r.read())
        splits = []
        for sg in data.get('stats', []):
            splits.extend(sg.get('splits', []))
        if not splits:
            return None
        stat = splits[0].get('stat', {}) if splits else {}
        if role == 'pitcher':
            era = stat.get('era')
            ip  = stat.get('inningsPitched', '0')
            hr  = stat.get('homeRuns', 0)
            bb  = stat.get('baseOnBalls', 0)
            k   = stat.get('strikeOuts', 0)
            return {
                'recent_era': safe_float(era),
                'recent_ip': safe_float(ip),
                'recent_hr_allowed': hr,
                'recent_bb': bb,
                'recent_k': k,
            }
        else:
            avg  = stat.get('avg', '.000')
            hr   = stat.get('homeRuns', 0)
            hits = stat.get('hits', 0)
            ab   = stat.get('atBats', 0)
            ops  = stat.get('ops', '.000')
            slg  = stat.get('sluggingPercentage', '.000')
            bb   = stat.get('baseOnBalls', 0)
            return {
                'recent_avg':  safe_float(avg),
                'recent_hr':   int(hr) if hr else 0,
                'recent_hits': int(hits) if hits else 0,
                'recent_ab':   int(ab) if ab else 0,
                'recent_ops':  safe_float(ops),
                'recent_slg':  safe_float(slg),
                'recent_bb':   int(bb) if bb else 0,
            }
    except Exception as e:
        return None


def fetch_umpire_tendency(game_date=None):
    """Fetch today's umpire K/BB tendency from Baseball Reference. Returns label or None."""
    try:
        import datetime
        date_str = game_date or datetime.date.today().strftime('%Y-%m-%d')
        url = f'https://www.baseball-reference.com/previews/{date_str[:4]}/{date_str}.shtml'
        req = urllib.request.Request(url, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=8) as r:
            html = r.read().decode('utf-8', errors='replace')
        # Look for umpire info
        ump_idx = html.find('Home Plate Umpire')
        if ump_idx == -1:
            ump_idx = html.find('umpire')
        if ump_idx > 0:
            snippet = html[ump_idx:ump_idx+200]
            # Extract umpire name
            m = re.search(r'([A-Z][a-z]+ [A-Z][a-z]+).*?(\d+\.?\d*)%.*?K', snippet)
            if m:
                return {'name': m.group(1), 'k_pct': safe_float(m.group(2))}
        return None
    except Exception:
        return None

def fetch_bullpen_era(team_name):
    """Fetch bullpen ERA from MLB Stats API (reliable, free, no scraping)."""
    key = normalize_name(team_name).lower()
    team_id = None
    for k, v in TEAM_IDS.items():
        if k in key or key in k:
            team_id = v
            break
    if not team_id:
        return {'era': None, 'tier': 'UNKNOWN'}

    try:
        # MLB Stats API: team bullpen stats (pitchers with 0 GS / relief appearances)
        url = (f'https://statsapi.mlb.com/api/v1/teams/{team_id}/stats'
               f'?stats=season&group=pitching&season={CURRENT_YEAR}&gameType=R')
        req = urllib.request.Request(url, headers={'User-Agent': 'SharpOracle/1.0', 'Accept': 'application/json'})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())

        # Team pitching stats — extract ERA and use team ERA as proxy for bullpen quality
        for stat_group in data.get('stats', []):
            splits = stat_group.get('splits', [])
            for split in splits:
                stat = split.get('stat', {})
                era = stat.get('era')
                if era:
                    try:
                        era_f = float(era)
                        if 0 < era_f < 15:
                            tier = 'WEAK' if era_f >= 5.50 else 'AVERAGE' if era_f >= 4.50 else 'SOLID' if era_f >= 3.50 else 'ELITE'
                            return {'era': round(era_f, 2), 'tier': tier}
                    except Exception:
                        pass
    except Exception as e:
        print(f"[BULLPEN] MLB Stats API failed for {team_name}: {e}")

    # Fallback: covers.com scrape
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
    abbrev = next((v for k, v in COVERS_ABBREVS.items() if k in key or key in k), None)
    if abbrev:
        try:
            url2 = f'https://www.covers.com/sport/baseball/mlb/statistics/team-bullpenera/{CURRENT_YEAR}'
            req2 = urllib.request.Request(url2, headers=_HEADERS)
            with urllib.request.urlopen(req2, timeout=10) as r2:
                html = r2.read().decode('utf-8', errors='replace')
            idx = html.find(f'>{abbrev}<')
            if idx == -1:
                idx = html.find(f'">{abbrev}')
            if idx > 0:
                nm = re.search(r'([0-9]+\.[0-9]+)', html[idx:idx+200])
                if nm:
                    era = safe_float(nm.group(1))
                    if era and 0 < era < 10:
                        tier = 'WEAK' if era >= 5.50 else 'AVERAGE' if era >= 4.50 else 'SOLID' if era >= 3.50 else 'ELITE'
                        return {'era': round(era, 2), 'tier': tier}
        except Exception:
            pass

# ─── LINEUP PARSING ──────────────────────────────────────────────────────────
def _pre_extract_lineup(raw):
    """
    Pre-extract lineup structure in Python before sending to Claude.
    Handles the MLB app's interleaved two-column format.
    Returns a structured dict or None if extraction fails.
    """
    import re as _re
    
    # Check format BEFORE stripping markdown (determines interleaved vs sequential)
    had_markdown_links = bool(_re.search(r'\[[^\]]+\]\([^)]+\)', raw))
    
    # Strip markdown links for processing
    raw_clean = _re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', raw)
    lines = [l.strip() for l in raw_clean.strip().split('\n') if l.strip()]
    
    # Find @ sign to get teams
    at_idx = next((i for i, l in enumerate(lines) if l == '@'), -1)
    if at_idx < 1:
        return None
    away_team = lines[at_idx - 1]
    home_team = lines[at_idx + 1] if at_idx + 1 < len(lines) else '?'
    
    # Find park name (appears after team records like "(31-44)")
    park_name = '?'
    skip_words = {away_team.lower(), home_team.lower(), 'rhs', 'lhp', 'rhp', 'lineup'}
    for i in range(at_idx + 1, min(at_idx + 12, len(lines))):
        l = lines[i]
        if (l and not l.startswith('(') and 'PM' not in l and 'AM' not in l 
                and '•' not in l and '@' not in l and 'ERA' not in l
                and l not in ('RHP', 'LHP') and 'Lineup' not in l
                and l.lower() not in skip_words
                and len(l) > 4):
            # Park names typically have "Park", "Field", "Stadium", "Center", "Way" etc
            # OR are multi-word proper nouns
            park_name = l
            break
    
    # Find lineup header positions
    lineup_hdrs = [(i, l) for i, l in enumerate(lines) if 'Lineup' in l and len(l) < 20]
    
    # Find pitchers (appear before lineup headers)
    pitchers = []
    pitcher_hands = []
    for i, l in enumerate(lines):
        if l in ('RHP', 'LHP'):
            # Previous non-empty line should be pitcher name
            for j in range(i-1, -1, -1):
                if lines[j].strip():
                    pitcher_name = lines[j].strip()
                    if pitcher_name not in ('RHP', 'LHP') and '(' not in pitcher_name and 'Lineup' not in pitcher_name:
                        pitchers.append(pitcher_name)
                        pitcher_hands.append('R' if l == 'RHP' else 'L')
                    break
    
    # Extract player lines (lines with hand indicator like "(R)" or "(L)" or "(S)")
    player_pat = _re.compile(r'^(.+?)\s+\(([RLS])\)\s+\w+$')
    all_players = []
    
    # Find where lineups start (after both lineup headers)
    lineup_start = 0
    if len(lineup_hdrs) >= 2:
        lineup_start = lineup_hdrs[-1][0] + 1
    elif len(lineup_hdrs) == 1:
        lineup_start = lineup_hdrs[0][0] + 1
    
    for line in lines[lineup_start:]:
        # Handle numbered format: "1. Name (R) POS" 
        line_clean = _re.sub(r'^\d+\.\s*', '', line)
        m = player_pat.match(line_clean)
        if m:
            name = m.group(1).strip()
            hand = m.group(2)
            all_players.append({'name': name, 'hand': hand})
    
    if len(all_players) < 4:
        return None  # Not enough players found
    
    # Split into away and home batters
    # MLB app format: interleaved (away1, home1, away2, home2...) OR sequential
    if len(all_players) >= 14:
        # Format already detected above: had_markdown_links = interleaved, else sequential
        mid = len(all_players) // 2
        
        if had_markdown_links:
            # Interleaved format: away1, home1, away2, home2...
            away_batters = [{'name': p['name'], 'hand': p['hand'], 'lineup_pos': i//2 + 1} 
                           for i, p in enumerate(all_players) if i % 2 == 0][:9]
            home_batters = [{'name': p['name'], 'hand': p['hand'], 'lineup_pos': i//2 + 1} 
                           for i, p in enumerate(all_players) if i % 2 == 1][:9]
        else:
            # Sequential format: first 9 = away, next 9 = home
            away_batters = [{'name': p['name'], 'hand': p['hand'], 'lineup_pos': i+1} 
                           for i, p in enumerate(all_players[:mid])]
            home_batters = [{'name': p['name'], 'hand': p['hand'], 'lineup_pos': i+1} 
                           for i, p in enumerate(all_players[mid:])]
    
    # Assign pitchers: first pitcher = away pitcher (faces home batters)
    away_pitcher = {'name': pitchers[0] if pitchers else '?', 'hand': pitcher_hands[0] if pitcher_hands else 'R'}
    home_pitcher = {'name': pitchers[1] if len(pitchers) > 1 else '?', 'hand': pitcher_hands[1] if len(pitcher_hands) > 1 else 'R'}
    
    return {
        'away_team': away_team,
        'home_team': home_team,
        'park_name': park_name,
        'away_pitcher': away_pitcher,
        'home_pitcher': home_pitcher,
        'away_batters': away_batters,
        'home_batters': home_batters,
        'game_date': '',
    }


def parse_lineup(raw, game_date=None):
    """
    Parse raw lineup paste using Python pre-extraction + Claude fallback.
    Handles any format: MLB.com, Rotowire, FantasyPros, plain text, etc.
    Extracts: away_team, home_team, away_pitcher, home_pitcher,
              away_batters, home_batters (with hand, lineup_pos).
    """
    # Strip markdown links before parsing: [Text](url) -> Text
    import re as _re_md
    raw = _re_md.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', raw)
    
    # Try Python pre-extraction first (more reliable than Claude for MLB app format)
    pre = _pre_extract_lineup(raw)
    if pre and pre.get('away_team', '?') != '?' and len(pre.get('away_batters', [])) >= 7:
        pre['game_date'] = game_date or ''
        # Log what we extracted
        print(f"[PARSE-PY] {pre['away_team']} @ {pre['home_team']} | "
              f"Away P: {pre['away_pitcher']['name']} | Home P: {pre['home_pitcher']['name']} | "
              f"Away batters: {len(pre['away_batters'])} | Home batters: {len(pre['home_batters'])}")
        return pre

    prompt = f"""You are parsing an MLB lineup from the MLB app or similar source.
Extract the information and return JSON only, no other text.

CRITICAL FORMAT NOTES:
- Teams appear as "AwayTeam@HomeTeam" (no spaces) OR "Away Team @ Home Team" (with spaces)
  Team BEFORE the @ = AWAY team. Team AFTER the @ = HOME team.
- Game status may show "warmup •", "Live", a time like "9:38 PM", or nothing — ignore it
- Park/stadium name appears after the team records like "(31-44)"
- Pitchers: listed with hand (LHP/RHP) and ERA stats. Away pitcher listed FIRST, home pitcher SECOND.
- Away pitcher FACES HOME batters. Home pitcher FACES AWAY batters.

LINEUP SECTION HEADERS (CRITICAL):
- You will see headers like "SEA Lineup" or "NYY Lineup" before each team's batters
- The team abbreviation in the header tells you which team those batters belong to
- "SEA Lineup" = Seattle Mariners batters → these go in away_batters (SEA is the @ team)
- "PIT Lineup" = Pittsburgh Pirates batters → these go in home_batters
- First lineup block = AWAY team batters. Second lineup block = HOME team batters.
- Players are numbered 1-9 in each block. The first 1-9 block = away team. Second 1-9 block = home team.

EXAMPLE:
  Mariners@Pirates (away=Mariners, home=Pirates)
  George Kirby RHP ← Mariners pitcher (away)
  Mitch Keller RHP ← Pirates pitcher (home)
  SEA Lineup ← Mariners batters start here (away_batters)
  1. Crawford ... 9. Emerson
  PIT Lineup ← Pirates batters start here (home_batters)  
  1. Horwitz ... 9. Triolo

- Batter hand: R=right, L=left, S=switch. Extract from (R)/(L)/(S) after name.
- If hand not listed, use "R" as default.

Return this exact JSON:
{{
  "away_team": "team nickname e.g. Mariners",
  "home_team": "team nickname e.g. Pirates",
  "away_pitcher": {{"name": "First Last", "hand": "R or L"}},
  "home_pitcher": {{"name": "First Last", "hand": "R or L"}},
  "away_batters": [{{"name": "First Last", "hand": "R/L/S", "lineup_pos": 1}}],
  "home_batters": [{{"name": "First Last", "hand": "R/L/S", "lineup_pos": 1}}],
  "park_name": "stadium name",
  "game_date": "{game_date or ''}"
}}

Lineup to parse:
{raw}"""

    resp = call_claude([{'role': 'user', 'content': prompt}], max_tokens=2000, model=MODEL_FAST)
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

def parse_multi_lineup(raw, game_date=None):
    """
    Parse multiple games from MLB app format or any multi-game paste.
    Returns list of parsed game dicts, one per game.
    """
    # Strip markdown links before parsing
    import re as _re_md2
    raw = _re_md2.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', raw)

    prompt = f"""You are parsing multiple MLB games from the MLB app format.

FORMAT: Each game block looks like:
  AwayTeam
  @
  HomeTeam
  (record) Time ParkName (record)
  AwayPitcherFirstLast / RHP or LHP / ERA stats
  HomePitcherFirstLast / RHP or LHP / ERA stats
  Then TWO lineup columns side by side:
    Left column = AWAY team batters (1-9)
    Right column = HOME team batters (1-9)
  Each batter line: "Name (H) POS" where H is R/L/S

CRITICAL RULES:
- Team BEFORE @ = AWAY team. Team AFTER @ = HOME team.
- Away pitcher FACES HOME batters. Home pitcher FACES AWAY batters.
- The two lineup columns appear side by side — split them correctly.
- Extract hand from (R), (L), (S) after each name.
- Include ALL 9 batters per team.
- Multiple games appear sequentially — parse EACH game separately.

Return ONLY a valid JSON array (one object per game), no other text:
[
  {{
    "away_team": "Yankees",
    "home_team": "Tigers",
    "park_name": "Comerica Park",
    "game_time": "6:10 PM",
    "away_pitcher": {{"name": "Gerrit Cole", "hand": "R", "era": "2.57"}},
    "home_pitcher": {{"name": "Framber Valdez", "hand": "L", "era": "4.09"}},
    "away_batters": [{{"name": "Amed Rosario", "hand": "R", "lineup_pos": 1, "position": "3B"}}],
    "home_batters": [{{"name": "Ben Rice", "hand": "L", "lineup_pos": 1, "position": "DH"}}]
  }}
]

Input to parse:
{raw}"""

    resp = call_claude([{'role': 'user', 'content': prompt}], max_tokens=8000)
    try:
        m = re.search(r'\[.*\]', resp, re.DOTALL)
        if m:
            games = json.loads(m.group())
            if isinstance(games, list):
                for g in games:
                    if 'game_date' not in g:
                        g['game_date'] = game_date or ''
                return games
    except Exception as e:
        print(f"[PARSE_MULTI] Error: {{e}}")
    return []


PARLAY_SYSTEM = """You are Marcus Cole. You've been capping sports for 15 years. You don't build spreadsheets — you find edges and bet them. Today you have picks across multiple MLB games and you need to find the BEST combinations to win money.

HOW YOU THINK:
You start by scanning everything for the single best bet on the board. Then you look for 2-3 picks that genuinely CONNECT — shared pitcher vulnerability, stacked wind conditions, regression bombs in the same environment, or a narrative the market hasn't figured out yet. You never pad a parlay just to hit a leg count. Every leg has to earn its spot.

You know that a crisp 2-leg parlay at +600 that hits 30% of the time prints money. You know that a 5-leg parlay needs a genuine thesis — not just "five good picks" but "five picks where the same edge is working simultaneously across different games."

You also know sleepers are where the real money is. A +600 sleeper HR from a 7-hole bat with 106 EV50 facing a pitcher with 2.27 HR/9 is BETTER than a +280 HR from the cleanup hitter. The market prices lineups by reputation. You price them by contact metrics and pitcher vulnerability.

YOUR APPROACH TODAY:
1. Read every pick from every game carefully
2. Find the 3-5 picks you'd actually bet yourself — with conviction
3. Build parlays around those conviction plays, adding complementary legs that share the same underlying edge
4. For sleepers: look at lineup spots 6-9 first — that's where the market sleeps
5. Use COLD gaps (+.050 or more) as your primary regression signal — the market prices wOBA, you price xwOBA
6. Pitcher HR/9 >= 1.5 means bet that pitcher's opponents hard, especially at booster parks or with wind OUT
7. Don't touch same-game HR parlays. Ever.

HARD RULES (non-negotiable):
- NO same-game HR parlays. One HR leg per game. Zero exceptions.
- Max 2 legs per game in hit parlays.  
- Wind-adjusted HR dist <370ft = dead. Skip it.
- ML/Totals: only use picks explicitly confirmed. Outside-the-box lean is fine but label it clearly.

WHAT TO BUILD:
Think of this as a betting card — the plays you'd actually give to someone with money on the line.

## HR CARD

**THE BEST HR BET TODAY**
[Name | Team vs Pitcher | Odds]
Tell me exactly why this is the best HR on the board today. Pitcher's vulnerability, batter's profile, park/wind edge, why the odds are wrong. Be specific. 2-4 sentences of real reasoning.
**Suggested: 0.5u**

**2-LEG HR PARLAY** | ~[odds]
[Two picks that share a genuine edge — both facing HR-vulnerable pitchers, both with wind boost, both COLD gap regression, whatever connects them]
The edge: [1-2 sentences on what these two have in common that makes this more than just two picks]
**Suggested: 0.5u**

**3-LEG HR PARLAY** | ~[odds]  
[Three picks — should have a clear thesis. Pitcher vulnerability stack, environmental stack, or value stack]
The edge: [what's the thesis]
**Suggested: 0.25u**

**4-LEG HR PARLAY** | ~[odds]
[Include at least one sleeper leg here — a spot 6-9 batter or a COLD gap bomb the market is ignoring]
The edge: [why these four]
**Suggested: 0.25u**

**5-LEG HR PARLAY** | ~[odds]
[Only build this if you have 5 genuinely strong legs. Don't force it.]
The edge: [the thesis]
**Suggested: 0.1u**
OR: **NO 5-LEG HR PARLAY TODAY** — [reason, e.g. "only 4 clean HR plays on the board"]

---

## HIT CARD

Think about hit parlays differently than HR parlays. Hits are about volume + contact quality + pitcher hittability. Your best hit anchors are:
- Leadoff/2-hole hitters getting 4.5 PA vs OPEN gate pitchers
- Batters with wOBA >= .360 regardless of lineup spot
- COLD gap batters where xwOBA > wOBA by .040+ (market is pricing the wrong number)
- Times-through-order: spots 1-4 facing a starter for the 3rd time in innings 6-7

MAX 5 LEGS PER HIT PARLAY. Spread across at least 3 games.

**2-LEG HIT PARLAY** | ~[odds]
[Two near-locks. Elite contact hitters vs hittable pitchers. This should hit 55%+]
Why: [specific edge for each leg in one line each]
**Suggested: 1u**

**3-LEG HIT PARLAY** | ~[odds]
Why: [what connects these three]
**Suggested: 0.75u**

**4-LEG HIT PARLAY** | ~[odds]
Why: [the angle — volume stack, COLD gap stack, dome anchors, whatever the thesis is]
**Suggested: 0.5u**

**5-LEG HIT PARLAY** | ~[odds]
[This is your max. 5 strong legs, 3+ different games, clear thesis]
Why: [the thesis that makes this more than just 5 picks]
**Suggested: 0.25u**

---

## ML/TOTALS CARD

**CONFIRMED PICKS FROM TODAY'S ANALYSES:**
[List what was explicitly picked — ML and Totals from each game]

**BEST ML/TOTALS PARLAY #1** | ~[odds] | [CONFIRMED]
[Use confirmed picks. 2-3 legs max for the best confidence play]
Why it works: [brief reasoning]
**Suggested: 0.5u**

**ML/TOTALS PARLAY #2** | ~[odds] | [SHARP LEAN]
[Can include 1 outside-the-box lean if the data is clear — label it]
Why: [reasoning]
**Suggested: 0.25u**

**ML/TOTALS PARLAY #3** | ~[odds] | [AGGRESSIVE]
[More legs or more aggressive leans — this is the lottery ticket version]
Why: [reasoning]
**Suggested: 0.1u**

---

## MARCUS'S CARD — THE PLAYS HE'D ACTUALLY BET

After building everything above, be direct:

**TOP PLAY OF THE DAY:** [single best bet on the board, any type]
**BEST PARLAY:** [the one parlay you'd actually bet today with actual conviction]
**SLEEPER OF THE SLATE:** [the most mispriced bet — something at +500 or better that has 3+ signals]
**FADE OF THE DAY:** [what looks good on paper but you're staying away from and why]
**UNIT SUMMARY:** [how you'd allocate 2 units across these plays — be specific]
"""



def generate_parlays(all_game_picks, game_summaries):
    """
    Generate sharp parlays from all picks across the slate.
    all_game_picks: list of {game, picks: [{name, team, type, odds, adj_hpi, signals}]}
    game_summaries: list of {game, park, park_cat, temp, wind_impact, pen_era}
    """
    # Build context for parlay Claude call
    lines = ["=== SLATE PICKS FOR PARLAY ANALYSIS ===\n"]
    lines.append("GAME ENVIRONMENTS:")
    for gs in game_summaries:
        lines.append(f"  {gs.get('game','?')}: {gs.get('park','?')} [{gs.get('park_cat','?')}] "
                     f"| {gs.get('temp','?')}F | Wind: {gs.get('wind_label','?')} "
                     f"| Pens: {gs.get('pen_summary','?')}")
    lines.append("")
    
    lines.append("ALL PICKS BY GAME:")
    hr_picks_flat = []
    hit_picks_flat = []
    
    for gp in all_game_picks:
        game = gp.get('game', '?')
        picks = gp.get('picks', [])
        if not picks:
            continue
        lines.append(f"\n{game}:")
        for p in picks:
            ptype = p.get('type', '?')
            name = p.get('name', '?')
            team = p.get('team', '?')
            odds = p.get('odds', '?')
            hpi = p.get('adj_hpi', '?')
            grade = p.get('grade', '?')
            sigs = p.get('signals', '')
            lines.append(f"  {ptype} | {name} ({team}) | Grade:{grade} HPI:{hpi} | {odds} | {sigs}")
            if 'HR' in ptype:
                hr_picks_flat.append({**p, 'game': game})
            else:
                hit_picks_flat.append({**p, 'game': game})
    
    lines.append(f"\n\nTotal HR picks: {len(hr_picks_flat)}")
    lines.append(f"Total HIT picks: {len(hit_picks_flat)}")
    lines.append("\nBuild the best parlays. Think outside the box.")
    
    ctx = "\n".join(lines)
    
    result = call_claude(
        [{'role': 'user', 'content': ctx}],
        system=PARLAY_SYSTEM,
        max_tokens=4000
    )
    return result

# ─── SCORING ─────────────────────────────────────────────────────────────────
def compute_pitcher_gate(p):
    ev   = p.get('exit_velocity')
    ev50 = p.get('ev50')
    hh   = p.get('hard_hit_pct')
    xw   = p.get('xwoba')
    brl  = p.get('barrel_pct')
    gb_ev = p.get('gb_ev')
    csw  = p.get('csw_pct')
    fbld = p.get('fbld_ev')
    fb_pct  = p.get('fly_ball_pct')   # NEW: actual fly ball rate
    gb_pct  = p.get('ground_ball_pct') # NEW: actual ground ball rate
    hr9  = p.get('hr_per_9')          # NEW: HR/9 from MLB Stats API

    # GATE: measures how DANGEROUS the pitcher is to batters
    # Score 1pt for each metric showing SUPPRESSED contact (low = good for pitcher)
    score = sum([
        1 if (ev  is not None and ev  <= 88.0) else 0,
        1 if (hh  is not None and hh  <= 38.0) else 0,
        1 if (xw  is not None and xw  <= 0.310) else 0,
        1 if (brl is not None and brl <= 7.0)  else 0,
    ])
    gate = 'OPEN' if score <= 1 else 'HALF' if score == 2 else 'CLOSED'

    # Fly ball rate override — most important HR predictor for pitchers
    fb_flag = ''
    if fb_pct is not None:
        if fb_pct >= 42:
            fb_flag = f' FB%={fb_pct}(FLY-BALL-PITCHER->HR-vulnerable)'
            if gate == 'HALF': gate = 'HALF->OPEN'
            elif gate == 'CLOSED': gate = 'CLOSED->HALF'
        elif fb_pct >= 38:
            fb_flag = f' FB%={fb_pct}(above-avg-fly-balls)'
        elif fb_pct <= 32:
            fb_flag = f' FB%={fb_pct}(GROUNDER-PITCHER->HR-safe)'
            if gate == 'OPEN': gate = 'OPEN->HALF'
        else:
            fb_flag = f' FB%={fb_pct}(neutral)'

    # HR/9 override — actual HR rate
    hr9_flag = ''
    if hr9 is not None:
        if hr9 >= 1.8:
            hr9_flag = f' HR/9={hr9}(ELITE-HR-RISK->bet-HR-hard)'
            if gate in ('HALF', 'CLOSED', 'HALF->OPEN', 'CLOSED->HALF'):
                gate = 'OPEN'  # HR/9 1.8+ overrides any gate closure
        elif hr9 >= 1.4:
            hr9_flag = f' HR/9={hr9}(HIGH-HR-RISK)'
            if gate == 'CLOSED': gate = 'HALF'
        elif hr9 <= 0.7:
            hr9_flag = f' HR/9={hr9}(HR-SUPPRESSOR)'
            if gate == 'OPEN': gate = 'OPEN->HALF'
        else:
            hr9_flag = f' HR/9={hr9}'

    # Danger flags for context (pitcher being hit hard)
    danger = []
    if ev  is not None and ev  >= 92.0: danger.append(f'EV={ev}(DANGER)')
    if hh  is not None and hh  >= 52.0: danger.append(f'HH%={hh}(DANGER)')
    if brl is not None and brl >= 16.0: danger.append(f'Brl%={brl}(DANGER)')
    if xw  is not None and xw  >= 0.370: danger.append(f'xwOBA={xw}(DANGER)')

    # GB-EV modifier
    gb_flag = ''
    if gb_ev is not None:
        if gb_ev <= 81:
            gb_flag = f' GB-EV={gb_ev}(ELITE-soft-grounders,bot-10%)'
            if gate == 'OPEN': gate = 'OPEN->HALF'
            elif gate == 'HALF': gate = 'HALF->CLOSED'
        elif gb_ev <= 85:
            gb_flag = f' GB-EV={gb_ev}(soft-contact,bot-25%)'
        elif gb_ev >= 90:
            gb_flag = f' GB-EV={gb_ev}(HARD-grounders-danger,top-10%)'
        else:
            gb_flag = f' GB-EV={gb_ev}(neutral)'

    # EV50 for pitchers
    ev50_flag = ''
    if ev50 is not None:
        if ev50 <= 74:    ev50_flag = f' EV50={ev50}(ELITE-soft-contact,top-10%)'
        elif ev50 <= 77:  ev50_flag = f' EV50={ev50}(PLUS-soft-contact,top-25%)'
        elif ev50 <= 80:  ev50_flag = f' EV50={ev50}(avg-soft-contact)'
        elif ev50 <= 83:  ev50_flag = f' EV50={ev50}(below-avg,batters-making-contact)'
        else:             ev50_flag = f' EV50={ev50}(DANGER-hard-contact,bottom-10%)'

    # FB/LD EV
    fbld_flag = ''
    if fbld is not None:
        if fbld >= 95:   fbld_flag = f' FB/LD={fbld}(HARD-fly-balls->HR-risk,top-12%)'
        elif fbld <= 90: fbld_flag = f' FB/LD={fbld}(SOFT-fly-balls->suppressor,bottom-10%)'

    pts = [
        f"EV={ev or 'N/A'}{'✓suppress' if ev and ev<=88 else '(hittable)' if ev else ''}",
        f"HH%={hh or 'N/A'}{'✓suppress' if hh and hh<=38 else '(hittable)' if hh else ''}",
        f"xwOBA={xw or 'N/A'}{'✓suppress' if xw and xw<=0.310 else '(hittable)' if xw else ''}",
        f"Brl%={brl or 'N/A'}{'✓suppress' if brl is not None and brl<=7 else '(hittable)' if brl is not None else ''}",
    ]
    danger_str = ' DANGER:'+','.join(danger) if danger else ''
    return score, gate, ' | '.join(pts) + danger_str + fb_flag + hr9_flag + gb_flag + ev50_flag + fbld_flag

    # GATE = pitcher suppression score
    # Score 1pt for each metric showing SUPPRESSED contact (low = good for pitcher)
    # 0-1 = OPEN (hittable, bet batters)
    # 2   = HALF (moderate suppression)
    # 3-4 = CLOSED (elite suppressor, fade batters HR)
    score = sum([
        1 if (ev  is not None and ev  <= 88.0) else 0,   # EV<=88 = soft contact (bot-25%)
        1 if (hh  is not None and hh  <= 38.0) else 0,   # HH%<=38 = low hard hits (bot-25%)
        1 if (xw  is not None and xw  <= 0.310) else 0,  # xwOBA<=.310 = quality suppressed
        1 if (brl is not None and brl <= 7.0)  else 0,   # Barrel%<=7 = barrels suppressed
    ])
    gate = 'OPEN' if score <= 1 else 'HALF' if score == 2 else 'CLOSED'

    # GB exit velocity modifier — lower = softer grounders = real suppressor
    # High GB EV means batters squaring up but hitting down — one mistake elevated = gone
    gb_flag = ''
    if gb_ev is not None:
        if gb_ev <= 81:
            gb_flag = f' GB-EV={gb_ev}(ELITE-soft-grounders->suppressor,bot-10%)'
            if gate == 'OPEN': gate = 'OPEN->HALF'
            elif gate == 'HALF': gate = 'HALF->CLOSED'
        elif gb_ev <= 85:
            gb_flag = f' GB-EV={gb_ev}(soft-contact,bot-25%)'
        elif gb_ev >= 90:
            gb_flag = f' GB-EV={gb_ev}(HARD-grounders->mistake-pitch-danger,top-10%)'
        else:
            gb_flag = f' GB-EV={gb_ev}(neutral)'

    # CSW% modifier
    csw_flag = ''
    if csw is not None:
        if csw >= 30:   csw_flag = f' CSW%={csw}(ELITE-MISS)'
        elif csw < 25:  csw_flag = f' CSW%={csw}(hittable)'

    # EV50 - for pitchers, LOWER ev50 = better (softer contact allowed)
    # Real distribution: Min=65, P10=73.8, P25=76.1, Median=78.4, P75=80.4, P90=82.2, Max=86.5
    ev50_flag = ''
    if ev50 is not None:
        if ev50 <= 74:    ev50_flag = f' EV50={ev50}(ELITE-soft-contact,top-10%)'
        elif ev50 <= 77:  ev50_flag = f' EV50={ev50}(PLUS-soft-contact,top-25%)'
        elif ev50 <= 80:  ev50_flag = f' EV50={ev50}(avg-soft-contact)'
        elif ev50 <= 83:  ev50_flag = f' EV50={ev50}(below-avg,batters-making-contact)'
        else:             ev50_flag = f' EV50={ev50}(DANGER-hard-contact,bottom-10%)'

    # FB/LD EV for pitchers - lower = better
    fbld_flag = ''
    if fbld is not None:
        if fbld >= 95:   fbld_flag = f' FB/LD={fbld}(HARD-fly-balls->HR-risk,top-12%)'
        elif fbld <= 90: fbld_flag = f' FB/LD={fbld}(SOFT-fly-balls->suppressor,bottom-10%)'

    # Danger flags for context (pitcher being hit hard)
    danger = []
    if ev  is not None and ev  >= 92.0: danger.append(f'EV={ev}(DANGER)')
    if hh  is not None and hh  >= 52.0: danger.append(f'HH%={hh}(DANGER)')
    if brl is not None and brl >= 16.0: danger.append(f'Brl%={brl}(DANGER)')
    if xw  is not None and xw  >= 0.370: danger.append(f'xwOBA={xw}(DANGER)')

    pts = [
        f"EV={ev or 'N/A'}{'✓suppress' if ev and ev<=88 else '(hittable)' if ev else ''}",
        f"HH%={hh or 'N/A'}{'✓suppress' if hh and hh<=38 else '(hittable)' if hh else ''}",
        f"xwOBA={xw or 'N/A'}{'✓suppress' if xw and xw<=0.310 else '(hittable)' if xw else ''}",
        f"Brl%={brl or 'N/A'}{'✓suppress' if brl is not None and brl<=7 else '(hittable)' if brl is not None else ''}",
    ]
    danger_str = ' DANGER:'+','.join(danger) if danger else ''
    return score, gate, ' | '.join(pts) + danger_str + gb_flag + csw_flag + ev50_flag + fbld_flag

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
        elif ev50 < 97:   ev50_flag = f' EV50={ev50}(WEAK,bot-25%)'
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
        if fbld >= 96:   fbld_flag = f' FB/LD-EV={fbld}(ELITE,top-12%)'
        elif fbld >= 94: fbld_flag = f' FB/LD-EV={fbld}(GOOD,top-25%)'
        elif fbld < 90:  fbld_flag = f' FB/LD-EV={fbld}(WEAK,bottom-10%)'

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

    # HR POWER INDEX — cross-reference all power signals into single score (0-10)
    # This gives Marcus a single number to rank candidates instead of mentally integrating
    hpi = 0.0
    hpi_signals = []

    # Core contact quality (0-3 pts)
    if brl is not None:
        if brl >= 20:   hpi += 1.5; hpi_signals.append(f'Brl%{brl}(ELITE)')
        elif brl >= 15: hpi += 1.0; hpi_signals.append(f'Brl%{brl}(GOOD)')
        elif brl >= 12: hpi += 0.5; hpi_signals.append(f'Brl%{brl}(OK)')
    if ev is not None:
        if ev >= 93:    hpi += 1.0; hpi_signals.append(f'EV{ev}(ELITE)')
        elif ev >= 91:  hpi += 0.5; hpi_signals.append(f'EV{ev}(GOOD)')
    if hh is not None:
        if hh >= 52:    hpi += 1.0; hpi_signals.append(f'HH%{hh}(ELITE)')
        elif hh >= 45:  hpi += 0.5; hpi_signals.append(f'HH%{hh}(GOOD)')

    # True power metrics (0-3 pts) — these are the real HR predictors
    if ev50 is not None:
        if ev50 >= 106: hpi += 2.0; hpi_signals.append(f'EV50={ev50}(ELITE++)')
        elif ev50 >= 104: hpi += 1.5; hpi_signals.append(f'EV50={ev50}(ELITE)')
        elif ev50 >= 102: hpi += 1.0; hpi_signals.append(f'EV50={ev50}(PLUS)')
        elif ev50 >= 100: hpi += 0.5; hpi_signals.append(f'EV50={ev50}(OK)')
    if fbld is not None:
        if fbld >= 99:  hpi += 1.5; hpi_signals.append(f'FB/LD={fbld}(ELITE++)')
        elif fbld >= 97: hpi += 1.0; hpi_signals.append(f'FB/LD={fbld}(ELITE)')
        elif fbld >= 94: hpi += 0.5; hpi_signals.append(f'FB/LD={fbld}(GOOD)')
    if brl_pa is not None:
        if brl_pa >= 12: hpi += 1.0; hpi_signals.append(f'Brl/PA={brl_pa}(ELITE)')
        elif brl_pa >= 8: hpi += 0.5; hpi_signals.append(f'Brl/PA={brl_pa}(GOOD)')

    # Carry (0-2 pts)
    if hr_dist and hr_dist > 0:
        if hr_dist >= 415:   hpi += 2.0; hpi_signals.append(f'DIST={int(hr_dist)}(ELITE++)')
        elif hr_dist >= 410: hpi += 1.5; hpi_signals.append(f'DIST={int(hr_dist)}(ELITE)')
        elif hr_dist >= 400: hpi += 1.0; hpi_signals.append(f'DIST={int(hr_dist)}(GOOD)')
        elif hr_dist >= 390: hpi += 0.5; hpi_signals.append(f'DIST={int(hr_dist)}(OK)')
        elif hr_dist < 370:  hpi -= 3.0; hpi_signals.append(f'DIST={int(hr_dist)}(HARD-STOP)')
        elif hr_dist < 385:  hpi -= 1.5; hpi_signals.append(f'DIST={int(hr_dist)}(SHORT)')

    # SS% launch angle bonus (0-0.5 pts)
    if ss is not None and ss >= 38:
        hpi += 0.5; hpi_signals.append(f'SS%={ss}(ELITE-LA)')

    # Batter fly ball rate bonus — higher = more HR chances
    bfb = b.get('batter_fb_pct')
    if bfb is not None:
        if bfb >= 48:   hpi += 1.0; hpi_signals.append(f'FB%={bfb}(elite-elevation)')
        elif bfb >= 40: hpi += 0.5; hpi_signals.append(f'FB%={bfb}(good-elevation)')

    # ISO bonus — raw power, market underweights this
    iso = b.get('iso')
    if iso is not None:
        if iso >= 0.250:  hpi += 1.0; hpi_signals.append(f'ISO={iso}(ELITE-power)')
        elif iso >= 0.200: hpi += 0.5; hpi_signals.append(f'ISO={iso}(GOOD-power)')

    # GAP modifier
    if gap is not None:
        if gap >= 0.100:  hpi += 1.0; hpi_signals.append('COLD-BUY')
        elif gap > 0:     hpi += 0.5; hpi_signals.append('COLD')
        elif gap < -0.080: hpi -= 1.5; hpi_signals.append('HOT-EXTREME')
        elif gap < 0:     hpi -= 0.5; hpi_signals.append('HOT')

    hpi = round(min(10.0, max(0.0, hpi)), 1)
    hpi_str = f' HPI={hpi}/10[{",".join(hpi_signals[:4])}]'

    # HR DISTANCE HARD STOPS — enforce in code, not just prompt
    hr_stop = ''
    if hr_dist and hr_dist > 0:
        if hr_dist < 370:
            hr_stop = f' ⛔HR-DIST={hr_dist}(HARD-STOP:<370-DISQUALIFIED-ANY-HR)'
        elif hr_dist < 385:
            hr_stop = f' ⚠HR-DIST={hr_dist}(DISQUALIFIED-SLEEPER-HR)'
        elif hr_dist < 390:
            hr_stop = f' HR-DIST={hr_dist}(MARGINAL-neutral-park)'
        elif hr_dist < 405:
            hr_stop = f' HR-DIST={hr_dist}(LIVE-booster/neutral)'
        elif hr_dist >= 410:
            hr_stop = f' HR-DIST={hr_dist}(ELITE-CARRY)'
        else:
            hr_stop = f' HR-DIST={hr_dist}(avg)'
        hrd_flag = ''  # replace hrd_flag with hr_stop

    # HOT-EXTREME gap hard stops
    gap_stop = ''
    if gap is not None:
        mag = abs(gap)
        if mag >= 0.120:
            gap_stop = f' ⛔HOT-EXTREME-{mag:.3f}(FADE-BOTH-HR-AND-HITS)'
        elif mag >= 0.080 and gap < 0:
            gap_stop = f' ⚠HOT-EXTREME-{mag:.3f}(FADE-HR-FLAG-HITS)'

    pts = [
        f"Brl={brl or 'N/A'}{'✓' if brl and brl>=15 else '✗'}",
        f"xwOBA={xw or 'N/A'}{'✓' if xw and xw>=0.350 else '✗'}",
        f"EV={ev or 'N/A'}{'✓' if ev and ev>=91 else '✗'}",
        f"HH%={hh or 'N/A'}{'✓' if hh and hh>=hh_threshold else '✗'}",
    ]
    # Max hit speed — ceiling power signal (different from avg EV50)
    mhs = b.get('max_hit_speed')
    mhs_flag = ''
    if mhs and mhs >= 115: mhs_flag = f' MAX-SPEED={mhs}(ELITE-ceiling)'
    elif mhs and mhs >= 112: mhs_flag = f' MAX-SPEED={mhs}(PLUS-ceiling)'

    extra = ev50_flag + ss_flag + fbld_flag + hr_stop + brl_pa_flag + mhs_flag + gap_stop + hpi_str
    upgrade_flags = upgrade2_flag + upgrade3_flag + upgrade10_flag + upgrade14_flag
    return score, ' | '.join(pts) + extra + upgrade_flags, gap_flag, hr_cap

def compute_platoon(bh, ph):
    bh, ph = str(bh).upper(), str(ph).upper()
    if bh == 'S': return 'FAV(SW)'
    if (bh=='L' and ph=='R') or (bh=='R' and ph=='L'): return 'FAV'
    return 'SAME'

# ─── CONTEXT BUILDER ─────────────────────────────────────────────────────────
def build_context(parsed, all_statcast, weather, park_name, park_cat, pen_era, recent_form=None):
    home = parsed.get('home_team', '?')
    away = parsed.get('away_team', '?')
    wx = weather
    temp_str = f"{wx['temp_f']}F" if wx.get('temp_f') else 'N/A'
    wind_impact = wx.get('wind_impact', {})
    wind_str = f"{wx.get('wind_mph','N/A')} mph"
    if wx.get('wind_dir'):
        wind_str += f" from {wx['wind_dir']}"
    wind_impact_str = wind_impact.get('label', 'unknown direction') if wind_impact else ''

    lines = [
        f"GAME: {away} @ {home}",
        f"PARK: {park_name} [{park_cat}]",
        f"WEATHER: {temp_str} | {wx.get('condition','N/A')} | Wind {wind_str} | {wx.get('flag','NEUTRAL')}",
        f"WIND IMPACT: {wind_impact_str}" if wind_impact_str else "WIND IMPACT: unknown",
        f"WEATHER NOTE: {wx.get('notes','')}",
        '',
        'VERIFIED ASSIGNMENTS:',
        f"  HOME team: {home}  -  plays at {park_name}",
        f"  AWAY team: {away}",
    ]

    # Pre-compute wind carry boost for HR distance adjustments
    wind_carry = 0
    wi = weather.get('wind_impact', {}) if weather else {}
    if wi:
        boost = wi.get('carry_boost', 0) or 0
        if wi.get('impact') in ('OUT', 'IN'):
            wind_carry = boost  # positive=OUT, negative=IN

    # Pitcher gates
    pitcher_gates = {}
    pitcher_hr9_bonus = {}  # team -> HPI bonus for batters vs that pitcher's HR/9
    lines.append('')
    lines.append('=== PITCHERS (gate pre-computed) ===')
    for p in all_statcast:
        if p.get('role') != 'PITCHER':
            continue
        score, gate, breakdown = compute_pitcher_gate(p)
        faces = p.get('faces_team', '?')
        pitcher_gates[faces] = {'gate': gate, 'score': score, 'hand': p.get('hand','?'), 'name': p.get('name','?')}

        # HR/9 bonus for batters facing this pitcher
        hr9 = p.get('hr_per_9')
        if hr9:
            if hr9 >= 1.8:   pitcher_hr9_bonus[faces] = 0.75
            elif hr9 >= 1.5: pitcher_hr9_bonus[faces] = 0.5
            elif hr9 >= 1.2: pitcher_hr9_bonus[faces] = 0.25
        g = p.get('gap')
        gs = f"{g:+.3f}" if g is not None else 'N/A'
        proxy = '[PROXY] ' if 'no stat' in str(p.get('fetch_status','')) else ''

        # Recent pitcher form
        pitcher_form_str = ''
        if recent_form and p.get('name') in recent_form:
            pf = recent_form[p['name']]
            if pf:
                era14 = pf.get('recent_era')
                ip14  = pf.get('recent_ip', 0)
                hr14  = pf.get('recent_hr_allowed', 0)
                if era14 is not None and ip14 and float(str(ip14).split('.')[0]) >= 5:
                    ptrend = 'HOT-STRETCH' if era14 <= 2.50 else ('STRUGGLING' if era14 >= 6.00 else 'NEUTRAL')
                    pitcher_form_str = f' | FORM14=ERA{era14:.2f}({ip14}IP,{hr14}HR){ptrend}'

        # Pitcher gap: positive = pitcher BETTER than wOBA suggests (opposite of batter)
        gap_note = ''
        if g is not None:
            if g > 0.030:   gap_note = '(PITCHER-OUTPERFORMING-xwOBA->gate-may-be-soft)'
            elif g < -0.030: gap_note = '(PITCHER-LUCKY->expect-more-hits-coming)'

        lines.append(
            f"  {proxy}{p.get('name','?')} ({p.get('hand','?')}HP) "
            f"pitches for {p.get('team','?')}, FACES {faces} batters | "
            f"GATE={score}/4={gate} | gap={gs}{gap_note}{pitcher_form_str} | {breakdown}"
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

            # Upgrade #13: No 2026 data = max B-grade cap already handled by PROXY tag
            u13 = ''
            if b.get('fetch_status','') and 'no stat' in str(b.get('fetch_status','')):
                u13 = ' [#13:NO-2026-DATA->MAX-B]'

            # Store HPI in batter dict for table display
            if 'HPI=' in breakdown:
                try:
                    b['hpi'] = float(breakdown.split('HPI=')[1].split('/')[0])
                except:
                    pass

            # Wind-adjusted HR distance
            hr_dist = b.get('avg_hr_dist') or 0
            wind_adj_str = ''
            if hr_dist and hr_dist > 0 and wind_carry != 0:
                adj_dist = round(hr_dist + wind_carry, 1)
                if adj_dist < 370:
                    wind_adj_str = f' WIND-ADJ-DIST={adj_dist}ft({hr_dist}+{wind_carry:+.0f})[HR-DISQUALIFIED-<370]'
                else:
                    wind_adj_str = f' WIND-ADJ-DIST={adj_dist}ft({hr_dist}+{wind_carry:+.0f})'
            elif hr_dist and hr_dist > 0 and wind_carry == 0:
                if hr_dist < 370:
                    wind_adj_str = f' HR-DIST={hr_dist}ft[HR-DISQUALIFIED-<370]'

            # Pitcher HR/9 bonus for this batter's team
            hr9_bonus = pitcher_hr9_bonus.get(b.get('team', ''), 0)
            hr9_bonus_str = f' PITCHER-HR9-BONUS=+{hr9_bonus}HPI' if hr9_bonus else ''

            # Recent form
            form_str = ''
            if recent_form and b.get('name') in recent_form:
                f = recent_form[b['name']]
                if f:
                    avg = f.get('recent_avg')
                    hr  = f.get('recent_hr', 0)
                    ab  = f.get('recent_ab', 0)
                    ops = f.get('recent_ops')
                    if avg is not None and ab and ab >= 10:
                        trend = 'HOT' if avg >= 0.310 else ('COLD-STREAK' if avg <= 0.185 else 'NEUTRAL')
                        hr_str = f' {hr}HR' if hr else ''
                        form_str = f' | FORM14={avg:.3f}{hr_str}({ab}AB){trend}'
                        if ops:
                            form_str += f' OPS={ops:.3f}'

            pos = b.get('lineup_pos', 5)
            pa_est = 4.5 if pos <= 2 else (4.0 if pos <= 5 else 3.3)
            pa_str = f'~{pa_est}PA'

            # Upgrade #5: Late Bullpen — spots 5-9 vs weak pen = flag for hits (pos now defined)
            u5 = ''
            pen_data = pen_era.get(opp, {})
            opp_pen_era = pen_data.get('era') if pen_data else None
            if opp_pen_era and opp_pen_era >= 5.00 and pos >= 5:
                brl_u5 = b.get('barrel_pct') or 0
                xw_u5 = b.get('xwoba') or 0
                if brl_u5 >= 15 and xw_u5 >= 0.350:
                    u5 = f' [#5:LATE-BULLPEN-ERA{opp_pen_era:.2f}->HIT-LIVE]'

            lines.append(
                f"  #{pos} {proxy}{b.get('name','?')} ({b.get('hand','?')}HB) | "
                f"SCORE={score}/4 GRADE={grade} | plat={platoon} | {pa_str} | "
                f"gap={gs}({gap_flag}){hr_cap}{u5}{u11}{u12}{u13}"
                f"{wind_adj_str}{hr9_bonus_str}{form_str} | wOBA={b.get('woba','N/A')} | {breakdown}"
            )

    lines.append('')
    lines.append('RULES: Use pre-computed SCORE, GATE, platoon exactly. Do not re-compute.')
    lines.append('HR-CAP-C = max C grade. HR-CAP-B = max B grade. HIT-PICK-YES = include in hits.')
    lines.append('HOT gap = fade HR only, does NOT suppress hit probability.')

    # === ML/TOTALS SECTION ===
    lines.append('')
    lines.append('=== MONEYLINE & TOTALS CONTEXT ===')

    # Pitcher quality comparison for ML
    home_p = next((p for p in all_statcast if p.get('role')=='PITCHER' and p.get('team')==home), None)
    away_p = next((p for p in all_statcast if p.get('role')=='PITCHER' and p.get('team')==away), None)

    def pitcher_summary(p):
        if not p: return 'unknown'
        name = p.get('name','?')
        xw = p.get('xwoba')
        ev = p.get('exit_velocity')
        hh = p.get('hard_hit_pct')
        hr9 = p.get('hr_per_9')
        fb = p.get('fly_ball_pct')
        gb = p.get('ground_ball_pct')
        gate_s, gate_l, _ = compute_pitcher_gate(p)
        quality = 'ELITE' if gate_s >= 3 else ('SOLID' if gate_s == 2 else 'HITTABLE')
        parts = [f"gate={gate_s}/4({quality})"]
        if xw: parts.append(f"xwOBA={xw:.3f}")
        if ev:  parts.append(f"EV-allowed={ev}")
        if hr9: parts.append(f"HR/9={hr9}")
        if fb:  parts.append(f"FB%={fb}")
        if gb:  parts.append(f"GB%={gb}")
        return f"{name}: {' | '.join(parts)}"

    lines.append(f"  HOME pitcher ({home}): {pitcher_summary(home_p)}")
    lines.append(f"  AWAY pitcher ({away}): {pitcher_summary(away_p)}")

    # Lineup quality for totals
    for team, opp_pitcher_p in [(home, away_p), (away, home_p)]:
        opp_hand = opp_pitcher_p.get('hand','R') if opp_pitcher_p else 'R'
        batters_t = [b for b in all_statcast if b.get('role')=='BATTER' and b.get('team')==team]
        if not batters_t: continue
        xwobas = [b.get('xwoba') for b in batters_t if b.get('xwoba')]
        wobas  = [b.get('woba')  for b in batters_t if b.get('woba')]
        fav_count = sum(1 for b in batters_t
                        if compute_platoon(b.get('hand','?'), opp_hand) in ('FAV','FAV(SW)'))
        cold_count = sum(1 for b in batters_t if (b.get('gap') or 0) > 0.030)
        avg_xwoba = round(sum(xwobas)/len(xwobas), 3) if xwobas else None
        avg_woba  = round(sum(wobas)/len(wobas), 3)  if wobas  else None
        lines.append(
            f"  {team} lineup vs {opp_hand}HP: avg-xwOBA={avg_xwoba} | avg-wOBA={avg_woba} | "
            f"FAV-platoon={fav_count}/{len(batters_t)} | COLD-gap-batters={cold_count}"
        )

    # Bullpen for totals
    home_pen = pen_era.get(home, {})
    away_pen = pen_era.get(away, {})
    lines.append(
        f"  Bullpen ERA: {home}={home_pen.get('era','?')} ({home_pen.get('tier','?')}) | "
        f"{away}={away_pen.get('era','?')} ({away_pen.get('tier','?')})"
    )

    # Team standings for ML context
    try:
        standings_url = (f'https://statsapi.mlb.com/api/v1/standings'
                         f'?leagueId=103,104&season={CURRENT_YEAR}&standingsTypes=regularSeason')
        req = urllib.request.Request(standings_url,
              headers={'User-Agent': 'SharpOracle/1.0', 'Accept': 'application/json'})
        with urllib.request.urlopen(req, timeout=8) as r:
            sdata = json.loads(r.read())
        team_records = {}
        for division in sdata.get('records', []):
            for tr in division.get('teamRecords', []):
                tname = tr.get('team', {}).get('name', '').lower()
                wins = tr.get('wins', 0)
                losses = tr.get('losses', 0)
                pct = tr.get('winningPercentage', '.000')
                run_diff = tr.get('runDifferential', 0)
                streak = tr.get('streak', {}).get('streakCode', '')
                team_records[tname] = {'w': wins, 'l': losses, 'pct': pct,
                                       'rdiff': run_diff, 'streak': streak}
        for team in [home, away]:
            tkey = normalize_name(team).lower()
            rec = next((v for k, v in team_records.items() if tkey in k or k in tkey), None)
            if rec:
                lines.append(f"  {team} record: {rec['w']}-{rec['l']} ({rec['pct']}) "
                             f"RunDiff={rec['rdiff']:+d} Streak={rec['streak']}")
    except Exception:
        pass

    # Wind impact for totals
    wi = weather.get('wind_impact', {})
    wi_impact = wi.get('impact','UNKNOWN')
    wi_label  = wi.get('label','')
    wi_carry  = wi.get('carry_boost', 0)
    if wi_impact == 'OUT' and weather.get('wind_mph',0) >= 8:
        lines.append(f"  WIND: {wi_label} → OVER lean (wind carrying fly balls out)")
    elif wi_impact == 'IN' and weather.get('wind_mph',0) >= 8:
        lines.append(f"  WIND: {wi_label} → UNDER lean (wind suppressing fly balls)")
    else:
        lines.append(f"  WIND: {wi_label or 'calm/crosswind'} → neutral totals impact")

    lines.append('')
    lines.append('ML/TOTALS GUIDANCE FOR MARCUS:')
    lines.append('  MONEYLINE: need 3+ of these — starter xwOBA gap >0.050 | bullpen tier gap | team run diff gap >20 | streak (W3+) | home field.')
    lines.append('  OVER: both pitchers hittable (gate 0-1) + wind OUT 8mph+ + warm temp (>75F) + weak pens (ERA>5.0).')
    lines.append('  UNDER: both pitchers elite (gate 2+) + wind IN 8mph+ + cold (<55F) + strong pens (ERA<3.50).')
    lines.append('  Team on W-streak 4+ with positive run diff = meaningful ML lean TOWARD them.')
    lines.append('  Only pick ML/Totals when 3+ factors clearly align. Never force.')
    lines.append('  Bullpen ERA UNKNOWN = skip ML pick, only give Totals if other factors align.')
    lines.append('  Only give ML/Totals picks when 3+ factors align. One factor is never enough.')

    return '\n'.join(lines)

# ─── MAIN JOB ────────────────────────────────────────────────────────────────
def run_slate(jid, sid, raw_lineup, game_date=None):
    """
    Multi-game analysis: parse N games, run each in parallel, then build parlays.
    Falls back to run_job if only 1 game detected.
    """
    with store_lock:
        jobs[jid]['status'] = 'running'
        jobs[jid]['is_slate'] = True

    try:
        # Split multi-game input by the MLB app separator "Gameday\n\nTickets"
        # Then parse each game individually using the reliable single-game parser
        import re as _re
        # Strip markdown links: [Text](url) -> Text (MLB app hyperlink format)
        raw_lineup = _re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', raw_lineup)
        # Split on Gameday separator (plain text after stripping)
        raw_blocks = _re.split(r'\nGameday\s*\n(?:Tickets[^\n]*\n)?', raw_lineup)
        raw_blocks = [b.strip() for b in raw_blocks if b.strip()]
        print(f"[SLATE] Detected {len(raw_blocks)} game block(s)")

        # Log first line of each block so we can verify games
        for i, b in enumerate(raw_blocks):
            first = next((l.strip() for l in b.split('\n') if l.strip()), '?')
            print(f"[SLATE] Block {i+1}: {first[:80]}")

        if len(raw_blocks) <= 1:
            # Single game or unrecognized format — use single game flow
            print("[SLATE] Single game or unrecognized format, routing to run_job")
            run_job(jid, sid, raw_lineup, game_date)
            return

        # Parse each block individually using the reliable single-game parser
        # Run in parallel with Haiku for speed
        def _parse_one(block):
            try:
                # Strip any remaining markdown links
                block = _re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', block)
                return parse_lineup(block, game_date)
            except Exception as e:
                print(f"[SLATE] Parse error on block: {e}")
                return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
            parsed_list = list(ex.map(_parse_one, raw_blocks))

        games = [g for g in parsed_list if g and g.get('home_team', '?') != '?']
        print(f"[SLATE] Successfully parsed {len(games)}/{len(raw_blocks)} games")
        for g in games:
            print(f"[SLATE] Parsed: {g.get('away_team','?')} @ {g.get('home_team','?')} | "
                  f"Park: {g.get('park_name','?')} | "
                  f"Away P: {g.get('away_pitcher',{}).get('name','?')} | "
                  f"Home P: {g.get('home_pitcher',{}).get('name','?')} | "
                  f"Away batters: {len(g.get('away_batters',[]))} | "
                  f"Home batters: {len(g.get('home_batters',[]))}")
        if not games:
            print("[SLATE] All parses failed, falling back to single game")
            run_job(jid, sid, raw_lineup, game_date)
            return
        if len(games) == 1:
            run_job(jid, sid, raw_blocks[0], game_date)
            return
        print("[SLATE] Games: " + ", ".join(
            f"{g.get('away_team','?')}@{g.get('home_team','?')}" for g in games))

        print(f"[SLATE] Parsed {len(games)} games")
        step_set(jid, 0, 'done', f'{len(games)} games parsed')

        # STEP 2: Environment fetch for all games in parallel
        step_set(jid, 1, 'active', f'Fetching weather/parks for {len(games)} games...')
        cache = load_stats_cache()

        def _fetch_env(g):
            park_name, park_cat = resolve_park(g.get('home_team', '?'))
            weather = fetch_weather(park_name)
            pen_era = {}
            for team in [g.get('home_team'), g.get('away_team')]:
                if team and team != '?':
                    pen_era[team] = fetch_bullpen_era(team)
            return {'game': g, 'park_name': park_name, 'park_cat': park_cat,
                    'weather': weather, 'pen_era': pen_era}

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
            env_results = list(ex.map(_fetch_env, games))
        # Build and store per-game info for UI display
        all_games_info = []
        for env in env_results:
            g = env['game']
            wx = env['weather']
            wi = wx.get('wind_impact', {}) or {}
            pen = env['pen_era']
            game_info = {
                'game':         f"{g.get('away_team','?')} @ {g.get('home_team','?')}",
                'park':         env['park_name'],
                'category':     env['park_cat'],
                'temp_f':       wx.get('temp_f'),
                'condition':    wx.get('condition',''),
                'wind_mph':     wx.get('wind_mph'),
                'wind_dir':     wx.get('wind_dir',''),
                'wind_impact':  wi.get('impact',''),
                'wind_label':   wi.get('label',''),
                'weather_flag': wx.get('flag','NEUTRAL'),
                'bullpen':      {t: {'era': d.get('era'), 'tier': d.get('tier','?')}
                                 for t, d in pen.items()},
            }
            all_games_info.append(game_info)

        with store_lock:
            jobs[jid]['all_games_info'] = all_games_info
            # Also set park_confirm to first game for backward compat
            if all_games_info:
                jobs[jid]['park_confirm'] = all_games_info[0]
                jobs[jid]['bullpen'] = all_games_info[0].get('bullpen', {})

        step_set(jid, 1, 'done', f'{len(games)} environments ready')

        # STEP 3: Statcast fetch for all players across all games
        step_set(jid, 2, 'active', 'Fetching Statcast for all games...')

        def _process_game(env):
            g = env['game']
            home = g.get('home_team', '?')
            away = g.get('away_team', '?')
            hp = g.get('home_pitcher', {})
            ap = g.get('away_pitcher', {})
            pitcher_list = []
            if hp.get('name'):
                pitcher_list.append({**hp, 'role': 'PITCHER', 'team': home, 'faces_team': away, 'lineup_pos': 0})
            if ap.get('name'):
                pitcher_list.append({**ap, 'role': 'PITCHER', 'team': away, 'faces_team': home, 'lineup_pos': 0})
            batter_list = []
            for b in g.get('home_batters', []):
                batter_list.append({**b, 'role': 'BATTER', 'team': home})
            for b in g.get('away_batters', []):
                batter_list.append({**b, 'role': 'BATTER', 'team': away})
            pitcher_stats = fetch_all_parallel(pitcher_list, workers=2, cache=cache)
            batter_stats  = fetch_all_parallel(batter_list, workers=2, cache=cache)
            all_statcast  = pitcher_stats + batter_stats
            with _recent_form_lock:
                recent_form = dict(_recent_form_cache)
            ctx = build_context(g, all_statcast, env['weather'], env['park_name'],
                                env['park_cat'], env['pen_era'], recent_form)
            return {'env': env, 'all_statcast': all_statcast, 'ctx': ctx}

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
            game_data = list(ex.map(_process_game, env_results))

        # Merge all statcast for combined table
        all_statcast_combined = []
        for gd in game_data:
            g = gd['env']['game']
            game_label = f"{g.get('away_team','?')}@{g.get('home_team','?')}"
            for p in gd['all_statcast']:
                p['game_label'] = game_label  # tag each player with their game
            all_statcast_combined.extend(gd['all_statcast'])

        slim_statcast = []
        for p in all_statcast_combined:
            slim_statcast.append({
                'name': p.get('name'), 'role': p.get('role'), 'team': p.get('team'),
                'game': p.get('game_label', ''),
                'barrel_pct': p.get('barrel_pct'), 'exit_velocity': p.get('exit_velocity'),
                'ev50': p.get('ev50'), 'hard_hit_pct': p.get('hard_hit_pct'),
                'xwoba': p.get('xwoba'), 'woba': p.get('woba'), 'gap': p.get('gap'),
                'sweet_spot_pct': p.get('sweet_spot_pct'),
                'fly_ball_pct': p.get('fly_ball_pct') or p.get('batter_fb_pct'),
                'iso': p.get('iso'), 'hr_per_9': p.get('hr_per_9'), 'hpi': p.get('hpi'),
                'fetch_status': p.get('fetch_status'),
            })

        with store_lock:
            jobs[jid]['statcast'] = slim_statcast
            jobs[jid]['statcast_total'] = len(slim_statcast)
        ok = sum(1 for x in all_statcast_combined if x.get('fetch_status') == 'ok')
        step_set(jid, 2, 'done', f'Stats: {ok}/{len(all_statcast_combined)} fetched across {len(games)} games')

        # STEP 4: Run all game analyses in parallel
        step_set(jid, 3, 'active', f'Analyzing {len(games)} games...')

        def _analyze_game(gd):
            result = call_claude(
                [{'role': 'user', 'content': gd['ctx']}],
                system=SYSTEM_PROMPT,
                max_tokens=4000
            )
            game_label = f"{gd['env']['game'].get('away_team','?')} @ {gd['env']['game'].get('home_team','?')}"
            return f"\n{'='*60}\n## {game_label}\n{'='*60}\n{result}"

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
            game_analyses = list(ex.map(_analyze_game, game_data))

        combined_analysis = '\n'.join(game_analyses)
        step_set(jid, 3, 'done', f'{len(games)} games analyzed')

        # STEP 5: Generate parlays
        step_set(jid, 4, 'active', 'Building parlays...')

        # Build game summaries for parlay context
        game_summaries = []
        for gd in game_data:
            env = gd['env']
            g = env['game']
            wi = env['weather'].get('wind_impact', {})
            game_summaries.append({
                'game': f"{g.get('away_team','?')} @ {g.get('home_team','?')}",
                'park': env['park_name'],
                'park_cat': env['park_cat'],
                'temp': env['weather'].get('temp_f', '?'),
                'wind_label': wi.get('label', 'unknown') if wi else 'unknown',
                'pen_summary': ' | '.join(f"{t}={d.get('era','?')}" for t,d in env['pen_era'].items()),
            })

        # Build parlay context directly from game data
        parlay_ctx_lines = ["=== SLATE PICKS FOR PARLAY CONSTRUCTION ===\n"]

        # Game environments with wind/park/pen context
        parlay_ctx_lines.append("GAME ENVIRONMENTS:")
        for gd in game_data:
            env = gd['env']
            g = env['game']
            wi = env['weather'].get('wind_impact', {}) or {}
            game_label = f"{g.get('away_team','?')} @ {g.get('home_team','?')}"
            pen_str = ' | '.join(f"{t}={d.get('era','?')}" for t,d in env['pen_era'].items())
            parlay_ctx_lines.append(
                f"  {game_label}: {env['park_name']} [{env['park_cat']}] | "
                f"{env['weather'].get('temp_f','?')}F | Wind: {wi.get('label','unknown')} | "
                f"Pens: {pen_str}")

        # Per-game picks — include game label + full picks section
        parlay_ctx_lines.append("\n\nPER-GAME PICKS (HR, HIT, SLEEPER, ML, TOTALS):")
        ml_totals_found = []

        for i, (ga, gd) in enumerate(zip(game_analyses, game_data)):
            env = gd['env']
            g = env['game']
            game_label = f"{g.get('away_team','?')} @ {g.get('home_team','?')}"

            # Extract picks section
            picks_start = ga.find('## PICKS')
            reads_start = ga.find('## GAME READS')
            if picks_start >= 0 and reads_start >= 0:
                picks_section = ga[picks_start:reads_start].strip()
            elif picks_start >= 0:
                picks_section = ga[picks_start:].strip()
            else:
                picks_section = ga[:3000]

            # Add game label header
            parlay_ctx_lines.append(f"\n--- {game_label} ---")
            parlay_ctx_lines.append(picks_section[:5000])

            # Separately extract ML/Totals from the FULL analysis (not truncated)
            for line in ga.split('\n'):
                s = line.strip()
                is_ml = s.startswith('**ML:') and 'NO ML EDGE' not in s and 'NO ML' not in s.upper()[:20]
                is_tot = s.startswith('**TOTALS:') and 'NO TOTALS EDGE' not in s and 'NO TOTALS' not in s.upper()[:25]
                if is_ml or is_tot:
                    ml_totals_found.append((game_label, s))

        # Explicit ML/Totals summary
        parlay_ctx_lines.append("\n\n=== CONFIRMED ML/TOTALS PICKS ACROSS SLATE ===")
        if ml_totals_found:
            for game_label, pick_line in ml_totals_found:
                parlay_ctx_lines.append(f"  [{game_label}] {pick_line}")
        else:
            parlay_ctx_lines.append("  (No explicit ML/Totals picks found — use outside-the-box rules from environment data)")

        # Final instructions
        parlay_ctx_lines.append(
            "\n\nBUILD ALL REQUIRED PARLAYS using the picks and environments above."
            "\nHARD RULES: No same-game HR parlays. Max 2 legs/game in hit parlays."
            "\nFor ML/Totals: use confirmed picks first, then apply outside-the-box rules"
            " (wind stacks, grounder pitcher + wind IN = UNDER, DOME + two CLOSED gates = UNDER lean, etc)"
            "\nBe creative and vary each parlay size — different themes, different angles."
        )

        parlay_analysis = call_claude(
            [{'role': 'user', 'content': '\n'.join(parlay_ctx_lines)}],
            system=PARLAY_SYSTEM,
            max_tokens=5000
        )
        step_set(jid, 4, 'done', 'Parlays built')

        # Combine everything
        final_output = combined_analysis + '\n\n' + '='*60 + '\n## SLATE PARLAYS\n' + '='*60 + '\n' + parlay_analysis

        with store_lock:
            jobs[jid]['result'] = final_output
            jobs[jid]['parlay_result'] = parlay_analysis
            jobs[jid]['status'] = 'done'

    except Exception as e:
        tb = traceback.format_exc()
        print(f"[RUN_SLATE ERROR] {e}\n{tb}")
        with store_lock:
            jobs[jid]['status'] = 'error'
            jobs[jid]['error'] = str(e)


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
                'wind_mph': weather['wind_mph'],
                'wind_dir': weather.get('wind_dir'),
                'wind_impact': weather.get('wind_impact', {}).get('impact','UNKNOWN'),
                'wind_label': weather.get('wind_impact', {}).get('label',''),
                'weather_flag': weather['flag'],
                'notes': weather.get('notes', ''),
            }
            jobs[jid]['bullpen'] = pen_era

        temp_str = f"{weather['temp_f']}F" if weather['temp_f'] else 'N/A'
        pen_summary = ' | '.join(f"{t}={d.get('era','N/A')}" for t,d in pen_era.items())
        step_set(jid, 1, 'done', f'{park_name} | {temp_str} {weather["flag"]} | Pen: {pen_summary}')

        # STEP 3: Statcast fetch
        step_set(jid, 2, 'active', 'Fetching Statcast...')

        # Wait until CSV download and cache load is fully complete
        # load_stats_cache() blocks until cache is fully ready
        # The background thread loads it at startup — just call and it returns when ready
        cache = load_stats_cache()
        print(f"[STATS] Cache ready: {len(cache)} players")

        hp = parsed.get('home_pitcher', {})
        ap = parsed.get('away_pitcher', {})
        pitcher_list = []
        if hp.get('name'):
            pitcher_list.append({**hp, 'role': 'PITCHER', 'team': home, 'faces_team': away, 'lineup_pos': 0})
            print(f"[ASSIGN] HOME pitcher: {hp.get('name')} ({home}) FACES {away} batters")
        if ap.get('name'):
            pitcher_list.append({**ap, 'role': 'PITCHER', 'team': away, 'faces_team': home, 'lineup_pos': 0})
            print(f"[ASSIGN] AWAY pitcher: {ap.get('name')} ({away}) FACES {home} batters")

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

        # Use pre-cached recent form (populated at startup by _fetch_all_recent_form)
        with _recent_form_lock:
            recent_form = dict(_recent_form_cache)
        print(f"[RECENT FORM] Using cached form for {len(recent_form)} players")

        ok = sum(1 for x in all_statcast if x.get('fetch_status') == 'ok')
        print(f"[STATS] {ok}/{len(all_statcast)} ok")

        slim_statcast = []
        for p in all_statcast:
            slim_statcast.append({
                'name':             p.get('name'),
                'role':             p.get('role'),
                'team':             p.get('team'),
                'barrel_pct':       p.get('barrel_pct'),
                'exit_velocity':    p.get('exit_velocity'),
                'ev50':             p.get('ev50'),
                'hard_hit_pct':     p.get('hard_hit_pct'),
                'xwoba':            p.get('xwoba'),
                'woba':             p.get('woba'),
                'gap':              p.get('gap'),
                'sweet_spot_pct':   p.get('sweet_spot_pct'),
                'fly_ball_pct':     p.get('fly_ball_pct') or p.get('batter_fb_pct'),
                'iso':              p.get('iso'),
                'hr_per_9':         p.get('hr_per_9'),
                'hpi':              p.get('hpi'),
                'fetch_status':     p.get('fetch_status'),
            })

        # Write statcast immediately so UI can show it while analysis runs
        with store_lock:
            jobs[jid]['statcast'] = slim_statcast
            jobs[jid]['statcast_total'] = len(slim_statcast)
        step_set(jid, 2, 'done', f'Stats: {ok}/{len(all_statcast)} fetched')

        # STEP 4: Analysis (runs in background, statcast already visible)
        step_set(jid, 3, 'active', 'Running model analysis...')
        ctx = build_context(parsed, all_statcast, weather, park_name, park_cat, pen_era, recent_form)
        analysis = call_claude(
            [{'role': 'user', 'content': ctx}],
            system=SYSTEM_PROMPT,
            max_tokens=6000
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
  <button class="nav-btn" onclick="show('parlays',this)">PARLAYS</button>
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
          <th>Player</th><th>Role</th><th>Team</th><th>Game</th>
          <th>BRL%</th><th>EV</th><th>EV50</th><th>HH%</th>
          <th>xwOBA</th><th>wOBA</th><th>GAP</th><th>SS%</th>
          <th>FB%</th><th>ISO</th><th>HR/9</th><th>HPI</th><th>OK</th>
        </tr></thead>
        <tbody id="statBody"><tr><td colspan="17" class="na" style="padding:20px;text-align:center">Run a lineup to populate</td></tr></tbody>
      </table>
    </div>
  </div>
</div>

<div id="panel-picks" class="panel">
  <div class="result-box" id="result">Run a lineup to see picks...</div>
</div>

<div id="panel-parlays" class="panel">
  <div class="result-box" id="parlay-result">Run a slate (multiple games) to see parlays...</div>
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
    if(d.all_games_info&&d.all_games_info.length>1){
      showSlateInfo(d.all_games_info);
    } else if(d.park_confirm&&Object.keys(d.park_confirm).length) {
      showInfo(d.park_confirm,d.bullpen||{});
    }

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
        if(res.parlay_result){
          document.getElementById('parlay-result').textContent=res.parlay_result;
        }
        if(res.is_slate){
          show('parlays',document.querySelectorAll('.nav-btn')[3]);
        } else {
          show('picks',document.querySelectorAll('.nav-btn')[2]);
        }
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
  const windDir=p.wind_dir?` ${p.wind_dir}`:'';
  const windImpact=p.wind_label?`<div class="pill" style="font-size:10px">Wind Impact: <b>${p.wind_label}</b></div>`:'';
  const penHtml=Object.entries(pen||{}).map(([t,dd])=>{
    const era=dd.era?dd.era.toFixed(2):'N/A';
    const pc=dd.tier==='WEAK'?'bad':dd.tier==='AVERAGE'?'warn':'good';
    return `<div class="pill ${pc}">${t}: <b>${era} [${dd.tier}]</b></div>`;
  }).join('');
  document.getElementById('pillRow').innerHTML=`
    <div class="pill">Park: <b>${p.park||'?'}</b></div>
    <div class="pill">Type: <b>${p.category||'?'}</b></div>
    <div class="pill ${wc}">Weather: <b>${temp} - ${p.weather_flag||'?'}</b></div>
    <div class="pill">Wind: <b>${wind}${windDir}</b></div>${windImpact}${penHtml}`;
  document.getElementById('infoCard').style.display='';
}

function showSlateInfo(games){
  // Multi-game: show a card per game
  const html=games.map(p=>{
    const wc=(p.weather_flag||'').includes('SUPPRESSOR')||(p.weather_flag||'')==='DOME'?'warn':(p.weather_flag||'').includes('BOOST')?'good':'';
    const temp=p.temp_f?p.temp_f+'F':'N/A';
    const wind=p.wind_mph?(p.wind_mph+' mph'+(p.wind_dir?' '+p.wind_dir:'')):'N/A';
    const penHtml=Object.entries(p.bullpen||{}).map(([t,dd])=>{
      const era=dd.era?dd.era.toFixed(2):'N/A';
      const pc=dd.tier==='WEAK'?'bad':dd.tier==='AVERAGE'?'warn':'good';
      return`<span class="pill ${pc}" style="font-size:9px">${t}:<b>${era}</b></span>`;
    }).join('');
    const windImpact=p.wind_label?`<span class="pill" style="font-size:9px">💨<b>${p.wind_label}</b></span>`:'';
    return`<div style="background:#080d18;border:1px solid #1e3a5f;border-radius:6px;padding:8px;margin-bottom:6px">
      <div style="font-size:11px;font-weight:700;color:#f7c948;margin-bottom:4px">${p.game||'?'}</div>
      <div style="display:flex;gap:6px;flex-wrap:wrap">
        <span class="pill" style="font-size:9px">🏟 <b>${p.park||'?'}</b> [${p.category||'?'}]</span>
        <span class="pill ${wc}" style="font-size:9px">🌡 <b>${temp} ${p.weather_flag||''}</b></span>
        <span class="pill" style="font-size:9px">💨 <b>${wind}</b></span>
        ${windImpact}${penHtml}
      </div>
    </div>`;
  }).join('');
  document.getElementById('pillRow').innerHTML=html;
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
          <td style="font-size:9px;color:#64748b">${p.game||''}</td>
          <td>${fv(p.barrel_pct,15)}</td>
          <td>${fv(p.exit_velocity,91)}</td>
          <td>${fv(p.ev50,100)}</td>
          <td>${fv(p.hard_hit_pct,50)}</td>
          <td>${fv(p.xwoba,0.350)}</td>
          <td>${fw(p.woba)}</td>
          <td class="${gc}">${gap}</td>
          <td>${fv(p.sweet_spot_pct,38)}</td>
          <td>${p.fly_ball_pct!=null?p.fly_ball_pct.toFixed(1):'<span class="na">-</span>'}</td>
          <td>${p.iso!=null?p.iso.toFixed(3):'<span class="na">-</span>'}</td>
          <td>${p.role==='PITCHER'&&p.hr_per_9!=null?p.hr_per_9.toFixed(2):'<span class="na">-</span>'}</td>
          <td>${p.hpi!=null?p.hpi.toFixed(1):'<span class="na">-</span>'}</td>
          <td>${p.fetch_status==='ok'?'OK':'!'}</td>
        </tr>`;
      }catch(e){
        return`<tr><td colspan="17" style="color:#ef4444">${p.name||'?'} - render error</td></tr>`;
      }
    }).join('');
    document.getElementById('statBody').innerHTML=rows||'<tr><td colspan="17">No data</td></tr>';
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
                snap = {
                    'status':         job['status'],
                    'steps':          job['steps'],
                    'park_confirm':   job['park_confirm'],
                    'bullpen':        job['bullpen'],
                    'all_games_info': job.get('all_games_info', []),
                    'error':          job['error'],
                    'has_statcast':   len(job.get('statcast', [])) > 0,
                    'has_result':     bool(job.get('result')),
                    'is_slate':       job.get('is_slate', False),
                    'has_parlays':    bool(job.get('parlay_result')),
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
                self._json({
                    'result': jobs[jid].get('result', ''),
                    'parlay_result': jobs[jid].get('parlay_result', ''),
                    'is_slate': jobs[jid].get('is_slate', False),
                })
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
                ('custom_batter_fb',
                 f'https://baseballsavant.mlb.com/leaderboard/custom?year={CURRENT_YEAR}&type=batter&filter=&min=1&selections=groundballs_percent,flyballs_percent,linedrives_percent,b_iso&chart=false&csv=true'),
                ('custom_pitcher_fb',
                 f'https://baseballsavant.mlb.com/leaderboard/custom?year={CURRENT_YEAR}&type=pitcher&filter=&min=1&selections=groundballs_percent,flyballs_percent,linedrives_percent,popups_percent&chart=false&csv=true'),
                ('expected_stats_pitcher',
                 f'https://baseballsavant.mlb.com/leaderboard/expected_statistics?type=pitcher&year={CURRENT_YEAR}&position=&team=&min=1&csv=false'),
                ('mlb_stats_api_pitching',
                 f'https://statsapi.mlb.com/api/v1/stats?stats=season&group=pitching&season={CURRENT_YEAR}&gameType=R&limit=10&sportId=1'),
                ('savant_pitching_leaderboard',
                 f'https://baseballsavant.mlb.com/leaderboard/pitching?min=1&type=season&year={CURRENT_YEAR}&csv=true'),
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

            # Cache inspection — what fields does this player have in cache?
            cache = load_stats_cache()
            tgt_key = normalize_name(name).lower()
            cached_row = cache.get(tgt_key)
            if cached_row:
                result['cache_keys'] = list(cached_row.keys())
                result['cache_fb_fields'] = {k: v for k, v in cached_row.items()
                    if any(x in k.lower() for x in ['fly','ground','iso','hr_per','mlb_fb','mlb_gb'])}
            else:
                result['cache_miss'] = f'No entry for key: {tgt_key}'
                # Try partial match
                matches = [k for k in cache.keys() if tgt_key.split()[0] in k][:5]
                result['cache_partial_matches'] = matches

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
            # Always use run_slate — it auto-detects single vs multi game
            t = threading.Thread(target=run_slate, args=(jid, sid, raw, game_date), daemon=True)
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
    
