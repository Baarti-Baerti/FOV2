"""
Fette Otter Runners Challenge — Backend API
Handles Strava OAuth, member storage, and team stats.

Deploy to Railway, Render, or Fly.io.  Required env vars:
  STRAVA_CLIENT_ID      — from https://www.strava.com/settings/api
  STRAVA_CLIENT_SECRET  — from your Strava API app
  FRONTEND_URL          — full URL of your HTML dashboard page
  BACKEND_URL           — full URL of this deployed backend

Run locally:
  uvicorn main:app --reload --port 8000
"""

import os, json, time, hmac, hashlib, secrets
from datetime import datetime, timezone, timedelta, date
from typing import Optional
from contextlib import asynccontextmanager
from urllib.parse import urlencode

import httpx
from fastapi import FastAPI, HTTPException, Query, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

# ─────────────────────────────────────────────────────────────
#  Config  (set as env vars in Railway / .env locally)
# ─────────────────────────────────────────────────────────────
STRAVA_CLIENT_ID     = os.getenv("STRAVA_CLIENT_ID",     "YOUR_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET", "YOUR_CLIENT_SECRET")
FRONTEND_URL         = os.getenv("FRONTEND_URL",  "http://localhost:5500")
BACKEND_URL          = os.getenv("BACKEND_URL",   "http://localhost:8000")
DB_PATH              = os.getenv("DB_PATH",        "/data/fette_otter.json")
SECRET_KEY           = os.getenv("SECRET_KEY",     secrets.token_hex(32))

# Garmin OAuth 2.0 — set these in Railway env vars
GARMIN_API_BASE = "https://apis.garmin.com"  # kept for reference

def is_strava_paused() -> bool:
    """Check if Strava syncing is paused (stored in DB so it survives restarts)."""
    db = load_db()
    return db.get("strava_paused", False)

STRAVA_AUTH_URL  = "https://www.strava.com/oauth/authorize"
STRAVA_TOKEN_URL = "https://www.strava.com/oauth/token"
STRAVA_API_BASE  = "https://www.strava.com/api/v3"

# ─────────────────────────────────────────────────────────────
#  Tiny JSON "database"
# ─────────────────────────────────────────────────────────────
import asyncio
_db_lock = asyncio.Lock()

def load_db() -> dict:
    if os.path.exists(DB_PATH):
        try:
            with open(DB_PATH) as f:
                data = json.load(f)
            # Validate structure — never return a broken db
            if isinstance(data, dict) and "members" in data and "next_id" in data:
                return data
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            print(f"[warn] DB read error ({e}) — loading backup or empty db")
            # Try backup
            backup = DB_PATH + ".bak"
            if os.path.exists(backup):
                try:
                    with open(backup) as f:
                        data = json.load(f)
                    if isinstance(data, dict) and "members" in data:
                        print("[warn] Restored from backup")
                        return data
                except Exception:
                    pass
    return {"members": [], "next_id": 1}

def save_db(db: dict):
    """Atomic write: write to temp file then rename, so the DB is never half-written.
    Also keeps a .bak copy of the previous version."""
    import shutil
    tmp = DB_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(db, f, indent=2, default=str)
    if os.path.exists(DB_PATH):
        shutil.copy2(DB_PATH, DB_PATH + ".bak")
    os.replace(tmp, DB_PATH)

# ─────────────────────────────────────────────────────────────
#  In-memory stats cache
# ─────────────────────────────────────────────────────────────
_cache: dict = {}
CACHE_TTL = 600  # 10 minutes

def _ck(mid: int, r: str) -> str: return f"{mid}:{r}"

def cache_get(mid: int, r: str):
    e = _cache.get(_ck(mid, r))
    return e["d"] if e and time.time() - e["t"] < CACHE_TTL else None

def cache_set(mid: int, r: str, d):
    _cache[_ck(mid, r)] = {"d": d, "t": time.time()}

def cache_bust(mid: int):
    for k in [k for k in _cache if k.startswith(f"{mid}:")]:
        del _cache[k]

# ─────────────────────────────────────────────────────────────
#  Date-range helpers
# ─────────────────────────────────────────────────────────────
def date_range(r: str) -> tuple:
    now, y, m = datetime.now(timezone.utc), datetime.now().year, datetime.now().month
    if r == "thismonth":
        s, e = datetime(y, m, 1, tzinfo=timezone.utc), now
    elif r == "lastmonth":
        lm, ly = (m-1) or 12, y if m > 1 else y-1
        s = datetime(ly, lm, 1, tzinfo=timezone.utc)
        e = datetime(y, m, 1, tzinfo=timezone.utc) - timedelta(seconds=1)
    elif r == "ytd":
        s, e = datetime(y, 1, 1, tzinfo=timezone.utc), now
    elif r.startswith("month-"):
        mo = int(r.split("-")[1])
        nm, ny = (mo+1) if mo < 12 else 1, y if mo < 12 else y+1
        s = datetime(y, mo, 1, tzinfo=timezone.utc)
        e = datetime(ny, nm, 1, tzinfo=timezone.utc) - timedelta(seconds=1)
    elif r == "q1": s, e = datetime(y,1,1,tzinfo=timezone.utc), datetime(y,4,1,tzinfo=timezone.utc)-timedelta(seconds=1)
    elif r == "q2": s, e = datetime(y,4,1,tzinfo=timezone.utc), datetime(y,7,1,tzinfo=timezone.utc)-timedelta(seconds=1)
    elif r == "q3": s, e = datetime(y,7,1,tzinfo=timezone.utc), datetime(y,10,1,tzinfo=timezone.utc)-timedelta(seconds=1)
    elif r == "q4": s, e = datetime(y,10,1,tzinfo=timezone.utc), datetime(y+1,1,1,tzinfo=timezone.utc)-timedelta(seconds=1)
    else:           s, e = datetime(y,1,1,tzinfo=timezone.utc), now
    return int(s.timestamp()), int(e.timestamp())

# ─────────────────────────────────────────────────────────────
#  Activity classification
# ─────────────────────────────────────────────────────────────
_TYPES = {
    "Run":"run","TrailRun":"run","VirtualRun":"run",
    "Ride":"ride","GravelRide":"ride","MountainBikeRide":"ride","EBikeRide":"ride",
    "VirtualRide":"virtual_ride",
    "Swim":"swim",
    "Walk":"walk","Hike":"walk",
}
def classify(t: str) -> str: return _TYPES.get(t, "other")

# ─────────────────────────────────────────────────────────────
#  Strava API helpers
# ─────────────────────────────────────────────────────────────
async def refresh(member: dict) -> dict:
    if time.time() < member.get("strava_expires_at", 0) - 60:
        return member
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(STRAVA_TOKEN_URL, data={
            "client_id": STRAVA_CLIENT_ID, "client_secret": STRAVA_CLIENT_SECRET,
            "grant_type": "refresh_token", "refresh_token": member["strava_refresh_token"],
        })
        r.raise_for_status()
        t = r.json()
    member.update({"strava_access_token": t["access_token"],
                   "strava_refresh_token": t["refresh_token"],
                   "strava_expires_at": t["expires_at"]})
    db = load_db()
    for m in db["members"]:
        if m["id"] == member["id"]:
            m.update({"strava_access_token": t["access_token"],
                      "strava_refresh_token": t["refresh_token"],
                      "strava_expires_at": t["expires_at"]})
    save_db(db)
    return member


# ─────────────────────────────────────────────────────────────
#  Persistent activity store — one JSON file per member
#  Stored at /data/acts_{member_id}.json
# ─────────────────────────────────────────────────────────────
def acts_path(mid: int) -> str:
    base = os.path.dirname(DB_PATH)
    return os.path.join(base, f"acts_{mid}.json")

def load_acts(mid: int) -> dict:
    """Load stored activities for a member. Returns {activities: [], last_fetch: 0}"""
    p = acts_path(mid)
    if os.path.exists(p):
        try:
            with open(p) as f:
                data = json.load(f)
            if isinstance(data, dict) and "activities" in data:
                return data
        except Exception:
            pass
    return {"activities": [], "last_fetch": 0}

def save_acts(mid: int, data: dict):
    """Atomically save activities for a member."""
    import shutil
    p = acts_path(mid)
    tmp = p + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    if os.path.exists(p):
        shutil.copy2(p, p + ".bak")
    os.replace(tmp, p)

async def sync_activities(member: dict) -> list:
    """
    Incremental sync: fetch new activities since last sync.
    - Strava members: uses Strava API with stored tokens
    - Garmin members: uses garminconnect with stored session token
    Returns stored activities without fetching if paused.
    """
    mid      = member["id"]
    now      = int(time.time())
    yr       = datetime.now(timezone.utc).year
    yr_start = int(datetime(yr, 1, 1, tzinfo=timezone.utc).timestamp())

    stored     = load_acts(mid)
    last_fetch = stored.get("last_fetch", 0)

    # If paused, just return what we have stored
    if is_strava_paused():
        print(f"[sync] PAUSED — skipping fetch for {member['name']}")
        return stored.get("activities", [])

    provider = member.get("provider", "strava")

    if provider == "garmin":
        return await _sync_garmin(member, stored, now, yr_start, last_fetch)
    else:
        return await _sync_strava(member, stored, now, yr_start, last_fetch)


async def _sync_strava(member: dict, stored: dict, now: int, yr_start: int, last_fetch: int) -> list:
    after = yr_start if last_fetch == 0 else max(yr_start, last_fetch - 6 * 3600)  # 6hr overlap catches delayed uploads
    member = await refresh(member)
    hdrs = {"Authorization": f"Bearer {member['strava_access_token']}"}

    new_acts = []
    hit_error = False
    page = 1
    async with httpx.AsyncClient(timeout=30) as c:
        while True:
            r = await c.get(f"{STRAVA_API_BASE}/athlete/activities", headers=hdrs,
                            params={"after": after, "before": now,
                                    "per_page": 100, "page": page})
            if r.status_code == 429:
                print(f"[warn] Strava rate limit for {member['name']}")
                hit_error = True
                break
            if r.status_code != 200:
                print(f"[warn] Strava API {r.status_code} for {member['name']}")
                hit_error = True
                break
            batch = r.json()
            if not isinstance(batch, list) or not batch:
                break
            new_acts.extend(batch)
            if len(batch) < 100:
                break
            page += 1

    print(f"[sync] {member['name']} (Strava): fetched {len(new_acts)} new activities")
    # Only advance last_fetch timestamp if we completed without errors
    save_ts = now if not hit_error else last_fetch
    return _merge_and_save(member["id"], stored, new_acts, save_ts, yr_start)


async def _sync_garmin(member: dict, stored: dict, now: int, yr_start: int, last_fetch: int) -> list:
    """Sync Garmin activities using stored session token."""
    import asyncio
    token_store = member.get("garmin_token_store")
    if not token_store:
        print(f"[warn] No Garmin token for {member['name']} — skipping sync")
        return stored.get("activities", [])

    after_dt  = datetime.fromtimestamp(
        yr_start if last_fetch == 0 else max(yr_start, last_fetch - 3 * 3600),
        tz=timezone.utc
    )
    before_dt = datetime.fromtimestamp(now, tz=timezone.utc)

    def do_garmin_sync():
        from garminconnect import Garmin
        client = Garmin()
        # token_store is a base64 string from client.dumps() — pass directly to loads()
        if isinstance(token_store, str):
            client.client.loads(token_store)
        else:
            # Legacy: dict format — convert back to string
            import json as _json
            client.client.loads(_json.dumps(token_store))
        client.display_name = member.get("name", "")
        acts = client.get_activities_by_date(
            after_dt.strftime("%Y-%m-%d"),
            before_dt.strftime("%Y-%m-%d"),
        )
        return acts

    try:
        loop     = asyncio.get_event_loop()
        raw_acts = await loop.run_in_executor(None, do_garmin_sync)
        print(f"[sync] {member['name']} (Garmin): raw fetch returned {len(raw_acts or [])} activities")
    except Exception as e:
        print(f"[warn] Garmin sync failed for {member['name']}: {type(e).__name__}: {e}")
        return stored.get("activities", [])

    # Normalise Garmin activities to match Strava-like shape
    new_acts = []
    for a in (raw_acts or []):
        act_type   = a.get("activityType", {}).get("typeKey", "other")
        type_map = {
            "running": "Run", "trail_running": "TrailRun", "treadmill_running": "Run",
            "cycling": "Ride", "road_biking": "Ride", "mountain_biking": "MountainBikeRide",
            "gravel_cycling": "Ride", "cyclocross": "Ride", "road_cycling": "Ride",
            "virtual_ride": "VirtualRide", "indoor_cycling": "VirtualRide",
            "zwift": "VirtualRide", "indoor_biking": "VirtualRide",
            "swimming": "Swim", "lap_swimming": "Swim", "open_water_swimming": "Swim",
            "walking": "Walk", "hiking": "Hike", "trail_hiking": "Hike",
        }
        sport_type = type_map.get(act_type, act_type.title().replace("_",""))
        start_str  = a.get("startTimeLocal", a.get("startTimeGMT", ""))
        # Garmin returns distance in metres
        dist_m = float(a.get("distance") or 0)
        new_acts.append({
            "id":               str(a.get("activityId", f"g_{id(a)}")),
            "sport_type":       sport_type,
            "start_date_local": start_str + "Z" if start_str and not start_str.endswith("Z") else start_str,
            "start_date":       a.get("startTimeGMT", start_str),
            "distance":         dist_m,
            "moving_time":      int((a.get("movingDuration") or a.get("duration") or 0)),
            "elapsed_time":     int((a.get("duration") or 0)),
            "average_speed":    (a.get("averageSpeed") or 0) / 3.6,  # km/h → m/s
            "calories":         a.get("calories") or 0,
            "kilojoules":       None,
            "name":             a.get("activityName", sport_type),
        })

    print(f"[sync] {member['name']} (Garmin): fetched {len(new_acts)} new activities")
    return _merge_and_save(member["id"], stored, new_acts, now, yr_start)


def _merge_and_save(mid: int, stored: dict, new_acts: list, now: int, yr_start: int) -> list:
    """Merge new activities into stored, deduplicate, save and return full list."""
    existing = {a["id"]: a for a in stored.get("activities", []) if "id" in a}
    for a in new_acts:
        if "id" in a:
            existing[a["id"]] = a
    all_acts = [a for a in existing.values() if _act_ts(a) >= yr_start]
    all_acts.sort(key=_act_ts, reverse=True)
    save_acts(mid, {"activities": all_acts, "last_fetch": now})
    return all_acts

def _act_ts(a: dict) -> int:
    """Get unix timestamp from activity start date."""
    ts = a.get("start_date_local") or a.get("start_date", "")
    try:
        return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
    except (ValueError, TypeError):
        return 0




# ─────────────────────────────────────────────────────────────
#  Stats aggregation
# ─────────────────────────────────────────────────────────────
MONTHLY_GOAL_KM    = 66.67        # challenge goal per month
WALK_MIN_SPEED_MS  = 6500 / 3600  # 6.5 km/h in m/s
WALK_MIN_MOVING_S  = 30 * 60 - 30  # 30 min minus 30s tolerance (Strava rounding)

def challenge_km_for_activity(a: dict) -> float:
    """Return the challenge-km contribution of a single activity.
    Walk/Hike: counts at dist/3 only if moving_time >= 30 min AND average_speed >= 6.5 km/h.
    """
    cat  = classify(a.get("sport_type") or a.get("type", ""))
    dist = (a.get("distance", 0) or 0) / 1000  # metres -> km
    if   cat == "run":          return dist
    elif cat == "ride":         return dist / 5
    elif cat == "virtual_ride": return dist / 4
    elif cat == "swim":         return dist * 4
    elif cat == "walk":
        moving_time   = a.get("moving_time",   0) or 0
        average_speed = a.get("average_speed", 0) or 0
        if moving_time >= WALK_MIN_MOVING_S and average_speed >= WALK_MIN_SPEED_MS:
            return dist / 3
        return 0.0
    return 0.0


# Categories that count toward durationSec (all others excluded)
_COUNTED_CATS = {"run", "ride", "virtual_ride", "swim", "walk"}

def aggregate(acts: list) -> dict:
    run = ride = vride = swim = walk = 0.0
    eligible_walk = 0.0   # walk km that pass the speed+duration filter
    secs = 0      # only time from counted activity types
    types = set()
    for a in acts:
        cat = classify(a.get("sport_type") or a.get("type", ""))
        d = a.get("distance", 0) or 0

        types.add(a.get("sport_type") or a.get("type") or "Unknown")
        if cat in _COUNTED_CATS:
            secs += a.get("elapsed_time", 0) or 0
        if   cat == "run":          run   += d
        elif cat == "ride":         ride  += d
        elif cat == "virtual_ride": vride += d
        elif cat == "swim":         swim  += d
        elif cat == "walk":
            walk += d
            # Check eligibility for points
            moving_time   = a.get("moving_time",   0) or 0
            average_speed = a.get("average_speed", 0) or 0
            if moving_time >= WALK_MIN_MOVING_S and average_speed >= WALK_MIN_SPEED_MS:
                eligible_walk += d

    def km(v): return round(v / 1000, 3)
    rk, ck_, vk, sk, wk = km(run), km(ride), km(vride), km(swim), km(walk)
    ewk = km(eligible_walk)
    ckm = round(sum(challenge_km_for_activity(a) for a in acts), 3)
    counted_workouts = sum(1 for a in acts if classify(a.get("sport_type") or a.get("type","")) in _COUNTED_CATS)
    return dict(runKm=rk, cycleKm=ck_, virtualKm=vk, swimKm=sk, walkKm=wk,
                eligibleWalkKm=ewk,
                km=round(rk+ck_+vk+sk+wk, 3), durationSec=secs,
                workouts=counted_workouts, challengeKm=ckm,
                types=sorted(types))


def monthly_breakdown(acts: list, year: int) -> list:
    buckets = {m: [] for m in range(1, 13)}
    for a in acts:
        ts = a.get("start_date_local") or a.get("start_date", "")
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if dt.year == year: buckets[dt.month].append(a)
        except ValueError: pass

    names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    result = []
    for m in range(1, 13):
        month_acts = buckets[m]
        s = aggregate(month_acts)

        # Daily activity dots
        days = [0] * 31
        for a in month_acts:
            ts = a.get("start_date_local") or a.get("start_date", "")
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                idx = dt.day - 1
                if 0 <= idx < 31:
                    days[idx] = 1
            except (ValueError, IndexError): pass

        # Per-category kJ totals for dynamic scoring (raw device data, no conversion)
        run_kj     = 0.0
        run_km_kj  = 0.0  # km of runs that have kJ data (for factor calculation)
        cat_kj = {"run": 0.0, "ride": 0.0, "virtual_ride": 0.0, "swim": 0.0, "walk": 0.0}
        for a in month_acts:
            cat  = classify(a.get("sport_type") or a.get("type", ""))
            kj   = a.get("kilojoules") or 0
            dist = (a.get("distance", 0) or 0) / 1000
            if cat in cat_kj and kj > 0:
                cat_kj[cat] += kj
            if cat == "run" and dist > 0 and kj > 0:
                run_kj    += kj
                run_km_kj += dist

        # kJ/km factor for running — None if no kJ data this month
        run_kcal_per_km = round(run_kj / run_km_kj, 2) if run_km_kj > 0.5 else None

        # ── Rule E: Multi-sport triathlon days (May only) ──────────────────
        # Olympic: 40km bike + 10km run + 1.5km swim in one calendar day → 2pts
        # Sprint:  20km bike + 5km run + 0.75km swim in one calendar day → 1pt
        # Maximum 2pts regardless of how many sprint days achieved
        rule_e_pts = 0
        if m == 5:  # May only
            # Group activities by local calendar day
            from collections import defaultdict
            day_acts = defaultdict(lambda: {"run":0.0,"ride":0.0,"swim":0.0})
            for a in month_acts:
                cat  = classify(a.get("sport_type") or a.get("type",""))
                dist = (a.get("distance",0) or 0) / 1000
                ts   = a.get("start_date_local") or a.get("start_date","")
                try:
                    day_key = datetime.fromisoformat(ts.replace("Z","+00:00")).strftime("%Y-%m-%d")
                except Exception:
                    continue
                if cat == "run":
                    day_acts[day_key]["run"] += dist
                elif cat in ("ride","virtual_ride"):
                    day_acts[day_key]["ride"] += dist
                elif cat == "swim":
                    day_acts[day_key]["swim"] += dist

            got_olympic = False
            got_sprint  = False
            for day, d in day_acts.items():
                if d["ride"] >= 40 and d["run"] >= 10 and d["swim"] >= 1.5:
                    got_olympic = True
                elif d["ride"] >= 20 and d["run"] >= 5 and d["swim"] >= 0.75:
                    got_sprint = True

            if got_olympic:
                rule_e_pts = 2
            elif got_sprint:
                rule_e_pts = 1
        # ── goalDay: which calendar day did cumulative challenge-km first hit the goal? ──
        goal_day = None
        if month_acts:
            dated = []
            for a in month_acts:
                ts = a.get("start_date_local") or a.get("start_date", "")
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    if dt.year == year and dt.month == m:
                        dated.append((dt.day, challenge_km_for_activity(a)))
                except ValueError: pass
            dated.sort(key=lambda x: x[0])
            cumulative = 0.0
            for day_num, ckm in dated:
                cumulative += ckm
                if cumulative >= MONTHLY_GOAL_KM:
                    goal_day = day_num
                    break

        result.append(dict(
            year=year, month=m, label=names[m-1],
            cal=0, sess=s["workouts"], km=s["km"],
            runKm=s["runKm"], cycleKm=s["cycleKm"], virtualKm=s["virtualKm"],
            swimKm=s["swimKm"], walkKm=s["walkKm"], eligibleWalkKm=s["eligibleWalkKm"], actKcal=0,
            durationSec=s["durationSec"], challengeKm=round(s["challengeKm"], 3),
            goalDay=goal_day,
            runKcalPerKm=run_kcal_per_km,   # kJ/km factor for running (None if no kJ data)
            runCalKm=round(run_km_kj, 3),   # km of runs that had kJ data
            ruleEPts=rule_e_pts,  # 0, 1 (sprint tri), or 2 (olympic tri) — May only
            # Per-category kJ for dynamic scoring
            runCals=round(cat_kj["run"], 1),
            rideCals=round(cat_kj["ride"], 1),
            virtualCals=round(cat_kj["virtual_ride"], 1),
            swimCals=round(cat_kj["swim"], 1),
            walkCals=round(cat_kj["walk"], 1),
            days=days,
        ))
    return result


def week_bits(acts: list) -> tuple:
    cutoff = time.time() - 7*24*3600
    week, wcal = [False]*7, [0]*7
    for a in acts:
        ts = a.get("start_date_local") or a.get("start_date", "")
        try:
            dt = datetime.fromisoformat(ts.replace("Z","+00:00"))
            if dt.timestamp() >= cutoff:
                d = dt.weekday()
                week[d] = True
                wcal[d] = 1
        except (ValueError, IndexError): pass
    return week, wcal

# ─────────────────────────────────────────────────────────────
#  Avatar helpers
# ─────────────────────────────────────────────────────────────
_EMOJIS = ["🦦","🐺","🦊","🐸","🦁","🐯","🐨","🦋","🐼","🦅","🦆","🦉","🦜","🐙","🦈"]
_COLORS = ["#818cf8","#f472b6","#34d399","#fb923c","#38bdf8","#a78bfa","#fbbf24","#4ade80",
           "#f87171","#60a5fa","#e879f9","#2dd4bf","#facc15","#fb7185","#c084fc"]
_BG     = ["#ede9fe","#fce7f3","#d1fae5","#ffedd5","#e0f2fe","#f3e8ff","#fef3c7","#dcfce7",
           "#fee2e2","#dbeafe","#fae8ff","#ccfbf1","#fef9c3","#ffe4e6","#f3e8ff"]

def fmt_member(m: dict, idx: int, s: dict) -> dict:
    w  = s.pop("_w",  [False]*7)
    wc = s.pop("_wc", [0]*7)

    # Find last completely finished month's running kcal/km factor
    # Calculate average running kJ/km factor across entire calendar year
    now_dt   = datetime.now(timezone.utc)
    cur_year = now_dt.year
    monthly  = s.get("monthly", [])

    year_run_kj = sum(mo.get("runCals", 0) or 0 for mo in monthly if mo.get("year") == cur_year)
    year_run_km = sum(mo.get("runCalKm", 0) or 0 for mo in monthly if mo.get("year") == cur_year)
    run_kcal_factor = round(year_run_kj / year_run_km, 2) if year_run_km > 0.5 else None

    # Weight log and current BMI
    weight_log = m.get("weight_log", [])
    height_m   = m.get("height_m")
    # Most recent weight entry
    latest_weight = weight_log[0] if weight_log else None
    current_bmi   = latest_weight["bmi"] if latest_weight else None

    # Build monthly BMI map: for each month, find the last weight entry in that month
    # (i.e. the entry with the highest date that is still within that month)
    monthly_bmi = {}  # { (year, month): bmi }
    if height_m and weight_log:
        for entry in weight_log:
            try:
                entry_date = datetime.fromisoformat(entry["date"]).date()
            except Exception:
                continue
            key = (entry_date.year, entry_date.month)
            # Keep the latest date in each month
            if key not in monthly_bmi or entry["date"] > monthly_bmi[key]["date"]:
                monthly_bmi[key] = entry

    # Attach monthly BMI to monthly breakdown
    monthly_data = s.get("monthly", [])
    for mo in monthly_data:
        key = (mo.get("year"), mo.get("month"))
        entry = monthly_bmi.get(key)
        mo["weightBmi"] = entry["bmi"] if entry and entry.get("bmi") else None

    return dict(
        id=m["id"], name=m["name"], provider="strava",
        emoji=m.get("emoji") or _EMOJIS[idx%len(_EMOJIS)],
        color=m.get("color") or _COLORS[idx%len(_COLORS)],
        bg=m.get("bg")       or _BG[idx%len(_BG)],
        picture=m.get("strava_picture",""), height_m=height_m,
        **{k: s.get(k,0) for k in ("km","runKm","cycleKm","virtualKm","swimKm","walkKm",
                                    "durationSec","workouts","challengeKm","eligibleWalkKm")},
        runKcalFactor=run_kcal_factor,
        bmi=current_bmi,
        weightLog=weight_log,
        types=s.get("types",[]), monthly=s.get("monthly",[]),
        recentActs=s.get("recentActs",[]),
        week=w, weekCalories=wc)

# ─────────────────────────────────────────────────────────────
#  App
# ─────────────────────────────────────────────────────────────
async def hourly_sync_job():
    """Background task: sync all members once per hour at the top of the hour."""
    while True:
        # Wait until the top of the next hour
        now = datetime.now(timezone.utc)
        secs_until_next_hour = (60 - now.minute) * 60 - now.second
        await asyncio.sleep(secs_until_next_hour)

        print(f"[hourly-sync] Starting sync for all members")
        if is_strava_paused():
            print(f"[hourly-sync] PAUSED — skipping")
            continue
        db = load_db()
        for m in db["members"]:
            try:
                await sync_activities(m)
                # Bust in-memory cache so next /api/team call reads fresh data
                cache_bust(m["id"])
                print(f"[hourly-sync] Synced {m['name']}")
            except Exception as e:
                print(f"[hourly-sync] Failed for {m['name']}: {e}")
            # Small delay between members to avoid bursting Strava API
            await asyncio.sleep(2)
        print(f"[hourly-sync] Done")


@asynccontextmanager
async def lifespan(app):
    # Ensure the data directory exists (Railway volume or local)
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
        print(f"[startup] Created data directory: {db_dir}")
    if not os.path.exists(DB_PATH):
        save_db({"members": [], "next_id": 1})
        print(f"[startup] Created new database at: {DB_PATH}")
    else:
        db = load_db()
        print(f"[startup] Loaded database: {len(db['members'])} member(s) from {DB_PATH}")
    # Start hourly background sync
    task = asyncio.create_task(hourly_sync_job())
    yield
    task.cancel()

app = FastAPI(title="Fette Otter API", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# Health
@app.get("/api/health")
async def health():
    db = load_db()
    return {"status":"ok","members":len(db["members"]),
            "strava_configured": STRAVA_CLIENT_ID != "YOUR_CLIENT_ID"}

# Status — returns last_fetch timestamps so frontend can detect stale cache
@app.get("/api/status")
async def status():
    db = load_db()
    result = {}
    for m in db["members"]:
        stored = load_acts(m["id"])
        result[str(m["id"])] = stored.get("last_fetch", 0)
    return {"last_fetch": result, "ts": int(time.time())}

# Strava OAuth — initiate
@app.get("/api/strava/auth")
async def strava_auth(name: str = Query(...)):
    payload = json.dumps({"name": name})
    sig     = hmac.new(SECRET_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()[:16]
    url     = STRAVA_AUTH_URL + "?" + urlencode({
        "client_id": STRAVA_CLIENT_ID,
        "redirect_uri": f"{BACKEND_URL}/api/strava/callback",
        "response_type": "code", "approval_prompt": "auto",
        "scope": "read,activity:read_all", "state": payload+"|"+sig,
    })
    return RedirectResponse(url)

# Strava OAuth — callback
@app.get("/api/strava/callback")
async def strava_callback(code: Optional[str]=Query(None),
                          state: Optional[str]=Query(None),
                          error: Optional[str]=Query(None)):
    if error:
        return RedirectResponse(f"{FRONTEND_URL}?strava_error={error}")
    if not code or not state:
        return RedirectResponse(f"{FRONTEND_URL}?strava_error=missing_params")
    try:
        payload_str, _ = state.rsplit("|", 1)
        payload = json.loads(payload_str)
    except Exception:
        return RedirectResponse(f"{FRONTEND_URL}?strava_error=invalid_state")

    name = payload.get("name", "Athlete")

    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(STRAVA_TOKEN_URL, data={
            "client_id": STRAVA_CLIENT_ID, "client_secret": STRAVA_CLIENT_SECRET,
            "code": code, "grant_type": "authorization_code"})

    if r.status_code != 200:
        return RedirectResponse(f"{FRONTEND_URL}?strava_error=token_exchange_failed")

    tokens = r.json()
    athlete = tokens.get("athlete", {})
    picture = athlete.get("profile_medium") or athlete.get("profile", "")
    strava_id = str(athlete.get("id", ""))

    # Lock the entire read-modify-write so two simultaneous signups never get the same next_id
    async with _db_lock:
        db = load_db()
        members = db["members"]
        # Match only by strava_id — this is the only reliable identifier
        # (user_id from the frontend is not trusted as it could belong to a different person)
        member = None
        if strava_id:
            member = next((m for m in members if m.get("strava_id") == strava_id), None)

        if member:
            member.update({"strava_access_token": tokens["access_token"],
                           "strava_refresh_token": tokens["refresh_token"],
                           "strava_expires_at": tokens["expires_at"],
                           "strava_picture": picture, "strava_id": strava_id})
        else:
            idx = len(members)
            first = athlete.get("firstname","")
            last  = athlete.get("lastname","")
            full  = f"{first} {last}".strip()
            member = {
                "id": db["next_id"],
                "name": name or full or "Athlete",
                "strava_id": strava_id,
                "strava_access_token": tokens["access_token"],
                "strava_refresh_token": tokens["refresh_token"],
                "strava_expires_at": tokens["expires_at"],
                "strava_picture": picture,
                "emoji": _EMOJIS[idx%len(_EMOJIS)],
                "color": _COLORS[idx%len(_COLORS)],
                "bg":    _BG[idx%len(_BG)],
                "height_m": None,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            members.append(member)
            db["next_id"] += 1
            print(f"[signup] New member #{member['id']}: {member['name']} (strava_id={strava_id}), total members: {len(members)}")

        # save_db internally also locks, but since we're already holding the lock
        # write directly here to avoid deadlock
        tmp = DB_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(db, f, indent=2, default=str)
        if os.path.exists(DB_PATH):
            import shutil
            shutil.copy2(DB_PATH, DB_PATH + ".bak")
        os.replace(tmp, DB_PATH)
    cache_bust(member["id"])
    return RedirectResponse(
        f"{FRONTEND_URL}?strava_ok=1&member_id={member['id']}&member_name={member['name']}")

# ─────────────────────────────────────────────────────────────
#  GARMIN LOGIN  (email + password via garminconnect library)
# ─────────────────────────────────────────────────────────────

class GarminLoginBody(BaseModel):
    name:     str
    email:    str
    password: str

@app.post("/api/garmin/login")
async def garmin_login(body: GarminLoginBody):
    """
    Authenticate with Garmin Connect using email/password.
    Credentials are used only to obtain a session token — never stored.
    """
    import asyncio
    from garminconnect import Garmin, GarminConnectAuthenticationError

    name  = body.name.strip()
    email = body.email.strip()
    if not name:
        raise HTTPException(400, "Name is required")
    if not email or not body.password:
        raise HTTPException(400, "Email and password are required")

    # Run blocking garminconnect login in thread pool
    def do_login():
        client = Garmin(email, body.password)
        client.login()
        return client

    try:
        loop   = asyncio.get_event_loop()
        client = await loop.run_in_executor(None, do_login)
    except GarminConnectAuthenticationError:
        raise HTTPException(401, "Invalid Garmin email or password")
    except Exception as e:
        err = str(e).lower()
        if "mfa" in err or "2fa" in err or "factor" in err or "verification" in err:
            raise HTTPException(202, "MFA required — please check your email for a code")
        raise HTTPException(502, f"Garmin login failed: {str(e)}")

    # Get user profile for garmin_id
    try:
        profile   = client.get_full_name() or email
        garmin_id = str(client.profile.get("userId", email))
    except Exception:
        garmin_id = email  # fall back to email as unique ID
        profile   = name

    # Serialize the session tokens for storage
    # garth.dumps() returns a base64 string (not JSON) — store it as-is
    token_store = None
    try:
        if hasattr(client, 'client') and hasattr(client.client, 'dumps'):
            raw = client.client.dumps()
            if raw and raw != 'W251bGwsIG51bGxd':  # skip empty/null token
                token_store = raw  # store as plain string
                print(f"[garmin] Token saved for {name} ({len(raw)} chars base64)")
            else:
                print(f"[warn] garth.dumps() returned empty/null token for {name}")
    except Exception as te:
        print(f"[warn] Could not serialize Garmin token for {name}: {te}")

    if not token_store:
        raise HTTPException(500, "Garmin login succeeded but session token could not be saved. Please try again in a few minutes.")

    async with _db_lock:
        db      = load_db()
        members = db["members"]
        member  = next((m for m in members if m.get("garmin_id") == garmin_id), None)

        if member:
            member["garmin_token_store"] = token_store
            member["provider"]           = "garmin"
        else:
            idx    = len(members)
            member = {
                "id":                 db["next_id"],
                "name":               name,
                "garmin_id":          garmin_id,
                "garmin_token_store": token_store,
                "provider":           "garmin",
                "emoji":              _EMOJIS[idx % len(_EMOJIS)],
                "color":              _COLORS[idx % len(_COLORS)],
                "bg":                 _BG[idx    % len(_BG)],
                "height_m":           None,
                "created_at":         datetime.now(timezone.utc).isoformat(),
            }
            members.append(member)
            db["next_id"] += 1
            print(f"[signup] New Garmin member #{member['id']}: {member['name']}")

        # Atomic save
        import shutil as _shutil
        tmp = DB_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(db, f, indent=2, default=str)
        if os.path.exists(DB_PATH):
            _shutil.copy2(DB_PATH, DB_PATH + ".bak")
        os.replace(tmp, DB_PATH)

    cache_bust(member["id"])

    return {
        "ok":     True,
        "member": {
            "id":      member["id"],
            "name":    member["name"],
            "provider":"garmin",
            "emoji":   member["emoji"],
            "color":   member["color"],
            "bg":      member["bg"],
            "picture": "",
        }
    }

# Team stats
SYNC_STALE_SECS = 60 * 60  # trigger a sync if last_fetch is older than 60 minutes

@app.get("/api/team")
async def get_team(range_: str = Query("thismonth", alias="range")):
    db   = load_db()
    after, before = date_range(range_)
    yr   = datetime.now(timezone.utc).year
    yr_start = int(datetime(yr, 1, 1, tzinfo=timezone.utc).timestamp())
    now  = int(time.time())
    result = []

    for idx, m in enumerate(db["members"]):
        # Serve from cache if available — cache is busted on sync
        cached = cache_get(m["id"], range_)
        if cached:
            result.append(cached)
            continue

        # Load stored activities
        stored = load_acts(m["id"])
        last_fetch = stored.get("last_fetch", 0)
        needs_sync = (not stored["activities"]) or (now - last_fetch > SYNC_STALE_SECS)

        if needs_sync:
            try:
                print(f"[get_team] Stale data for {m['name']} (last_fetch {now - last_fetch}s ago) — syncing")
                year_acts = await sync_activities(m)
                cache_bust(m["id"])  # bust ALL period caches so stale data isn't served
            except Exception as e:
                print(f"[warn] sync failed for {m['name']}: {e}")
                year_acts = stored.get("activities", [])
        else:
            year_acts = stored["activities"]

        # Filter to requested period — no API call needed
        period_acts = [a for a in year_acts if after <= _act_ts(a) <= before]

        s = aggregate(period_acts)
        s["monthly"] = monthly_breakdown(year_acts, yr)
        w, wc = week_bits(year_acts)
        s["_w"] = w; s["_wc"] = wc
        # Include ALL year activities so frontend can filter for any selected period
        s["recentActs"] = [
            {
                "id":            str(a.get("id") or ""),
                "name":          str(a.get("name") or ""),
                "sport_type":    str(a.get("sport_type") or a.get("type") or ""),
                "date":          str(a.get("start_date_local") or a.get("start_date") or ""),
                "dist_km":       round(float(a.get("distance") or 0) / 1000, 2),
                "moving_time":   int(a.get("moving_time") or 0),
                "average_speed": float(a.get("average_speed") or 0),
                "kj":            float(a.get("kilojoules") or 0),
                "hr":            float(a.get("average_heartrate") or 0),
            }
            for a in sorted(year_acts, key=_act_ts, reverse=True)
        ]
        entry = fmt_member(m, idx, s)
        cache_set(m["id"], range_, entry)
        result.append(entry)

    return result

# Members list
@app.get("/api/members")
async def get_members():
    db = load_db()
    return [{"id":m["id"],"name":m["name"],"provider":"strava",
             "emoji":m.get("emoji","🦦"),"color":m.get("color","#818cf8"),
             "bg":m.get("bg","#ede9fe"),"picture":m.get("strava_picture","")}
            for m in db["members"]]

# Height — self-service (no admin required)
class HeightBody(BaseModel):
    height_cm: float
    admin_name: str = ""  # kept for backward compat, ignored

@app.post("/api/members/{mid}/height")
async def set_height(mid: int, body: HeightBody):
    if not (100 <= body.height_cm <= 250):
        raise HTTPException(400, "Height must be between 100–250 cm")
    db = load_db()
    m = next((x for x in db["members"] if x["id"] == mid), None)
    if not m: raise HTTPException(404, "Member not found")
    m["height_m"] = round(body.height_cm / 100, 3)
    save_db(db); cache_bust(mid)
    return {"ok": True, "height_m": m["height_m"]}

# Weight log — add a weight entry
class WeightBody(BaseModel):
    weight_kg: float
    date: str  # ISO date string e.g. "2026-04-15"

@app.post("/api/members/{mid}/weight")
async def log_weight(mid: int, body: WeightBody):
    if not (20 <= body.weight_kg <= 300):
        raise HTTPException(400, "Weight must be between 20–300 kg")
    # Validate date — must be within current year and not before Jan 1
    try:
        entry_date = datetime.fromisoformat(body.date).date()
    except ValueError:
        raise HTTPException(400, "Invalid date format")
    yr = datetime.now(timezone.utc).year
    jan1 = date(yr, 1, 1)
    today = datetime.now(timezone.utc).date()
    if entry_date < jan1 or entry_date > today:
        raise HTTPException(400, f"Date must be between Jan 1 {yr} and today")

    db = load_db()
    m = next((x for x in db["members"] if x["id"] == mid), None)
    if not m: raise HTTPException(404, "Member not found")

    height_m = m.get("height_m")
    bmi = round(body.weight_kg / (height_m ** 2), 1) if height_m else None

    if "weight_log" not in m:
        m["weight_log"] = []

    # Update existing entry for same date, or add new
    existing = next((e for e in m["weight_log"] if e["date"] == body.date), None)
    if existing:
        existing["weight_kg"] = round(body.weight_kg, 1)
        existing["bmi"]       = bmi
    else:
        m["weight_log"].append({
            "date":      body.date,
            "weight_kg": round(body.weight_kg, 1),
            "bmi":       bmi,
        })

    # Sort log by date descending
    m["weight_log"].sort(key=lambda e: e["date"], reverse=True)
    save_db(db); cache_bust(mid)
    return {"ok": True, "bmi": bmi, "weight_kg": round(body.weight_kg, 1)}

@app.get("/api/members/{mid}/weight")
async def get_weight_log(mid: int):
    db = load_db()
    m = next((x for x in db["members"] if x["id"] == mid), None)
    if not m: raise HTTPException(404, "Member not found")
    return {"weight_log": m.get("weight_log", []), "height_m": m.get("height_m")}

# Delete a specific activity from a member's stored file
@app.delete("/api/admin/members/{mid}/activity/{act_id}")
async def delete_activity(mid: int, act_id: str):
    stored = load_acts(mid)
    acts = stored.get("activities", [])
    before = len(acts)
    stored["activities"] = [a for a in acts if str(a.get("id", "")) != act_id]
    if len(stored["activities"]) == before:
        raise HTTPException(404, f"Activity {act_id} not found for member {mid}")
    save_acts(mid, stored)
    cache_bust(mid)
    return {"ok": True, "removed": before - len(stored["activities"])}

# Delete a specific weight entry from a member
@app.delete("/api/admin/members/{mid}/weight/{date}")
async def delete_weight_entry(mid: int, date: str):
    db = load_db()
    m = next((x for x in db["members"] if x["id"] == mid), None)
    if not m: raise HTTPException(404, "Member not found")
    before = len(m.get("weight_log", []))
    m["weight_log"] = [e for e in m.get("weight_log", []) if e.get("date") != date]
    if len(m["weight_log"]) == before:
        raise HTTPException(404, f"Weight entry for {date} not found")
    save_db(db)
    cache_bust(mid)
    return {"ok": True}


# Rename member
class RenameBody(BaseModel):
    name: str

@app.post("/api/members/{mid}/rename")
async def rename_member(mid: int, body: RenameBody):
    name = body.name.strip()
    if not name:
        raise HTTPException(400, "Name cannot be empty")
    if len(name) > 80:
        raise HTTPException(400, "Name too long (max 80 chars)")
    db = load_db()
    m = next((x for x in db["members"] if x["id"] == mid), None)
    if not m: raise HTTPException(404, "Member not found")
    old_name = m["name"]
    m["name"] = name
    save_db(db); cache_bust(mid)
    print(f"[rename] Member #{mid}: '{old_name}' → '{name}'")
    return {"ok": True, "id": mid, "old_name": old_name, "new_name": name}

# Remove member
class AdminBody(BaseModel):
    admin_name: Optional[str] = None

@app.delete("/api/members/{mid}")
async def remove_member(mid: int, body: AdminBody = Body(default=AdminBody())):
    db = load_db()
    n = len(db["members"])
    db["members"] = [m for m in db["members"] if m["id"] != mid]
    if len(db["members"]) == n: raise HTTPException(404, "Not found")
    save_db(db); cache_bust(mid)
    return {"ok": True}

@app.get("/api/admin/reset-sync")
async def reset_sync():
    """Reset last_fetch to 0 for all members, forcing a full year resync on next sync call."""
    db = load_db()
    for m in db["members"]:
        mid = m["id"]
        stored = load_acts(mid)
        stored["last_fetch"] = 0
        stored["activities"] = []
        save_acts(mid, stored)
    _cache.clear()
    return {"ok": True, "message": "Reset complete — run /api/admin/sync to fetch all data"}

@app.get("/api/admin/reset-sync/{mid}")
async def reset_sync_member(mid: int):
    """Reset last_fetch to 0 for a single member only."""
    db = load_db()
    m = next((x for x in db["members"] if x["id"] == mid), None)
    if not m:
        raise HTTPException(404, "Member not found")
    stored = load_acts(mid)
    stored["last_fetch"] = 0
    stored["activities"] = []
    save_acts(mid, stored)
    cache_bust(mid)
    return {"ok": True, "member": m["name"], "message": f"Reset complete for {m['name']} — run /api/admin/sync to fetch their data"}

@app.get("/api/admin/debug-garmin/{mid}")
async def debug_garmin(mid: int):
    """Debug Garmin sync for a specific member — shows token status and raw fetch attempt."""
    import asyncio
    db = load_db()
    m = next((x for x in db["members"] if x["id"] == mid), None)
    if not m: raise HTTPException(404, "Member not found")

    provider      = m.get("provider", "strava")
    has_token  = bool(m.get("garmin_token_store"))
    token_store_val = m.get("garmin_token_store")
    token_info = f"base64 string ({len(token_store_val)} chars)" if isinstance(token_store_val, str) else (list(token_store_val.keys()) if isinstance(token_store_val, dict) else "unknown")
    stored        = load_acts(mid)
    stored_count  = len(stored.get("activities", []))
    last_fetch    = stored.get("last_fetch", 0)

    if provider != "garmin":
        return {"member": m["name"], "provider": provider, "error": "Not a Garmin member"}
    if not has_token:
        return {"member": m["name"], "provider": "garmin", "error": "No garmin_token_store — user needs to re-login"}

    # Try a live fetch for last 7 days to test the token
    now      = int(time.time())
    after_dt = datetime.fromtimestamp(now - 7 * 86400, tz=timezone.utc)
    before_dt = datetime.fromtimestamp(now, tz=timezone.utc)

    def do_test_fetch():
        from garminconnect import Garmin
        token_store = m.get("garmin_token_store")
        client = Garmin()
        if isinstance(token_store, str):
            client.client.loads(token_store)
        else:
            import json as _j
            client.client.loads(_j.dumps(token_store))
        acts = client.get_activities_by_date(
            after_dt.strftime("%Y-%m-%d"),
            before_dt.strftime("%Y-%m-%d"),
        )
        return acts

    loop = asyncio.get_event_loop()
    try:
        raw = await loop.run_in_executor(None, do_test_fetch)
        sample = [
            {
                "name":           a.get("activityName"),
                "type":           a.get("activityType", {}).get("typeKey"),
                "date":           a.get("startTimeLocal"),
                "raw_distance":   a.get("distance"),   # raw value from Garmin API
                "dist_km_if_m":   round((a.get("distance") or 0) / 1000, 2),   # if Garmin returns metres
                "dist_km_if_km":  round((a.get("distance") or 0), 2),          # if Garmin returns km
                "duration_m":     round((a.get("duration") or 0) / 60, 1),
                "averageSpeed":   a.get("averageSpeed"),
            }
            for a in (raw or [])[:10]
        ]
        return {
            "member":       m["name"],
            "provider":     "garmin",
            "garmin_id":    m.get("garmin_id"),
            "token_info":   token_info,
            "stored_count": stored_count,
            "last_fetch_ago_h": round((now - last_fetch) / 3600, 1) if last_fetch else None,
            "fetch_last_7d_count": len(raw or []),
            "sample":       sample,
        }
    except Exception as e:
        return {
            "member":     m["name"],
            "provider":   "garmin",
            "token_info": token_info,
            "error":      str(e),
            "hint":       "Token likely expired — user needs to re-login via Garmin Connect button",
        }

# Toggle Strava sync pause
@app.post("/api/admin/strava-pause")
async def toggle_strava_pause():
    db = load_db()
    current = db.get("strava_paused", False)
    db["strava_paused"] = not current
    save_db(db)
    state = "paused" if db["strava_paused"] else "active"
    print(f"[admin] Strava sync {state}")
    return {"ok": True, "paused": db["strava_paused"], "state": state}

@app.get("/api/admin/strava-status")
async def strava_status():
    return {"paused": is_strava_paused()}

# Clear cache
@app.get("/api/admin/clear-cache")
async def clear_cache():
    _cache.clear()
    return {"ok": True}

# Manually trigger a sync for all members (use after deploy)
@app.get("/api/admin/sync")
async def manual_sync():
    if is_strava_paused():
        return {"ok": False, "message": "Strava sync is currently paused — toggle it on first"}
    db = load_db()
    results = []
    for m in db["members"]:
        try:
            acts = await sync_activities(m)
            cache_bust(m["id"])
            results.append({"member": m["name"], "activities": len(acts), "ok": True})
        except Exception as e:
            results.append({"member": m["name"], "error": str(e), "ok": False})
        await asyncio.sleep(2)  # be polite to Strava API
    return {"synced": len(results), "results": results}

# Debug — inspect raw Strava activity data for a specific member
@app.get("/api/admin/debug-activities/{mid}")
async def debug_activities(mid: int, request: Request, limit: int = 5):
    db = load_db()
    m = next((x for x in db["members"] if x["id"] == mid), None)
    if not m: raise HTTPException(404, "Member not found")
    try:
        m = await refresh(m)
        # Read from stored file by default, fetch live from Strava if ?live=1
        live = request.query_params.get('live', '0') == '1'
        if live:
            hdrs = {"Authorization": f"Bearer {m['strava_access_token']}"}
            async with httpx.AsyncClient(timeout=30) as c:
                r = await c.get(f"{STRAVA_API_BASE}/athlete/activities", headers=hdrs,
                                params={"per_page": limit, "page": 1})
            acts = r.json() if r.status_code == 200 else []
            source = "strava_live"
        else:
            stored = load_acts(m["id"])
            acts = sorted(stored.get("activities", []), key=lambda a: a.get("start_date",""), reverse=True)[:limit]
            source = "stored_file"
        return {
            "member": m["name"],
            "strava_id": m.get("strava_id"),
            "source": source,
            "activity_count": len(acts),
            "activities": [
                {
                    "name":          a.get("name"),
                    "sport_type":    a.get("sport_type"),
                    "date":          a.get("start_date_local"),
                    "distance_km":   round((a.get("distance") or 0) / 1000, 2),
                    "moving_time_s": a.get("moving_time"),
                    "moving_time_m": round((a.get("moving_time") or 0) / 60, 1),
                    "average_speed_ms": a.get("average_speed"),
                    "average_speed_kmh": round((a.get("average_speed") or 0) * 3.6, 2),
                    "calories":      a.get("calories"),
                    "kilojoules":    a.get("kilojoules"),
                    "classified_as": classify(a.get("sport_type") or a.get("type") or ""),
                    "eligible_walk": (
                        classify(a.get("sport_type") or a.get("type") or "") == "walk"
                        and (a.get("moving_time") or 0) >= WALK_MIN_MOVING_S
                        and (a.get("average_speed") or 0) >= WALK_MIN_SPEED_MS
                    ),
                }
                for a in (acts if isinstance(acts, list) else [])
            ]
        }
    except Exception as e:
        raise HTTPException(500, str(e))

# Debug — raw sync result for one member showing timestamps
@app.get("/api/admin/debug-sync/{mid}")
async def debug_sync(mid: int):
    db = load_db()
    m = next((x for x in db["members"] if x["id"] == mid), None)
    if not m: raise HTTPException(404, "Not found")

    yr = datetime.now(timezone.utc).year
    yr_start = int(datetime(yr, 1, 1, tzinfo=timezone.utc).timestamp())
    now = int(time.time())

    m = await refresh(m)
    hdrs = {"Authorization": f"Bearer {m['strava_access_token']}"}

    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.get(f"{STRAVA_API_BASE}/athlete/activities", headers=hdrs,
                        params={"after": yr_start, "before": now, "per_page": 5, "page": 1})
    
    raw = r.json() if r.status_code == 200 else {"error": r.status_code, "body": r.text}
    
    samples = []
    if isinstance(raw, list):
        for a in raw[:3]:
            ts = _act_ts(a)
            samples.append({
                "name": a.get("name"),
                "start_date_local": a.get("start_date_local"),
                "start_date": a.get("start_date"),
                "_act_ts": ts,
                "yr_start": yr_start,
                "passes_filter": ts >= yr_start,
            })

    return {
        "member": m["name"],
        "yr_start": yr_start,
        "now": now,
        "http_status": r.status_code,
        "raw_count": len(raw) if isinstance(raw, list) else 0,
        "samples": samples,
    }

# Debug — check stored activity files
@app.get("/api/admin/debug-store")
async def debug_store():
    db = load_db()
    result = []
    for m in db["members"]:
        stored = load_acts(m["id"])
        acts = stored.get("activities", [])
        result.append({
            "member": m["name"],
            "id": m["id"],
            "stored_count": len(acts),
            "last_fetch": stored.get("last_fetch", 0),
            "file_exists": os.path.exists(acts_path(m["id"])),
            "sample": acts[0] if acts else None,
        })
    return result

# Debug — inspect raw DB (member names + IDs only, no tokens)
@app.get("/api/admin/debug-db")
async def debug_db():
    db = load_db()
    return {
        "next_id": db["next_id"],
        "member_count": len(db["members"]),
        "members": [
            {"id": m["id"], "name": m["name"], "strava_id": m.get("strava_id"),
             "created_at": m.get("created_at")}
            for m in db["members"]
        ]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=True)
