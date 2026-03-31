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
from datetime import datetime, timezone, timedelta
from typing import Optional
from contextlib import asynccontextmanager
from urllib.parse import urlencode

import httpx
from fastapi import FastAPI, HTTPException, Query, Body
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
    Incremental sync: only fetch activities newer than what we already have.
    Returns stored activities without fetching if STRAVA_PAUSED=true.
    """
    mid      = member["id"]
    now      = int(time.time())
    yr       = datetime.now(timezone.utc).year
    yr_start = int(datetime(yr, 1, 1, tzinfo=timezone.utc).timestamp())

    stored     = load_acts(mid)
    last_fetch = stored.get("last_fetch", 0)

    # If paused, just return what we have stored
    if is_strava_paused():
        print(f"[sync] PAUSED — skipping Strava fetch for {member['name']}")
        return stored.get("activities", [])

    # First time: fetch from Jan 1. Otherwise: fetch from last sync minus 1hr overlap
    after = yr_start if last_fetch == 0 else max(yr_start, last_fetch - 3600)

    member = await refresh(member)
    hdrs = {"Authorization": f"Bearer {member['strava_access_token']}"}

    new_acts = []
    page = 1
    async with httpx.AsyncClient(timeout=30) as c:
        while True:
            r = await c.get(f"{STRAVA_API_BASE}/athlete/activities", headers=hdrs,
                            params={"after": after, "before": now,
                                    "per_page": 100, "page": page})
            if r.status_code == 429:
                print(f"[warn] Strava rate limit for {member['name']}")
                break
            if r.status_code != 200:
                print(f"[warn] Strava API {r.status_code} for {member['name']}")
                break
            batch = r.json()
            if not isinstance(batch, list) or not batch:
                break
            new_acts.extend(batch)
            if len(batch) < 100:
                break
            page += 1

    print(f"[sync] {member['name']}: fetched {len(new_acts)} new activities")

    # Merge: build a dict keyed by activity ID so duplicates are overwritten cleanly
    existing = {a["id"]: a for a in stored.get("activities", []) if "id" in a}
    for a in new_acts:
        if "id" in a:
            existing[a["id"]] = a  # insert or update

    # Keep only current year, sorted newest first
    all_acts = [a for a in existing.values() if _act_ts(a) >= yr_start]
    all_acts.sort(key=_act_ts, reverse=True)

    # Always save with updated last_fetch so next sync is incremental
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
WALK_MIN_MOVING_S  = 30 * 60      # 30 minutes in seconds

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
    secs = 0      # only time from counted activity types
    types = set()
    for a in acts:
        cat = classify(a.get("sport_type") or a.get("type", ""))
        d = a.get("distance", 0) or 0

        types.add(a.get("sport_type") or a.get("type") or "Unknown")
        if cat in _COUNTED_CATS:
            # Only count time for the 5 tracked activity types
            secs += a.get("elapsed_time", 0) or 0
        if   cat == "run":          run   += d
        elif cat == "ride":         ride  += d
        elif cat == "virtual_ride": vride += d
        elif cat == "swim":         swim  += d
        elif cat == "walk":         walk  += d
    # Use 3 decimal places for full precision — no rounding until display
    def km(v): return round(v / 1000, 3)
    rk, ck_, vk, sk, wk = km(run), km(ride), km(vride), km(swim), km(walk)
    # challengeKm: sum per-activity so walk filter is applied individually
    ckm = round(sum(challenge_km_for_activity(a) for a in acts), 3)
    # Only count sessions from the 5 tracked activity types
    counted_workouts = sum(1 for a in acts if classify(a.get("sport_type") or a.get("type","")) in _COUNTED_CATS)
    return dict(runKm=rk, cycleKm=ck_, virtualKm=vk, swimKm=sk, walkKm=wk,
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

        # Daily calories heatmap (for the activity dots)
        days = [0] * 31
        for a in month_acts:
            ts = a.get("start_date_local") or a.get("start_date", "")
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                idx = dt.day - 1
                if 0 <= idx < 31:
                    days[idx] = 1
            except (ValueError, IndexError): pass

        # ── goalDay: which calendar day did cumulative challenge-km first hit 66.67? ──
        # Sort activities chronologically, accumulate challenge-km day by day.
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
                    break  # first day the goal is crossed

        result.append(dict(
            year=year, month=m, label=names[m-1],
            cal=0, sess=s["workouts"], km=s["km"],
            runKm=s["runKm"], cycleKm=s["cycleKm"], virtualKm=s["virtualKm"],
            swimKm=s["swimKm"], walkKm=s["walkKm"], actKcal=0,
            durationSec=s["durationSec"], challengeKm=round(s["challengeKm"], 3),
            goalDay=goal_day,  # None if goal not yet reached this month
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
    return dict(
        id=m["id"], name=m["name"], provider="strava",
        emoji=m.get("emoji") or _EMOJIS[idx%len(_EMOJIS)],
        color=m.get("color") or _COLORS[idx%len(_COLORS)],
        bg=m.get("bg")       or _BG[idx%len(_BG)],
        picture=m.get("strava_picture",""), height_m=m.get("height_m"),
        **{k: s.get(k,0) for k in ("km","runKm","cycleKm","virtualKm","swimKm","walkKm",
                                    "durationSec","workouts","challengeKm")},
        types=s.get("types",[]), monthly=s.get("monthly",[]),
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

# Team stats
@app.get("/api/team")
async def get_team(range_: str = Query("thismonth", alias="range")):
    db   = load_db()
    after, before = date_range(range_)
    yr   = datetime.now(timezone.utc).year
    yr_start = int(datetime(yr, 1, 1, tzinfo=timezone.utc).timestamp())
    result = []

    for idx, m in enumerate(db["members"]):
        # Serve from cache if available — cache is only busted on hourly sync
        cached = cache_get(m["id"], range_)
        if cached:
            result.append(cached)
            continue

        # Load stored activities (incremental sync already done by hourly job)
        # Fall back to a live sync if the store is empty (first ever load)
        stored = load_acts(m["id"])
        if not stored["activities"]:
            try:
                year_acts = await sync_activities(m)
            except Exception as e:
                print(f"[warn] initial sync failed for {m['name']}: {e}")
                year_acts = []
        else:
            year_acts = stored["activities"]

        # Filter to requested period — no API call needed
        period_acts = [a for a in year_acts if after <= _act_ts(a) <= before]

        s = aggregate(period_acts)
        s["monthly"] = monthly_breakdown(year_acts, yr)
        w, wc = week_bits(year_acts)
        s["_w"] = w; s["_wc"] = wc
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

# Height
class HeightBody(BaseModel):
    admin_name: str
    height_cm: float

@app.post("/api/members/{mid}/height")
async def set_height(mid: int, body: HeightBody):
    if not (100 <= body.height_cm <= 250):
        raise HTTPException(400, "Height 100–250 cm")
    db = load_db()
    m = next((x for x in db["members"] if x["id"] == mid), None)
    if not m: raise HTTPException(404, "Not found")
    m["height_m"] = round(body.height_cm/100, 3)
    save_db(db); cache_bust(mid)
    return {"ok": True, "height_m": m["height_m"]}

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
async def debug_activities(mid: int, limit: int = 5):
    db = load_db()
    m = next((x for x in db["members"] if x["id"] == mid), None)
    if not m: raise HTTPException(404, "Member not found")
    try:
        m = await refresh(m)
        hdrs = {"Authorization": f"Bearer {m['strava_access_token']}"}
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(f"{STRAVA_API_BASE}/athlete/activities", headers=hdrs,
                            params={"per_page": limit, "page": 1})
        acts = r.json()
        return {
            "member": m["name"],
            "strava_id": m.get("strava_id"),
            "activity_count": len(acts),
            "activities": [
                {
                    "name":          a.get("name"),
                    "sport_type":    a.get("sport_type"),
                    "date":          a.get("start_date_local"),
                    "distance_km":   round((a.get("distance") or 0) / 1000, 2),

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
