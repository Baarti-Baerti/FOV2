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
DB_PATH              = os.getenv("DB_PATH",        "fette_otter.json")
SECRET_KEY           = os.getenv("SECRET_KEY",     secrets.token_hex(32))

STRAVA_AUTH_URL  = "https://www.strava.com/oauth/authorize"
STRAVA_TOKEN_URL = "https://www.strava.com/oauth/token"
STRAVA_API_BASE  = "https://www.strava.com/api/v3"

# ─────────────────────────────────────────────────────────────
#  Tiny JSON "database"
# ─────────────────────────────────────────────────────────────
def load_db() -> dict:
    if os.path.exists(DB_PATH):
        with open(DB_PATH) as f:
            return json.load(f)
    return {"members": [], "next_id": 1}

def save_db(db: dict):
    with open(DB_PATH, "w") as f:
        json.dump(db, f, indent=2, default=str)

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


async def fetch_activities(member: dict, after: int, before: int) -> list:
    member = await refresh(member)
    hdrs = {"Authorization": f"Bearer {member['strava_access_token']}"}
    acts, page = [], 1
    async with httpx.AsyncClient(timeout=30) as c:
        while True:
            r = await c.get(f"{STRAVA_API_BASE}/athlete/activities", headers=hdrs,
                            params={"after": after, "before": before, "per_page": 100, "page": page})
            if r.status_code != 200: break
            batch = r.json()
            if not isinstance(batch, list) or not batch: break
            acts.extend(batch)
            if len(batch) < 100: break
            page += 1
    return acts

# ─────────────────────────────────────────────────────────────
#  Stats aggregation
# ─────────────────────────────────────────────────────────────
def aggregate(acts: list) -> dict:
    run = ride = vride = swim = walk = 0.0
    secs = cals = 0
    kcal = 0.0
    types = set()
    for a in acts:
        cat = classify(a.get("sport_type") or a.get("type", ""))
        d = a.get("distance", 0) or 0
        secs += a.get("elapsed_time", 0) or 0
        cals += a.get("calories", 0) or 0
        kcal += (a.get("kilojoules", 0) or 0) * 0.239
        types.add(a.get("sport_type") or a.get("type") or "Unknown")
        if   cat == "run":          run   += d
        elif cat == "ride":         ride  += d
        elif cat == "virtual_ride": vride += d
        elif cat == "swim":         swim  += d
        elif cat == "walk":         walk  += d
    def km(v): return round(v / 1000, 2)
    rk, ck_, vk, sk, wk = km(run), km(ride), km(vride), km(swim), km(walk)
    return dict(runKm=rk, cycleKm=ck_, virtualKm=vk, swimKm=sk, walkKm=wk,
                km=round(rk+ck_+vk+sk+wk, 2), durationSec=secs,
                calories=round(cals), actKcal=round(kcal),
                workouts=len(acts), challengeKm=round(rk+ck_/5+vk/4+sk*4, 2),
                types=sorted(types))


MONTHLY_GOAL_KM = 66.67  # challenge goal per month

def challenge_km_for_activity(a: dict) -> float:
    """Return the challenge-km contribution of a single activity."""
    cat  = classify(a.get("sport_type") or a.get("type", ""))
    dist = (a.get("distance", 0) or 0) / 1000  # metres → km
    if   cat == "run":          return dist
    elif cat == "ride":         return dist / 5
    elif cat == "virtual_ride": return dist / 4
    elif cat == "swim":         return dist * 4
    elif cat == "walk":         return dist  # backend counts all walk; frontend can apply pace filter
    return 0.0


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
                    days[idx] = max(days[idx], a.get("calories", 0) or 0)
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
            cal=s["calories"], sess=s["workouts"], km=s["km"],
            runKm=s["runKm"], cycleKm=s["cycleKm"], virtualKm=s["virtualKm"],
            swimKm=s["swimKm"], walkKm=s["walkKm"], actKcal=s["actKcal"],
            durationSec=s["durationSec"], challengeKm=s["challengeKm"],
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
                wcal[d] = max(wcal[d], a.get("calories",0) or 0)
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
                                    "durationSec","calories","actKcal","workouts","challengeKm")},
        types=s.get("types",[]), monthly=s.get("monthly",[]),
        week=w, weekCalories=wc)

# ─────────────────────────────────────────────────────────────
#  App
# ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app):
    if not os.path.exists(DB_PATH):
        save_db({"members": [], "next_id": 1})
    yield

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
async def strava_auth(name: str = Query(...), user_id: Optional[int] = Query(None)):
    payload = json.dumps({"name": name, "user_id": user_id})
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

    name, user_id = payload.get("name","Athlete"), payload.get("user_id")

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

    db = load_db()
    members = db["members"]
    member = None
    if user_id:
        member = next((m for m in members if m["id"] == user_id), None)
    if not member and strava_id:
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

    save_db(db)
    cache_bust(member["id"])
    return RedirectResponse(
        f"{FRONTEND_URL}?strava_ok=1&member_id={member['id']}&member_name={member['name']}")

# Team stats
@app.get("/api/team")
async def get_team(range_: str = Query("thismonth", alias="range")):
    db = load_db()
    after, before = date_range(range_)
    yr = datetime.now(timezone.utc).year
    yr_after  = int(datetime(yr,1,1,tzinfo=timezone.utc).timestamp())
    yr_before = int(datetime.now(timezone.utc).timestamp())
    result = []
    for idx, m in enumerate(db["members"]):
        cached = cache_get(m["id"], range_)
        if cached:
            result.append(cached); continue
        try:
            period_acts = await fetch_activities(m, after, before)
            year_acts   = await fetch_activities(m, yr_after, yr_before)
        except Exception as e:
            print(f"[warn] {m['name']}: {e}")
            period_acts = year_acts = []
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

# Clear cache
@app.get("/api/admin/clear-cache")
async def clear_cache():
    _cache.clear(); return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=True)
