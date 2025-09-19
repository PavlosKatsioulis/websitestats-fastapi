from fastapi import FastAPI, HTTPException, Query, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from pydantic import BaseModel
from collections import defaultdict
import json
import os
import redis
from elasticsearch import Elasticsearch
from jose import JWTError, jwt

# Routers
from kpi import router as kpi_router
from search_api import router as search_router
from troubleshooting_docs import router as docs_router
from sheet_data import router as sheet_router
from installations import router as installations_router
from notifications import router as notifications_router
from websocket_manager import manager
from login import router as login_router, SECRET_KEY, ALGORITHM  # SECRET_KEY/ALGORITHM used in WS auth
from sales import router as sales_router

# ------------------ Config (ENV-driven) ------------------
ELASTIC_HOST = os.getenv("ELASTIC_HOST", "http://elasticsearch:9200")
REDIS_URL    = os.getenv("REDIS_URL", "redis://redis:6379/0")

DATA_DIR      = Path(os.getenv("DATA_DIR", str(Path(__file__).parent / "data")))
COMPANIES_DIR = Path(os.getenv("COMPANIES_DIR", str(DATA_DIR / "companies")))

# CORS: comma-separated in env -> list. If not provided, use explicit safe defaults.
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "").strip()
if CORS_ORIGINS:
    ALLOW_ORIGINS = [o.strip() for o in CORS_ORIGINS.split(",") if o.strip()]
else:
    # Defaults for your setup: local dev, LAN, tailscale
    ALLOW_ORIGINS = [
        "http://localhost:3000", "http://127.0.0.1:3000",
        "http://localhost:3001", "http://127.0.0.1:3001",
        "http://192.168.1.187", "http://192.168.1.187:3000", "http://192.168.1.187:3001",
        "http://100.105.15.125:3000", "http://100.105.15.125:3001",  # tailscale current
    ]

# ------------------ App & Clients ------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=86400,
)

print(f"[CORS] allow_origins={ALLOW_ORIGINS}")

# Static files (served from backend/images/)
app.mount("/static", StaticFiles(directory="images"), name="static")

# Elasticsearch client (sync)
es = Elasticsearch(ELASTIC_HOST)

# Redis client
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# ------------------ Routers ------------------
app.include_router(search_router)
app.include_router(docs_router)
app.include_router(sheet_router)
app.include_router(installations_router)
app.include_router(login_router)
app.include_router(notifications_router)
app.include_router(sales_router)
app.include_router(kpi_router)

# ------------------ Models ------------------
class TicketFilterRequest(BaseModel):
    myidstring: str
    start_date: str  # YYYY-MM-DD
    end_date: str    # YYYY-MM-DD

class CompanySearchRequest(BaseModel):
    mapaddress: Optional[str] = None
    taxafm: Optional[str] = None
    fiscal: Optional[str] = None
    country: Optional[str] = None
    primaryphone: Optional[str] = None
    mobilephone: Optional[str] = None
    eft_pos: Optional[List[str]] = None
    include_services: Optional[List[str]] = None  # NEW
    exclude_services: Optional[List[str]] = None  # NEW

SERVICES_INDEX = os.getenv("SERVICES_INDEX", "services")

def _service_clause_for_token(token: str) -> List[Dict]:
    token = (token or "").strip().upper()
    if not token:
        return []

    if token == "EDS":
        return [
            {"term": {"IDTEXT": "EDS"}},
            {"terms": {"VAR_IDNAME": ["EFOOD", "WOLT", "BOX"]}},
        ]

    if ":" in token:
        left, right = token.split(":", 1)
        return [
            {"term": {"IDTEXT": left.strip()}},
            {"term": {"VAR_IDNAME": right.strip()}},
        ]

    return [{"term": {"IDTEXT": token}}]


def _company_ids_for_services(es_client: Elasticsearch, tokens: List[str]) -> Set[str]:
    should = []
    for t in tokens or []:
        clause = _service_clause_for_token(t)
        if clause:
            should.append({"bool": {"must": clause}})

    if not should:
        return set()

    body = {
        "size": 10000,
        "_source": ["MYIDSTRING", "COMPANY_ID"],
        "query": {"bool": {"should": should, "minimum_should_match": 1}},
        "track_total_hits": True,
        "sort": [{"_doc": "asc"}],
    }

    ids: Set[str] = set()
    resp = es_client.search(index=SERVICES_INDEX, body=body)
    hits = resp.get("hits", {}).get("hits", [])
    while hits:
        for h in hits:
            src = h.get("_source") or {}
            mid = src.get("MYIDSTRING") or src.get("COMPANY_ID")
            if mid:
                ids.add(mid)
        break

    return ids
    
def _extract_company_id(src: dict) -> Optional[str]:
    return (src or {}).get("MYIDSTRING") or (src or {}).get("COMPANY_ID")


def _company_ids_for_single_token(es_client: Elasticsearch, token: str) -> set[str]:
    must = _service_clause_for_token(token)
    if not must:
        return set()
    body = {
        "size": 10000,
        "_source": ["MYIDSTRING", "COMPANY_ID"],
        "query": {"bool": {"must": must}},
    }
    resp = es_client.search(index=SERVICES_INDEX, body=body)
    ids = set()
    for h in resp.get("hits", {}).get("hits", []):
        cid = _extract_company_id(h.get("_source"))
        if cid:
            ids.add(cid)
    return ids


def _company_ids_for_services_any(es_client: Elasticsearch, tokens: list[str]) -> set[str]:
    union_ids: set[str] = set()
    for t in tokens or []:
        union_ids |= _company_ids_for_single_token(es_client, t)
    return union_ids


def _company_ids_for_services_all(es_client: Elasticsearch, tokens: list[str]) -> set[str]:
    tokens = [t for t in (tokens or []) if (t or "").strip()]
    if not tokens:
        return set()

    # start with first token's set
    acc = _company_ids_for_single_token(es_client, tokens[0])
    if not acc:
        return set()

    for t in tokens[1:]:
        ids = _company_ids_for_single_token(es_client, t)
        if not ids:
            return set()  # early exit—can't satisfy AND if any token returns nothing
        acc &= ids
        if not acc:
            return set()
    return acc


# ------------------ Health ------------------
@app.get("/health")
def health():
    try:
        ok_es = es.ping()
    except Exception:
        ok_es = False
    try:
        redis_client.ping()
        ok_redis = True
    except Exception:
        ok_redis = False
    return {"ok": ok_es and ok_redis, "elasticsearch": ok_es, "redis": ok_redis}

# ------------------ WebSocket (token via query) ------------------
@app.websocket("/ws/live")
async def websocket_endpoint(websocket: WebSocket, token: str = Query(...)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = int(payload.get("sub") or payload.get("id"))
        await manager.connect(user_id, websocket)
    except JWTError as e:
        print(f"WebSocket authentication failed: {e}")
        await websocket.close(code=1008)
        return

    try:
        while True:
            await websocket.receive_text()
    except Exception as e:
        print(f"WebSocket disconnect for user {user_id}: {e}")
        manager.disconnect(user_id)

# ------------------ Endpoints ------------------
@app.get("/tickets/details")
def get_ticket_details(
    infoname: str = Query(...),
    from_date: str = Query(...),  # YYYY-MM-DD
    to_date: str = Query(...)
):
    ticket_dir = COMPANIES_DIR / infoname / "tickets"
    if not ticket_dir.exists():
        raise HTTPException(status_code=404, detail="Company tickets folder not found.")

    from_dt = datetime.strptime(from_date, "%Y-%m-%d")
    to_dt   = datetime.strptime(to_date, "%Y-%m-%d")
    tickets = []

    current = from_dt
    while current <= to_dt:
        year = current.strftime("%Y"); month = current.strftime("%m"); day = current.strftime("%d")
        day_path = ticket_dir / year / month / day
        if day_path.exists():
            for file in day_path.glob("*.json"):
                try:
                    tickets.append(json.loads(file.read_text(encoding="utf-8")))
                except json.JSONDecodeError:
                    continue
        current += timedelta(days=1)

    return tickets

@app.get("/companies/active", response_model=List[Dict[str, str]])
def get_active_infonames():
    cache_key = "active_companies"
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    query = {
        "query": {"terms": {"_STATUS": ["ACTIVE", "SUSPENDED", "FROZEN", "DRAFT"]}},
        "_source": ["MYIDSTRING", "INFONAME"],
        "size": 5000,
    }
    resp = es.search(index="companies", body=query)
    results = [
        {"myidstring": h["_source"]["MYIDSTRING"], "infoname": h["_source"]["INFONAME"]}
        for h in resp["hits"]["hits"]
        if h["_source"].get("INFONAME") and h["_source"].get("MYIDSTRING")
    ]
    results_sorted = sorted(results, key=lambda x: x["infoname"].lower())
    redis_client.setex(cache_key, 43200, json.dumps(results_sorted))
    return results_sorted

@app.get("/companies/{myidstring}/full-details")
def get_company_full_details(myidstring: str):
    company = es.get(index="companies", id=myidstring, ignore=[404])
    if not company or not company.get("found"):
        raise HTTPException(status_code=404, detail="Company not found")

    details = [company["_source"]]

    services_query = {"query": {"term": {"MYIDSTRING": myidstring}}, "size": 500}
    services_resp = es.search(index="services", body=services_query)
    services = [hit["_source"] for hit in services_resp["hits"]["hits"]]

    stores = []
    is_central = False
    central_ms_id = details[0].get("MS_ID")

    if details[0].get("PROJECT") == "X4CENTRAL" and central_ms_id:
        is_central = True
        linked_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"MS_ID": central_ms_id}},
                        {"bool": {"must_not": {"term": {"MYIDSTRING": myidstring}}}},
                    ]
                }
            },
            "_source": ["INFONAME", "IDNAME", "MYIDSTRING"],
            "size": 1000,
        }
        sub_resp = es.search(index="companies", body=linked_query)
        stores = [hit["_source"] for hit in sub_resp["hits"]["hits"]]

    metrics = {}
    if is_central:
        company_ids = [myidstring] + [s["MYIDSTRING"] for s in stores]
        for platform in ["efood", "wolt", "box"]:
            all_hits = []
            for cid in company_ids:
                q = {
                    "query": {"bool": {"must": [{"term": {"_company": cid}}, {"term": {"metric_type": platform}}]}},
                    "_source": ["thedate", "counter"],
                    "size": 10000,
                }
                r = es.search(index="metrics", body=q)
                all_hits.extend(r["hits"]["hits"])
            agg = defaultdict(int)
            for h in all_hits:
                src = h["_source"]
                agg[src["thedate"]] += int(src.get("counter", 0) or 0)
            metrics[platform] = [{"thedate": d, "counter": c} for d, c in sorted(agg.items())]
    else:
        for platform in ["efood", "wolt", "box"]:
            q = {
                "query": {"bool": {"must": [{"term": {"_company": myidstring}}, {"term": {"metric_type": platform}}]}},
                "sort": [{"thedate": "asc"}],
                "_source": ["thedate", "counter"],
                "size": 10000,
            }
            r = es.search(index="metrics", body=q)
            metrics[platform] = [h["_source"] for h in r["hits"]["hits"]]

    return {"details": details, "services": services, "is_central": is_central, "stores": stores, "metrics": metrics}

@app.post("/tickets/by-company-range")
def get_tickets_by_company_date_range(req: TicketFilterRequest):
    try:
        q = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"company_id": req.myidstring}},
                        {"range": {"crstamp": {"gte": req.start_date, "lte": req.end_date}}},
                    ]
                }
            },
            "size": 1000,
        }
        r = es.search(index="tickets", body=q)
        return [h["_source"] for h in r["hits"]["hits"]]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/companies/search")
def search_companies_post(req: CompanySearchRequest):
    must = []
    must_not = []

    if req.mapaddress:
        must.append({"match_phrase": {"MAPADDRESS": req.mapaddress}})

    if req.taxafm:
        afm = str(req.taxafm).strip()
        must.append({
            "bool": {
                "should": [
                    {"term": {"TAXAFM.keyword": afm}},
                    {"term": {"TAXAFM": afm}},
                    {"wildcard": {"TAXAFM": f"*{afm}*"}}
                ],
                "minimum_should_match": 1
            }
        })

    # fiscal (IMPACT / FIMAS)
    if req.fiscal:
        f_raw = req.fiscal.strip()
        variants = {f_raw, f_raw.upper(), f_raw.lower()}
        should_fiscal = []
        for v in variants:
            should_fiscal += [
                {"term": {"fiscal.keyword": v}},
                {"term": {"fiscal": v}},
                {"match_phrase": {"fiscal": v}},
            ]
        must.append({"bool": {"should": should_fiscal, "minimum_should_match": 1}})

    if req.country:
        c_raw = req.country.strip()
        variants = {c_raw, c_raw.upper(), c_raw.lower()}
        should_country = []
        for v in variants:
            should_country += [
                {"term": {"_COUNTRY.keyword": v}},
                {"term": {"_COUNTRY": v}},
                {"match_phrase": {"_COUNTRY": v}},
            ]
        must.append({"bool": {"should": should_country, "minimum_should_match": 1}})

    if req.primaryphone:
        phone = req.primaryphone.strip().replace(" ", "")
        must.append({
            "bool": {
                "should": [
                    {"wildcard": {"PRIMARYPHONE": f"*{phone}*"}},
                    {"wildcard": {"MOBILEPHONE": f"*{phone}*"}},
                ],
                "minimum_should_match": 1
            }
        })

    if req.eft_pos:
        should_pos = []
        for t in req.eft_pos:
            code = (t or "").strip().upper()
            if not code:
                continue
            should_pos += [
                {"wildcard": {"eft_pos_names.keyword": f"*{code}*"}},
                {"wildcard": {"eft_pos_names": f"*{code}*"}}
            ]
        if should_pos:
            must.append({"bool": {"should": should_pos, "minimum_should_match": 1}})

    include_ids = set()
    exclude_ids = set()

    if req.include_services:
        include_ids = _company_ids_for_services_all(es, req.include_services)
        if not include_ids:
            return []  # none match all requested services
        must.append({"terms": {"MYIDSTRING": list(include_ids)}})

    if req.exclude_services:
        exclude_ids = _company_ids_for_services_any(es, req.exclude_services)
        if exclude_ids:
            must_not.append({"terms": {"MYIDSTRING": list(exclude_ids)}})

    if not must and not must_not:
        raise HTTPException(status_code=400, detail="At least one search parameter is required.")

    q = {
        "query": {"bool": {"must": must, "must_not": must_not}},
        "_source": [
            "MYIDSTRING","INFONAME","MAPADDRESS","TAXAFM","fiscal",
            "_COUNTRY","PRIMARYPHONE","MOBILEPHONE","PRIMARYEMAIL","eft_pos_names"
        ],
        "size": 1000
    }
    r = es.search(index="companies", body=q)
    return [h["_source"] for h in r["hits"]["hits"]]



# Dev-only runner
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
