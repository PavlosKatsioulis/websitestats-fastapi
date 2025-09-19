from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from elasticsearch import Elasticsearch
from datetime import datetime
import os
import logging



router = APIRouter()

# ---------- logging to stdout (Docker-friendly) ----------
logger = logging.getLogger("search_api")
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(h)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# ---------- Elasticsearch client (env-driven) ----------
ES_HOST = os.getenv("ELASTIC_HOST", "http://elasticsearch:9200")
TICKETS_INDEX = os.getenv("TICKETS_INDEX", "tickets")
es = Elasticsearch(ES_HOST)
COMPANIES_INDEX = os.getenv("COMPANIES_INDEX", "companies")

class TicketSearchFilters(BaseModel):
    start_date: Optional[str] = Field(None, description="YYYY-MM-DD")
    end_date:   Optional[str] = Field(None, description="YYYY-MM-DD")
    charge:     Optional[int] = None
    status:     Optional[str] = None
    username:   Optional[str] = None
    origin:     Optional[str] = None
    typename:   Optional[str] = None
    fiscal:     Optional[str] = None
    keywords:   Optional[str] = None
    keywords_operator: Optional[str] = Field("and", description="'and' or 'any'")

def _company_ids_by_fiscal(es: Elasticsearch, fiscals: List[str]) -> List[str]:
    if not fiscals:
        return []

    should = []
    for raw in fiscals:
        v = (raw or "").strip()
        if not v:
            continue
        should += [
            {"term": {"fiscal.keyword": v}},
            {"term": {"fiscal": v}},
            {"match_phrase": {"fiscal": v}},
        ]

    if not should:
        return []

    q = {
        "size": 10000,
        "_source": ["MYIDSTRING"],
        "query": {"bool": {"should": should, "minimum_should_match": 1}},
    }
    resp = es.search(index=COMPANIES_INDEX, body=q)
    hits = resp.get("hits", {}).get("hits", [])
    ids = []
    for h in hits:
        src = h.get("_source") or {}
        mid = src.get("MYIDSTRING")
        if mid:
            ids.append(mid)
    return ids

def _parse_minutes(val: Any) -> int:
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return max(0, int(round(val)))
    s = str(val).strip()
    if not s:
        return 0

    if ":" in s:
        parts = s.split(":")
        try:
            if len(parts) == 3:
                h, m, sec = int(parts[0]), int(parts[1]), int(parts[2])
                return max(0, h * 60 + m + int(sec / 60))
            if len(parts) == 2:
                h, m = int(parts[0]), int(parts[1])
                return max(0, h * 60 + m)
        except Exception:
            return 0

    try:
        if "." in s:
            d = float(s)
            return max(0, int(d * 60 + 0.5))
        return max(0, int(s))
    except Exception:
        return 0

def _ym_key(dt: datetime) -> str:
    return f"{dt.year}-{str(dt.month).zfill(2)}"

def _week_key(dt: datetime) -> str:
    try:
        iso = dt.isocalendar()
        return f"{dt.year}-W{str(iso[1]).zfill(2)}"
    except Exception:
        iso = dt.isocalendar()
        return f"{dt.year}-W{str(iso[1]).zfill(2)}"
        
def _compute_time_metrics(es: Elasticsearch, index: str, base_must: List[Dict[str, Any]]) -> Dict[str, Any]:
    query = {"bool": {"must": base_must}}
    fields = ["crstamp", "wrktime", "sumtime"]

    total_wrk = 0
    total_sum = 0
    per_month: Dict[str, Dict[str, int]] = {}
    per_week: Dict[str, Dict[str, int]] = {}

    sort = [{"crstamp": {"order": "asc", "missing": "_last"}}]
    page_size = 1000
    search_after = None

    while True:
        body = {
            "size": page_size,
            "_source": fields,
            "stored_fields": [],
            "sort": sort,
            "query": query,
            "track_total_hits": False,
        }
        if search_after:
            body["search_after"] = search_after

        resp = es.search(index=index, body=body)
        hits = resp.get("hits", {}).get("hits", [])
        if not hits:
            break

        for h in hits:
            src = h.get("_source", {})
            cs = src.get("crstamp")
            try:
                if isinstance(cs, (int, float)):
                    ts = float(cs) / (1000.0 if float(cs) > 1e12 else 1.0)
                    dt = datetime.utcfromtimestamp(ts)
                else:
                    s = str(cs)
                    dt = (datetime.fromisoformat(s.replace("Z", "+00:00"))
                          if "T" in s or "Z" in s or "+" in s
                          else datetime.strptime(s, "%Y-%m-%d"))
            except Exception:
                try:
                    dt = datetime.fromisoformat(str(cs))
                except Exception:
                    continue

            wrk = _parse_minutes(src.get("wrktime"))
            sm  = _parse_minutes(src.get("sumtime"))

            total_wrk += wrk
            total_sum += sm

            mk = _ym_key(dt)
            wk = _week_key(dt)

            mslot = per_month.setdefault(mk, {"wrk": 0, "sum": 0})
            wslot = per_week.setdefault(wk, {"wrk": 0, "sum": 0})
            mslot["wrk"] += wrk
            mslot["sum"] += sm
            wslot["wrk"] += wrk
            wslot["sum"] += sm

        search_after = hits[-1].get("sort")
        if not search_after:
            break

    month_buckets = [
        {"key_as_string": k, "wrk": {"value": v["wrk"]}, "sum": {"value": v["sum"]}}
        for k, v in sorted(per_month.items(), key=lambda x: x[0])
    ]
    week_buckets = [
        {"key_as_string": k, "wrk": {"value": v["wrk"]}, "sum": {"value": v["sum"]}}
        for k, v in sorted(per_week.items(), key=lambda x: x[0])
    ]

    return {
        "worktime_total": {"value": total_wrk},
        "sumtime_total":  {"value": total_sum},
        "time_per_month": {"buckets": month_buckets},
        "time_per_week":  {"buckets": week_buckets},
    }


@router.get("/search/recommendations")
def get_recommendations(query: str = Query(..., min_length=1)):
    try:
        es_query = {
            "size": 5,
            "_source": ["ticket_id", "subject", "description", "comment"],
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["subject", "description", "comment"],
                    "operator": "or",
                }
            },
        }
        resp = es.search(index=TICKETS_INDEX, body=es_query)
        hits = resp.get("hits", {}).get("hits", [])
        suggestions = [{
            "ticket_id": h["_source"].get("ticket_id"),
            "subject":   h["_source"].get("subject"),
            "description": h["_source"].get("description"),
            "comment":   h["_source"].get("comment"),
        } for h in hits]
        return {"suggestions": suggestions}
    except Exception as e:
        logger.exception("recommendations failed")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/search/results")
def get_results(query: str = Query(..., min_length=1)):
    try:
        es_query = {
            "size": 5000,
            "query": {
                "simple_query_string": {
                    "query": query,
                    "fields": ["subject", "description", "comment"],
                    "default_operator": "and",
                }
            },
        }
        resp = es.search(index=TICKETS_INDEX, body=es_query)
        hits = resp.get("hits", {}).get("hits", [])
        return {"results": [h["_source"] for h in hits]}
    except Exception as e:
        logger.exception("results failed")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/search/advanced-results")
def advanced_search(filters: TicketSearchFilters = Body(...)):
    try:
        must = []

        # default range: last 2 years
        today = datetime.today()
        start = filters.start_date or today.replace(year=today.year - 2).strftime("%Y-%m-%d")
        end   = filters.end_date   or today.strftime("%Y-%m-%d")

        must.append({"range": {"crstamp": {"gte": start, "lte": end}}})

        if filters.charge is not None:
            must.append({"term": {"charge": filters.charge}})
        if filters.status:
            must.append({"term": {"status": filters.status}})
        if filters.username:
            must.append({"term": {"username": filters.username}})
        if filters.origin:
            must.append({"term": {"origin": filters.origin}})
        if filters.typename:
            must.append({"term": {"typename": filters.typename}})
        if filters.fiscal:
            values = [v.strip() for v in filters.fiscal.split(",") if v.strip()]
            company_ids = _company_ids_by_fiscal(es, values)
            logger.info("Fiscal filter %s -> %d company ids", values, len(company_ids))
            if company_ids:
                must.append({"terms": {"company_id": company_ids}})
        if filters.keywords:
            op = "or" if (filters.keywords_operator or "").lower() in {"or", "any"} else "and"
            must.append({
                "simple_query_string": {
                    "query": filters.keywords,
                    "fields": ["subject", "description", "comment"],
                    "default_operator": op,
                }
            })

        query = {"bool": {"must": must}}
        es_query = {
            "size": 0,
            "query": query,
            "aggs": {
                "tickets_by_year": {
                    "date_histogram": {
                        "field": "crstamp",
                        "calendar_interval": "year",
                        "format": "yyyy",
                    },
                    "aggs": {
                        "per_month": {
                            "date_histogram": {
                                "field": "crstamp",
                                "calendar_interval": "month",
                                "format": "yyyy-MM",
                            }
                        },
                        "per_week": {
                            "date_histogram": {
                                "field": "crstamp",
                                "calendar_interval": "week",
                                "format": "yyyy-'W'ww",
                            }
                        },
                    },
                },
                "tickets_per_username": {"terms": {"field": "username", "size": 20}},
                "tickets_per_typename": {"terms": {"field": "typename", "size": 20}},
                "tickets_per_charge":   {"terms": {"field": "charge", "size": 10}},
            },
        }


        logger.info("Advanced filters: %s", filters.model_dump())
        resp = es.search(index=TICKETS_INDEX, body=es_query)
        aggs = resp.get("aggregations", {}) or {}

        must_for_metrics = must[:]

        extra = _compute_time_metrics(es, TICKETS_INDEX, must_for_metrics)

        aggs.update(extra)

        return {"query_executed": es_query, "aggregations": aggs}
    except Exception as e:
        logger.exception("advanced_search failed")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/search/latest-tickets")
def latest_tickets(filters: TicketSearchFilters = Body(...)):
    try:
        must = []

        today = datetime.today()
        start = filters.start_date or today.replace(year=today.year - 2).strftime("%Y-%m-%d")
        end   = filters.end_date   or today.strftime("%Y-%m-%d")
        must.append({"range": {"crstamp": {"gte": start, "lte": end}}})

        if filters.charge is not None:
            must.append({"term": {"charge": filters.charge}})
        if filters.status:
            must.append({"term": {"status": filters.status}})
        if filters.username:
            must.append({"term": {"username": filters.username}})
        if filters.origin:
            must.append({"term": {"origin": filters.origin}})
        if filters.typename:
            must.append({"term": {"typename": filters.typename}})
        if filters.fiscal:
            values = [v.strip() for v in filters.fiscal.split(",") if v.strip()]
            company_ids = _company_ids_by_fiscal(es, values)
            logger.info("Fiscal filter %s -> %d company ids", values, len(company_ids))
            must.append({"terms": {"company_id": company_ids}})
        if filters.keywords:
            op = "or" if (filters.keywords_operator or "").lower() in {"or", "any"} else "and"
            must.append({
                "simple_query_string": {
                    "query": filters.keywords,
                    "fields": ["subject", "description", "comment"],
                    "default_operator": op,
                }
            })

        es_query = {
            "size": 5000,
            "sort": [{"crstamp": {"order": "desc"}}],
            "query": {"bool": {"must": must}},
        }
        resp = es.search(index=TICKETS_INDEX, body=es_query)
        hits = resp.get("hits", {}).get("hits", [])
        return {"tickets": [h["_source"] for h in hits]}
    except Exception as e:
        logger.exception("latest_tickets failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search/options")
def options(filters: TicketSearchFilters = Body(...)):
    try:
        must = []
        today = datetime.today()
        start = filters.start_date or today.replace(year=today.year - 2).strftime("%Y-%m-%d")
        end   = filters.end_date   or today.strftime("%Y-%m-%d")
        must.append({"range": {"crstamp": {"gte": start, "lte": end}}})

        tickets_opts_query = {
            "size": 0,
            "query": {"bool": {"must": must}},
            "aggs": {
                "statuses":  {"terms": {"field": "status", "size": 100, "order": {"_key": "asc"}}},
                "usernames": {"terms": {"field": "username", "size": 200, "order": {"_key": "asc"}}},
                "origins":   {"terms": {"field": "origin", "size": 100, "order": {"_key": "asc"}}},
                "typenames": {"terms": {"field": "typename", "size": 200, "order": {"_key": "asc"}}},
                "charges":   {"terms": {"field": "charge", "size": 10,  "order": {"_key": "asc"}}},
            },
        }
        t_resp = es.search(index=TICKETS_INDEX, body=tickets_opts_query)
        t_aggs = t_resp.get("aggregations", {})

        statuses  = [b["key"] for b in t_aggs.get("statuses", {}).get("buckets", []) if b.get("key") not in (None, "")]
        usernames = [b["key"] for b in t_aggs.get("usernames", {}).get("buckets", []) if b.get("key")]
        origins   = [b["key"] for b in t_aggs.get("origins", {}).get("buckets", []) if b.get("key")]
        typenames = [b["key"] for b in t_aggs.get("typenames", {}).get("buckets", []) if b.get("key")]
        charges   = [b.get("key") for b in t_aggs.get("charges", {}).get("buckets", []) if b.get("key") is not None]

        fiscals = ["", "IMPACT", "FIMAS"]

        return {
            "status": statuses,
            "username": usernames,
            "origin": origins,
            "typename": typenames,
            "charge": charges,
            "fiscal": fiscals,
        }
    except Exception as e:
        logger.exception("options failed")
        raise HTTPException(status_code=500, detail=str(e))