# kpi.py
from fastapi import APIRouter, HTTPException, Depends
from datetime import date, datetime, timedelta
import os
import mysql.connector
from elasticsearch import Elasticsearch
from typing import Dict, Any, List, Optional
from login import get_current_user, CurrentUser  # protect endpoint like your other routes

import random
from dotenv import load_dotenv

load_dotenv()

router = APIRouter()

ELASTIC_HOST = os.getenv("ELASTIC_HOST", "http://elasticsearch:9200")
es = Elasticsearch(ELASTIC_HOST)

DB_HOST = os.getenv("DB_HOST", "mysql")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASSWORD", "55_66")
DB_NAME = os.getenv("DB_NAME", "webstatssite")

# --- Simple in-memory quote cache ---
_QUOTE_CACHE: Dict[str, Any] = {"text": None, "expires": datetime.utcnow()}
_QUOTE_TTL_SECONDS = int(os.getenv("QUOTE_TTL_SECONDS", "21600"))

_FALLBACK_QUOTES = [
    "Η συνέπεια χτίζει εμπιστοσύνη πιο γρήγορα από τα μεγάλα λόγια.",
    "Η καινοτομία δεν είναι πολυτέλεια· είναι τρόπος επιβίωσης.",
    "Κάθε δυσκολία κρύβει μια ανεκμετάλλευτη ευκαιρία.",
    "Ο χρόνος είναι το πιο ακριβό κεφάλαιο· επένδυσέ τον σωστά.",
    "Η επιτυχία ξεκινά όταν σταματήσεις να ψάχνεις δικαιολογίες.",
    "Συνεργασία σημαίνει να μοιράζεσαι ευθύνη, όχι μόνο δόξα.",
    "Η πειθαρχία κάνει τα αδύνατα, εφικτά.",
    "Δεν υπάρχει πρόοδος χωρίς συνεχή μάθηση.",
    "Η αγορά ανταμείβει όσους λύνουν προβλήματα, όχι όσους μιλούν για αυτά.",
    "Οι πελάτες θυμούνται την εμπειρία, όχι το τι τους πούλησες.",
    "Η σαφήνεια στους στόχους μειώνει το άγχος στην πορεία.",
    "Μικρά βήματα καθημερινά φτιάχνουν μεγάλες αλλαγές.",
    "Κάθε ομάδα είναι τόσο δυνατή όσο η επικοινωνία της.",
    "Η εμπιστοσύνη κερδίζεται με πράξεις, όχι με υποσχέσεις.",
    "Ο χρόνος που αφιερώνεις στον σχεδιασμό, σώζει χρόνο στην εκτέλεση.",
    "Η κρίση δεν καταστρέφει· αποκαλύπτει τις αδυναμίες.",
    "Το να λες 'όχι' είναι εξίσου σημαντικό με το να λες 'ναι'.",
    "Η ταχύτητα είναι χρήσιμη, αλλά η κατεύθυνση είναι καθοριστική.",
    "Η επιχειρηματικότητα είναι τέχνη επίλυσης προβλημάτων.",
    "Η κουλτούρα μιας εταιρίας τρώει τη στρατηγική για πρωινό.",
]

def get_db():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME
    )

def _trend(a: int, b: int) -> str:
    if a > b:  return "up"
    if a < b:  return "down"
    return "flat"

def _day_range_str(d: date):
    start = f"{d.isoformat()} 00:00:00"
    end   = f"{(d + timedelta(days=1)).isoformat()} 00:00:00"
    return start, end

def _tickets_count_for_day(day: date) -> int:
    start, end = _day_range_str(day)
    try:
        resp = es.count(index="tickets", body={
            "query": {
                "range": {
                    "crstamp": { "gte": start, "lt": end }
                }
            }
        })
        return int(resp.get("count", 0))
    except Exception as e:
        print("tickets count error:", e)
        return 0

def _platform_sums_for_day(day: date) -> Dict[str, int]:
    try:
        resp = es.search(index="metrics", body={
            "size": 0,
            "query": { "term": { "thedate": day.isoformat() }},  # thedate = 'YYYY-MM-DD'
            "aggs": {
                "by_type": {
                    "terms": { "field": "metric_type", "size": 10 },
                    "aggs": { "sum_counter": { "sum": { "field": "counter" } } }
                }
            }
        })
        out = {}
        for bucket in resp.get("aggregations", {}).get("by_type", {}).get("buckets", []):
            out[bucket["key"]] = int(bucket["sum_counter"]["value"] or 0)
        for k in ("efood", "wolt", "box"):
            out.setdefault(k, 0)
        return out
    except Exception as e:
        print("platform agg error:", e)
        return {"efood": 0, "wolt": 0, "box": 0}

@router.get("/kpi/overview")
def kpi_overview(current_user: CurrentUser = Depends(get_current_user)) -> Dict[str, Any]:
    today = date.today()
    yday = today - timedelta(days=1)
    d2   = today - timedelta(days=2)

    # --- Tickets: compare yesterday vs two-days-ago ---
    tickets_yday = _tickets_count_for_day(yday)
    tickets_d2   = _tickets_count_for_day(d2)

    # --- Platform sums: per day, per platform ---
    sums_yday = _platform_sums_for_day(yday)  # dict per platform
    sums_d2   = _platform_sums_for_day(d2)

    platforms = {}
    for k in ("efood", "wolt", "box"):
        platforms[k] = {
            "yesterday": sums_yday.get(k, 0),
            "two_days_ago": sums_d2.get(k, 0),
            "trend": _trend(sums_yday.get(k, 0), sums_d2.get(k, 0))
        }

    # --- MySQL parts ---
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    try:
        # Jobs undone total + top companies
        cur.execute("""
            SELECT COUNT(*) AS c FROM company_jobs WHERE IFNULL(is_done,0)=0
        """)
        undone_total = int((cur.fetchone() or {}).get("c", 0))

        cur.execute("""
            SELECT c.id AS company_id, c.name, SUM(IF(IFNULL(j.is_done,0)=0,1,0)) AS undone
            FROM companies c
            JOIN company_jobs j ON j.company_id = c.id
            GROUP BY c.id, c.name
            HAVING undone > 0
            ORDER BY undone DESC
            LIMIT 10
        """)
        top_rows = cur.fetchall() or []
        top_companies = [
            {"company_id": r["company_id"], "name": r["name"], "undone": int(r["undone"])} for r in top_rows
        ]

        # Follow-ups (sales_leads)
        cur.execute("""
            SELECT id, company_name, contact_name, email
            FROM sales_leads
            WHERE next_follow_up_date = CURDATE()
              AND (stage IS NULL OR stage NOT IN ('Won','Lost'))
            ORDER BY company_name
        """)
        followups_today = cur.fetchall() or []

        # Installations with probable date today
        cur.execute("""
            SELECT id, name, offer_link, probable_installation_date
            FROM companies
            WHERE probable_installation_date = CURDATE()
            ORDER BY name
        """)
        installs_today = cur.fetchall() or []

        # My assigned, undone jobs (for KPI panel)
        my_user_id = int(current_user.id)
        cur.execute("""
            SELECT
                cj.company_id,
                c.name  AS company_name,
                cj.job_id,
                j.name  AS job_name,
                IFNULL(cj.is_done,0) AS is_done,
                cj.hours_spent,
                cj.job_notes,
                cj.assigned_user_id
            FROM company_jobs cj
            JOIN companies c ON c.id = cj.company_id
            JOIN jobs j      ON j.id = cj.job_id
            WHERE IFNULL(cj.is_done,0) = 0
              AND cj.assigned_user_id = %s
            ORDER BY c.name ASC, j.name ASC
            LIMIT 200
        """, (my_user_id,))
        my_rows = cur.fetchall() or []
        my_assigned_undone = [
            {
                "company": {"id": r["company_id"], "name": r["company_name"]},
                "job_id": r["job_id"],
                "job_name": r["job_name"],
                "is_done": bool(r["is_done"]),
                "hours_spent": float(r["hours_spent"]) if r["hours_spent"] is not None else 0.0,
                "job_notes": r.get("job_notes") or "",
                "assigned_user_id": r.get("assigned_user_id"),
            }
            for r in my_rows
        ]

    finally:
        cur.close(); conn.close()

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "tickets": {
            "yesterday": tickets_yday,
            "two_days_ago": tickets_d2,
            "trend": _trend(tickets_yday, tickets_d2),
        },
        "platforms": platforms,
        "jobs": {
            "undone_total": undone_total,
            "top_companies": top_companies,
            "my_assigned_undone": {
                "count": len(my_assigned_undone),
                "items": my_assigned_undone,
            },
        },
        "followups_today": followups_today,
        "installations_today": installs_today,
    }

@router.get("/kpi/quote")
def kpi_quote(current_user: CurrentUser = Depends(get_current_user)) -> Dict[str, str]:
    now = datetime.utcnow()

    if _QUOTE_CACHE.get("text") and now < _QUOTE_CACHE.get("expires", now):
        return {"quote": _QUOTE_CACHE["text"]}

    quote_text = random.choice(_FALLBACK_QUOTES)

    _QUOTE_CACHE["text"] = quote_text
    _QUOTE_CACHE["expires"] = now + timedelta(seconds=_QUOTE_TTL_SECONDS)

    return {"quote": quote_text}