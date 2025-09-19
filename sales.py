# sales.py
from fastapi import APIRouter, HTTPException, Depends, Query, Body
from pydantic import BaseModel, Field
from typing import Optional, List, Literal, Dict, Any
from datetime import datetime, date
import os
import json
import mysql.connector
from login import get_current_user
from websocket_manager import manager

router = APIRouter()


DB_HOST = os.getenv("DB_HOST", "mysql")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASSWORD", "55_66")
DB_NAME = os.getenv("DB_NAME", "webstatssite")

def get_db():
    conn = mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME, autocommit=False
    )
    cur = conn.cursor()
    cur.execute("SET time_zone = '+03:00'")
    cur.close()
    return conn

# ---- Models
Stage = Literal["New","Contacted","Presented","Negotiating","Won","Lost"]

class LeadCreate(BaseModel):
    company_name: str
    contact_name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    has_other_system: Optional[bool] = False
    other_system_name: Optional[str] = None
    first_offer_date: Optional[date] = None
    presentation_date: Optional[date] = None
    expected_start_date: Optional[date] = None
    stage: Stage = "New"
    next_follow_up_date: Optional[date] = None
    notes: Optional[str] = None
    owner_user_id: Optional[int] = None
    company_id: Optional[int] = None
    deal_value: Optional[float] = None
    probability: Optional[int] = Field(default=None, ge=0, le=100)
    expected_close_date: Optional[date] = None
    lead_source: Optional[str] = None
    loss_reason: Optional[str] = None

class LeadUpdate(BaseModel):
    company_name: Optional[str] = None
    contact_name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    has_other_system: Optional[bool] = None
    other_system_name: Optional[str] = None
    first_offer_date: Optional[date] = None
    presentation_date: Optional[date] = None
    expected_start_date: Optional[date] = None
    stage: Optional[Stage] = None
    next_follow_up_date: Optional[date] = None
    notes: Optional[str] = None
    owner_user_id: Optional[int] = None
    company_id: Optional[int] = None
    # NEW sales fields
    deal_value: Optional[float] = None
    probability: Optional[int] = Field(default=None, ge=0, le=100)
    expected_close_date: Optional[date] = None
    lead_source: Optional[str] = None
    loss_reason: Optional[str] = None


ActivityType = Literal[
    "note","call","email_out","email_in","meeting","demo",
    "offer_sent","offer_viewed","status_change","field_change","task_completed"
]

class ActivityCreate(BaseModel):
    type: ActivityType
    content: Optional[str] = None

OfferItemIn = Dict[str, Any]
class OfferUpdate(BaseModel):
    status: Optional[Literal['draft','sent','viewed','accepted','rejected','withdrawn']] = None
    valid_until: Optional[date] = None
    currency: Optional[str] = None
    notes: Optional[str] = None
    subtotal: Optional[float] = None
    discount_total: Optional[float] = None
    tax_total: Optional[float] = None
    total: Optional[float] = None
    pdf_url: Optional[str] = None
    items: Optional[List[OfferItemIn]] = None

# ---- Helpers
def _iso(dt):
    if not dt:
        return None
    if isinstance(dt, (datetime,)):
        return dt.isoformat()
    if isinstance(dt, (date,)):
        return dt.isoformat()
    try:
        return str(dt)
    except:
        return None

def _none_if_empty(v):
    return None if (isinstance(v, str) and v.strip() == "") else v

def _user_id(user):
    try:
        if isinstance(user, dict):
            return user.get("id") or user.get("user_id") or user.get("uid")
        return getattr(user, "id", None) or getattr(user, "user_id", None) or getattr(user, "uid", None)
    except Exception:
        return None

def row_to_lead_dict(row):
    return {
        "id": row["id"],
        "company_name": row["company_name"],
        "contact_name": row["contact_name"],
        "phone": row["phone"],
        "email": row["email"],
        "has_other_system": bool(row["has_other_system"]),
        "other_system_name": row["other_system_name"],
        "first_offer_date": _iso(row["first_offer_date"]),
        "presentation_date": _iso(row["presentation_date"]),
        "expected_start_date": _iso(row["expected_start_date"]),
        "stage": row["stage"],
        "next_follow_up_date": _iso(row["next_follow_up_date"]),
        "last_activity_at": _iso(row["last_activity_at"]),
        "owner_user_id": row["owner_user_id"],
        "notes": row["notes"],
        "company_id": row.get("company_id"),
        "deal_value": row.get("deal_value"),
        "probability": row.get("probability"),
        "expected_close_date": _iso(row.get("expected_close_date")),
        "lead_source": row.get("lead_source"),
        "loss_reason": row.get("loss_reason"),
        "created_at": _iso(row.get("created_at")),
        "updated_at": _iso(row.get("updated_at")),
    }

def _now_utc_str():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def log_activity(conn, lead_id: int, type_: str, content: Optional[str] = None,
                 meta: Optional[Dict[str, Any]] = None, user_id: Optional[int] = None):
    cur = conn.cursor()
    payload_meta = json.dumps(meta) if meta is not None else None

    try:
        cur.execute(
            "INSERT INTO sales_activities (lead_id, user_id, type, content, meta) "
            "VALUES (%s,%s,%s,%s,%s)",
            (lead_id, user_id, type_, content, payload_meta)
        )
    except mysql.connector.Error:
        try:
            cur.execute(
                "INSERT INTO sales_activities (lead_id, user_id, type, content) "
                "VALUES (%s,%s,%s,%s)",
                (lead_id, user_id, type_, content)
            )
        except mysql.connector.Error:
            try:
                cur.execute(
                    "INSERT INTO sales_activities (lead_id, type, content) "
                    "VALUES (%s,%s,%s)",
                    (lead_id, type_, content)
                )
            except mysql.connector.Error as e:
                print("log_activity failed:", repr(e))

    try:
        cur.execute(
            "UPDATE sales_leads SET last_activity_at=%s WHERE id=%s",
            (datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), lead_id)
        )
    except mysql.connector.Error:
        pass


# ---- Endpoints
@router.post("/sales/leads")
def create_lead(payload: LeadCreate, user=Depends(get_current_user)):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO sales_leads (
              company_name, contact_name, phone, email,
              has_other_system, other_system_name,
              first_offer_date, presentation_date, expected_start_date,
              stage, next_follow_up_date, last_activity_at,
              owner_user_id, notes,
              deal_value, probability, expected_close_date, lead_source, loss_reason
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, NULL, %s,%s,%s,%s,%s,%s,%s)
        """, (
            payload.company_name, payload.contact_name, payload.phone, payload.email,
            int(bool(payload.has_other_system)), payload.other_system_name,
            payload.first_offer_date, payload.presentation_date, payload.expected_start_date,
            payload.stage, payload.next_follow_up_date,
            payload.owner_user_id, payload.notes,
            payload.deal_value, payload.probability, payload.expected_close_date, payload.lead_source, payload.loss_reason
        ))
        lead_id = cur.lastrowid
        conn.commit()

        try:
            log_activity(conn, lead_id, "field_change", "Lead created", user_id=_user_id(user))
            conn.commit()
        except Exception as e:
            print("non-fatal: log_activity after create failed:", repr(e))

        return {"id": lead_id, "status": "ok"}
    except Exception as e:
        conn.rollback()
        print("Create lead error:", repr(e))
        raise HTTPException(500, "Failed to create lead")
    finally:
        cur.close(); conn.close()

@router.get("/sales/leads")
def list_leads(
    stage: Optional[Stage] = None,
    owner: Optional[int] = None,
    q: Optional[str] = None,
    due: Optional[Literal["followup","stale"]] = None,
    limit: int = 50,
    offset: int = 0,
    user=Depends(get_current_user),
):
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    try:
        where = []
        args = []
        if stage:
            where.append("stage=%s"); args.append(stage)
        if owner:
            where.append("owner_user_id=%s"); args.append(owner)
        if q:
            where.append("(company_name LIKE %s OR contact_name LIKE %s OR email LIKE %s)")
            args += [f"%{q}%", f"%{q}%", f"%{q}%"]

        if due == "followup":
            where.append("stage NOT IN ('Won','Lost') AND next_follow_up_date = CURDATE()")
        elif due == "stale":
            where.append("""stage NOT IN ('Won','Lost')
                            AND first_offer_date IS NOT NULL
                            AND DATEDIFF(CURDATE(), first_offer_date) >= 5
                            AND (last_activity_at IS NULL OR TIMESTAMPDIFF(DAY, last_activity_at, NOW()) >= 5)""")

        clause = ("WHERE " + " AND ".join(where)) if where else ""
        sql = f"""
            SELECT *
            FROM sales_leads
            {clause}
            ORDER BY updated_at DESC
            LIMIT %s OFFSET %s
        """
        cur.execute(sql, (*args, limit, offset))
        rows = cur.fetchall() or []
        return [row_to_lead_dict(r) for r in rows]
    finally:
        cur.close(); conn.close()

@router.get("/sales/leads/{lead_id}")
def get_lead(lead_id: int, user=Depends(get_current_user)):
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM sales_leads WHERE id=%s", (lead_id,))
        lead = cur.fetchone()
        if not lead:
            raise HTTPException(404, "Lead not found")
        cur.execute("SELECT id, lead_id, type, content, created_at FROM sales_activities WHERE lead_id=%s ORDER BY created_at DESC LIMIT 100", (lead_id,))
        activities = cur.fetchall() or []
        return {
            "lead": row_to_lead_dict(lead),
            "activities": activities
        }
    finally:
        cur.close(); conn.close()

@router.put("/sales/leads/{lead_id}")
def update_lead(lead_id: int, payload: LeadUpdate, user=Depends(get_current_user)):
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    try:
        print("DBG update_lead IN:", {"lead_id": lead_id, **payload.dict(exclude_unset=True)})
        cur.execute("SELECT stage FROM sales_leads WHERE id=%s", (lead_id,))
        before = cur.fetchone()
        if not before:
            raise HTTPException(404, "Lead not found")
        before_stage = before["stage"]

        if payload.stage == "Lost" and not payload.loss_reason:
            raise HTTPException(400, "Moving to Lost requires 'loss_reason'")

        fields, args = [], []
        for k, v in payload.dict(exclude_unset=True).items():
            v = _none_if_empty(v)  # <<< important
            fields.append(f"{k}=%s")
            args.append(int(v) if isinstance(v, bool) else v)

        if not fields:
            return {"status": "ok", "updated": 0}

        sql = f"UPDATE sales_leads SET {', '.join(fields)} WHERE id=%s"
        cur2 = conn.cursor()
        cur2.execute(sql, (*args, lead_id))

        changed_fields = list(payload.dict(exclude_unset=True).keys())
        log_activity(conn, lead_id, "field_change", "Lead updated",
                     meta={"fields": changed_fields}, user_id=_user_id(user))
        if payload.stage and payload.stage != before_stage:
            log_activity(conn, lead_id, "status_change",
                         f"Stage: {before_stage} → {payload.stage}", user_id=_user_id(user))

        conn.commit()
        print("DBG update_lead OUT:", {"updated": cur2.rowcount})
        return {"status": "ok", "updated": cur2.rowcount}
    except mysql.connector.Error as err:
        print("ERR update_lead MYSQL:", {"errno": err.errno, "sqlstate": err.sqlstate, "msg": err.msg})
        conn.rollback()
        raise HTTPException(500, err.msg)
    except HTTPException:
        raise
    except Exception as e:
        print("ERR update_lead PY:", repr(e))
        conn.rollback()
        raise HTTPException(500, "update_lead failed")
    finally:
        cur.close(); conn.close()

@router.post("/sales/leads/{lead_id}/activity")
def add_activity(lead_id: int, payload: ActivityCreate, user=Depends(get_current_user)):
    conn = get_db()
    try:
        print("DBG add_activity IN:", {"lead_id": lead_id, **payload.dict()})
        log_activity(conn, lead_id, payload.type, payload.content, user_id=_user_id(user))
        conn.commit()
        print("DBG add_activity OUT: ok")
        return {"status": "ok"}
    except mysql.connector.Error as err:
        print("ERR add_activity MYSQL:", {"errno": err.errno, "sqlstate": err.sqlstate, "msg": err.msg})
        conn.rollback()
        raise HTTPException(500, err.msg)
    except Exception as e:
        print("ERR add_activity PY:", repr(e))
        conn.rollback()
        raise HTTPException(500, "add_activity failed")
    finally:
        conn.close()

# ---------- OFFERS

@router.get("/sales/leads/{lead_id}/offers")
def list_offers(lead_id: int, user=Depends(get_current_user)):
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT id, lead_id, version, status, valid_until, currency, notes,
                   subtotal, discount_total, tax_total, total, pdf_url,
                   created_at, updated_at
              FROM sales_offers
             WHERE lead_id=%s
             ORDER BY version DESC
        """, (lead_id,))
        return cur.fetchall() or []
    finally:
        cur.close(); conn.close()

@router.post("/sales/leads/{lead_id}/offers")
def create_offer(lead_id: int, payload: Dict[str, Any] = Body(default=None), user=Depends(get_current_user)):
    conn = get_db()
    cur = conn.cursor()
    try:
        print("DBG create_offer IN:", {"lead_id": lead_id, "payload": payload})
        cur.execute("SELECT COALESCE(MAX(version),0)+1 FROM sales_offers WHERE lead_id=%s", (lead_id,))
        version = cur.fetchone()[0]
        currency = (payload or {}).get("currency", "EUR")
        valid_until = (payload or {}).get("valid_until")
        notes = (payload or {}).get("notes")
        cur.execute("""INSERT INTO sales_offers
            (lead_id, version, status, currency, valid_until, notes)
            VALUES (%s,%s,'draft',%s,%s,%s)""",
            (lead_id, version, currency, valid_until, notes)
        )
        offer_id = cur.lastrowid
        log_activity(conn, lead_id, "field_change", f"Offer v{version} created",
                     meta={"offer_id": offer_id, "version": version}, user_id=_user_id(user))
        conn.commit()
        print("DBG create_offer OUT:", {"offer_id": offer_id, "version": version})
        return {"ok": True, "offer_id": offer_id, "version": version}
    except mysql.connector.Error as err:
        print("ERR create_offer MYSQL:", {"errno": err.errno, "sqlstate": err.sqlstate, "msg": err.msg})
        conn.rollback()
        raise HTTPException(500, err.msg)
    except Exception as e:
        print("ERR create_offer PY:", repr(e))
        conn.rollback()
        raise HTTPException(500, "create_offer failed")
    finally:
        cur.close(); conn.close()

@router.get("/sales/offers/{offer_id}")
def get_offer(offer_id: int, user=Depends(get_current_user)):
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""SELECT id, lead_id, version, status, valid_until, currency, notes,
                              subtotal, discount_total, tax_total, total, pdf_url
                         FROM sales_offers WHERE id=%s""", (offer_id,))
        head = cur.fetchone()
        if not head:
            raise HTTPException(404, "Offer not found")
        cur.execute("""SELECT id, product_name, description, qty, unit_price,
                              discount_pct, vat_pct, sort_order
                         FROM sales_offer_items
                        WHERE offer_id=%s
                     ORDER BY sort_order, id""", (offer_id,))
        head["items"] = cur.fetchall() or []
        return head
    finally:
        cur.close(); conn.close()

@router.put("/sales/offers/{offer_id}")
def update_offer(offer_id: int, payload: OfferUpdate, user=Depends(get_current_user)):
    conn = get_db()
    cur = conn.cursor()
    try:
        d = payload.dict(exclude_unset=True)
        fields = []
        vals = []
        for f in ["status","valid_until","currency","notes","subtotal","discount_total","tax_total","total","pdf_url"]:
            if f in d:
                fields.append(f"{f}=%s")
                vals.append(d[f])
        if fields:
            sql = "UPDATE sales_offers SET " + ",".join(fields) + " WHERE id=%s"
            vals.append(offer_id)
            cur.execute(sql, tuple(vals))

        if "items" in d and isinstance(d["items"], list):
            cur.execute("DELETE FROM sales_offer_items WHERE offer_id=%s", (offer_id,))
            for idx, it in enumerate(d["items"]):
                cur.execute("""INSERT INTO sales_offer_items
                    (offer_id, product_name, description, qty, unit_price, discount_pct, vat_pct, sort_order)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        offer_id,
                        it.get("product_name","Item"),
                        it.get("description"),
                        it.get("qty",1),
                        it.get("unit_price",0),
                        it.get("discount_pct",0),
                        it.get("vat_pct",24),
                        it.get("sort_order", idx)
                    )
                )
        cur2 = conn.cursor()
        cur2.execute("SELECT lead_id, version FROM sales_offers WHERE id=%s", (offer_id,))
        row = cur2.fetchone()
        if row:
            lead_id, version = row
            log_activity(conn, lead_id, "field_change", f"Offer v{version} updated", meta={"offer_id": offer_id}, user_id=_user_id(user))

        conn.commit()
        return {"ok": True}
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close(); conn.close()

@router.post("/sales/offers/{offer_id}/send")
def send_offer(offer_id: int, user=Depends(get_current_user)):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE sales_offers SET status='sent' WHERE id=%s", (offer_id,))
        cur.execute("SELECT lead_id, version FROM sales_offers WHERE id=%s", (offer_id,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            raise HTTPException(404, "Offer not found")
        lead_id, version = row
        log_activity(conn, lead_id, "offer_sent", f"Offer v{version} sent", meta={"offer_id": offer_id}, user_id=_user_id(user))
        conn.commit()
        return {"ok": True}
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close(); conn.close()

@router.post("/sales/offers/{offer_id}/status")
def update_offer_status(offer_id: int, status: Literal['draft','sent','viewed','accepted','rejected','withdrawn'] = Body(..., embed=True), user=Depends(get_current_user)):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE sales_offers SET status=%s WHERE id=%s", (status, offer_id))
        cur.execute("SELECT lead_id, version FROM sales_offers WHERE id=%s", (offer_id,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            raise HTTPException(404, "Offer not found")
        lead_id, version = row

        typemap = {"viewed":"offer_viewed", "sent":"offer_sent"}
        log_activity(conn, lead_id, typemap.get(status,"field_change"), f"Offer v{version} {status}", meta={"offer_id": offer_id}, user_id=_user_id(user))

        # Optional: auto-win when accepted
        if status == "accepted":
            cur.execute("UPDATE sales_leads SET stage='Won' WHERE id=%s", (lead_id,))
            log_activity(conn, lead_id, "status_change", "Stage → Won (offer accepted)", user_id=_user_id(user))

        conn.commit()
        return {"ok": True}
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close(); conn.close()

# --- Notifications job ---
@router.post("/sales/notifications/run")
async def run_sales_notifications(user=Depends(get_current_user)):
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    ins_cur = conn.cursor()

    try:
        cur.execute("""
          SELECT id, owner_user_id, company_name
          FROM sales_leads
          WHERE stage NOT IN ('Won','Lost')
            AND next_follow_up_date = CURDATE()
        """)
        due_list = cur.fetchall() or []

        for row in due_list:
            if not row["owner_user_id"]:
                continue
            msg = f"Follow-up due today: {row['company_name']}"
            ins_cur.execute(
                "INSERT INTO notifications (user_id, message, type, data) VALUES (%s,%s,%s,%s)",
                (row["owner_user_id"], msg, "sales_followup_due", None)
            )
            await manager.send_personal_message(
                {"event":"sales_followup_due","type":"sales","message":msg,"data":{"lead_id":row["id"]}},
                row["owner_user_id"]
            )

        cur.execute("""
          SELECT id, owner_user_id, company_name
          FROM sales_leads
          WHERE stage NOT IN ('Won','Lost')
            AND first_offer_date IS NOT NULL
            AND DATEDIFF(CURDATE(), first_offer_date) >= 5
            AND (last_activity_at IS NULL OR TIMESTAMPDIFF(DAY, last_activity_at, NOW()) >= 5)
        """)
        stale_list = cur.fetchall() or []

        for row in stale_list:
            if not row["owner_user_id"]:
                continue
            msg = f"5+ days since offer: {row['company_name']}"
            ins_cur.execute(
                "INSERT INTO notifications (user_id, message, type, data) VALUES (%s,%s,%s,%s)",
                (row["owner_user_id"], msg, "sales_offer_stale", None)
            )
            await manager.send_personal_message(
                {"event":"sales_offer_stale","type":"sales","message":msg,"data":{"lead_id":row["id"]}},
                row["owner_user_id"]
            )

        conn.commit()
        return {"status": "ok", "followup": len(due_list), "stale": len(stale_list)}
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close(); ins_cur.close(); conn.close()

@router.get("/sales/leads/{lead_id}/activity")
def get_activity(lead_id: int, user=Depends(get_current_user)):
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT id, type, content, created_at
            FROM sales_activities
            WHERE lead_id=%s
            ORDER BY created_at DESC, id DESC
            LIMIT 200
        """, (lead_id,))
        return cur.fetchall() or []
    finally:
        cur.close(); conn.close()