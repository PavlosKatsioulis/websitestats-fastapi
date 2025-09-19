from fastapi import APIRouter, HTTPException, Depends, Query, Request
from pydantic import BaseModel, model_validator
from typing import List, Optional, Dict, Any
from datetime import datetime
import os
import json
import logging
import traceback
import mysql.connector

from websocket_manager import manager
from login import get_current_user
from google_calendar import upsert_installation_event, delete_installation_event
from login import get_current_user, CurrentUser
from zoneinfo import ZoneInfo

ATHENS_TZ = ZoneInfo("Europe/Athens")
router = APIRouter()


# Logging (stdout for Docker)
logger = logging.getLogger("installations")
logger.info("INSTALLATIONS.PY LOADED build_tag=updjob-v4")

if not logger.handlers:
    handler = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# -------------------------
# DB Connection (env-driven)
DB_HOST = os.getenv("DB_HOST", "mysql")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASSWORD", "55_66")
DB_NAME = os.getenv("DB_NAME", "webstatssite")

def get_db():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        port=DB_PORT
    )

# Pydantic Models
class CompanyPayload(BaseModel):
    name: str
    offer_link: Optional[str]
    probable_installation_date: Optional[str]
    final_installation_date: Optional[str]
    menu_delivery_date: Optional[str] = None
    menu_completion_date: Optional[str] = None
    offer_hours: Optional[int]
    notes: Optional[str]
    selected_jobs: List[int]
    job_notes: Optional[Dict[int, str]] = None
    start_keys: Optional[str]
    got_keys: Optional[str]
    company_key: Optional[str] = None
    assigned_users: Optional[Dict[int, int]] = None
    job_due_dates: Optional[Dict[int, str]] = None
    
    @model_validator(mode="before")
    @classmethod
    def convert_job_note_keys(cls, data):
        job_notes = data.get("job_notes")
        assigned = data.get("assigned_users")
        if job_notes and isinstance(job_notes, dict):
            try:
                data["job_notes"] = {int(k): v for k, v in job_notes.items()}
            except Exception as e:
                raise ValueError(f"Invalid job_notes keys: {e}")
        if assigned and isinstance(assigned, dict):
            data["assigned_users"] = {int(k): (int(v) if v is not None else None) for k, v in assigned.items()}
        return data

class UpdateJobPayload(BaseModel):
    company_id: int
    job_id: int
    is_done: bool
    hours_spent: float | None = None
    job_notes: str | None = None
    assigned_user_id: Optional[int] = None
    due_date: Optional[str] = None

# Endpoint: Insert or Update
@router.post("/installations/create-full")
async def create_full_installation(
    data: CompanyPayload,
    current_user: CurrentUser = Depends(get_current_user),
):
    conn = get_db()
    cursor = conn.cursor()
    now_str = datetime.now(ATHENS_TZ).strftime("%Y-%m-%d %H:%M:%S")

    try:
        cursor.execute(
            """
            INSERT INTO companies (
                name, company_key, offer_link, probable_installation_date,
                final_installation_date,
                offer_hours, notes, start_keys, got_keys,
                menu_delivery_date, menu_completion_date, creation_date, created_by
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                data.name,
                data.company_key,
                data.offer_link,
                data.probable_installation_date,
                data.final_installation_date,
                data.offer_hours,
                data.notes,
                data.start_keys,
                data.got_keys,
                data.menu_delivery_date,
                data.menu_completion_date,
                now_str,
                current_user.id,
            ),
        )
        company_id = cursor.lastrowid

        for job_id in data.selected_jobs:
            note = (data.job_notes or {}).get(job_id)
            assigned_user_id = (data.assigned_users or {}).get(job_id)
            due_date = (data.job_due_dates or {}).get(job_id)

            cursor.execute(
                """
                INSERT INTO company_jobs (company_id, job_id, job_notes, assigned_user_id, due_date)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (company_id, job_id, note, assigned_user_id, due_date),
            )

        #Calendar upsert if we have a probable date
        if data.probable_installation_date:
            try:
                event_id = upsert_installation_event(
                    company_name=data.name,
                    probable_installation_date=data.probable_installation_date,
                    offer_link=data.offer_link,
                    notes=data.notes,
                    address=None,
                    existing_event_id=None,
                )
                cursor.execute(
                    "UPDATE companies SET calendar_event_id=%s WHERE id=%s",
                    (event_id, company_id)
                )
            except Exception:
                logger.exception("Calendar upsert failed on create")

        conn.commit()

        cursor.execute("SELECT id, name FROM jobs")
        job_lookup = {row[0]: row[1] for row in cursor.fetchall()}
        job_list = [
            {
                "id": job_id,
                "name": job_lookup.get(job_id),
                "notes": data.job_notes.get(job_id) if data.job_notes else None,
            }
            for job_id in data.selected_jobs
        ]

        payload = {
            "company": {
                "id": company_id,
                "name": data.name,
                "offer_link": data.offer_link,
                "probable_installation_date": data.probable_installation_date,
                "offer_hours": data.offer_hours,
                "notes": data.notes,
                "creation_date": now_str,
            },
            "jobs": job_list,
        }

        # 5) recipients + WS push
        cursor.execute(
            """
            SELECT u.id, u.username
            FROM users u
            JOIN roles r ON u.role_id = r.id
            JOIN organizations o ON r.organization_id = o.id
            WHERE o.name IN ('prognosi_technicians', 'prognosi_secretar')
            """
        )
        recipients = cursor.fetchall()

        message = f"Νέα εγκατάσταση: {data.name}"
        payload_json = json.dumps(payload, ensure_ascii=False)

        for user in recipients:
            uid = user[0]
            cursor.execute(
                "INSERT INTO notifications (user_id, message, type, data) VALUES (%s,%s,%s,%s)",
                (uid, message, "new_installation", payload_json),
            )
            await manager.send_personal_message(
                {
                    "event": "new_installation",
                    "type": "new_installation",
                    "message": message,
                    "data": payload,
                    "timestamp": now_str,
                },
                uid,
            )

        conn.commit()

        return {
            "status": "ok",
            "company": {
                "id": company_id,
                "name": data.name,
                "offer_link": data.offer_link,
                "probable_installation_date": data.probable_installation_date,
                "offer_hours": data.offer_hours,
                "notes": data.notes,
                "creation_date": now_str,
                "selected_jobs": data.selected_jobs,
            },
        }

    except Exception as e:
        conn.rollback()
        logger.exception("Installation creation failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@router.put("/installations/{company_id}/update")
def update_full_installation(
    company_id: int,
    data: CompanyPayload,
    current_user: CurrentUser = Depends(get_current_user),
):
    conn = None
    read_cur = None
    cur = None
    try:
        logger.info(
            "update_full_installation called by user_id=%s username=%s role=%s org=%s",
            current_user.id, current_user.username, current_user.role, current_user.organization
        )

        conn = get_db()
        read_cur = conn.cursor(dictionary=True)
        cur = conn.cursor()

        read_cur.execute(
            "SELECT name, probable_installation_date, offer_link, notes, calendar_event_id FROM companies WHERE id=%s",
            (company_id,)
        )
        prev = read_cur.fetchone() or {}
        prev_event_id = prev.get("calendar_event_id")

        # 1) Update the company fields
        cur.execute(
            """
            UPDATE companies
            SET name=%s,
                company_key=%s,
                offer_link=%s,
                probable_installation_date=%s,
                final_installation_date=%s,
                offer_hours=%s,
                notes=%s,
                start_keys=%s,
                got_keys=%s,
                menu_delivery_date=%s,
                menu_completion_date=%s
            WHERE id=%s
            """,
            (
                data.name,
                data.company_key,
                data.offer_link,
                data.probable_installation_date,
                data.final_installation_date,
                data.offer_hours,
                data.notes,
                data.start_keys,
                data.got_keys,
                data.menu_delivery_date,
                data.menu_completion_date,
                company_id,
            ),
        )

        # 2) Job diffs (+ assigned_user_id support)
        read_cur.execute(
            "SELECT job_id, job_notes FROM company_jobs WHERE company_id=%s",
            (company_id,),
        )
        existing_rows = read_cur.fetchall() or []
        existing_ids = {int(r["job_id"]) for r in existing_rows}

        desired_ids = {int(j) for j in (data.selected_jobs or [])}
        to_add    = desired_ids - existing_ids
        to_remove = existing_ids - desired_ids
        to_keep   = desired_ids & existing_ids

        if to_add:
            for job_id in to_add:
                note = (data.job_notes or {}).get(job_id)
                assigned_user_id = (data.assigned_users or {}).get(job_id)
                due_date = (data.job_due_dates or {}).get(job_id)

                cur.execute(
                    """
                    INSERT INTO company_jobs (company_id, job_id, job_notes, assigned_user_id, due_date)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (company_id, job_id, note, assigned_user_id, due_date),
                )

        if to_remove:
            placeholders = ",".join(["%s"] * len(to_remove))
            cur.execute(
                f"DELETE FROM company_jobs WHERE company_id=%s AND job_id IN ({placeholders})",
                (company_id, *to_remove),
            )

        if (data.job_notes or data.assigned_users) and to_keep:
            for job_id in to_keep:
                sets, vals = [], []
                if data.job_notes and (job_id in data.job_notes):
                    sets.append("job_notes=%s")
                    vals.append(data.job_notes[job_id])
                if data.assigned_users and (job_id in data.assigned_users):
                    sets.append("assigned_user_id=%s")
                    vals.append(data.assigned_users[job_id])
                if data.job_due_dates and (job_id in data.job_due_dates):
                    sets.append("due_date=%s")
                    vals.append(data.job_due_dates[job_id])
                if sets:
                    vals.extend([company_id, job_id])
                    cur.execute(
                        f"UPDATE company_jobs SET {', '.join(sets)} WHERE company_id=%s AND job_id=%s",
                        tuple(vals),
                    )

        # 3) Calendar upsert/delete based on current probable date
        read_cur.execute(
            "SELECT name, probable_installation_date, offer_link, notes, calendar_event_id FROM companies WHERE id=%s",
            (company_id,)
        )
        row = read_cur.fetchone() or {}
        new_name   = row.get("name")
        new_date   = row.get("probable_installation_date")
        offer_link = row.get("offer_link")
        notes      = row.get("notes")
        event_id   = row.get("calendar_event_id")

        try:
            if new_date:
                new_event_id = upsert_installation_event(
                    company_name=new_name,
                    probable_installation_date=new_date,
                    offer_link=offer_link,
                    notes=notes,
                    address=None,
                    existing_event_id=event_id,
                )
                if new_event_id != event_id:
                    cur.execute(
                        "UPDATE companies SET calendar_event_id=%s WHERE id=%s",
                        (new_event_id, company_id)
                    )
            else:
                if event_id:
                    delete_installation_event(event_id)
                    cur.execute(
                        "UPDATE companies SET calendar_event_id=NULL WHERE id=%s",
                        (company_id,)
                    )
        except Exception:
            logger.exception("Calendar sync failed on update")

        conn.commit()
        return {
            "status": "ok",
            "message": "Company and jobs updated successfully",
            "updated_by": {
                "id": current_user.id,
                "username": current_user.username,
                "role": current_user.role,
                "organization": current_user.organization,
            },
        }

    except Exception as e:
        if conn:
            conn.rollback()
        logger.exception("update_full_installation failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if read_cur: read_cur.close()
        if cur: cur.close()
        if conn: conn.close()



@router.get("/installations/jobs")
def get_jobs():
    conn = None
    cursor = None
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT id, name, is_default FROM jobs ORDER BY id ASC")
        return cursor.fetchall()
    except Exception as e:
        logger.exception("get_jobs failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@router.get("/installations/recent")
def get_recent_installations():
    conn = None
    cursor = None
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)

        cursor.execute(
            "SELECT * FROM companies ORDER BY creation_date DESC LIMIT 100"
        )
        companies = cursor.fetchall()

        for company in companies:
            cursor.execute(
                """
                SELECT
                    cj.job_id,
                    j.name AS job_name,
                    cj.is_done,
                    cj.hours_spent,
                    cj.job_notes,
                    cj.technician_id,
                    tu.name
                FROM company_jobs cj
                JOIN jobs j ON cj.job_id = j.id
                LEFT JOIN users tu  ON tu.id  = cj.technician_id
                WHERE cj.company_id = %s
                """,
                (company["id"],),
            )
            company["jobs"] = cursor.fetchall()

        return companies
    except Exception as e:
        logger.exception("get_recent_installations failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@router.post("/installations/update-dates")
def update_installation_dates(data: dict):
    conn = None
    cursor = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE companies
            SET menu_delivery_date=%s, menu_completion_date=%s, start_keys=%s, got_keys=%s
            WHERE id=%s
            """,
            (
                data["menu_delivery_date"],
                data["menu_completion_date"],
                data.get("start_keys"),
                data.get("got_keys"),
                data["company_id"],
            ),
        )
        conn.commit()
        return {"status": "ok"}
    except Exception as e:
        if conn: conn.rollback()
        logger.exception("update_installation_dates failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@router.post("/installations/update-job")
def update_company_job(
    data: dict,
    current_user: CurrentUser = Depends(get_current_user),
):
    conn = None
    cur = None
    due_date = data.get("due_date")
    try:
        is_done = 1 if (data.get("is_done") is True or data.get("is_done") == 1) else 0
        hrs_raw = data.get("hours_spent")
        if isinstance(hrs_raw, str):
            hrs_raw = hrs_raw.replace(",", ".").strip()
            hours_spent = float(hrs_raw) if hrs_raw else None
        else:
            hours_spent = hrs_raw

        job_notes = data.get("job_notes")
        company_id = int(data["company_id"])
        job_id = int(data["job_id"])

        conn = get_db()
        cur = conn.cursor(dictionary=True)

        # ----- (for debugging) -----
        cur.execute(
            "SELECT id, company_id, job_id, is_done, hours_spent, job_notes, technician_id "
            "FROM company_jobs WHERE company_id=%s AND job_id=%s",
            (company_id, job_id),
        )
        before = cur.fetchone()
        logger.info("update-job BEFORE row=%s", before)

        cur.close()
        cur = conn.cursor()

        cur.execute(
            """
            UPDATE company_jobs
            SET
              is_done = %s,
              hours_spent = %s,
              job_notes = %s,
              due_date = %s,
              technician_id = CASE WHEN %s = 1 THEN %s ELSE technician_id END
            WHERE company_id = %s AND job_id = %s
            """,
            (is_done, hours_spent, job_notes, due_date, is_done, current_user.id, company_id, job_id),
        )

        cur.execute(
            "SELECT COUNT(*) FROM company_jobs WHERE company_id=%s AND is_done=0",
            (company_id,),
        )
        unfinished_count = cur.fetchone()[0]
        final_date_set = False
        if unfinished_count == 0:
            today = datetime.now(ATHENS_TZ).strftime("%Y-%m-%d")
            cur.execute(
                "UPDATE companies SET final_installation_date=%s WHERE id=%s",
                (today, company_id),
            )
            final_date_set = True

        conn.commit()

        cur.close()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id, company_id, job_id, is_done, hours_spent, job_notes, technician_id "
            "FROM company_jobs WHERE company_id=%s AND job_id=%s",
            (company_id, job_id),
        )
        after = cur.fetchone()
        logger.info(
            "update-job AFTER row=%s (stamped by user_id=%s username=%s is_done=%s)",
            after, current_user.id, current_user.username, is_done
        )

        return {
            "status": "ok",
            "final_installation_date_set": final_date_set,
            "job": after,  # so the UI can reflect technician_id immediately if you want
        }

    except Exception as e:
        if conn:
            conn.rollback()
        logger.exception("update_company_job failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()



@router.get("/technicians")
def get_technicians():
    conn = None
    cursor = None
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT id, name FROM users where role_id=3 or role_id=4 ORDER BY name")
        return cursor.fetchall()
    except Exception as e:
        logger.exception("get_technicians failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@router.get("/installations/undone-jobs")
def get_undone_jobs(request: Request):
    conn = None
    cur = None
    try:
        qp = request.query_params

        def to_int(name: str, default: Optional[int]) -> Optional[int]:
            v = qp.get(name, None)
            if v is None or v == "": return default
            try:
                return int(v)
            except Exception:
                return default

        company_id = to_int("company_id", None)
        assigned_user_id = to_int("assigned_user_id", None)

        q = qp.get("q") or None

        try:
            page_i = max(1, int(qp.get("page", "1")))
        except Exception:
            page_i = 1

        try:
            page_size_i = int(qp.get("page_size", "50"))
            page_size_i = max(1, min(200, page_size_i))
        except Exception:
            page_size_i = 50

        sort_val = (qp.get("sort") or "company_asc").lower().strip()
        if sort_val not in {"company_asc","company_desc","job_asc","job_desc"}:
            sort_val = "company_asc"

        conn = get_db()
        cur = conn.cursor(dictionary=True)

        where = ["cj.is_done = 0"]
        params: list[Any] = []

        if company_id is not None:
            where.append("c.id = %s")
            params.append(company_id)

        if assigned_user_id is not None:
            where.append("cj.assigned_user_id = %s")
            params.append(assigned_user_id)

        if q:
            like = f"%{q}%"
            where.append("(c.name LIKE %s OR j.name LIKE %s OR cj.job_notes LIKE %s)")
            params.extend([like, like, like])

        where_sql = " AND ".join(where) if where else "1=1"

        sort_map = {
            "company_asc":  "c.name ASC, j.name ASC",
            "company_desc": "c.name DESC, j.name ASC",
            "job_asc":      "j.name ASC, c.name ASC",
            "job_desc":     "j.name DESC, c.name ASC",
        }
        order_by = sort_map.get(sort_val, "c.name ASC, j.name ASC")

        count_sql = f"""
            SELECT COUNT(*) AS cnt
            FROM company_jobs cj
            JOIN companies c ON c.id = cj.company_id
            JOIN jobs j      ON j.id = cj.job_id
            WHERE {where_sql}
        """
        cur.execute(count_sql, params)
        row = cur.fetchone() or {}
        total = row.get("cnt", 0)

        offset = (page_i - 1) * page_size_i

        data_sql = f"""
            SELECT
                cj.company_id,
                c.name              AS company_name,
                c.offer_link,
                c.offer_hours,
                c.probable_installation_date,
                c.final_installation_date,
                cj.job_id,
                j.name              AS job_name,
                cj.is_done,
                cj.hours_spent,
                cj.job_notes,
                cj.technician_id,
                tu.name             AS technician_name,
                cj.assigned_user_id,
                au.name             AS assigned_user_name
            FROM company_jobs cj
            JOIN companies c ON c.id = cj.company_id
            JOIN jobs j      ON j.id = cj.job_id
            LEFT JOIN users tu ON tu.id = cj.technician_id
            LEFT JOIN users au ON au.id = cj.assigned_user_id
            WHERE {where_sql}
            ORDER BY {order_by}
            LIMIT %s OFFSET %s
        """
        cur.execute(data_sql, params + [page_size_i, offset])
        rows = cur.fetchall() or []

        items = []
        for r in rows:
            items.append({
                "job_id": r["job_id"],
                "job_name": r["job_name"],
                "is_done": bool(r["is_done"]),
                "hours_spent": float(r["hours_spent"]) if r["hours_spent"] is not None else 0.0,
                "job_notes": r.get("job_notes") or "",
                "technician_id": r.get("technician_id"),
                "technician_name": r.get("technician_name"),
                "assigned_user_id": r.get("assigned_user_id"),
                "assigned_user_name": r.get("assigned_user_name"),
                "updated_at": None,
                "created_at": None,
                "company": {
                    "id": r["company_id"],
                    "name": r["company_name"],
                    "offer_link": r.get("offer_link"),
                    "offer_hours": r.get("offer_hours"),
                    "probable_installation_date": r.get("probable_installation_date"),
                    "final_installation_date": r.get("final_installation_date"),
                },
            })

        return {"items": items, "total": total, "page": page_i, "page_size": page_size_i}

    except Exception as e:
        logger.exception("get_undone_jobs failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()

@router.get("/installations/company/{company_id}")
def get_single_installation(company_id: int):
    conn = None
    cursor = None
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM companies WHERE id=%s", (company_id,))
        company = cursor.fetchone()
        if not company:
            raise HTTPException(status_code=404, detail="Company not found")

        cursor.execute(
            """
            SELECT
                cj.job_id,
                j.name AS job_name,
                cj.is_done,
                cj.hours_spent,
                cj.job_notes,
                tu.name as technician_done,
                au.name
            FROM company_jobs cj
            JOIN jobs j ON cj.job_id = j.id
            LEFT JOIN users au  ON au.id  = cj.assigned_user_id
            LEFT JOIN users tu  ON tu.id  = cj.technician_id
            WHERE cj.company_id=%s
            """,
            (company_id,),
        )
        company["jobs"] = cursor.fetchall()
        print(company)
        return company
    except Exception as e:
        logger.exception("get_single_installation failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

