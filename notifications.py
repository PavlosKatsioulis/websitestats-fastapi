# notifications.py
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import os
import mysql.connector
from jose import jwt, JWTError

from login import SECRET_KEY, ALGORITHM

router = APIRouter()

DB_HOST = os.getenv("DB_HOST", "mysql")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASSWORD", "55_66")
DB_NAME = os.getenv("DB_NAME", "webstatssite")

def get_db():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME
    )

# ---- Auth helper ----
def get_current_user_id(authorization: str = Header(None)) -> int:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    token = authorization.split()[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    user_id = payload.get("sub") or payload.get("id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")
    return int(user_id)

# ---- Models ----
class NotificationOut(BaseModel):
    id: int
    user_id: int
    message: str
    type: Optional[str] = "general"
    is_read: bool
    timestamp: datetime
    data: Optional[str] = None


@router.get("/notifications", response_model=List[NotificationOut])
def list_notifications(
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    unread_only: bool = Query(False),
    user_id: int = Depends(get_current_user_id),
):
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    try:
        sql = """
            SELECT id, user_id, message, type, is_read, timestamp, data
            FROM notifications
            WHERE user_id=%s
        """
        params = [user_id]
        if unread_only:
            sql += " AND is_read=0"
        sql += " ORDER BY timestamp DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        cur.execute(sql, params)
        rows = cur.fetchall()
        for r in rows:
            r["is_read"] = bool(r["is_read"])
        return rows
    finally:
        cur.close(); conn.close()

@router.get("/notifications/unread-count")
def unread_count(user_id: int = Depends(get_current_user_id)):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM notifications WHERE user_id=%s AND is_read=0", (user_id,))
        (count,) = cur.fetchone()
        return {"count": int(count)}
    finally:
        cur.close(); conn.close()

@router.post("/notifications/mark-read")
def mark_all_read(user_id: int = Depends(get_current_user_id)):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE notifications SET is_read=1 WHERE user_id=%s AND is_read=0", (user_id,))
        conn.commit()
        return {"updated": cur.rowcount}
    finally:
        cur.close(); conn.close()

@router.post("/notifications/{notif_id}/mark-read")
def mark_single_read(notif_id: int, user_id: int = Depends(get_current_user_id)):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE notifications SET is_read=1 WHERE id=%s AND user_id=%s",
            (notif_id, user_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Notification not found")
        return {"updated": 1}
    finally:
        cur.close(); conn.close()
