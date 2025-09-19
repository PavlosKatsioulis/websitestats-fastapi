# troubleshooting_docs.py
from fastapi import APIRouter, HTTPException, Form
from pathlib import Path
import os
import sqlite3
import logging
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

router = APIRouter()

# ---------- logging (stdout) ----------
log = logging.getLogger("troubleshoot_docs")
if not log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    log.addHandler(h)
    log.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# ---------- DB path (env override; default: backend/troubleshoot.db sqlite) ----------
DEFAULT_DB_PATH = Path(__file__).parent / "troubleshoot.db"
DB_PATH = Path(os.getenv("TROUBLESHOOT_DB", str(DEFAULT_DB_PATH)))

def _connect() -> sqlite3.Connection:
    if not DB_PATH.parent.exists():
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH, check_same_thread=False)

class Step(BaseModel):
    sub_subcategory_id: int
    title: str
    description: str
    solution: str
    image_path: str = ""
    status: str = "active"

def query_db(route: str, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
    try:
        log.info("[%s] SQL: %s | %s", route, query, params)
        with _connect() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute(query, params)
            rows = [dict(r) for r in cur.fetchall()]
        return rows
    except Exception as e:
        log.exception("[%s] DB query error", route)
        raise HTTPException(status_code=500, detail=str(e))

def execute_db(query: str, params: tuple = (), return_lastrowid: bool = False) -> Optional[int]:
    try:
        with _connect() as conn:
            cur = conn.execute(query, params)
            conn.commit()
            return cur.lastrowid if return_lastrowid else None
    except Exception as e:
        log.exception("DB execute error")
        raise HTTPException(status_code=500, detail=str(e))

# ---------- 1) Categories ----------
@router.post("/docs/categories")
def add_category(name: str = Form(...)):
    execute_db("INSERT INTO categories (name) VALUES (?)", (name,))
    return {"message": "Category added successfully"}

@router.get("/docs/categories")
def get_categories():
    return query_db("/docs/categories", "SELECT id, name FROM categories ORDER BY id")

# ---------- 2) Subcategories ----------
@router.post("/docs/subcategories")
def add_subcategory(name: str = Form(...), category_id: int = Form(...)):
    execute_db("INSERT INTO subcategories (name, category_id) VALUES (?, ?)", (name, category_id))
    return {"message": "Subcategory added successfully"}

@router.get("/docs/subcategories/{category_id}")
def get_subcategories(category_id: int):
    return query_db(
        "/docs/subcategories",
        "SELECT id, name FROM subcategories WHERE category_id = ? ORDER BY id",
        (category_id,),
    )

# ---------- 3) Sub-subcategories (topics) ----------
@router.post("/docs/subsubcategories")
def add_subsubcategory(name: str = Form(...), subcategory_id: int = Form(...)):
    new_id = execute_db(
        "INSERT INTO sub_subcategories (name, subcategory_id) VALUES (?, ?)",
        (name, subcategory_id),
        return_lastrowid=True,
    )
    return {"message": "Sub-subcategory (topic) added successfully", "id": new_id}

@router.get("/docs/subsubcategories/{subcategory_id}")
def get_sub_subcategories(subcategory_id: int):
    return query_db(
        "/docs/subsubcategories",
        "SELECT id, name FROM sub_subcategories WHERE subcategory_id = ? ORDER BY id",
        (subcategory_id,),
    )

# ---------- 4) Steps ----------
@router.post("/docs/steps")
def add_solution_step(step: Step):
    execute_db(
        """
        INSERT INTO results (sub_subcategory_id, title, description, solution, image_path, status)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (step.sub_subcategory_id, step.title, step.description, step.solution, step.image_path, step.status),
    )
    return {"message": "Step added successfully"}

@router.get("/docs/steps/{sub_subcategory_id}")
def get_solution_steps(sub_subcategory_id: int):
    return query_db(
        "/docs/steps",
        """
        SELECT id, sub_subcategory_id, title, description, solution, image_path, status
        FROM results
        WHERE sub_subcategory_id = ?
        ORDER BY id ASC
        """,
        (sub_subcategory_id,),
    )
