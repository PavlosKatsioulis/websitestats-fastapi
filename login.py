# login.py
from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, timedelta
import os
import logging
import mysql.connector
from passlib.context import CryptContext
from jose import jwt, JWTError

router = APIRouter()


logger = logging.getLogger("login")
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(h)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


# JWT / Auth config
SECRET_KEY = os.getenv(
    "JWT_SECRET",
    "atestingsecretkey"  # fallback
)
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 365  # 1 year

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

DB_HOST = os.getenv("DB_HOST", "mysql")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASSWORD", "55_66")
DB_NAME = os.getenv("DB_NAME", "webstatssite")

def get_db():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME
    )

# Models

class RegisterRequest(BaseModel):
    username: str
    password: str
    name: str
    role_id: int

class LoginRequest(BaseModel):
    username: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str
    user_id: int
    name: str
    role: str
    organization: str
    
class CurrentUser(BaseModel):
    id: int
    username: str
    name: str
    role: str
    organization: str

# Helpers
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# Routes
@router.post("/auth/register")
def register_user(req: RegisterRequest):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM users WHERE username=%s", (req.username,))
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="Username already exists")

        hashed = pwd_context.hash(req.password)
        cur.execute(
            "INSERT INTO users (username, password_hash, name, role_id) VALUES (%s,%s,%s,%s)",
            (req.username, hashed, req.name, req.role_id),
        )
        conn.commit()
        return {"status": "ok", "message": "User registered successfully"}
    except Exception as e:
        conn.rollback()
        logger.exception("register_user failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close(); conn.close()

@router.post("/auth/login", response_model=TokenResponse)
def login_user(req: LoginRequest):
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT u.id, u.password_hash, u.name, r.name AS role, o.name AS organization
            FROM users u
            JOIN roles r ON u.role_id = r.id
            JOIN organizations o ON r.organization_id = o.id
            WHERE u.username=%s
            """,
            (req.username,),
        )
        user = cur.fetchone()
        if not user or not pwd_context.verify(req.password, user["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid username or password")

        cur.execute("UPDATE users SET last_login=NOW() WHERE id=%s", (user["id"],))
        conn.commit()


        token_payload = {
            "sub": str(user["id"]),
            "username": req.username,
            "role": user["role"],
            "org": user["organization"],
        }
        token = create_access_token(
            data=token_payload, expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        )

        return TokenResponse(
            access_token=token,
            token_type="bearer",
            user_id=user["id"],
            name=user["name"],
            role=user["role"],
            organization=user["organization"],
            notifications=[],
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("login_user failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close(); conn.close()

@router.get("/auth/validate")
def validate_token(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return {
            "id": int(payload["sub"]),
            "username": payload["username"],
            "role": payload["role"],
            "organization": payload["org"],
        }
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

@router.get("/auth/roles")
def get_roles():
    conn = get_db()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT id, name, organization_id FROM roles")
        roles = cur.fetchall()
        excluded = {"Admin", "Developer", "Technical_Manager"}
        return [r for r in roles if r["name"] not in excluded]
    except Exception as e:
        logger.exception("get_roles failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close(); conn.close()

def get_current_user(token: str = Depends(oauth2_scheme)) -> CurrentUser:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    user_id = payload.get("sub")
    if user_id is None:
        raise HTTPException(status_code=401, detail="Token missing subject")

    token_username = payload.get("username")

    conn = get_db()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT u.id,
                   u.username,
                   u.name,
                   r.name AS role,
                   o.name AS organization
            FROM users u
            JOIN roles r ON u.role_id = r.id
            JOIN organizations o ON r.organization_id = o.id
            WHERE u.id = %s
            """,
            (int(user_id),),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="User not found")

        if token_username and row.get("username") and token_username != row["username"]:
            logger.warning("JWT username mismatch (token=%s, db=%s)", token_username, row["username"])

        return CurrentUser(**row)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("get_current_user failed")
        raise HTTPException(status_code=500, detail="Failed to resolve current user")
    finally:
        cur.close()
        conn.close()
