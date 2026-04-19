from datetime import datetime
from typing import Optional, Dict, Any, List
import sqlite3

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


DB_PATH = "royal_guardian.db"


app = FastAPI(
    title="Royal Guardian Backend",
    version="0.2.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # برای تست. بعداً فقط دامنه Netlify را مجاز می‌کنیم.
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def now_iso() -> str:
    return datetime.utcnow().isoformat()


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def row_to_dict(row) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    return dict(row)


def calculate_stage(streak_days: int, verified_proofs_count: int) -> str:
    if streak_days >= 60:
        return "نگهبان سلطنتی"
    if streak_days >= 30:
        return "شیر طلایی"
    if streak_days >= 14:
        return "شیر جوان"
    if streak_days >= 7:
        return "نگهبان بیدار"
    if verified_proofs_count >= 1 or streak_days >= 3:
        return "شکاف طلایی"
    return "هسته خاموش"


def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                telegram_id TEXT PRIMARY KEY,
                first_name TEXT NOT NULL,
                username TEXT,
                xp INTEGER NOT NULL DEFAULT 240,
                streak_days INTEGER NOT NULL DEFAULT 5,
                guardian_stage TEXT NOT NULL DEFAULT 'شکاف طلایی',
                verified_proofs_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id TEXT NOT NULL,
                title TEXT NOT NULL,
                deadline TEXT NOT NULL,
                proof_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (telegram_id) REFERENCES users (telegram_id)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS proofs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id TEXT NOT NULL,
                task_id INTEGER NOT NULL,
                proof_text TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'submitted',
                created_at TEXT NOT NULL,
                FOREIGN KEY (telegram_id) REFERENCES users (telegram_id),
                FOREIGN KEY (task_id) REFERENCES tasks (id)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS progression_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                xp_delta INTEGER NOT NULL DEFAULT 0,
                old_stage TEXT,
                new_stage TEXT,
                metadata TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (telegram_id) REFERENCES users (telegram_id)
            )
        """)

        conn.commit()


@app.on_event("startup")
def on_startup():
    init_db()


class AuthRequest(BaseModel):
    telegram_id: str
    first_name: Optional[str] = None
    username: Optional[str] = None


class TaskCreateRequest(BaseModel):
    telegram_id: str
    title: str
    deadline: str = "18:00"
    proof_type: str = "متن یا لینک"


class ProofCreateRequest(BaseModel):
    telegram_id: str
    task_id: int
    proof_text: str


@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "Royal Guardian backend is running",
        "database": DB_PATH
    }


@app.get("/health")
def health():
    return {
        "ok": True,
        "time": now_iso(),
        "database": DB_PATH
    }


@app.post("/auth/telegram")
def auth_telegram(data: AuthRequest):
    telegram_id = data.telegram_id.strip()
    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")

    with get_db() as conn:
        user = conn.execute(
            "SELECT * FROM users WHERE telegram_id = ?",
            (telegram_id,)
        ).fetchone()

        if user is None:
            created = now_iso()
            conn.execute(
                """
                INSERT INTO users (
                    telegram_id, first_name, username, xp, streak_days,
                    guardian_stage, verified_proofs_count, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    telegram_id,
                    data.first_name or "کاربر",
                    data.username,
                    240,
                    5,
                    "شکاف طلایی",
                    0,
                    created,
                    created
                )
            )
            conn.commit()

        user = conn.execute(
            "SELECT * FROM users WHERE telegram_id = ?",
            (telegram_id,)
        ).fetchone()

    return {
        "ok": True,
        "user": row_to_dict(user)
    }


@app.get("/today")
def get_today(telegram_id: str):
    telegram_id = telegram_id.strip()
    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")

    with get_db() as conn:
        user = conn.execute(
            "SELECT * FROM users WHERE telegram_id = ?",
            (telegram_id,)
        ).fetchone()

        if user is None:
            created = now_iso()
            conn.execute(
                """
                INSERT INTO users (
                    telegram_id, first_name, username, xp, streak_days,
                    guardian_stage, verified_proofs_count, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    telegram_id,
                    "کاربر",
                    None,
                    240,
                    5,
                    "شکاف طلایی",
                    0,
                    created,
                    created
                )
            )
            conn.commit()

        task = conn.execute(
            """
            SELECT * FROM tasks
            WHERE telegram_id = ? AND status = 'active'
            ORDER BY id DESC
            LIMIT 1
            """,
            (telegram_id,)
        ).fetchone()

        if task is None:
            created = now_iso()
            cursor = conn.execute(
                """
                INSERT INTO tasks (
                    telegram_id, title, deadline, proof_type, status, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    telegram_id,
                    "۳۰ دقیقه کار عمیق",
                    "18:00",
                    "متن یا لینک",
                    "active",
                    created,
                    created
                )
            )
            conn.commit()
            task_id = cursor.lastrowid
            task = conn.execute(
                "SELECT * FROM tasks WHERE id = ?",
                (task_id,)
            ).fetchone()

    return {
        "ok": True,
        "task": row_to_dict(task)
    }


@app.post("/tasks")
def create_task(data: TaskCreateRequest):
    telegram_id = data.telegram_id.strip()
    title = data.title.strip()
    deadline = data.deadline.strip() or "18:00"
    proof_type = data.proof_type.strip() or "متن یا لینک"

    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")

    if not title:
        raise HTTPException(status_code=400, detail="عنوان تعهد الزامی است")

    with get_db() as conn:
        user = conn.execute(
            "SELECT * FROM users WHERE telegram_id = ?",
            (telegram_id,)
        ).fetchone()

        if user is None:
            created = now_iso()
            conn.execute(
                """
                INSERT INTO users (
                    telegram_id, first_name, username, xp, streak_days,
                    guardian_stage, verified_proofs_count, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    telegram_id,
                    "کاربر",
                    None,
                    240,
                    5,
                    "شکاف طلایی",
                    0,
                    created,
                    created
                )
            )

        created = now_iso()
        cursor = conn.execute(
            """
            INSERT INTO tasks (
                telegram_id, title, deadline, proof_type, status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                telegram_id,
                title,
                deadline,
                proof_type,
                "active",
                created,
                created
            )
        )
        conn.commit()

        task = conn.execute(
            "SELECT * FROM tasks WHERE id = ?",
            (cursor.lastrowid,)
        ).fetchone()

    return {
        "ok": True,
        "task": row_to_dict(task)
    }


@app.post("/proofs")
def create_proof(data: ProofCreateRequest):
    telegram_id = data.telegram_id.strip()
    proof_text = data.proof_text.strip()

    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")

    if not proof_text:
        raise HTTPException(status_code=400, detail="متن اثبات الزامی است")

    with get_db() as conn:
        user = conn.execute(
            "SELECT * FROM users WHERE telegram_id = ?",
            (telegram_id,)
        ).fetchone()

        if user is None:
            created = now_iso()
            conn.execute(
                """
                INSERT INTO users (
                    telegram_id, first_name, username, xp, streak_days,
                    guardian_stage, verified_proofs_count, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    telegram_id,
                    "کاربر",
                    None,
                    240,
                    5,
                    "شکاف طلایی",
                    0,
                    created,
                    created
                )
            )
            conn.commit()

        task = conn.execute(
            "SELECT * FROM tasks WHERE id = ? AND telegram_id = ?",
            (data.task_id, telegram_id)
        ).fetchone()

        if task is None:
            raise HTTPException(status_code=404, detail="تعهد پیدا نشد")

        created = now_iso()

        cursor = conn.execute(
            """
            INSERT INTO proofs (
                telegram_id, task_id, proof_text, status, created_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                telegram_id,
                data.task_id,
                proof_text,
                "submitted",
                created
            )
        )

        user = conn.execute(
            "SELECT * FROM users WHERE telegram_id = ?",
            (telegram_id,)
        ).fetchone()

        old_stage = user["guardian_stage"]
        new_xp = int(user["xp"]) + 25
        new_streak = int(user["streak_days"]) + 1
        new_verified_count = int(user["verified_proofs_count"]) + 1
        new_stage = calculate_stage(new_streak, new_verified_count)

        conn.execute(
            """
            UPDATE users
            SET xp = ?, streak_days = ?, verified_proofs_count = ?,
                guardian_stage = ?, updated_at = ?
            WHERE telegram_id = ?
            """,
            (
                new_xp,
                new_streak,
                new_verified_count,
                new_stage,
                now_iso(),
                telegram_id
            )
        )

        conn.execute(
            """
            INSERT INTO progression_events (
                telegram_id, event_type, xp_delta, old_stage, new_stage, metadata, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                telegram_id,
                "proof_submitted",
                25,
                old_stage,
                new_stage,
                f"task_id={data.task_id}",
                now_iso()
            )
        )

        conn.commit()

        proof = conn.execute(
            "SELECT * FROM proofs WHERE id = ?",
            (cursor.lastrowid,)
        ).fetchone()

        updated_user = conn.execute(
            "SELECT * FROM users WHERE telegram_id = ?",
            (telegram_id,)
        ).fetchone()

    return {
        "ok": True,
        "proof": row_to_dict(proof),
        "user": row_to_dict(updated_user)
    }


@app.get("/me/progress")
def get_progress(telegram_id: str):
    telegram_id = telegram_id.strip()
    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")

    with get_db() as conn:
        user = conn.execute(
            "SELECT * FROM users WHERE telegram_id = ?",
            (telegram_id,)
        ).fetchone()

    if user is None:
        return {
            "ok": False,
            "message": "کاربر هنوز ساخته نشده است"
        }

    return {
        "ok": True,
        "progress": {
            "xp": user["xp"],
            "streak_days": user["streak_days"],
            "guardian_stage": user["guardian_stage"],
            "verified_proofs_count": user["verified_proofs_count"]
        }
    }


@app.get("/debug/state")
def debug_state():
    with get_db() as conn:
        users = [row_to_dict(r) for r in conn.execute("SELECT * FROM users ORDER BY created_at DESC").fetchall()]
        tasks = [row_to_dict(r) for r in conn.execute("SELECT * FROM tasks ORDER BY id DESC").fetchall()]
        proofs = [row_to_dict(r) for r in conn.execute("SELECT * FROM proofs ORDER BY id DESC").fetchall()]
        events = [row_to_dict(r) for r in conn.execute("SELECT * FROM progression_events ORDER BY id DESC").fetchall()]

    return {
        "ok": True,
        "users": users,
        "tasks": tasks,
        "proofs": proofs,
        "events": events
    }
