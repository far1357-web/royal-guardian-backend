from datetime import datetime
from typing import Optional, Dict, Any
import os
import sqlite3
import urllib.parse
import urllib.request

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


DB_PATH = "royal_guardian.db"
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()


app = FastAPI(
    title="Royal Guardian Backend",
    version="0.4.0-contract-core"
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


def send_bot_message(chat_id: str, text: str) -> bool:
    """
    ارسال پیام از طریق Bot API تلگرام.
    اگر توکن تنظیم نشده باشد یا ارسال شکست بخورد، منطق اصلی بک‌اند نباید خراب شود.
    """
    if not BOT_TOKEN:
        print("[telegram_notify_skipped] BOT_TOKEN is not configured")
        return False

    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = urllib.parse.urlencode({
            "chat_id": str(chat_id),
            "text": text
        }).encode("utf-8")

        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST"
        )

        with urllib.request.urlopen(req, timeout=8) as response:
            ok = 200 <= response.status < 300
            print(f"[telegram_notify] chat_id={chat_id} status={response.status} ok={ok}")
            return ok

    except Exception as exc:
        print(f"[telegram_notify_failed] chat_id={chat_id} error={exc}")
        return False


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


def ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    """
    مهاجرت ساده SQLite: اگر ستون وجود نداشت، اضافه می‌شود.
    این روش برای MVP امن است و داده‌های قبلی را پاک نمی‌کند.
    """
    existing_columns = {
        row["name"]
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }

    if column not in existing_columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


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

        # Contract Core V1 fields on top of the old tasks table.
        # We keep the table name "tasks" for compatibility with the current Mini App.
        ensure_column(conn, "tasks", "done_definition", "TEXT")
        ensure_column(conn, "tasks", "if_then_trigger", "TEXT")
        ensure_column(conn, "tasks", "if_then_action", "TEXT")
        ensure_column(conn, "tasks", "micro_fallback", "TEXT")
        ensure_column(conn, "tasks", "estimated_minutes", "INTEGER NOT NULL DEFAULT 30")
        ensure_column(conn, "tasks", "contract_type", "TEXT NOT NULL DEFAULT 'execution_contract'")
        ensure_column(conn, "tasks", "difficulty", "TEXT NOT NULL DEFAULT 'normal'")
        ensure_column(conn, "tasks", "source", "TEXT NOT NULL DEFAULT 'mini_app'")
        ensure_column(conn, "proofs", "review_status", "TEXT NOT NULL DEFAULT 'auto_accepted'")
        ensure_column(conn, "proofs", "quality_note", "TEXT")
        ensure_column(conn, "proofs", "xp_awarded", "INTEGER NOT NULL DEFAULT 25")

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

    # Contract Core V1
    done_definition: Optional[str] = None
    if_then_trigger: Optional[str] = None
    if_then_action: Optional[str] = None
    micro_fallback: Optional[str] = None
    estimated_minutes: int = 30
    difficulty: str = "normal"


class ProofCreateRequest(BaseModel):
    telegram_id: str
    task_id: int
    proof_text: str


@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "Royal Guardian backend is running",
        "database": DB_PATH,
        "version": "0.4.0-contract-core"
    }


@app.get("/health")
def health():
    return {
        "ok": True,
        "time": now_iso(),
        "database": DB_PATH,
        "bot_token_configured": bool(BOT_TOKEN),
        "version": "0.4.0-contract-core"
    }


@app.get("/bot/status")
def bot_status():
    return {
        "ok": True,
        "bot_token_configured": bool(BOT_TOKEN)
    }


@app.post("/bot/test-message")
def bot_test_message(telegram_id: str):
    sent = send_bot_message(
        telegram_id,
        "✅ اتصال بک‌اند به بات فعال است.\n\nاین پیام تست از Render Backend ارسال شده است."
    )
    return {
        "ok": True,
        "telegram_id": telegram_id,
        "sent": sent
    }


def ensure_user_exists(conn: sqlite3.Connection, telegram_id: str, first_name: str = "کاربر", username: Optional[str] = None):
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
                first_name or "کاربر",
                username,
                240,
                5,
                "شکاف طلایی",
                0,
                created,
                created
            )
        )

    return conn.execute(
        "SELECT * FROM users WHERE telegram_id = ?",
        (telegram_id,)
    ).fetchone()


def normalize_contract_payload(data: TaskCreateRequest) -> Dict[str, Any]:
    title = data.title.strip()
    deadline = data.deadline.strip() or "18:00"
    proof_type = data.proof_type.strip() or "متن یا لینک"

    done_definition = (data.done_definition or "").strip()
    if not done_definition:
        done_definition = f"این تعهد زمانی کامل است که «{title}» انجام شده و اثبات قابل بررسی ثبت شود."

    if_then_trigger = (data.if_then_trigger or "").strip()
    if_then_action = (data.if_then_action or "").strip()
    micro_fallback = (data.micro_fallback or "").strip()

    if not micro_fallback:
        micro_fallback = "اگر زمان یا انرژی کم بود، حداقل ۵ دقیقه نسخه کوچک‌تر همین تعهد را انجام می‌دهم."

    estimated_minutes = int(data.estimated_minutes or 30)
    if estimated_minutes < 1:
        estimated_minutes = 1
    if estimated_minutes > 600:
        estimated_minutes = 600

    difficulty = (data.difficulty or "normal").strip().lower()
    if difficulty not in {"easy", "normal", "hard"}:
        difficulty = "normal"

    return {
        "title": title,
        "deadline": deadline,
        "proof_type": proof_type,
        "done_definition": done_definition,
        "if_then_trigger": if_then_trigger,
        "if_then_action": if_then_action,
        "micro_fallback": micro_fallback,
        "estimated_minutes": estimated_minutes,
        "difficulty": difficulty,
    }


def create_contract_record(data: TaskCreateRequest):
    telegram_id = data.telegram_id.strip()
    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")

    contract = normalize_contract_payload(data)

    if not contract["title"]:
        raise HTTPException(status_code=400, detail="عنوان تعهد الزامی است")

    with get_db() as conn:
        ensure_user_exists(conn, telegram_id)

        created = now_iso()
        cursor = conn.execute(
            """
            INSERT INTO tasks (
                telegram_id, title, deadline, proof_type, status, created_at, updated_at,
                done_definition, if_then_trigger, if_then_action, micro_fallback,
                estimated_minutes, contract_type, difficulty, source
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                telegram_id,
                contract["title"],
                contract["deadline"],
                contract["proof_type"],
                "active",
                created,
                created,
                contract["done_definition"],
                contract["if_then_trigger"],
                contract["if_then_action"],
                contract["micro_fallback"],
                contract["estimated_minutes"],
                "execution_contract",
                contract["difficulty"],
                "mini_app"
            )
        )
        conn.commit()

        task = conn.execute(
            "SELECT * FROM tasks WHERE id = ?",
            (cursor.lastrowid,)
        ).fetchone()

    send_bot_message(
        telegram_id,
        (
            "✅ قرارداد اجرایی تازه ثبت شد.\n\n"
            f"عنوان: {contract['title']}\n"
            f"تعریف انجام‌شدن: {contract['done_definition']}\n"
            f"مهلت: {contract['deadline']}\n"
            f"نوع اثبات: {contract['proof_type']}\n"
            f"نسخه اضطراری: {contract['micro_fallback']}"
        )
    )

    task_dict = row_to_dict(task)
    return {
        "ok": True,
        "task": task_dict,
        "contract": task_dict
    }


@app.post("/auth/telegram")
def auth_telegram(data: AuthRequest):
    telegram_id = data.telegram_id.strip()
    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")

    with get_db() as conn:
        user = ensure_user_exists(
            conn,
            telegram_id,
            first_name=data.first_name or "کاربر",
            username=data.username
        )
        conn.commit()

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
        ensure_user_exists(conn, telegram_id)

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
                    telegram_id, title, deadline, proof_type, status, created_at, updated_at,
                    done_definition, if_then_trigger, if_then_action, micro_fallback,
                    estimated_minutes, contract_type, difficulty, source
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    telegram_id,
                    "۳۰ دقیقه کار عمیق",
                    "18:00",
                    "متن یا لینک",
                    "active",
                    created,
                    created,
                    "۳۰ دقیقه تمرکز واقعی بدون حواس‌پرتی، همراه با اثبات قابل بررسی.",
                    "اگر امروز کار اصلی عقب افتاد",
                    "قبل از ساعت ۱۸:۰۰ حداقل ۳۰ دقیقه کار عمیق انجام می‌دهم.",
                    "اگر نتوانستم ۳۰ دقیقه انجام دهم، حداقل ۵ دقیقه شروع واقعی انجام می‌دهم.",
                    30,
                    "execution_contract",
                    "normal",
                    "auto_seed"
                )
            )
            conn.commit()
            task_id = cursor.lastrowid
            task = conn.execute(
                "SELECT * FROM tasks WHERE id = ?",
                (task_id,)
            ).fetchone()

    task_dict = row_to_dict(task)
    return {
        "ok": True,
        "task": task_dict,
        "contract": task_dict
    }


@app.get("/contracts/today")
def get_today_contract(telegram_id: str):
    return get_today(telegram_id)


@app.post("/tasks")
def create_task(data: TaskCreateRequest):
    return create_contract_record(data)


@app.post("/contracts")
def create_contract(data: TaskCreateRequest):
    return create_contract_record(data)


@app.post("/proofs")
def create_proof(data: ProofCreateRequest):
    telegram_id = data.telegram_id.strip()
    proof_text = data.proof_text.strip()

    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")

    if not proof_text:
        raise HTTPException(status_code=400, detail="متن اثبات الزامی است")

    with get_db() as conn:
        ensure_user_exists(conn, telegram_id)

        task = conn.execute(
            "SELECT * FROM tasks WHERE id = ? AND telegram_id = ?",
            (data.task_id, telegram_id)
        ).fetchone()

        if task is None:
            raise HTTPException(status_code=404, detail="تعهد پیدا نشد")

        created = now_iso()

        xp_awarded = 25
        cursor = conn.execute(
            """
            INSERT INTO proofs (
                telegram_id, task_id, proof_text, status, created_at,
                review_status, quality_note, xp_awarded
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                telegram_id,
                data.task_id,
                proof_text,
                "submitted",
                created,
                "auto_accepted",
                "MVP auto-accepted proof",
                xp_awarded
            )
        )

        user = conn.execute(
            "SELECT * FROM users WHERE telegram_id = ?",
            (telegram_id,)
        ).fetchone()

        old_stage = user["guardian_stage"]
        new_xp = int(user["xp"]) + xp_awarded
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
                xp_awarded,
                old_stage,
                new_stage,
                f"task_id={data.task_id};review_status=auto_accepted",
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

    send_bot_message(
        telegram_id,
        (
            "✅ اثبات قرارداد ثبت شد.\n\n"
            f"عنوان قرارداد: {task['title']}\n"
            f"امتیاز اجرایی: {updated_user['xp']}\n"
            f"زنجیره اجرا: {updated_user['streak_days']} روز\n"
            f"مرحله نگهبان: {updated_user['guardian_stage']}"
        )
    )

    return {
        "ok": True,
        "proof": row_to_dict(proof),
        "user": row_to_dict(updated_user),
        "contract": row_to_dict(task)
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


@app.get("/contracts/history")
def contracts_history(telegram_id: str, limit: int = 20):
    telegram_id = telegram_id.strip()
    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")

    limit = max(1, min(int(limit or 20), 100))

    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT * FROM tasks
            WHERE telegram_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (telegram_id, limit)
        ).fetchall()

    return {
        "ok": True,
        "contracts": [row_to_dict(row) for row in rows]
    }


@app.get("/proofs/history")
def proofs_history(telegram_id: str, limit: int = 20):
    telegram_id = telegram_id.strip()
    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")

    limit = max(1, min(int(limit or 20), 100))

    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT * FROM proofs
            WHERE telegram_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (telegram_id, limit)
        ).fetchall()

    return {
        "ok": True,
        "proofs": [row_to_dict(row) for row in rows]
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
