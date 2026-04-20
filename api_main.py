from datetime import datetime
from typing import Optional, Dict, Any
import os
import re
import sqlite3
import urllib.parse
import urllib.request

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


DB_PATH = "royal_guardian.db"
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
V36C_VALIDATION_VERSION = "v36c-contract-validation-lite-1"
PROOF_CORE_VERSION = "v36c-proof-core-lite-1"
EXECUTION_INTEGRITY_VERSION = "v36c-execution-integrity-lite-1"
DAILY_LIFECYCLE_VERSION = "v36c-daily-lifecycle-lite-1"

VALID_PROOF_TYPE_MAP = {
    "text": "text",
    "link": "link",
    "file_note": "file_note",
    "screenshot_note": "screenshot_note",
    "متن": "text",
    "لینک": "link",
    "متن یا لینک": "text",
    "متن یا عکس": "screenshot_note",
    "تصویر": "screenshot_note",
    "عکس": "screenshot_note",
    "فایل": "file_note",
}

AMBIGUOUS_WORDS = {
    "work", "study", "improve", "progress", "better", "project", "learn", "practice", "task",
    "کار", "مطالعه", "درس", "پروژه", "تمرین", "یادگیری", "یاد گرفتن", "بهبود", "پیشرفت", "بررسی", "پیگیری",
}

AMBIGUOUS_PREFIXES = (
    "work on", "study ", "improve", "learn ", "practice ",
    "روی ", "کار روی", "کار ", "مطالعه ", "درس ", "تمرین ", "یادگیری ", "یاد گرفتن ", "بهبود ", "پیشرفت ", "بررسی ", "پیگیری ",
)


app = FastAPI(
    title="Royal Guardian Backend",
    version="0.8.0-daily-lifecycle-v1"
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


def today_iso_date() -> str:
    return datetime.utcnow().date().isoformat()


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



def _normalized_text_for_validation(value: str) -> str:
    return (
        str(value or "")
        .strip()
        .lower()
        .replace("ي", "ی")
        .replace("ك", "ک")
        .replace("‌", " ")
    )


def normalize_proof_type(value: str) -> str:
    key = str(value or "").strip()
    return VALID_PROOF_TYPE_MAP.get(key, VALID_PROOF_TYPE_MAP.get(key.lower(), "text"))


def validate_done_definition(value: str) -> Optional[str]:
    lowered = _normalized_text_for_validation(value)
    if len(lowered) < 8:
        return "تعریف انجام‌شدن خیلی کوتاه است. خروجی نهایی را دقیق‌تر بنویس."
    if lowered in AMBIGUOUS_WORDS:
        return "تعریف انجام‌شدن مبهم است. دقیقاً بگو چه چیزی یعنی کار تمام شده."
    if any(lowered.startswith(prefix) for prefix in AMBIGUOUS_PREFIXES):
        return "تعریف انجام‌شدن با فعل مبهم شروع شده. خروجی قابل مشاهده را مشخص کن."
    return None


def evaluate_contract_quality(contract: Dict[str, Any]) -> Dict[str, Any]:
    """
    نسخه سبک‌شده از منطق V36C برای ارزیابی قرارداد.
    این مرحله عمداً soft-gate است: قرارداد را رد نمی‌کند، فقط کیفیت و ریسک را ثبت می‌کند.
    """
    errors = []
    warnings = []
    score = 20

    title = str(contract.get("title") or "").strip()
    done_definition = str(contract.get("done_definition") or "").strip()
    if_then_trigger = str(contract.get("if_then_trigger") or "").strip()
    if_then_action = str(contract.get("if_then_action") or "").strip()
    micro_fallback = str(contract.get("micro_fallback") or "").strip()
    proof_type = str(contract.get("proof_type") or "").strip()
    normalized_proof_type = normalize_proof_type(proof_type)

    if not title:
        errors.append("عنوان قرارداد خالی است.")
    elif len(title) >= 6:
        score += 5
    else:
        warnings.append("عنوان قرارداد بهتر است مشخص‌تر باشد.")

    done_error = validate_done_definition(done_definition)
    if done_error:
        warnings.append(done_error)
    else:
        score += 12

    if len(done_definition) >= 18:
        score += 18
    elif done_definition:
        score += 6

    if len(if_then_trigger) >= 12 and len(if_then_action) >= 12:
        score += 15
    else:
        warnings.append("بخش اگر/آنگاه را دقیق‌تر کن تا در لحظه مقاومت، مسیر اجرا روشن باشد.")

    if len(micro_fallback) >= 8:
        score += 15
    else:
        warnings.append("نسخه اضطراری را واضح‌تر بنویس؛ مثلاً حداقل ۵ دقیقه یا یک خروجی کوچک.")

    if normalized_proof_type in {"link", "file_note", "screenshot_note"}:
        score += 10
    else:
        score += 5

    try:
        minutes = int(contract.get("estimated_minutes") or 30)
    except Exception:
        minutes = 30
        warnings.append("زمان تخمینی قابل خواندن نبود؛ مقدار ۳۰ دقیقه فرض شد.")

    if 1 <= minutes <= 60:
        score += 10
    elif minutes <= 120:
        score += 5
    else:
        warnings.append("زمان تخمینی سنگین است. بهتر است قرارداد را کوچک‌تر یا دو مرحله‌ای کنی.")

    if str(contract.get("deadline") or "").strip():
        score += 5
    else:
        warnings.append("مهلت قرارداد مشخص نیست.")

    score = max(0, min(int(score), 100))

    risk_points = 0
    if score < 60:
        risk_points += 25
    elif score < 75:
        risk_points += 10
    if minutes > 90:
        risk_points += 15
    if not if_then_trigger or not if_then_action:
        risk_points += 10
    if not micro_fallback:
        risk_points += 10
    if str(contract.get("difficulty") or "normal").lower() == "hard":
        risk_points += 8

    if risk_points >= 35:
        predicted_risk = "high"
    elif risk_points >= 15:
        predicted_risk = "medium"
    else:
        predicted_risk = "low"

    if errors or score < 55:
        validation_status = "needs_refinement"
    elif score >= 75 and predicted_risk != "high":
        validation_status = "strong"
    else:
        validation_status = "usable"

    notes = []
    if errors:
        notes.extend(errors)
    notes.extend(warnings[:3])
    if not notes:
        notes.append("قرارداد شفاف، قابل اجرا و قابل اثبات است.")

    return {
        "ok": not errors,
        "errors": errors,
        "warnings": warnings,
        "quality_score": score,
        "predicted_risk": predicted_risk,
        "validation_status": validation_status,
        "validation_notes": " | ".join(notes),
        "normalized_proof_type": normalized_proof_type,
        "validation_version": V36C_VALIDATION_VERSION,
    }


def risk_label_fa(risk: str) -> str:
    return {
        "low": "پایین",
        "medium": "متوسط",
        "high": "بالا",
    }.get(str(risk), "نامشخص")


def status_label_fa(status: str) -> str:
    return {
        "strong": "قوی",
        "usable": "قابل اجرا",
        "needs_refinement": "نیازمند اصلاح",
    }.get(str(status), "نامشخص")


def count_words_loose(value: str) -> int:
    cleaned = str(value or "").strip()
    if not cleaned:
        return 0
    return len([part for part in re.split(r"\s+", cleaned) if part])


def extract_links(value: str) -> list[str]:
    return re.findall(r"https?://[^\s]+|www\.[^\s]+", str(value or ""), flags=re.IGNORECASE)


def evaluate_proof_quality(proof_text: str, task: sqlite3.Row) -> Dict[str, Any]:
    """
    Proof Core V1: نسخه سبک‌شده از منطق اثبات V36C.
    هدف فعلی: امتیازدهی، تشخیص ریسک، review_status و پیام شفاف؛ بدون شکستن جریان MVP.
    """
    text_value = str(proof_text or "").strip()
    lowered = _normalized_text_for_validation(text_value)
    links = extract_links(text_value)
    word_count = count_words_loose(text_value)
    char_count = len(text_value)

    task_dict = row_to_dict(task) or {}
    normalized_proof_type = str(task_dict.get("normalized_proof_type") or normalize_proof_type(task_dict.get("proof_type") or "text"))

    warnings = []
    errors = []
    score = 15

    if char_count < 8:
        errors.append("متن اثبات خیلی کوتاه است.")
    elif char_count < 25:
        warnings.append("اثبات کوتاه است؛ بهتر است خروجی مشخص‌تر نوشته شود.")
        score += 8
    elif char_count < 80:
        score += 18
    else:
        score += 28

    if word_count >= 8:
        score += 12
    elif word_count >= 4:
        score += 6
    else:
        warnings.append("اثبات بهتر است شامل چند جزئیات قابل بررسی باشد.")

    if links:
        score += 18
    elif normalized_proof_type == "link":
        warnings.append("نوع اثبات لینک است، اما لینکی در متن پیدا نشد.")

    proof_detail_markers = [
        "انجام", "کامل", "خلاصه", "نوشتم", "خواندم", "ساختم", "ارسال", "ثبت", "تمام",
        "done", "completed", "summary", "finished", "built", "sent"
    ]
    if any(marker in lowered for marker in proof_detail_markers):
        score += 10
    else:
        warnings.append("اثبات بهتر است واضح بگوید دقیقاً چه خروجی‌ای انجام شد.")

    title = _normalized_text_for_validation(task_dict.get("title") or "")
    done_definition = _normalized_text_for_validation(task_dict.get("done_definition") or "")
    if title and any(token for token in title.split() if len(token) >= 4 and token in lowered):
        score += 8
    if done_definition and any(token for token in done_definition.split() if len(token) >= 5 and token in lowered):
        score += 7

    estimated_minutes = int(task_dict.get("estimated_minutes") or 30)
    if estimated_minutes > 90 and char_count < 60 and not links:
        warnings.append("برای قرارداد سنگین، اثبات فعلی کمی سبک است.")

    score = max(0, min(int(score), 100))

    if errors:
        review_status = "needs_review"
        proof_risk = "high"
    elif score >= 75:
        review_status = "auto_accepted"
        proof_risk = "low"
    elif score >= 55:
        review_status = "auto_accepted"
        proof_risk = "medium"
    else:
        review_status = "needs_review"
        proof_risk = "high"

    if review_status == "auto_accepted":
        if score >= 85:
            xp_awarded = 35
        elif score >= 70:
            xp_awarded = 25
        else:
            xp_awarded = 15
    else:
        xp_awarded = 5

    notes = []
    if errors:
        notes.extend(errors)
    notes.extend(warnings[:4])
    if not notes:
        notes.append("اثبات قابل قبول و هم‌راستا با قرارداد است.")

    return {
        "ok": not errors,
        "proof_kind": normalized_proof_type,
        "proof_quality_score": score,
        "proof_risk": proof_risk,
        "proof_validation_status": review_status,
        "proof_validation_notes": " | ".join(notes),
        "proof_validation_version": PROOF_CORE_VERSION,
        "review_status": review_status,
        "quality_note": " | ".join(notes),
        "xp_awarded": xp_awarded,
        "detected_links": len(links),
        "word_count": word_count,
        "char_count": char_count,
    }


def proof_risk_label_fa(risk: str) -> str:
    return {
        "low": "پایین",
        "medium": "متوسط",
        "high": "بالا",
    }.get(str(risk), "نامشخص")


def review_status_label_fa(status: str) -> str:
    return {
        "auto_accepted": "پذیرفته‌شده خودکار",
        "needs_review": "نیازمند بررسی",
        "duplicate_submission": "تکراری؛ امتیاز دوباره ثبت نشد",
        "rejected": "رد شده",
    }.get(str(status), "نامشخص")


def build_task_lifecycle(conn: sqlite3.Connection, task_row) -> Dict[str, Any]:
    task = row_to_dict(task_row)
    if not task:
        return {
            "lifecycle_version": DAILY_LIFECYCLE_VERSION,
            "has_contract": False,
            "can_submit_proof": False,
            "needs_new_contract": True,
            "state": "empty",
            "message": "برای امروز هنوز قراردادی ثبت نشده است."
        }

    proof_count = conn.execute(
        "SELECT COUNT(*) AS count FROM proofs WHERE task_id = ?",
        (task["id"],)
    ).fetchone()["count"]

    status = str(task.get("status") or "active")
    integrity_status = str(task.get("integrity_status") or "open")

    is_completed = status == "completed" or integrity_status == "completed_once" or proof_count > 0
    is_active = status == "active" and not is_completed

    if is_completed:
        state = "completed"
        can_submit = False
        needs_new = True
        message = "قرارداد قبلی کامل شده است. برای اجرای بعدی، قرارداد تازه بساز."
    elif is_active:
        state = "active"
        can_submit = True
        needs_new = False
        message = "قرارداد فعال است و هنوز اثبات معتبر ثبت نشده است."
    else:
        state = status
        can_submit = False
        needs_new = True
        message = "این قرارداد بسته شده است. برای ادامه، قرارداد تازه بساز."

    return {
        "lifecycle_version": DAILY_LIFECYCLE_VERSION,
        "has_contract": True,
        "state": state,
        "status": status,
        "integrity_status": integrity_status,
        "can_submit_proof": can_submit,
        "needs_new_contract": needs_new,
        "proof_count": proof_count,
        "execution_date": task.get("execution_date"),
        "submitted_at": task.get("submitted_at"),
        "completed_at": task.get("completed_at"),
        "message": message
    }


def close_previous_active_contracts(conn: sqlite3.Connection, telegram_id: str) -> int:
    rows = conn.execute(
        """
        SELECT id FROM tasks
        WHERE telegram_id = ? AND status = 'active'
        """,
        (telegram_id,)
    ).fetchall()

    if not rows:
        return 0

    conn.execute(
        """
        UPDATE tasks
        SET status = 'superseded',
            lifecycle_status = 'superseded',
            integrity_status = 'superseded_by_new_contract',
            closed_at = ?,
            close_reason = 'replaced_by_new_contract',
            updated_at = ?
        WHERE telegram_id = ? AND status = 'active'
        """,
        (now_iso(), now_iso(), telegram_id)
    )

    return len(rows)

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
        ensure_column(conn, "tasks", "normalized_proof_type", "TEXT NOT NULL DEFAULT 'text'")
        ensure_column(conn, "tasks", "contract_quality_score", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "tasks", "predicted_risk", "TEXT NOT NULL DEFAULT 'unknown'")
        ensure_column(conn, "tasks", "validation_status", "TEXT NOT NULL DEFAULT 'unknown'")
        ensure_column(conn, "tasks", "validation_notes", "TEXT")
        ensure_column(conn, "tasks", "validation_version", "TEXT")
        ensure_column(conn, "proofs", "review_status", "TEXT NOT NULL DEFAULT 'auto_accepted'")
        ensure_column(conn, "proofs", "quality_note", "TEXT")
        ensure_column(conn, "proofs", "xp_awarded", "INTEGER NOT NULL DEFAULT 25")
        ensure_column(conn, "proofs", "proof_kind", "TEXT NOT NULL DEFAULT 'text'")
        ensure_column(conn, "proofs", "proof_quality_score", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "proofs", "proof_risk", "TEXT NOT NULL DEFAULT 'unknown'")
        ensure_column(conn, "proofs", "proof_validation_status", "TEXT NOT NULL DEFAULT 'unknown'")
        ensure_column(conn, "proofs", "proof_validation_notes", "TEXT")
        ensure_column(conn, "proofs", "proof_validation_version", "TEXT")
        ensure_column(conn, "proofs", "detected_links", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "proofs", "word_count", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "proofs", "integrity_flag", "TEXT NOT NULL DEFAULT 'first_submission'")
        ensure_column(conn, "proofs", "duplicate_of_proof_id", "INTEGER")
        ensure_column(conn, "proofs", "integrity_version", "TEXT")
        ensure_column(conn, "tasks", "submitted_at", "TEXT")
        ensure_column(conn, "tasks", "completed_at", "TEXT")
        ensure_column(conn, "tasks", "integrity_status", "TEXT NOT NULL DEFAULT 'open'")
        ensure_column(conn, "tasks", "proof_attempts_count", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "tasks", "execution_date", "TEXT")
        ensure_column(conn, "tasks", "lifecycle_status", "TEXT NOT NULL DEFAULT 'active'")
        ensure_column(conn, "tasks", "closed_at", "TEXT")
        ensure_column(conn, "tasks", "close_reason", "TEXT")
        ensure_column(conn, "tasks", "lifecycle_version", "TEXT")

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
    strict_validation: bool = False


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
        "version": "0.8.0-daily-lifecycle-v1"
    }


@app.get("/health")
def health():
    return {
        "ok": True,
        "time": now_iso(),
        "database": DB_PATH,
        "bot_token_configured": bool(BOT_TOKEN),
        "version": "0.8.0-daily-lifecycle-v1"
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
        "normalized_proof_type": normalize_proof_type(proof_type),
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

    validation = evaluate_contract_quality(contract)
    if data.strict_validation and validation["validation_status"] == "needs_refinement":
        raise HTTPException(status_code=422, detail=validation["validation_notes"])

    with get_db() as conn:
        ensure_user_exists(conn, telegram_id)

        close_previous_active_contracts(conn, telegram_id)

        created = now_iso()
        cursor = conn.execute(
            """
            INSERT INTO tasks (
                telegram_id, title, deadline, proof_type, status, created_at, updated_at,
                done_definition, if_then_trigger, if_then_action, micro_fallback,
                estimated_minutes, contract_type, difficulty, source,
                normalized_proof_type, contract_quality_score, predicted_risk, validation_status, validation_notes, validation_version,
                execution_date, lifecycle_status, lifecycle_version
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                "mini_app",
                validation["normalized_proof_type"],
                validation["quality_score"],
                validation["predicted_risk"],
                validation["validation_status"],
                validation["validation_notes"],
                validation["validation_version"],
                today_iso_date(),
                "active",
                DAILY_LIFECYCLE_VERSION
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
            f"نسخه اضطراری: {contract['micro_fallback']}\n"
            f"اگر: {contract['if_then_trigger'] or 'ثبت نشده'}\n"
            f"آنگاه: {contract['if_then_action'] or 'ثبت نشده'}\n"
            f"کیفیت قرارداد: {validation['quality_score']} از ۱۰۰ ({status_label_fa(validation['validation_status'])})\n"
            f"ریسک اجرا: {risk_label_fa(validation['predicted_risk'])}\n"
            f"یادداشت: {validation['validation_notes']}"
        )
    )

    with get_db() as conn:
        lifecycle = build_task_lifecycle(conn, task)

    task_dict = row_to_dict(task)
    return {
        "ok": True,
        "task": task_dict,
        "contract": task_dict,
        "lifecycle": lifecycle
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
            task = conn.execute(
                """
                SELECT * FROM tasks
                WHERE telegram_id = ?
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
                    estimated_minutes, contract_type, difficulty, source,
                    execution_date, lifecycle_status, lifecycle_version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    "auto_seed",
                    today_iso_date(),
                    "active",
                    DAILY_LIFECYCLE_VERSION
                )
            )
            conn.commit()
            task_id = cursor.lastrowid
            task = conn.execute(
                "SELECT * FROM tasks WHERE id = ?",
                (task_id,)
            ).fetchone()

    with get_db() as conn:
        lifecycle = build_task_lifecycle(conn, task)

    task_dict = row_to_dict(task)
    return {
        "ok": True,
        "task": task_dict,
        "contract": task_dict,
        "lifecycle": lifecycle
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



@app.post("/contracts/validate")
def validate_contract(data: TaskCreateRequest):
    contract = normalize_contract_payload(data)
    validation = evaluate_contract_quality(contract)
    return {
        "ok": True,
        "contract": contract,
        "validation": validation
    }



@app.post("/proofs/validate")
def validate_proof(data: ProofCreateRequest):
    telegram_id = data.telegram_id.strip()
    proof_text = data.proof_text.strip()

    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")
    if not proof_text:
        raise HTTPException(status_code=400, detail="متن اثبات الزامی است")

    with get_db() as conn:
        task = conn.execute(
            "SELECT * FROM tasks WHERE id = ? AND telegram_id = ?",
            (data.task_id, telegram_id)
        ).fetchone()

    if task is None:
        raise HTTPException(status_code=404, detail="تعهد پیدا نشد")

    validation = evaluate_proof_quality(proof_text, task)

    return {
        "ok": True,
        "proof_validation": validation,
        "contract": row_to_dict(task)
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
        ensure_user_exists(conn, telegram_id)

        task = conn.execute(
            "SELECT * FROM tasks WHERE id = ? AND telegram_id = ?",
            (data.task_id, telegram_id)
        ).fetchone()

        if task is None:
            raise HTTPException(status_code=404, detail="تعهد پیدا نشد")

        created = now_iso()

        existing_proof = conn.execute(
            """
            SELECT * FROM proofs
            WHERE telegram_id = ? AND task_id = ?
            ORDER BY id ASC
            LIMIT 1
            """,
            (telegram_id, data.task_id)
        ).fetchone()

        user = conn.execute(
            "SELECT * FROM users WHERE telegram_id = ?",
            (telegram_id,)
        ).fetchone()

        old_stage = user["guardian_stage"]

        if existing_proof is not None:
            proof_validation = {
                "proof_kind": existing_proof["proof_kind"] if "proof_kind" in existing_proof.keys() else "text",
                "proof_quality_score": existing_proof["proof_quality_score"] if "proof_quality_score" in existing_proof.keys() else 0,
                "proof_risk": existing_proof["proof_risk"] if "proof_risk" in existing_proof.keys() else "medium",
                "proof_validation_status": "duplicate_submission",
                "proof_validation_notes": "ثبت نشد؛ ثبت تکراری معتبر نیست. برای این قرارداد قبلاً اثبات ثبت شده است و امتیاز یا زنجیره دوباره اضافه نشد.",
                "proof_validation_version": PROOF_CORE_VERSION,
                "review_status": "duplicate_submission",
                "quality_note": "Duplicate proof blocked by Execution Integrity V1.",
                "xp_awarded": 0,
                "detected_links": 0,
                "word_count": count_words_loose(proof_text),
            }

            conn.execute(
                """
                UPDATE tasks
                SET proof_attempts_count = proof_attempts_count + 1,
                    integrity_status = 'duplicate_attempt_blocked',
                    lifecycle_version = ?,
                    updated_at = ?
                WHERE id = ? AND telegram_id = ?
                """,
                (DAILY_LIFECYCLE_VERSION, now_iso(), data.task_id, telegram_id)
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
                    "duplicate_proof_blocked",
                    0,
                    old_stage,
                    old_stage,
                    f"task_id={data.task_id};duplicate_of_proof_id={existing_proof['id']};not_registered=true",
                    now_iso()
                )
            )

            conn.commit()

            proof = None

            updated_user = conn.execute(
                "SELECT * FROM users WHERE telegram_id = ?",
                (telegram_id,)
            ).fetchone()

        else:
            proof_validation = evaluate_proof_quality(proof_text, task)
            xp_awarded = proof_validation["xp_awarded"]

            cursor = conn.execute(
                """
                INSERT INTO proofs (
                    telegram_id, task_id, proof_text, status, created_at,
                    review_status, quality_note, xp_awarded,
                    proof_kind, proof_quality_score, proof_risk,
                    proof_validation_status, proof_validation_notes, proof_validation_version,
                    detected_links, word_count, integrity_flag, duplicate_of_proof_id, integrity_version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    telegram_id,
                    data.task_id,
                    proof_text,
                    "submitted",
                    created,
                    proof_validation["review_status"],
                    proof_validation["quality_note"],
                    xp_awarded,
                    proof_validation["proof_kind"],
                    proof_validation["proof_quality_score"],
                    proof_validation["proof_risk"],
                    proof_validation["proof_validation_status"],
                    proof_validation["proof_validation_notes"],
                    proof_validation["proof_validation_version"],
                    proof_validation["detected_links"],
                    proof_validation["word_count"],
                    "first_submission",
                    None,
                    EXECUTION_INTEGRITY_VERSION,
                )
            )

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
                UPDATE tasks
                SET status = 'completed',
                    lifecycle_status = 'completed',
                    submitted_at = ?,
                    completed_at = ?,
                    proof_attempts_count = proof_attempts_count + 1,
                    integrity_status = 'completed_once',
                    lifecycle_version = ?,
                    updated_at = ?
                WHERE id = ? AND telegram_id = ?
                """,
                (created, created, DAILY_LIFECYCLE_VERSION, now_iso(), data.task_id, telegram_id)
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
                    f"task_id={data.task_id};review_status={proof_validation['review_status']};proof_score={proof_validation['proof_quality_score']}",
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

    proof_message_title = "✅ اثبات قرارداد ثبت شد."
    if proof_validation.get("review_status") == "duplicate_submission":
        proof_message_title = "⚠️ ثبت نشد؛ ثبت تکراری معتبر نیست."

    send_bot_message(
        telegram_id,
        (
            f"{proof_message_title}\n\n"
            f"عنوان قرارداد: {task['title']}\n"
            f"کیفیت اثبات: {proof_validation['proof_quality_score']} از ۱۰۰\n"
            f"وضعیت بررسی: {review_status_label_fa(proof_validation['review_status'])}\n"
            f"ریسک اثبات: {proof_risk_label_fa(proof_validation['proof_risk'])}\n"
            f"امتیاز افزوده‌شده: {proof_validation['xp_awarded']}\n"
            f"امتیاز اجرایی: {updated_user['xp']}\n"
            f"زنجیره اجرا: {updated_user['streak_days']} روز\n"
            f"مرحله نگهبان: {updated_user['guardian_stage']}\n"
            f"یادداشت: {proof_validation['proof_validation_notes']}"
        )
    )

    is_duplicate = proof_validation.get("review_status") == "duplicate_submission"

    with get_db() as conn:
        refreshed_task = conn.execute(
            "SELECT * FROM tasks WHERE id = ?",
            (data.task_id,)
        ).fetchone()
        lifecycle = build_task_lifecycle(conn, refreshed_task)

    return {
        "ok": not is_duplicate,
        "code": "duplicate_proof" if is_duplicate else "proof_submitted",
        "message": "ثبت نشد؛ ثبت تکراری معتبر نیست." if is_duplicate else "اثبات قرارداد ثبت شد.",
        "proof": row_to_dict(proof),
        "proof_validation": proof_validation,
        "user": row_to_dict(updated_user),
        "contract": row_to_dict(refreshed_task),
        "lifecycle": lifecycle
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



@app.get("/lifecycle/today")
def lifecycle_today(telegram_id: str):
    telegram_id = telegram_id.strip()
    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")

    with get_db() as conn:
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
            task = conn.execute(
                """
                SELECT * FROM tasks
                WHERE telegram_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (telegram_id,)
            ).fetchone()

        lifecycle = build_task_lifecycle(conn, task)

        user = conn.execute(
            "SELECT * FROM users WHERE telegram_id = ?",
            (telegram_id,)
        ).fetchone()

    return {
        "ok": True,
        "contract": row_to_dict(task),
        "task": row_to_dict(task),
        "lifecycle": lifecycle,
        "progress": {
            "xp": user["xp"],
            "streak_days": user["streak_days"],
            "guardian_stage": user["guardian_stage"],
            "verified_proofs_count": user["verified_proofs_count"]
        } if user else None
    }


@app.get("/contracts/current")
def contracts_current(telegram_id: str):
    return lifecycle_today(telegram_id)


@app.get("/contracts/latest-validation")
def latest_contract_validation(telegram_id: str):
    telegram_id = telegram_id.strip()
    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")

    with get_db() as conn:
        row = conn.execute(
            """
            SELECT id, title, done_definition, if_then_trigger, if_then_action,
                   micro_fallback, estimated_minutes, proof_type,
                   normalized_proof_type, contract_quality_score, predicted_risk,
                   validation_status, validation_notes, validation_version
            FROM tasks
            WHERE telegram_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (telegram_id,)
        ).fetchone()

    if row is None:
        return {
            "ok": False,
            "message": "قراردادی پیدا نشد"
        }

    return {
        "ok": True,
        "contract_validation": row_to_dict(row)
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




@app.get("/execution/integrity")
def execution_integrity(telegram_id: str):
    telegram_id = telegram_id.strip()
    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")

    with get_db() as conn:
        latest_task = conn.execute(
            """
            SELECT id, title, status, integrity_status, proof_attempts_count,
                   submitted_at, completed_at, updated_at
            FROM tasks
            WHERE telegram_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (telegram_id,)
        ).fetchone()

        duplicate_count = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM progression_events
            WHERE telegram_id = ? AND event_type = 'duplicate_proof_blocked'
            """,
            (telegram_id,)
        ).fetchone()["count"]

    return {
        "ok": True,
        "integrity_version": EXECUTION_INTEGRITY_VERSION,
        "latest_task": row_to_dict(latest_task),
        "duplicate_proofs_blocked": duplicate_count
    }


@app.get("/proofs/latest-validation")
def latest_proof_validation(telegram_id: str):
    telegram_id = telegram_id.strip()
    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")

    with get_db() as conn:
        row = conn.execute(
            """
            SELECT id, telegram_id, task_id, proof_text, status, review_status,
                   quality_note, xp_awarded, proof_kind, proof_quality_score,
                   proof_risk, proof_validation_status, proof_validation_notes,
                   proof_validation_version, detected_links, word_count, created_at
            FROM proofs
            WHERE telegram_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (telegram_id,)
        ).fetchone()

    if row is None:
        return {
            "ok": False,
            "message": "اثباتی پیدا نشد"
        }

    return {
        "ok": True,
        "proof_validation": row_to_dict(row)
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
