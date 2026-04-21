from datetime import datetime
from typing import Optional, Dict, Any
import os
import re
import sqlite3
import urllib.parse
import urllib.request

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # psycopg is only required when DATABASE_URL is configured
    psycopg = None
    dict_row = None

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


DB_PATH = "royal_guardian.db"
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_BACKEND = "postgres" if DATABASE_URL else "sqlite"
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
V36C_VALIDATION_VERSION = "v36c-contract-validation-lite-1"
PROOF_CORE_VERSION = "v36c-proof-core-lite-1"
EXECUTION_INTEGRITY_VERSION = "v36c-execution-integrity-lite-1"
DAILY_LIFECYCLE_VERSION = "v36c-daily-lifecycle-lite-1"
REVIEW_APPEAL_VERSION = "v36c-review-appeal-lite-1"
DASHBOARD_TIMELINE_VERSION = "v36c-dashboard-timeline-lite-1"

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
    version="0.11.0-postgres-persistence-v1"
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


def _convert_sql_for_postgres(sql: str) -> str:
    # This codebase uses SQLite-style ? placeholders. psycopg uses %s.
    return sql.replace("?", "%s")


def _insert_table_name(sql: str) -> Optional[str]:
    match = re.search(r"^\s*INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1).lower()


class CursorResult:
    def __init__(self, cursor, lastrowid=None):
        self._cursor = cursor
        self.lastrowid = lastrowid

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()


class PostgresConnection:
    def __init__(self):
        if psycopg is None:
            raise RuntimeError("DATABASE_URL is configured, but psycopg is not installed. Add psycopg[binary] to requirements.txt")
        self._conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)

    def execute(self, sql: str, params=()):
        pg_sql = _convert_sql_for_postgres(sql)
        cur = self._conn.cursor()
        cur.execute(pg_sql, params or ())

        lastrowid = None
        table = _insert_table_name(sql)
        if table in {"tasks", "proofs", "progression_events", "proof_appeals"}:
            # PostgreSQL sequence value for the last INSERT in this connection.
            # Used to preserve sqlite-style cursor.lastrowid behavior.
            seq_cur = self._conn.cursor()
            seq_cur.execute("SELECT LASTVAL() AS id")
            row = seq_cur.fetchone()
            if row:
                lastrowid = row["id"]

        return CursorResult(cur, lastrowid=lastrowid)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self._conn.commit()
            else:
                self._conn.rollback()
        finally:
            self._conn.close()


def get_db():
    if DB_BACKEND == "postgres":
        return PostgresConnection()

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
        "accepted": "پذیرفته‌شده",
        "accepted_after_appeal": "پذیرفته‌شده پس از اعتراض",
        "needs_revision": "نیازمند اصلاح",
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


def calculate_review_priority(proof_validation: Dict[str, Any], task) -> int:
    score = int(proof_validation.get("proof_quality_score") or 0)
    risk = proof_validation.get("proof_risk") or "medium"
    status = proof_validation.get("review_status") or proof_validation.get("proof_validation_status") or "unknown"

    priority = 30
    if status == "needs_review":
        priority += 35
    if risk == "high":
        priority += 25
    elif risk == "medium":
        priority += 10
    if score < 40:
        priority += 20
    elif score < 60:
        priority += 10

    task_dict = row_to_dict(task) or {}
    if str(task_dict.get("difficulty") or "").lower() == "hard":
        priority += 5

    return max(0, min(priority, 100))


def apply_review_award(
    conn: sqlite3.Connection,
    *,
    proof,
    task,
    reviewer_id: str,
    note: str,
    xp_award: Optional[int],
    event_type: str,
    accepted_status: str,
) -> Dict[str, Any]:
    """
    امتیازدهی بعد از review یا appeal.
    محافظت می‌کند که یک proof دوبار award نشود.
    """
    proof_dict = row_to_dict(proof) or {}
    task_dict = row_to_dict(task) or {}
    telegram_id = proof_dict["telegram_id"]

    user = conn.execute(
        "SELECT * FROM users WHERE telegram_id = ?",
        (telegram_id,)
    ).fetchone()

    if user is None:
        raise HTTPException(status_code=404, detail="کاربر پیدا نشد")

    already_awarded = int(proof_dict.get("awarded_after_review") or 0) == 1
    if already_awarded or proof_dict.get("review_status") in {"auto_accepted", "accepted", "accepted_after_appeal"}:
        return {
            "awarded": False,
            "reason": "این اثبات قبلاً پذیرفته یا امتیازدهی شده است.",
            "user": row_to_dict(user),
        }

    quality_score = int(proof_dict.get("proof_quality_score") or 0)
    if xp_award is None:
        if quality_score >= 85:
            xp_award = 35
        elif quality_score >= 70:
            xp_award = 25
        elif quality_score >= 50:
            xp_award = 15
        else:
            xp_award = 10

    xp_award = max(0, min(int(xp_award), 50))

    old_stage = user["guardian_stage"]
    new_xp = int(user["xp"]) + xp_award
    new_streak = int(user["streak_days"]) + 1
    new_verified_count = int(user["verified_proofs_count"]) + 1
    new_stage = calculate_stage(new_streak, new_verified_count)
    reviewed_at = now_iso()

    conn.execute(
        """
        UPDATE users
        SET xp = ?, streak_days = ?, verified_proofs_count = ?,
            guardian_stage = ?, updated_at = ?
        WHERE telegram_id = ?
        """,
        (new_xp, new_streak, new_verified_count, new_stage, reviewed_at, telegram_id)
    )

    conn.execute(
        """
        UPDATE proofs
        SET status = 'accepted',
            review_status = ?,
            review_decision = ?,
            reviewer_id = ?,
            reviewer_note = ?,
            reviewed_at = ?,
            xp_awarded = ?,
            awarded_after_review = 1,
            review_version = ?
        WHERE id = ?
        """,
        (
            accepted_status,
            accepted_status,
            reviewer_id,
            note,
            reviewed_at,
            xp_award,
            REVIEW_APPEAL_VERSION,
            proof_dict["id"],
        )
    )

    conn.execute(
        """
        UPDATE tasks
        SET status = 'completed',
            lifecycle_status = 'completed',
            completed_at = ?,
            integrity_status = 'completed_after_review',
            lifecycle_version = ?,
            updated_at = ?
        WHERE id = ? AND telegram_id = ?
        """,
        (reviewed_at, DAILY_LIFECYCLE_VERSION, reviewed_at, task_dict["id"], telegram_id)
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
            event_type,
            xp_award,
            old_stage,
            new_stage,
            f"proof_id={proof_dict['id']};task_id={task_dict['id']};reviewer_id={reviewer_id}",
            reviewed_at,
        )
    )

    updated_user = conn.execute(
        "SELECT * FROM users WHERE telegram_id = ?",
        (telegram_id,)
    ).fetchone()

    return {
        "awarded": True,
        "xp_awarded": xp_award,
        "user": row_to_dict(updated_user),
    }

def ensure_column(conn, table: str, column: str, definition: str) -> None:
    """
    Safe MVP migration: add missing columns without deleting existing data.
    Works for both SQLite and PostgreSQL.
    """
    if DB_BACKEND == "postgres":
        row = conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = ?
              AND column_name = ?
            LIMIT 1
            """,
            (table, column)
        ).fetchone()

        if row is None:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        return

    existing_columns = {
        row["name"]
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }

    if column not in existing_columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def create_base_tables_postgres(conn) -> None:
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
            id SERIAL PRIMARY KEY,
            telegram_id TEXT NOT NULL REFERENCES users (telegram_id),
            title TEXT NOT NULL,
            deadline TEXT NOT NULL,
            proof_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS proofs (
            id SERIAL PRIMARY KEY,
            telegram_id TEXT NOT NULL REFERENCES users (telegram_id),
            task_id INTEGER NOT NULL REFERENCES tasks (id),
            proof_text TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'submitted',
            created_at TEXT NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS progression_events (
            id SERIAL PRIMARY KEY,
            telegram_id TEXT NOT NULL REFERENCES users (telegram_id),
            event_type TEXT NOT NULL,
            xp_delta INTEGER NOT NULL DEFAULT 0,
            old_stage TEXT,
            new_stage TEXT,
            metadata TEXT,
            created_at TEXT NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS proof_appeals (
            id SERIAL PRIMARY KEY,
            telegram_id TEXT NOT NULL REFERENCES users (telegram_id),
            proof_id INTEGER NOT NULL REFERENCES proofs (id),
            task_id INTEGER NOT NULL REFERENCES tasks (id),
            appeal_text TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            reviewer_id TEXT,
            resolution_note TEXT,
            created_at TEXT NOT NULL,
            resolved_at TEXT
        )
    """)


def create_base_tables_sqlite(conn) -> None:
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

    conn.execute("""
        CREATE TABLE IF NOT EXISTS proof_appeals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id TEXT NOT NULL,
            proof_id INTEGER NOT NULL,
            task_id INTEGER NOT NULL,
            appeal_text TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            reviewer_id TEXT,
            resolution_note TEXT,
            created_at TEXT NOT NULL,
            resolved_at TEXT,
            FOREIGN KEY (telegram_id) REFERENCES users (telegram_id),
            FOREIGN KEY (proof_id) REFERENCES proofs (id),
            FOREIGN KEY (task_id) REFERENCES tasks (id)
        )
    """)


def init_db():
    with get_db() as conn:
        if DB_BACKEND == "postgres":
            create_base_tables_postgres(conn)
        else:
            create_base_tables_sqlite(conn)

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
        ensure_column(conn, "proofs", "review_priority", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "proofs", "reviewer_id", "TEXT")
        ensure_column(conn, "proofs", "reviewer_note", "TEXT")
        ensure_column(conn, "proofs", "reviewed_at", "TEXT")
        ensure_column(conn, "proofs", "review_decision", "TEXT")
        ensure_column(conn, "proofs", "review_version", "TEXT")
        ensure_column(conn, "proofs", "awarded_after_review", "INTEGER NOT NULL DEFAULT 0")

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


class ReviewDecisionRequest(BaseModel):
    proof_id: int
    reviewer_id: str = "admin"
    decision: str
    note: Optional[str] = None
    xp_award: Optional[int] = None


class AppealCreateRequest(BaseModel):
    telegram_id: str
    proof_id: int
    appeal_text: str


class AppealResolveRequest(BaseModel):
    appeal_id: int
    reviewer_id: str = "admin"
    decision: str
    resolution_note: Optional[str] = None
    xp_award: Optional[int] = None


@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "Royal Guardian backend is running",
        "database": DB_PATH,
        "db_backend": DB_BACKEND,
        "database_url_configured": bool(DATABASE_URL),
        "version": "0.11.0-postgres-persistence-v1"
    }


@app.get("/health")
def health():
    return {
        "ok": True,
        "time": now_iso(),
        "database": DB_PATH,
        "db_backend": DB_BACKEND,
        "database_url_configured": bool(DATABASE_URL),
        "bot_token_configured": bool(BOT_TOKEN),
        "version": "0.11.0-postgres-persistence-v1"
    }



@app.get("/db/status")
def db_status():
    with get_db() as conn:
        users_count = conn.execute("SELECT COUNT(*) AS count FROM users").fetchone()["count"]
        tasks_count = conn.execute("SELECT COUNT(*) AS count FROM tasks").fetchone()["count"]
        proofs_count = conn.execute("SELECT COUNT(*) AS count FROM proofs").fetchone()["count"]
        appeals_count = conn.execute("SELECT COUNT(*) AS count FROM proof_appeals").fetchone()["count"]
        events_count = conn.execute("SELECT COUNT(*) AS count FROM progression_events").fetchone()["count"]

    return {
        "ok": True,
        "db_backend": DB_BACKEND,
        "database_url_configured": bool(DATABASE_URL),
        "counts": {
            "users": users_count,
            "tasks": tasks_count,
            "proofs": proofs_count,
            "appeals": appeals_count,
            "events": events_count,
        }
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
              AND review_status IN (
                'auto_accepted', 'accepted', 'accepted_after_appeal',
                'needs_review', 'duplicate_submission'
              )
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
            needs_review = proof_validation["review_status"] == "needs_review"
            xp_awarded = 0 if needs_review else proof_validation["xp_awarded"]
            review_priority = calculate_review_priority(proof_validation, task)

            cursor = conn.execute(
                """
                INSERT INTO proofs (
                    telegram_id, task_id, proof_text, status, created_at,
                    review_status, quality_note, xp_awarded,
                    proof_kind, proof_quality_score, proof_risk,
                    proof_validation_status, proof_validation_notes, proof_validation_version,
                    detected_links, word_count, integrity_flag, duplicate_of_proof_id, integrity_version,
                    review_priority, review_version, awarded_after_review
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    review_priority,
                    REVIEW_APPEAL_VERSION,
                    0,
                )
            )

            if needs_review:
                conn.execute(
                    """
                    UPDATE tasks
                    SET status = 'under_review',
                        lifecycle_status = 'under_review',
                        submitted_at = ?,
                        proof_attempts_count = proof_attempts_count + 1,
                        integrity_status = 'pending_review',
                        lifecycle_version = ?,
                        updated_at = ?
                    WHERE id = ? AND telegram_id = ?
                    """,
                    (created, DAILY_LIFECYCLE_VERSION, now_iso(), data.task_id, telegram_id)
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
                        "proof_needs_review",
                        0,
                        old_stage,
                        old_stage,
                        f"task_id={data.task_id};proof_score={proof_validation['proof_quality_score']};priority={review_priority}",
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

            else:
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
    if proof_validation.get("review_status") == "needs_review":
        proof_message_title = "🟡 اثبات ثبت شد و در صف بررسی قرار گرفت."
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






def event_label_fa(event_type: str) -> str:
    return {
        "proof_submitted": "اثبات پذیرفته شد",
        "proof_needs_review": "اثبات وارد صف بررسی شد",
        "duplicate_proof_blocked": "ثبت تکراری نامعتبر بود",
        "review_accepted": "اثبات پس از بررسی پذیرفته شد",
        "review_rejected": "اثبات رد شد",
        "review_needs_revision": "اثبات نیازمند اصلاح شد",
        "appeal_created": "اعتراض ثبت شد",
        "appeal_accepted": "اعتراض پذیرفته شد",
        "appeal_denied": "اعتراض رد شد",
        "contract_created": "قرارداد ثبت شد",
    }.get(event_type or "", event_type or "رویداد")


def build_next_action(lifecycle: Dict[str, Any], latest_proof=None, open_appeal=None) -> Dict[str, Any]:
    latest_proof_dict = row_to_dict(latest_proof)
    open_appeal_dict = row_to_dict(open_appeal)

    if open_appeal_dict:
        return {
            "code": "appeal_pending",
            "title": "اعتراض در انتظار بررسی است",
            "description": "فعلاً اقدام جدید لازم نیست؛ نتیجه اعتراض بعداً مشخص می‌شود.",
            "primary_action": "wait_for_appeal_review"
        }

    if latest_proof_dict and latest_proof_dict.get("review_status") == "needs_review":
        return {
            "code": "proof_pending_review",
            "title": "اثبات در صف بررسی است",
            "description": "اثبات ثبت شده و منتظر تصمیم reviewer است.",
            "primary_action": "wait_for_review"
        }

    if lifecycle.get("state") == "completed":
        return {
            "code": "create_new_contract",
            "title": "قرارداد تازه بساز",
            "description": "قرارداد قبلی کامل شده است. برای اجرای بعدی یک قرارداد تازه ثبت کن.",
            "primary_action": "create_contract"
        }

    if lifecycle.get("can_submit_proof"):
        return {
            "code": "submit_proof",
            "title": "اثبات قرارداد را ثبت کن",
            "description": "قرارداد فعال است و هنوز اثبات معتبر ثبت نشده است.",
            "primary_action": "submit_proof"
        }

    return {
        "code": "create_contract",
        "title": "قرارداد اجرایی بساز",
        "description": "برای شروع اجرا، یک قرارداد مشخص و قابل اثبات ثبت کن.",
        "primary_action": "create_contract"
    }


def get_user_dashboard_data(telegram_id: str) -> Dict[str, Any]:
    with get_db() as conn:
        user = conn.execute(
            "SELECT * FROM users WHERE telegram_id = ?",
            (telegram_id,)
        ).fetchone()

        latest_task = conn.execute(
            """
            SELECT * FROM tasks
            WHERE telegram_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (telegram_id,)
        ).fetchone()

        active_task = conn.execute(
            """
            SELECT * FROM tasks
            WHERE telegram_id = ? AND status = 'active'
            ORDER BY id DESC
            LIMIT 1
            """,
            (telegram_id,)
        ).fetchone()

        latest_proof = conn.execute(
            """
            SELECT * FROM proofs
            WHERE telegram_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (telegram_id,)
        ).fetchone()

        open_appeal = conn.execute(
            """
            SELECT * FROM proof_appeals
            WHERE telegram_id = ? AND status = 'open'
            ORDER BY id DESC
            LIMIT 1
            """,
            (telegram_id,)
        ).fetchone()

        lifecycle = build_task_lifecycle(conn, active_task or latest_task)

        contract_counts = {
            row["status"]: row["count"]
            for row in conn.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM tasks
                WHERE telegram_id = ?
                GROUP BY status
                """,
                (telegram_id,)
            ).fetchall()
        }

        proof_counts = {
            row["review_status"]: row["count"]
            for row in conn.execute(
                """
                SELECT review_status, COUNT(*) AS count
                FROM proofs
                WHERE telegram_id = ?
                GROUP BY review_status
                """,
                (telegram_id,)
            ).fetchall()
        }

        totals = conn.execute(
            """
            SELECT
                COUNT(*) AS total_proofs,
                COALESCE(SUM(xp_awarded), 0) AS total_xp_from_proofs,
                COALESCE(AVG(proof_quality_score), 0) AS avg_proof_quality
            FROM proofs
            WHERE telegram_id = ?
            """,
            (telegram_id,)
        ).fetchone()

        appeal_counts = {
            row["status"]: row["count"]
            for row in conn.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM proof_appeals
                WHERE telegram_id = ?
                GROUP BY status
                """,
                (telegram_id,)
            ).fetchall()
        }

    return {
        "dashboard_version": DASHBOARD_TIMELINE_VERSION,
        "user": row_to_dict(user),
        "active_contract": row_to_dict(active_task),
        "latest_contract": row_to_dict(latest_task),
        "latest_proof": row_to_dict(latest_proof),
        "open_appeal": row_to_dict(open_appeal),
        "lifecycle": lifecycle,
        "next_action": build_next_action(lifecycle, latest_proof, open_appeal),
        "stats": {
            "contracts": contract_counts,
            "proofs": proof_counts,
            "appeals": appeal_counts,
            "total_proofs": totals["total_proofs"] if totals else 0,
            "total_xp_from_proofs": totals["total_xp_from_proofs"] if totals else 0,
            "avg_proof_quality": round(float(totals["avg_proof_quality"] or 0), 2) if totals else 0,
        }
    }


@app.get("/me/dashboard")
def me_dashboard(telegram_id: str):
    telegram_id = telegram_id.strip()
    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")

    return {
        "ok": True,
        **get_user_dashboard_data(telegram_id)
    }


@app.get("/me/next-action")
def me_next_action(telegram_id: str):
    telegram_id = telegram_id.strip()
    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")

    data = get_user_dashboard_data(telegram_id)
    return {
        "ok": True,
        "dashboard_version": DASHBOARD_TIMELINE_VERSION,
        "next_action": data["next_action"],
        "lifecycle": data["lifecycle"]
    }


@app.get("/me/timeline")
def me_timeline(telegram_id: str, limit: int = 30):
    telegram_id = telegram_id.strip()
    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")

    limit = max(1, min(int(limit or 30), 100))

    with get_db() as conn:
        events = conn.execute(
            """
            SELECT * FROM progression_events
            WHERE telegram_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (telegram_id, limit)
        ).fetchall()

    timeline = []
    for row in events:
        item = row_to_dict(row)
        item["label"] = event_label_fa(item.get("event_type"))
        timeline.append(item)

    return {
        "ok": True,
        "dashboard_version": DASHBOARD_TIMELINE_VERSION,
        "timeline": timeline
    }


@app.get("/me/history")
def me_history(telegram_id: str, limit: int = 20):
    telegram_id = telegram_id.strip()
    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")

    limit = max(1, min(int(limit or 20), 100))

    with get_db() as conn:
        contracts = [
            row_to_dict(row)
            for row in conn.execute(
                """
                SELECT * FROM tasks
                WHERE telegram_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (telegram_id, limit)
            ).fetchall()
        ]

        proofs = [
            row_to_dict(row)
            for row in conn.execute(
                """
                SELECT * FROM proofs
                WHERE telegram_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (telegram_id, limit)
            ).fetchall()
        ]

        appeals = [
            row_to_dict(row)
            for row in conn.execute(
                """
                SELECT * FROM proof_appeals
                WHERE telegram_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (telegram_id, limit)
            ).fetchall()
        ]

    return {
        "ok": True,
        "dashboard_version": DASHBOARD_TIMELINE_VERSION,
        "contracts": contracts,
        "proofs": proofs,
        "appeals": appeals
    }


@app.get("/review/queue")
def review_queue(limit: int = 20):
    limit = max(1, min(int(limit or 20), 100))

    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT
                p.*,
                t.title AS contract_title,
                t.done_definition AS contract_done_definition,
                t.deadline AS contract_deadline,
                t.estimated_minutes AS contract_estimated_minutes,
                u.first_name AS user_first_name,
                u.username AS user_username
            FROM proofs p
            JOIN tasks t ON t.id = p.task_id
            LEFT JOIN users u ON u.telegram_id = p.telegram_id
            WHERE p.review_status = 'needs_review'
            ORDER BY p.review_priority DESC, p.created_at ASC
            LIMIT ?
            """,
            (limit,)
        ).fetchall()

    return {
        "ok": True,
        "review_version": REVIEW_APPEAL_VERSION,
        "items": [row_to_dict(row) for row in rows]
    }


@app.post("/review/decision")
def review_decision(data: ReviewDecisionRequest):
    decision = data.decision.strip().lower()
    if decision not in {"accepted", "rejected", "needs_revision"}:
        raise HTTPException(status_code=400, detail="تصمیم review نامعتبر است")

    reviewer_id = data.reviewer_id.strip() or "admin"
    note = (data.note or "").strip()

    with get_db() as conn:
        proof = conn.execute(
            "SELECT * FROM proofs WHERE id = ?",
            (data.proof_id,)
        ).fetchone()

        if proof is None:
            raise HTTPException(status_code=404, detail="اثبات پیدا نشد")

        task = conn.execute(
            "SELECT * FROM tasks WHERE id = ?",
            (proof["task_id"],)
        ).fetchone()

        if task is None:
            raise HTTPException(status_code=404, detail="قرارداد پیدا نشد")

        if decision == "accepted":
            result = apply_review_award(
                conn,
                proof=proof,
                task=task,
                reviewer_id=reviewer_id,
                note=note or "Proof accepted by reviewer.",
                xp_award=data.xp_award,
                event_type="review_accepted",
                accepted_status="accepted",
            )
            conn.commit()

            send_bot_message(
                proof["telegram_id"],
                (
                    "✅ اثبات پس از بررسی پذیرفته شد.\n\n"
                    f"عنوان قرارداد: {task['title']}\n"
                    f"امتیاز افزوده‌شده: {result.get('xp_awarded', 0)}\n"
                    f"یادداشت بررسی: {note or '—'}"
                )
            )

            return {
                "ok": True,
                "decision": decision,
                "result": result
            }

        reviewed_at = now_iso()
        new_task_status = "active"
        new_lifecycle_status = "active"
        integrity_status = "review_rejected_resubmission_allowed" if decision == "rejected" else "needs_revision_resubmission_allowed"

        conn.execute(
            """
            UPDATE proofs
            SET status = ?,
                review_status = ?,
                review_decision = ?,
                reviewer_id = ?,
                reviewer_note = ?,
                reviewed_at = ?,
                review_version = ?
            WHERE id = ?
            """,
            (
                decision,
                decision,
                decision,
                reviewer_id,
                note,
                reviewed_at,
                REVIEW_APPEAL_VERSION,
                data.proof_id,
            )
        )

        conn.execute(
            """
            UPDATE tasks
            SET status = ?,
                lifecycle_status = ?,
                integrity_status = ?,
                lifecycle_version = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                new_task_status,
                new_lifecycle_status,
                integrity_status,
                DAILY_LIFECYCLE_VERSION,
                reviewed_at,
                task["id"],
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
                proof["telegram_id"],
                f"review_{decision}",
                0,
                None,
                None,
                f"proof_id={proof['id']};task_id={task['id']};reviewer_id={reviewer_id}",
                reviewed_at,
            )
        )

        conn.commit()

    label = "رد شد" if decision == "rejected" else "نیازمند اصلاح شد"
    send_bot_message(
        proof["telegram_id"],
        (
            f"⚠️ نتیجه بررسی: اثبات {label}.\n\n"
            f"عنوان قرارداد: {task['title']}\n"
            f"یادداشت بررسی: {note or '—'}\n"
            "می‌توانی اثبات اصلاح‌شده ارسال کنی یا اگر رد شده، اعتراض ثبت کنی."
        )
    )

    return {
        "ok": True,
        "decision": decision,
        "message": f"review decision saved: {decision}"
    }


@app.post("/appeals")
def create_appeal(data: AppealCreateRequest):
    telegram_id = data.telegram_id.strip()
    appeal_text = data.appeal_text.strip()

    if not telegram_id:
        raise HTTPException(status_code=400, detail="شناسه تلگرام الزامی است")
    if not appeal_text:
        raise HTTPException(status_code=400, detail="متن اعتراض الزامی است")

    with get_db() as conn:
        proof = conn.execute(
            "SELECT * FROM proofs WHERE id = ? AND telegram_id = ?",
            (data.proof_id, telegram_id)
        ).fetchone()

        if proof is None:
            raise HTTPException(status_code=404, detail="اثبات پیدا نشد")

        if proof["review_status"] not in {"rejected", "needs_revision"}:
            raise HTTPException(status_code=400, detail="برای این اثبات امکان اعتراض فعال نیست")

        existing = conn.execute(
            """
            SELECT * FROM proof_appeals
            WHERE proof_id = ? AND status = 'open'
            ORDER BY id DESC
            LIMIT 1
            """,
            (data.proof_id,)
        ).fetchone()

        if existing is not None:
            return {
                "ok": False,
                "code": "appeal_already_open",
                "message": "برای این اثبات قبلاً یک اعتراض باز ثبت شده است.",
                "appeal": row_to_dict(existing)
            }

        created = now_iso()
        cursor = conn.execute(
            """
            INSERT INTO proof_appeals (
                telegram_id, proof_id, task_id, appeal_text, status, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (telegram_id, data.proof_id, proof["task_id"], appeal_text, "open", created)
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
                "appeal_created",
                0,
                None,
                None,
                f"proof_id={data.proof_id};appeal_id={cursor.lastrowid}",
                created,
            )
        )

        conn.commit()

        appeal = conn.execute(
            "SELECT * FROM proof_appeals WHERE id = ?",
            (cursor.lastrowid,)
        ).fetchone()

    send_bot_message(
        telegram_id,
        (
            "📨 اعتراض ثبت شد و در صف بررسی قرار گرفت.\n\n"
            f"شناسه اثبات: {data.proof_id}"
        )
    )

    return {
        "ok": True,
        "appeal": row_to_dict(appeal)
    }


@app.get("/appeals/queue")
def appeals_queue(limit: int = 20):
    limit = max(1, min(int(limit or 20), 100))

    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT
                a.*,
                p.proof_text,
                p.review_status,
                t.title AS contract_title
            FROM proof_appeals a
            JOIN proofs p ON p.id = a.proof_id
            JOIN tasks t ON t.id = a.task_id
            WHERE a.status = 'open'
            ORDER BY a.created_at ASC
            LIMIT ?
            """,
            (limit,)
        ).fetchall()

    return {
        "ok": True,
        "review_version": REVIEW_APPEAL_VERSION,
        "appeals": [row_to_dict(row) for row in rows]
    }


@app.post("/appeals/resolve")
def resolve_appeal(data: AppealResolveRequest):
    decision = data.decision.strip().lower()
    if decision not in {"accepted", "denied"}:
        raise HTTPException(status_code=400, detail="تصمیم اعتراض نامعتبر است")

    reviewer_id = data.reviewer_id.strip() or "admin"
    resolution_note = (data.resolution_note or "").strip()

    with get_db() as conn:
        appeal = conn.execute(
            "SELECT * FROM proof_appeals WHERE id = ?",
            (data.appeal_id,)
        ).fetchone()

        if appeal is None:
            raise HTTPException(status_code=404, detail="اعتراض پیدا نشد")

        if appeal["status"] != "open":
            return {
                "ok": False,
                "code": "appeal_already_resolved",
                "message": "این اعتراض قبلاً تعیین تکلیف شده است.",
                "appeal": row_to_dict(appeal)
            }

        proof = conn.execute(
            "SELECT * FROM proofs WHERE id = ?",
            (appeal["proof_id"],)
        ).fetchone()
        task = conn.execute(
            "SELECT * FROM tasks WHERE id = ?",
            (appeal["task_id"],)
        ).fetchone()

        if proof is None or task is None:
            raise HTTPException(status_code=404, detail="اثبات یا قرارداد مرتبط پیدا نشد")

        resolved_at = now_iso()

        if decision == "accepted":
            result = apply_review_award(
                conn,
                proof=proof,
                task=task,
                reviewer_id=reviewer_id,
                note=resolution_note or "Appeal accepted.",
                xp_award=data.xp_award,
                event_type="appeal_accepted",
                accepted_status="accepted_after_appeal",
            )
        else:
            result = {
                "awarded": False,
                "xp_awarded": 0,
            }

        conn.execute(
            """
            UPDATE proof_appeals
            SET status = ?,
                reviewer_id = ?,
                resolution_note = ?,
                resolved_at = ?
            WHERE id = ?
            """,
            (
                decision,
                reviewer_id,
                resolution_note,
                resolved_at,
                data.appeal_id,
            )
        )

        if decision == "denied":
            conn.execute(
                """
                INSERT INTO progression_events (
                    telegram_id, event_type, xp_delta, old_stage, new_stage, metadata, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    appeal["telegram_id"],
                    "appeal_denied",
                    0,
                    None,
                    None,
                    f"appeal_id={appeal['id']};proof_id={proof['id']}",
                    resolved_at,
                )
            )

        conn.commit()

    if decision == "accepted":
        send_bot_message(
            appeal["telegram_id"],
            (
                "✅ اعتراض پذیرفته شد.\n\n"
                f"عنوان قرارداد: {task['title']}\n"
                f"امتیاز افزوده‌شده: {result.get('xp_awarded', 0)}\n"
                f"یادداشت: {resolution_note or '—'}"
            )
        )
    else:
        send_bot_message(
            appeal["telegram_id"],
            (
                "⚠️ اعتراض رد شد.\n\n"
                f"عنوان قرارداد: {task['title']}\n"
                f"یادداشت: {resolution_note or '—'}"
            )
        )

    return {
        "ok": True,
        "decision": decision,
        "result": result
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
        appeals = [row_to_dict(r) for r in conn.execute("SELECT * FROM proof_appeals ORDER BY id DESC").fetchall()]

    return {
        "ok": True,
        "users": users,
        "tasks": tasks,
        "proofs": proofs,
        "appeals": appeals,
        "events": events
    }
