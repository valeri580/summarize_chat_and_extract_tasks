# ========================
# MAIN.PY (PRODUCTION READY + PROMPT SEED)
# ========================

import sqlite3
import threading
import time
import queue
import httpx
import random
import logging

from fastapi import Request, Header, HTTPException
from pydantic import BaseModel, Field, validator
from datetime import datetime

# ========================
# SCHEMAS (STRICT API CONTRACT)
# ========================

class MessageIn(BaseModel):
    bot_token: str = Field(..., min_length=10)

    telegram_chat_id: int
    telegram_message_id: int

    user_name: str = Field(default="", max_length=255)
    text: str = Field(..., min_length=1, max_length=5000)

    timestamp: datetime

    # ========================
    # NORMALIZATION
    # ========================
    @validator("text")
    def clean_text(cls, v):
        v = v.strip()
        if not v:
            raise ValueError("text cannot be empty")
        return v

    @validator("user_name")
    def clean_username(cls, v):
        return v.strip() if v else ""

class SummaryRequest(BaseModel):
    bot_token: str = Field(..., min_length=10)
    chat_id: int
    period: str = "day"

# ========================
# TRACE HELPER (PRODUCTION)
# ========================
def trace_log(step, trace_id, chat_id=None, status="ok", msg="", **kwargs):
    try:
        extra = " ".join([f"{k}={v}" for k, v in kwargs.items()])

        logging.info(
            f"[TRACE] trace={trace_id} step={step} chat={chat_id} "
            f"status={status} msg={msg} {extra}"
        )
    except Exception as e:
        logging.error(f"[TRACE_LOG_ERROR] {e}")

from fastapi import FastAPI, HTTPException

app = FastAPI()


def log_event(event, **kwargs):
    try:
        payload = " ".join([f"{k}={v}" for k, v in kwargs.items()])
        logging.info(f"[{event}] {payload}")
    except Exception as e:
        logging.error(f"[LOG_EVENT_ERROR] {e}")

# ========================
# HTTPS ENFORCEMENT (GLOBAL MIDDLEWARE)
# ========================
@app.middleware("http")
async def enforce_https(request: Request, call_next):
    # локалка (разрешаем)
    host = request.headers.get("host", "")
    if "localhost" in host or "127.0.0.1" in host:
        return await call_next(request)

    # стандартный proxy header (nginx / cloudflare / render)
    forwarded_proto = request.headers.get("x-forwarded-proto")

    # fallback — если прямой HTTPS
    scheme = request.url.scheme

    if forwarded_proto:
        if forwarded_proto != "https":
            raise HTTPException(status_code=403, detail="HTTPS required")
    else:
        if scheme != "https":
            raise HTTPException(status_code=403, detail="HTTPS required")

    return await call_next(request)

# ========================
# GLOBAL API RATE LIMIT
# ========================
API_RATE_LIMIT = {}
API_RATE_LIMIT_LOCK = threading.Lock()

API_WINDOW = 10
API_MAX = 100

GLOBAL_RATE_LIMIT = []

from datetime import datetime, timedelta

DB = "app.db"

# ========================
# DEFAULT FALLBACK SUMMARY
# ========================
DEFAULT_SUMMARY = (
    "СИТУАЦИЯ: недостаточно данных\n"
    "ПРОБЛЕМЫ: не выявлено\n"
    "РИСКИ: не выявлено\n"
    "ЗАДАЧИ: не выявлено"
)

SMAIPL_URL = "https://api.smaipl.ai/run"

task_queue = queue.PriorityQueue()

# ========================
# RATE LIMIT (BACKEND)
# ========================
RATE_LIMIT_USER = {}
RATE_LIMIT_GLOBAL = []

# ========================
# RATE LIMIT LOCK (THREAD SAFE)
# ========================
rate_limit_lock = threading.Lock()

USER_LIMIT_WINDOW = 30   # сек
USER_LIMIT_MAX = 3       # max summary / окно

GLOBAL_LIMIT_WINDOW = 10
GLOBAL_LIMIT_MAX = 20

logging.basicConfig(level=logging.INFO)

# ========================
# TASK PRIORITY CONFIG
# ========================
TASK_TYPES = {
    "message": {"priority": 1},
    "summary": {"priority": 5},
    "summary_retry": {"priority": 10},  # самый высокий
}

def create_task(task_type, payload, priority=None):
    import time

    base_priority = TASK_TYPES.get(task_type, {}).get("priority", 5)

    return (
        priority or base_priority,
        time.time(),
        {
            "type": task_type,
            "payload": payload,
            "retry": 0,
            "max_retries": 3,
            "created_at": time.time(),
	    "run_at": time.time() 
        }
    )

def should_retry(task):
    return task["retry"] < task["max_retries"]


def get_backoff_delay(retry):
    return min(2 ** retry, 30)


def requeue_task(task):
    import time

    task["retry"] += 1

    delay = get_backoff_delay(task["retry"])

    task["run_at"] = time.time() + delay

    log_event(
    	"RETRY",
    	task_type=task["type"],
    	retry=task["retry"],
    	delay=delay
    )

    new_priority = TASK_TYPES[task["type"]]["priority"] + task["retry"]

    task_queue.put((
        new_priority,
        time.time(),
        task
    ))

dead_letter_queue = []


def send_to_dlq(task, error):
    log_event(
    	"DLQ",
    	task=task["type"],
    	error=error
    )

    dead_letter_queue.append({
        "task": task,
        "error": str(error),
        "failed_at": time.time()
    })

# ========================
# DB
# ========================
def conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA foreign_keys = ON;")
    return c

def init_db():
    c = conn()
    cur = c.cursor()

    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA busy_timeout=5000;")

    c.commit()
    c.close()

# ========================
# MIGRATION HELPER (GLOBAL)
# ========================
def ensure_column_exists(cur, table, column, ddl):
    cur.execute(f"PRAGMA table_info({table})")
    columns = [row[1] for row in cur.fetchall()]

    if column not in columns:
        logging.warning(f"[MIGRATION] Adding column {column} to {table}")
        cur.execute(ddl)


def ensure_schema():
    init_db() 
    c = conn()
    cur = c.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS usage_logs (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        function_name TEXT,
        tokens_used FLOAT,
        prompt_version TEXT,
	trace_id TEXT,
        created_at TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name TEXT,
        telegram_user_id TEXT,
        created_at TIMESTAMP
    )
    """)
     
    cur.execute("""
    CREATE TABLE IF NOT EXISTS prompts (
        id INTEGER PRIMARY KEY,
        type TEXT,
        name TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS prompt_versions (
        id INTEGER PRIMARY KEY,
        prompt_id INTEGER,
        domain TEXT,
        version TEXT,
        system_prompt TEXT,
        user_template TEXT,
        is_active BOOLEAN,
        created_at TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS prompt_tests (
        id INTEGER PRIMARY KEY,
        prompt_id INTEGER,
        domain TEXT,
        version_a TEXT,
        version_b TEXT,
        traffic_split FLOAT,
        is_active BOOLEAN
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS chats (
        id INTEGER PRIMARY KEY,
        project_id INTEGER,
        bot_id INTEGER,
        telegram_chat_id TEXT,
        chat_type TEXT,
        title TEXT,
        created_at TIMESTAMP,
	FOREIGN KEY (project_id) REFERENCES projects(id),
	FOREIGN KEY (bot_id) REFERENCES bots(id)
    )
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS bots (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        bot_token TEXT UNIQUE,
        bot_name TEXT,
        webhook_url TEXT,
	webhook_secret TEXT,
        is_active BOOLEAN DEFAULT 1,
        created_at TIMESTAMP,
	FOREIGN KEY (user_id) REFERENCES users(id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS projects (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        name TEXT,
        domain TEXT,
        created_at TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY,
        chat_id INTEGER,
        telegram_message_id TEXT,
        user_name TEXT,
        text TEXT,
        timestamp TIMESTAMP,
	FOREIGN KEY (chat_id) REFERENCES chats(id)
    )
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_unique
        ON messages(chat_id, telegram_message_id)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS summaries (
        id INTEGER PRIMARY KEY,
        chat_id INTEGER,
        type TEXT,
	period_start TIMESTAMP,
	period_end TIMESTAMP,
        text TEXT,
        created_at TIMESTAMP,
	status TEXT DEFAULT 'done',
	FOREIGN KEY (chat_id) REFERENCES chats(id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS project_memory (
        id INTEGER PRIMARY KEY,
        project_id INTEGER,
        summary TEXT,
        updated_at TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS chat_settings (
        id INTEGER PRIMARY KEY,
        chat_id INTEGER,
        summary_enabled BOOLEAN,
        summary_type TEXT,
        summary_time TEXT,
        summary_day TEXT,
        period_type TEXT,
	period_start TIMESTAMP,
    	period_end TIMESTAMP,
        timezone TEXT,
	FOREIGN KEY (chat_id) REFERENCES chats(id)
    )
    """)

    # индексы
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_chat_time ON messages(chat_id, timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_summaries_chat ON summaries(chat_id, created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bots_token ON bots(bot_token)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_chats_type ON chats(chat_type)")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_chats_unique ON chats(bot_id, telegram_chat_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp)")

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_projects_user_domain
        ON projects(user_id, domain)
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_summaries_period_unique
        ON summaries(chat_id, type, period_start, period_end)
    """)

    # ========================
    # MIGRATIONS
    # ========================
    ensure_column_exists(
    	cur,
    	"chats",
    	"last_run_at",
    	"ALTER TABLE chats ADD COLUMN last_run_at TIMESTAMP"
    )

    ensure_column_exists(
        cur,
        "summaries",
        "period_start",
        "ALTER TABLE summaries ADD COLUMN period_start TIMESTAMP"
    )

    ensure_column_exists(
        cur,
        "summaries",
        "period_end",
        "ALTER TABLE summaries ADD COLUMN period_end TIMESTAMP"
    )

    ensure_column_exists(
    	cur,
    	"summaries",
    	"retry_count",
    	"ALTER TABLE summaries ADD COLUMN retry_count INTEGER DEFAULT 0"
    )

    ensure_column_exists(
    	cur,
    	"bots",
    	"bot_name",
    	"ALTER TABLE bots ADD COLUMN bot_name TEXT"
    )

    ensure_column_exists(
    	cur,
    	"bots",
    	"webhook_url",
    	"ALTER TABLE bots ADD COLUMN webhook_url TEXT"
    )

    ensure_column_exists(
    	cur,
    	"chat_settings",
    	"period_start",
    	"ALTER TABLE chat_settings ADD COLUMN period_start TIMESTAMP"
    )

    ensure_column_exists(
    	cur,
    	"chat_settings",
    	"period_end",
    	"ALTER TABLE chat_settings ADD COLUMN period_end TIMESTAMP"
    )

    ensure_column_exists(
        cur,
        "bots",
        "webhook_secret",
        "ALTER TABLE bots ADD COLUMN webhook_secret TEXT"
    )

    c.commit()
    c.close()


# ========================
#  PROMPT SEED ENGINE
# ========================
def ensure_prompts_for_domain(cur, domain, smaipl_call, force=False, debug=False):
    """
    smaipl_call(function_name, arguments) -> SMAIPL response
    """

    import logging
    from datetime import datetime
    import time

    if not domain:
        domain = "default"

    # ========================
    # CHECK EXISTS
    # ========================
    cur.execute("""
        SELECT COUNT(*)
        FROM prompt_versions
        WHERE domain=?
    """, (domain,))
    count = cur.fetchone()[0]

    if count > 0 and not force:
        logging.info(f"[PROMPT_EXISTS] domain={domain}")
        return

    logging.warning(f"[PROMPT_GENERATE] domain={domain}")

    # ========================
    # CONFIG
    # ========================
    PROMPT_TYPES = ["summarize", "adapt"]
    MAX_RETRIES = 3

    # ========================
    # HELPERS
    # ========================
    def validate(prompt):
        if not prompt:
            return False
        if len(prompt.get("system_prompt", "")) < 50:
            return False
        if len(prompt.get("user_template", "")) < 20:
            return False
        return True

    def fallback(prompt_type):
        if prompt_type == "summarize":
            return {
                "system_prompt": f"Ты аналитик в сфере {domain}. Анализируй чат.",
                "user_template": "Проанализируй сообщения:\n{messages}"
            }
        else:
            return {
                "system_prompt": f"Ты менеджер в сфере {domain}. Пиши клиенту.",
                "user_template": "Перепиши summary:\n{summary}"
            }

    # ========================
    # GENERATE LOOP
    # ========================
    for prompt_type in PROMPT_TYPES:

        prompt_data = None

        for attempt in range(MAX_RETRIES):
            try:
                res = smaipl_call(
                    "func_generate_prompt_for_domain",
                    {
                        "domain": domain,
                        "prompt_type": prompt_type
                    }
                )

                if debug:
                    logging.info(f"[PROMPT_RAW] {res}")

                if res.get("status") != "success":
                    raise Exception("SMAIPL error")

                prompt_data = res["result"]["data"]

                if validate(prompt_data):
                    break

                raise Exception("Validation failed")

            except Exception as e:
                logging.warning(f"[PROMPT_RETRY] {prompt_type} attempt={attempt+1} err={e}")
                time.sleep(1.5 * (attempt + 1))

        # ========================
        # FALLBACK
        # ========================
        if not prompt_data or not validate(prompt_data):
            logging.error(f"[PROMPT_FALLBACK] {prompt_type}")
            prompt_data = fallback(prompt_type)

        # ========================
	# VERSION AUTO-INCREMENT
	# ========================
	cur.execute("""
    	    SELECT version
    	    FROM prompt_versions pv
    	    JOIN prompts p ON pv.prompt_id = p.id
    	    WHERE pv.domain=? AND p.type=?
    	    ORDER BY created_at DESC
    	    LIMIT 1
	""", (domain, prompt_type))

	row = cur.fetchone()

	if row:
    	    last_version = row[0]
    	    try:
        	v_num = int(last_version.split("_v")[-1]) + 1
    	    except:
        	v_num = 1
	else:
    	    v_num = 1

	version = f"{prompt_type}_{domain}_v{v_num}"

        # ========================
        # GET prompt_id
        # ========================
        cur.execute("""
            SELECT id FROM prompts WHERE type=?
        """, (prompt_type,))
        row = cur.fetchone()

        if not row:
            raise Exception(f"Prompt type not found: {prompt_type}")

        prompt_id = row[0]

        # ========================
        # INSERT
        # ========================
        cur.execute("""
            INSERT INTO prompt_versions
            (prompt_id, domain, version, system_prompt, user_template, is_active, created_at)
            VALUES (?, ?, ?, ?, ?, 1, ?)
        """, (
            prompt_id,
            domain,
            version,
            prompt_data["system_prompt"],
            prompt_data["user_template"],
            datetime.utcnow().isoformat()
        ))

        logging.info(f"[PROMPT_CREATED] {version}")

# ========================
# MULTI-TENANT
# ========================
def get_bot_or_fail(cur, bot_token):
    cur.execute("""
        SELECT id, user_id, is_active
        FROM bots
        WHERE bot_token=?
    """, (bot_token,))
    row = cur.fetchone()

    if not row:
        raise Exception("Bot not found")

    bot_id, user_id, is_active = row

    if not is_active:
        raise Exception("Bot inactive")

    return bot_id, user_id

def validate_bot_token(cur, bot_token):
    """
    Проверка bot_token без выброса Exception (для API слоя)
    """

    if not bot_token:
        return None

    cur.execute("""
        SELECT id, user_id, is_active
        FROM bots
        WHERE bot_token=?
    """, (bot_token,))

    row = cur.fetchone()

    if not row:
        return None

    bot_id, user_id, is_active = row

    if not is_active:
        return None

    return {
        "bot_id": bot_id,
        "user_id": user_id
    }

# ========================
# AUTH HELPERS (PRODUCTION)
# ========================
def extract_token(auth_header: str):
    if not auth_header:
        return None

    if not auth_header.startswith("Bearer "):
        return None

    return auth_header.split(" ", 1)[1].strip()


def get_user_from_auth(cur, authorization):
    token = extract_token(authorization)

    if not token:
        raise HTTPException(status_code=401, detail="Missing token")

    bot = validate_bot_token(cur, token)

    if not bot:
        raise HTTPException(status_code=403, detail="Invalid bot")

    return bot


def get_user_from_payload(cur, payload):
    bot_token = payload.get("bot_token")

    if not bot_token:
        raise HTTPException(status_code=401, detail="Missing bot_token")

    bot = validate_bot_token(cur, bot_token)

    if not bot:
        raise HTTPException(status_code=403, detail="Invalid bot")

    return bot


def set_webhook(bot_token, webhook_url):
    import requests

    url = f"https://api.telegram.org/bot{bot_token}/setWebhook"

    r = requests.post(url, json={"url": webhook_url}, timeout=10)

    return r.json()

# ========================
# PROMPT ENGINE
# ========================
def get_prompt(cur, prompt_type, domain):

    cur.execute("""
        SELECT pt.version_a, pt.version_b, pt.traffic_split
        FROM prompt_tests pt
        JOIN prompts p ON pt.prompt_id = p.id
        WHERE pt.domain=? AND pt.is_active=1 AND p.type=?
    """, (domain, prompt_type))

    test = cur.fetchone()

    if test:
        version = test[0] if random.random() < test[2] else test[1]
    else:
        cur.execute("""
            SELECT pv.version
            FROM prompt_versions pv
            JOIN prompts p ON pv.prompt_id = p.id
            WHERE pv.domain=? AND pv.is_active=1 AND p.type=?
            LIMIT 1
        """, (domain, prompt_type))

        row = cur.fetchone()

        if row:
            version = row[0]
        else:
            cur.execute("""
                SELECT pv.version
                FROM prompt_versions pv
                JOIN prompts p ON pv.prompt_id = p.id
                WHERE pv.domain='default' AND pv.is_active=1 AND p.type=?
                LIMIT 1
            """, (prompt_type,))
            version = cur.fetchone()[0]

    cur.execute("""
        SELECT system_prompt, user_template, version
        FROM prompt_versions
        WHERE version=?
    """, (version,))

    return cur.fetchone()


# ========================
# USAGE LOG
# ========================
def log_usage(cur, user_id, fn, tokens, version, trace_id=None):
    try:
        cur.execute("""
            INSERT INTO usage_logs
	    (user_id, function_name, tokens_used, prompt_version, trace_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            user_id,
            fn,
            float(tokens or 0),
            version,
 	    trace_id,
            datetime.utcnow().isoformat()
        ))
    except Exception as e:
        logging.error(f"[USAGE_LOG_ERROR] err={e}")

# ========================
# RECOVERY: FAILED SUMMARIES
# ========================
def get_failed_summaries(cur, limit=10):
    """
    Ищем fallback summary, которые можно пересобрать
    """

    cur.execute("""
        SELECT DISTINCT chat_id, period_start, period_end, retry_count
        FROM summaries
        WHERE status='fallback'

	-- LIMIT по времени
        AND created_at > datetime('now', '-7 days')

        -- BACKOFF (не сразу)
        AND created_at < datetime('now', '-10 minutes')

        -- RETRY LIMIT
        AND retry_count < 3


        ORDER BY created_at DESC
        LIMIT ?
    """, (limit,))

    return cur.fetchall()

def summary_exists(cur, chat_id, period_start, period_end):
    cur.execute("""
        SELECT 1 FROM summaries
        WHERE chat_id=?
        AND type='manager'
        AND period_start=?
        AND period_end=?
        LIMIT 1
    """, (chat_id, period_start, period_end))

    return cur.fetchone() is not None

def save_fallback_summary(cur, c, chat_id, period_start, period_end, text, trace_id):
    """
    Idempotent fallback:
    - НЕ создаёт новую запись
    - Обновляет только уже CLAIM-нутую summary
    """

    cur.execute("""
        UPDATE summaries
        SET text=?, status='fallback'
        WHERE chat_id=?
        AND type='manager'
        AND period_start=?
        AND period_end=?
        AND status='processing'
    """, (
        text,
        chat_id,
        period_start,
        period_end
    ))

    # ========================
    # NOTHING UPDATED → DIAGNOSTIC
    # ========================
    if cur.rowcount == 0:
        logging.warning(
            f"[FALLBACK_SKIP_NO_ROW] chat={chat_id} "
            f"period=({period_start},{period_end})"
        )
        return

    c.commit()

    trace_log(
        "FALLBACK_SAVED",
        trace_id,
        chat_id,
        msg="fallback summary saved"
    )


def cleanup_stuck_processing(cur):
    cur.execute("""
        UPDATE summaries
        SET status='failed'
        WHERE status='processing'
        AND created_at < datetime('now', '-10 minutes')
    """)


def retry_failed_summaries():
    c = conn()
    cur = c.cursor()

    rows = get_failed_summaries(cur)

    for chat_id, period_start, period_end, retry_count in rows: 
	
	# ========================
	# SKIP IF ALREADY FIXED (CRITICAL)
	# ========================
	if summary_exists(cur, chat_id, period_start, period_end):
    	    logging.info(f"[RETRY_SKIP_ALREADY_DONE] chat={chat_id}")
    	    continue

        # ========================
	# CLAIM ROW (ANTI-DUPLICATE RETRY)
	# ========================
	cur.execute("""
    	    UPDATE summaries
    	    SET retry_count = COALESCE(retry_count, 0) + 1
    	    WHERE chat_id=? 
    	    AND period_start=? 
    	    AND period_end=? 
    	    AND status='fallback'
    	    AND retry_count = ?
	""", (chat_id, period_start, period_end, retry_count))

	if cur.rowcount == 0:
    	    # другой поток уже взял задачу
    	    logging.warning(f"[RETRY_SKIP_RACE] chat={chat_id}")
    	    continue
 
	c.commit()

        # получаем bot_token + domain
        cur.execute("""
            SELECT b.bot_token, p.domain
            FROM chats c
            JOIN bots b ON c.bot_id = b.id
            JOIN projects p ON c.project_id = p.id
            WHERE c.id=?
        """, (chat_id,))

        row = cur.fetchone()
        if not row:
            continue

        bot_token, domain = row

        logging.info(
            f"[RETRY_SUMMARY] chat={chat_id} retry={retry_count}"
        )

        task_queue.put(create_task(
    	    "summary_retry",
    	    {
        	"chat_id": chat_id,
        	"bot_token": bot_token,
        	"domain": domain,
        	"period_start": period_start,
        	"period_end": period_end
    	    },
    	    priority=TASK_TYPES["summary_retry"]["priority"]
	))

    c.commit()
    c.close()
# ========================
# SMAIPL CALL
# ========================
def call_smaipl(function, arguments):
    r = httpx.post(SMAIPL_URL, json={
        "function": function,
        "arguments": arguments
    }, timeout=30)
    return r.json()

def call_smaipl_with_retry(function, arguments, trace_id=None, chat_id=None, retries=3):
    for attempt in range(retries):
        try:
            res = call_smaipl(function, arguments)

	    try:
    		res = ensure_contract(res)
	    except Exception as e:
    		log_event(
    		    "SMAIPL_CONTRACT_BROKEN",
    		    error=str(e)
		)
    		continue

            if res.get("status") == "success":
                return res

            logging.warning(f"[SMAIPL_BAD_STATUS] attempt={attempt+1} res={res}")

        except Exception as e:
            log_event(
    		"SMAIPL_ERROR",
    		trace=trace_id,
    		chat=chat_id,
    		function=function,
    		attempt=attempt + 1
	    )

        time.sleep(1.5 * (attempt + 1))

    return {
        "status": "error",
        "error": "SMAIPL failed after retries"
    }

def ensure_contract(res):
    if not isinstance(res, dict):
        raise Exception("SMAIPL response must be dict")

    if "result" not in res:
        raise Exception("Invalid SMAIPL response: no result")

    if "success" not in res["result"]:
        raise Exception("Invalid SMAIPL response: missing success")

    if "status" not in res:
        raise Exception("Invalid SMAIPL response: missing status")

    return res

# ========================
# LLM VALIDATION
# ========================
def is_valid_summary(text: str) -> bool:
    if not text:
        return False

    text = text.strip()

    if len(text) < 50:
        return False

    bad_patterns = [
        "ok",
        "готово",
        "done",
        "нет данных",
        "n/a"
    ]

    low = text.lower()

    if any(p in low for p in bad_patterns):
        return False

    return True

def validate_summary_against_messages(summary, messages):
    if not summary:
        return False
 
    text = summary.lower()

    hallucination_patterns = [
        "возможно",
        "скорее всего",
        "вероятно",
        "предположительно"
    ]

    if any(p in text for p in hallucination_patterns):
        return False

    message_text = " ".join(messages).lower()

    words = set(text.split())
    msg_words = set(message_text.split())

    unknown_words = words - msg_words

    if len(words) > 0 and len(unknown_words) > len(words) * 0.4:
        return False

    return True

def llm_validate_summary(summary, messages, chat, model="gpt-4o-mini"):
    try:
        prompt = f"""
Проверь summary на достоверность.

SUMMARY:
{summary}

MESSAGES:
{messages[:50]}

Задача:
- есть ли факты, которых нет в сообщениях?
- есть ли искажения?

Ответ строго:
OK или FAIL
"""

        res = chat(
            model=model,
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )

        text = res['choices'][0]['message']['content']

        return "OK" in text

    except Exception:
        return False

# ========================
# CHUNKING (PRODUCTION)
# ========================
def chunk_by_tokens(messages, max_chars=6000):
    chunks = []
    current = []
    size = 0

    for m in messages:
        size += len(m)

        if size > max_chars:
            chunks.append(current)
            current = [m]
            size = len(m)
        else:
            current.append(m)

    if current:
        chunks.append(current)

    return chunks


def add_overlap(chunks, overlap=3):
    result = []

    for i, chunk in enumerate(chunks):
        if i == 0:
            result.append(chunk)
        else:
            prev = chunks[i-1][-overlap:]
            result.append(prev + chunk)

    return result

# ========================
# STRUCTURE VALIDATION
# ========================
def validate_summary_structure(text: str):
    required = ["СИТУАЦИЯ", "ПРОБЛЕМЫ", "РИСКИ", "ЗАДАЧИ"]

    return all(r in text for r in required)


# ========================
# UPDATE PROJECT MEMORY
# ========================

def update_project_memory(cur, project_id, old_memory, summary, domain, trace_id):
    res = call_smaipl_with_retry(
        "func_update_project_memory",
        {
            "old_memory": old_memory,
            "new_summary": summary,
            "domain": domain
        },
        trace_id=trace_id
    )

    if res.get("status") != "success":
        return None

    return res["result"].get("data")

# ========================
# PIPELINE
# ========================
def process_summary(task):
    chat_id = task["chat_id"]
    bot_token = task["bot_token"]
    domain = task.get("domain", "default")
    
    import uuid
    trace_id = task.get("trace_id") or str(uuid.uuid4())[:8]

    trace_log(
    	"START",
    	trace_id,
    	chat_id,
    	msg="summary started"
    )
	
    log_event(
    	"SUMMARY_START",
    	chat_id=chat_id,
    	domain=domain,
    	trace=trace_id
    )

    c = conn()
    cur = c.cursor()

    try:
        # ========================
        # BOT VALIDATION
        # ========================
        bot_id, user_id = get_bot_or_fail(cur, bot_token)

        # ========================
        # GET CHAT TYPE (CRITICAL)
        # ========================
        cur.execute("SELECT chat_type FROM chats WHERE id=?", (chat_id,))
        row = cur.fetchone()
        raw_chat_type = row[0] if row else None

	if raw_chat_type not in VALID_CHAT_TYPES:
    	    logging.warning(
        	f"[CHAT_TYPE_INVALID] chat_id={chat_id} value={raw_chat_type} → fallback=team"
    	    )
    	    chat_type = "team"
	else:
    	    chat_type = raw_chat_type

        # ========================
	# LOAD MESSAGES + PERIOD (UPGRADED)
	# ========================
	from datetime import datetime, timedelta

	period_start = task.get("period_start")
	period_end = task.get("period_end")

	period = task.get("period", "day")

	from datetime import datetime, timedelta

	now_dt = datetime.utcnow()

	if period_start and period_end:
    	    period_start_dt = datetime.fromisoformat(period_start)
    	    period_end_dt = datetime.fromisoformat(period_end)

	elif period == "day":
    	    period_start_dt = now_dt - timedelta(days=1)
    	    period_end_dt = now_dt

	elif period == "week":
    	    period_start_dt = now_dt - timedelta(days=7)
    	    period_end_dt = now_dt

	else:
    	    period_start_dt = None
    	    period_end_dt = None

	period_start = period_start_dt.isoformat() if period_start_dt else None
	period_end = period_end_dt.isoformat() if period_end_dt else None

	# ========================
	# IDEMPOTENCY CLAIM (CRITICAL)
	# ========================
	try:
    	    cur.execute("""
        	INSERT INTO summaries
        	(chat_id, type, period_start, period_end, text, created_at, status)
        	VALUES (?, ?, ?, ?, ?, ?, 'processing')
    	    """, (
        	chat_id,
        	"manager",
        	period_start,
        	period_end,
        	"",
        	datetime.utcnow().isoformat()
    	    ))

    	    c.commit()

    	    logging.info(f"[EVENT] key=value key2=value2")

	    logging.info(
    		f"[EXACTLY_ONCE_CLAIMED] chat={chat_id} period=({period_start},{period_end})"
	    )

	except sqlite3.IntegrityError:
    	    logging.info(f"[CLAIM_SKIP_ALREADY_EXISTS] chat={chat_id}")
    	    return

	# ========================
	# LOAD MESSAGES (единый SQL)
	# ========================
	if period_start and period_end:
    	    cur.execute("""
        	SELECT text FROM messages
        	WHERE chat_id=?
        	AND timestamp >= ?
        	AND timestamp <= ?
        	ORDER BY timestamp ASC
    	""", (chat_id, period_start, period_end))
	else:
    	    cur.execute("""
        	SELECT text FROM messages
        	WHERE chat_id=?
        	ORDER BY timestamp ASC
    	    """, (chat_id,))

	rows = cur.fetchall()
	raw_messages = [r[0] for r in rows]

	trace_log(
    	    "LOAD_MESSAGES",
    	    trace_id,
    	    chat_id,
    	    msg="messages loaded",
    	    count=len(raw_messages)
	)

	# ========================
	# CLEAN MESSAGES (LLM HYGIENE)
	# ========================
	def clean_messages(messages):
    	    cleaned = []

    	    bad = {
        	"ок", "ok", "+", "ага", "ясно",
        	"сделано", "готово", "done",
       		"👍", "👌", "окей"
    	    }

    	    for m in messages:
        	if not m:
            	    continue

        	text = str(m).strip()

        	if len(text) < 3:
                    continue

        	if text.startswith("/"):
            	    continue

        	if "http" in text:
            	    continue

        	low = text.lower()

        	if low in bad:
            	    continue

        	if cleaned and cleaned[-1] == text:
            	    continue

        	if len(text) > 1000:
                    text = text[:1000]

        	cleaned.append(text)

    	    return cleaned


	messages = clean_messages(raw_messages)

	trace_log(
    	    "CLEAN",
    	    trace_id,
    	    chat_id,
    	    msg="messages cleaned",
    	    before=len(raw_messages),
    	    after=len(messages)
	)

	# ========================
	# LOAD PROJECT MEMORY
	# ========================
	cur.execute("SELECT project_id FROM chats WHERE id=?", (chat_id,))
	row = cur.fetchone()
	project_id = row[0] if row else None

	project_memory = ""

	if project_id:
    	    cur.execute("""
        	SELECT summary FROM project_memory
        	WHERE project_id=?
        	LIMIT 1
    	    """, (project_id,))
    
    	    row = cur.fetchone()
    	    project_memory = row[0] if row else ""

	    # LIMIT MEMORY SIZE
	    MAX_MEMORY_CHARS = 2000

	    if len(project_memory) > MAX_MEMORY_CHARS:
    		project_memory = project_memory[-MAX_MEMORY_CHARS:]
	    
	  
	log_event(
    	    "MEMORY_LOADED",
    	    project_id=project_id,
    	    size=len(project_memory)
	)

	log_event(
    	    "MESSAGES_CLEANED",
    	    before=len(raw_messages),
    	    after=len(messages)
	)

	# ========================
	# LIMIT
	# ========================
	MAX_MESSAGES = 300

	if len(messages) > MAX_MESSAGES:
    	    log_event(
    		"MESSAGES_TRIMMED",
    		before=len(messages),
    		limit=MAX_MESSAGES
	    )
            messages = messages[-MAX_MESSAGES:]

	trace_log(
    	    "LIMIT",
    	    trace_id,
    	    chat_id,
    	    msg="messages trimmed",
    	    limit=MAX_MESSAGES,
    	    final=len(messages)
	)

        # ========================
	# SUMMARIZE (PRODUCTION: CHUNK + STRUCTURE)
	# ========================
	
	ensure_prompts_for_domain(
    	    cur,
    	    domain,
    	    call_smaipl,
    	    debug=False
	)

	system_prompt, user_template, version = get_prompt(cur, "summarize", domain)

	fallback_prompt = get_prompt(cur, "summarize", "default")

	if fallback_prompt:
    	    fb_system_prompt, fb_user_template, fb_version = fallback_prompt
	else:
     	    fb_system_prompt, fb_user_template, fb_version = system_prompt, user_template, version

	# ------------------------
	# CHUNKING
	# ------------------------
	chunks = chunk_by_tokens(messages)
	chunks = add_overlap(chunks)
	
	trace_log(
    	    "CHUNKING",
    	    trace_id,
    	    chat_id,
    	    msg="chunks created",
    	    count=len(chunks)
	)

	partial_summaries = []

	# ------------------------
	# STEP 1: partial summaries
	# ------------------------
	for chunk in chunks:
    	    
	    trace_log(
    		"LLM_CHUNK",
    		trace_id,
    		chat_id,
    		msg="processing chunk",
    		chunk_size=len(chunk)
	    )

	    # ========================
    	    # MEMORY AUGMENTATION
    	    # ========================
    	    context_messages = "\n".join(chunk)

	    context_prefix = ""

	    if project_memory:
    		context_prefix = f"""
	    КОНТЕКСТ ПРОЕКТА (НАКОПЛЕННАЯ ПАМЯТЬ):
	    {project_memory}

	    ---
	    """
	    
	    augmented_chunk = [context_prefix + context_messages]

    	    res = call_smaipl_with_retry(
        	"func_summarize_chat_and_extract_tasks",
        	{
            	    "messages": augmented_chunk,
		    "mode": "messages",
            	    "domain": domain,
            	    "system_prompt": system_prompt,
            	    "user_template": user_template
        	},
		trace_id=trace_id,
    		chat_id=chat_id
    	    )

    	    if (
    		res.get("status") != "success"
    		or not res.get("result", {}).get("success")
	    ):
        	logging.warning(f"[CHUNK_SUMMARY_FAILED] trace={trace_id} chat={chat_id}")
		continue

    	    text = res["result"].get("data")

	    if not is_valid_summary(text):
    		trace_log(
        	    "CHUNK_INVALID",
        	    trace_id,
        	    chat_id,
        	    status="warn",
        	    msg="invalid summary from chunk"
    		)
    		continue

    	    if is_valid_summary(text):
        	partial_summaries.append(text)

		trace_log(
    		    "CHUNK_OK",
    		    trace_id,
    		    chat_id,
    		    msg="partial summary created",
    		    total=len(partial_summaries)
		)

	trace_log(
    	    "CHUNKS_DONE",
    	    trace_id,
    	    chat_id,
    	    msg="all chunks processed",
    	    count=len(partial_summaries)
	)

	# ------------------------
	# STEP 2: FINAL SUMMARY (with retry + fallback)
	# ------------------------
	MAX_LLM_RETRIES = 2
	summary = None

	for attempt in range(MAX_LLM_RETRIES):

    	    if attempt == 0:
        	sp, ut = system_prompt, user_template
        	used_version = version
    	    else:
        	sp, ut = fb_system_prompt, fb_user_template
        	used_version = fb_version

        	log_event(
    		    "PROMPT_FALLBACK",
    	            chat_id=chat_id,
    		    version=used_version
		)
        	logging.warning(f"[FALLBACK_VERSION] {used_version}")

    	    context = "\n\n".join(partial_summaries)

	    # ========================
	    # MEMORY IN FINAL STEP
 	    # ========================
	    if project_memory:
    	 	context = f"""
	    КОНТЕКСТ ПРОЕКТА (ИСТОРИЯ):
	    {project_memory}

	    ---

	    ТЕКУЩИЕ SUMMARY:
	    {context}
	    """

    	    res = call_smaipl_with_retry(
        	"func_summarize_chat_and_extract_tasks",
        	{
            	    "messages": partial_summaries,
		    "mode": "summaries",
            	    "domain": domain,
            	    "system_prompt": sp,
            	    "user_template": ut + """

	СТРОГАЯ СТРУКТУРА:
	СИТУАЦИЯ:
	ПРОБЛЕМЫ:
	РИСКИ:
	ЗАДАЧИ:

	Если нет данных — пиши "не выявлено"
	"""
        	}
    	    )

    	    if (
    		res.get("status") != "success"
    		or not res.get("result", {}).get("success")
	    ):
		trace_log(
    		    "FINAL_FAIL",
    		    trace_id,
    		    chat_id,
    		    status="warn",
    		    msg="llm failed for chunk"
		)
		log_event(
    		    "SUMMARY_LLM_FAIL",
    		    trace=trace_id,
    		    chat=chat_id
		)	
        	continue

    	    candidate = res["result"].get("data")

	    if (
    		is_valid_summary(candidate)
    		and validate_summary_structure(candidate)
    		and validate_summary_against_messages(candidate, messages)
	    ):
    		# ========================
    		# LLM VALIDATOR (CRITICAL)
    		# ========================
    		VALIDATOR_RETRIES = 2
		validator_ok = False
		validator_api_failed = False

		for v_attempt in range(VALIDATOR_RETRIES):

    		    res_val = call_smaipl_with_retry(
        		"func_validate_summary",
        		{
            		    "summary": candidate,
            		    "messages": messages[:50]
        		},
        		trace_id=trace_id,
        		chat_id=chat_id
    		    )

		    # ========================
    		    # API ERROR
    		    # ========================

    		    if (
    			res_val.get("status") != "success"
    			or not res_val.get("result", {}).get("success")
		    ):
			validator_api_failed = True

        		trace_log(
            		    "VALIDATION_API_FAIL",
            		    trace_id,
            		    chat_id,
            		    status="warn",
            		    msg=f"validator API failed attempt={v_attempt+1}"
        		)
        		continue

		    # ========================
    		    # VALIDATION RESULT
    		    # ========================

    		    if res_val["result"].get("data") == "OK":
        		validator_ok = True
        		break
    		    else:
        		trace_log(
            		    "VALIDATION_REJECT",
            		    trace_id,
            		    chat_id,
            		    status="warn",
            		    msg=f"LLM rejected summary attempt={v_attempt+1}"
        		)
  
		# ========================
		# FALLBACK: validator unavailable
		# ========================

		if not validator_ok and validator_api_failed:
    		    logging.warning(f"[VALIDATOR_DOWN_FALLBACK] trace={trace_id} chat={chat_id}")

    		    if validate_summary_against_messages(candidate, messages):
        	        validator_ok = True

        		trace_log(
            		    "VALIDATOR_FALLBACK_USED",
            		    trace_id,
            		    chat_id,
            		    status="info",
            		    msg="heuristic validation used"
        		)

		# ========================
		# FINAL DECISION
		# ========================

		if not validator_ok:
    		    log_event(
    			"SUMMARY_VALIDATION_FAIL",
    			chat_id=chat_id,
    			trace=trace_id
		    )
		    continue

		# ========================
		# ACCEPT SUMMARY
		# ========================

		summary = candidate
    		break

	if not summary:
	    trace_log(
        	"FINAL_FAIL",
        	trace_id,
        	chat_id,
        	status="error",
        	msg="no valid summary after retries"
    	    )
    	        	    
	    try:
    		if partial_summaries:
        	    fallback_summary = "\n\n".join(partial_summaries[:3])
    		else:
        	    fallback_summary = DEFAULT_SUMMARY

    		if not validate_summary_structure(fallback_summary):
        	    fallback_summary = DEFAULT_SUMMARY

		    log_event(
    			"SUMMARY_FALLBACK",
    			chat_id=chat_id,
    			trace=trace_id
		    )

    		save_fallback_summary(
        	    cur,
        	    c,
        	    chat_id,
        	    period_start,
        	    period_end,
        	    fallback_summary,
        	    trace_id
    		)

	    except Exception as e:
    		trace_log(
        	    "FALLBACK_SAVE_ERROR",
        	    trace_id,
        	    chat_id,
        	    status="error",
        	    msg="failed to save fallback summary",
        	    err=str(e)
    		)

    		log_event(
    		    "SUMMARY_FALLBACK_SAVE_ERROR",
    		    trace=trace_id,
    		    chat=chat_id
		)

		logging.exception("[SUMMARY_FALLBACK_SAVE_ERROR_STACK]")

    	    return

	log_event(
    	    "SUMMARY_READY",
    	    chat_id=chat_id,
    	    trace=trace_id,
    	    length=len(summary)
	)

	# ========================
        # ADAPT (ONLY FOR CLIENT)
        # ========================
        client_text = None

        if chat_type == "client":
    	    system_prompt2, user_template2, version2 = get_prompt(cur, "adapt", domain)

    	    res2 = call_smaipl_with_retry(
        	"func_adapt_summary_for_client",
        	{
            	     "summary": summary,
                     "domain": domain,
            	     "system_prompt": system_prompt2,
            	     "user_template": user_template2
        	}
    	    )

    	    if (
    		res2.get("status") == "success"
    		and res2.get("result", {}).get("success")
	    ):
        	candidate = res2["result"].get("data")

        	if is_valid_summary(candidate):
            	    client_text = candidate

		    trace_log(
    			"CLIENT_READY",
    			trace_id,
    			chat_id,
    			msg="client draft ready"
		    )

            	    log_usage(
                	cur,
                	user_id,
                	"adapt",
                	res2.get("usage", {}).get("total_tokens"),
                	version2,
			trace_id
            	    )
        	else:
            	    trace_log(
    			"CLIENT_BAD",
    			trace_id,
    			chat_id,
    			status="warn",
    			msg="invalid client draft"
		    )

		# ========================
		# CONSISTENCY GUARD (CRITICAL)
		# ========================
		if chat_type == "client" and not client_text:
    		    logging.warning(f"[CLIENT_DRAFT_MISSING] chat={chat_id} → skip client summary")
    	
       
        now = datetime.utcnow().isoformat()

	try:
    	    # ========================
    	    # SAVE MANAGER
    	    # ========================
    	    cur.execute("""
    		UPDATE summaries
    		SET text=?, status='done'
    		WHERE chat_id=?
    		AND type='manager'
    		AND period_start=?
    		AND period_end=?
	    """, (
    		summary,
    		chat_id,
    		period_start,
    		period_end
	    ))
  
	    # ========================
	    # INVALIDATE FALLBACK (CRITICAL)
	    # ========================
	    cur.execute("""
    		DELETE FROM summaries
    		WHERE chat_id=?
    		AND type='manager_fallback'
    		AND period_start=?
    		AND period_end=?
	    """, (chat_id, period_start, period_end))

    	    # ========================
    	    # SAVE CLIENT (ONLY IF VALID)
    	    # ========================
    	    if chat_type == "client" and client_text:
        	cur.execute("""
            	    INSERT INTO summaries
            	    (chat_id, type, period_start, period_end, text, created_at)
            	    VALUES (?, ?, ?, ?, ?, ?)
        	""", (
            	    chat_id,
            	    "client_draft",
            	    period_start,
            	    period_end,
            	    client_text,
            	    now
        	))

        	logging.info(f"[SUMMARY_SAVE] chat={chat_id} type=client_draft")

    	    c.commit()

	except sqlite3.IntegrityError:
    	    logging.warning(f"[DUPLICATE_SUMMARY_SKIPPED] chat={chat_id}")
    	    return

	except Exception as e:
    	    logging.error(f"[SUMMARY_INSERT_ERROR] chat={chat_id} err={e}")
    	    return

        
	# ========================
	# MEMORY UPDATE (PROJECT LEVEL)
	# ========================

	# 1. получаем project_id
	cur.execute("SELECT project_id FROM chats WHERE id=?", (chat_id,))
	row = cur.fetchone()
	project_id = row[0] if row else None

	if project_id:

    	    # ------------------------
    	    # GET EXISTING MEMORY
   	    # ------------------------
    	    cur.execute("""
        	SELECT summary FROM project_memory
        	WHERE project_id=?
        	LIMIT 1
    	    """, (project_id,))

    	    row = cur.fetchone()
    	    old_memory = row[0] if row else ""

    	    # ------------------------
    	    # BUILD MEMORY PROMPT
    	    # ------------------------
    	    new_memory = update_project_memory(
    		cur,
    		project_id,
    		old_memory,
    		summary,
    		domain,
    		trace_id
	    )

    	    if new_memory and is_valid_summary(new_memory):

        	if is_valid_summary(new_memory):

            	    # ------------------------
            	    # UPSERT MEMORY
            	    # ------------------------
            	    try:
                	cur.execute("""
                    	    INSERT INTO project_memory (project_id, summary, updated_at)
                    	    VALUES (?, ?, ?)
                	""", (
                    	    project_id,
                    	    new_memory,
                    	    datetime.utcnow().isoformat()
                	))

            	    except:
                	cur.execute("""
                    	    UPDATE project_memory
                    	    SET summary=?, updated_at=?
                    	    WHERE project_id=?
                	""", (
                    	    new_memory,
                    	    datetime.utcnow().isoformat(),
                    	    project_id
                	))

            	    logging.info(f"[MEMORY_UPDATED] trace={trace_id} project={project_id}")

	except Exception as e:
    	    logging.warning(f"[DUPLICATE_MANAGER] chat={chat_id} err={e}")
    	    return  # ВАЖНО: выходим

	
# ========================
# WORKER
# ========================
def process_message_batch(tasks, retries=5):
    import time
    from datetime import datetime 

    for attempt in range(retries):
        c = conn()
        cur = c.cursor()

        try:
            rows = []

	    for t in tasks:
    		bot_id = t.get("bot_id")
    		tg_chat_id = t.get("telegram_chat_id")
    		payload = t.get("payload", {})

    		# ========================
		# GET OR CREATE CHAT
		# ========================
		cur.execute("""
    		    SELECT id FROM chats
    		    WHERE bot_id=? AND telegram_chat_id=?
		""", (bot_id, tg_chat_id))

		row = cur.fetchone()

		if row:
    		    chat_id = row[0]
		else:
    		    # ========================
    		    # GET PROJECT_ID
    		    # ========================
    		    cur.execute("""
        		SELECT id FROM projects
        		WHERE user_id=(
            		    SELECT user_id FROM bots WHERE id=?
        	    	)
			ORDER BY created_at DESC
        	    	LIMIT 1
    		    """, (bot_id,))

    		    project = cur.fetchone()
		    project_id = project[0] if project else None

		    # ========================
		    # AUTO-CREATE PROJECT (SENIOR FIX)
		    # ========================
		    if not project_id:
    			logging.warning(f"[PROJECT_AUTO_CREATE] bot_id={bot_id}")

			cur.execute("""
    			    SELECT user_id FROM bots WHERE id=?
			""", (bot_id,))
			row = cur.fetchone()

			if not row:
    			    raise Exception("Bot not found for project creation")

			user_id = row[0]

    			cur.execute("""
        		    INSERT INTO projects (user_id, name, domain, created_at)
        		    VALUES (?, ?, ?, ?)
      			""", (
        		    bot_id,
        		    f"AutoProject_{bot_id}",
        		    "default",
        		    datetime.utcnow().isoformat()
    			))

    			project_id = cur.lastrowid

    		    # ========================
		    # INSERT CHAT (SAFE)
		    # ========================
		    chat_type = validate_chat_type("team")

		    cur.execute("""
    			INSERT OR IGNORE INTO chats (project_id, bot_id, telegram_chat_id, chat_type, created_at)
    			VALUES (?, ?, ?, ?, ?)
		    """, (
    			project_id,
    			bot_id,
    			tg_chat_id,
			chat_type,
    			datetime.utcnow().isoformat()
		    ))

		    # ========================
		    # CRITICAL: ALWAYS SELECT AFTER INSERT
		    # ========================
		    cur.execute("""
    			SELECT id FROM chats
    			WHERE bot_id=? AND telegram_chat_id=?
		    """, (bot_id, tg_chat_id))

		    row = cur.fetchone()

		    if not row:
    			raise Exception("Failed to get chat_id after insert")

		    chat_id = row[0]

    		# ========================
    		# BUILD MESSAGE ROW
    		# ========================
    		rows.append((
        	    chat_id,
        	    payload.get("telegram_message_id"),
        	    payload.get("user_name"),
        	    payload.get("text"),
        	    payload.get("timestamp")
    		))

            cur.executemany("""
                INSERT OR IGNORE INTO messages
                (chat_id, telegram_message_id, user_name, text, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, rows)

            c.commit()
            c.close()

            return True

        except Exception as e:
            c.rollback()
            c.close()

            logging.error(f"[BATCH_INSERT_ERROR] err={e} attempt={attempt}")

    	    base_delay = min(0.1 * (2 ** attempt), 2)
    	    jitter = random.uniform(0, 0.1)

    	    logging.warning(
        	f"[RETRY_SLEEP] base={base_delay:.3f} jitter={jitter:.3f}"
    	    )

    	    time.sleep(base_delay + jitter)

    return False

def processor():
    while True:
        try:
            priority, _, task = task_queue.get(timeout=1)

	    # ========================
	    # DELAYED TASK EXECUTION
	    # ========================
	    run_at = task.get("run_at", 0)

	    if time.time() < run_at:
    		task_queue.put((priority, time.time(), task))
    		time.sleep(0.1)
    		continue

            task_type = task["type"]
            payload = task["payload"]

            # TTL CHECK
            if time.time() - task["created_at"] > 300:
                send_to_dlq(task, "expired")
                continue

            try:
                if task_type == "message":
    		    tasks = [{
        		"bot_id": payload["bot_id"],
        		"telegram_chat_id": payload["telegram_chat_id"],
        		"payload": payload["payload"]
    		    }]

    		    # ========================
    		    # BATCH COLLECT (CRITICAL)
    		    # ========================
    		    while len(tasks) < 50:
        		try:
            		    next_priority, _, next_task = task_queue.get_nowait()

            		    if next_task["type"] != "message":
                		# вернуть назад, если не message
                		task_queue.put((next_priority, time.time(), next_task))
                		break

            		    tasks.append({
                		"bot_id": next_task["payload"]["bot_id"],
                		"telegram_chat_id": next_task["payload"]["telegram_chat_id"],
                		"payload": next_task["payload"]["payload"]
            		    })

        		except queue.Empty:
            		    break

    		    process_message_batch(tasks)

                elif task_type in ["summary", "summary_retry"]:
                    process_summary(payload)

                else:
                    logging.warning(f"[UNKNOWN_TASK] {task_type}")

            except Exception as e:
                logging.error(f"[TASK_ERROR] type={task_type} err={e}")

                if should_retry(task):
                    requeue_task(task)
                else:
                    send_to_dlq(task, e)

        except queue.Empty:
            continue

        except Exception as e:
            logging.error(f"[PROCESSOR_FATAL] {e}")

# ========================
# SCHEDULER (FIXED: respects settings + timezone)
# ========================
def scheduler():
    import pytz
    from datetime import datetime, timedelta

    while True:
        try:

	    logging.info(f"[SCHEDULER_HEARTBEAT] ts={datetime.utcnow().isoformat()}")

            c = conn()
            cur = c.cursor()

            # берем только активные чаты с настройками
            cur.execute("""
                SELECT 
                    c.id,
                    c.bot_id,
                    p.domain,
                    cs.summary_enabled,
                    cs.summary_type,
                    cs.summary_time,
                    cs.summary_day,
                    cs.period_type,
                    cs.timezone,
                    c.last_run_at
                FROM chats c
                JOIN projects p ON c.project_id = p.id
                JOIN chat_settings cs ON cs.chat_id = c.id
                WHERE cs.summary_enabled = 1
            """)

            rows = cur.fetchall()

            for row in rows:
                (
                    chat_id,
                    bot_id,
                    domain,
                    enabled,
                    summary_type,
                    summary_time,
                    summary_day,
                    period_type,
                    timezone,
                    last_run_at
                ) = row

		logging.info(f"[SCHEDULER_CHECK] chat={chat_id}")

                # ========================
                # VALIDATION
                # ========================
                if not summary_time or not timezone:
                    continue

                try:
                    tz = pytz.timezone(timezone)
                except Exception:
                    continue

                now = datetime.now(tz)

		logging.info(
    		    f"[SCHEDULER_TIME] chat={chat_id} now={now.isoformat()} target={summary_time}"
		)

                # текущее время HH:MM
                current_time = now.strftime("%H:%M")

                # ========================
		# TIME MATCH (FIXED)
		# ========================
		from datetime import datetime as dt

		target_time = dt.strptime(summary_time, "%H:%M").time()

		target_dt = now.replace(
    		    hour=target_time.hour,
    		    minute=target_time.minute,
    		    second=0,
    		    microsecond=0
		)

		delta = abs((now - target_dt).total_seconds())

		# допускаем запуск если время уже прошло,
		# но не позже чем на 5 минут
		if delta < 0 or delta > 300:
    		    continue

                # ========================
                # WEEKLY CHECK
                # ========================
                if summary_type == "weekly":
                    current_day = now.strftime("%A").lower()  # monday, tuesday
                    if not summary_day or summary_day.lower() != current_day:
                        continue

                # ========================
                # DAILY / WEEKLY / CUSTOM
                # ========================
                should_run = False

                if summary_type == "daily":
                    should_run = True

                elif summary_type == "weekly":
                    should_run = True

                elif summary_type == "custom":
                    # можно расширить под period_start / period_end
                    should_run = True

                if not should_run:
                    continue

                # ========================
                # ANTI-DUPLICATE (CRITICAL)
                # ========================
                if last_run_at:
    		    try:
                        last_dt = datetime.fromisoformat(last_run_at)
                    except Exception:
                        last_dt = None

                    if last_dt is None:
                        pass
                    else:
                        if last_dt.tzinfo is None:
                            last_dt = tz.localize(last_dt)
                        else:
                            last_dt = last_dt.astimezone(tz)

                        if summary_type == "daily":
                            if last_dt.date() == now.date():
                                continue

                        if summary_type == "weekly":
                            if last_dt.isocalendar()[1] == now.isocalendar()[1]:
                                continue

                # ========================
                # GET BOT TOKEN
                # ========================
                cur.execute("SELECT bot_token FROM bots WHERE id=?", (bot_id,))
                bot_row = cur.fetchone()

                if not bot_row:
                    continue

                bot_token = bot_row[0]

                # ========================
                # PUSH TASK
                # ========================
                log_event(
    		    "SCHEDULER_TRIGGER",
    		    chat_id=chat_id
		)

		# ========================
		# CLEANUP OLD MESSAGES
		# ========================
		try:
    		    cur.execute("""
        	        DELETE FROM messages
        	        WHERE timestamp < datetime('now', '-45 days')
    		    """)

		    cur.execute("""
        		DELETE FROM usage_logs
        		WHERE created_at < datetime('now', '-60 days')
    		    """)

    		    deleted = cur.rowcount

    		    if deleted > 0:
        		log_event("CLEANUP_USAGE_LOGS", deleted=deleted)


    		    c.commit()

	        except Exception as e:
    		    logging.error(f"[CLEANUP_ERROR] {e}")


                # ========================
		# JITTER (ANTI SPIKE)
		# ========================
		
		time.sleep(random.uniform(0, 2))

		task_queue.put(create_task(
    		    "summary",
    		    {
        		"chat_id": chat_id,
        		"bot_token": bot_token,
        		"domain": domain,
        		"period": period_type or "day"
    		    }
		))

		logging.info(f"[TASK_ENQUEUED] type=summary chat={chat_id}")

                # ========================
                # UPDATE last_run_at
                # ========================
                cur.execute("""
                    UPDATE chats
                    SET last_run_at=?
                    WHERE id=?
                """, (datetime.utcnow().isoformat(), chat_id))

                c.commit()

            c.close()

        except Exception as e:
            logging.error(f"[SCHEDULER_ERROR] err={e}")

	# ========================
        # RECOVERY LOOP
        # ========================
        try:
            retry_failed_summaries()
        except Exception as e:
            logging.error(f"[RECOVERY_LOOP_ERROR] {e}")

        time.sleep(60)

# ========================
# BOT REGISTER (UPDATED)
# ========================
def register_bot(bot_token, user_id, domain="default"):
    c = conn()
    cur = c.cursor()

    try:
        from datetime import datetime
        import secrets
        import requests

        now = datetime.utcnow().isoformat()

        # ========================
        # IDEMPOTENCY CHECK (CRITICAL)
        # ========================
        cur.execute("""
            SELECT id, is_active
            FROM bots
            WHERE bot_token=?
        """, (bot_token,))

        row = cur.fetchone()

        if row:
            bot_id, is_active = row

            if is_active:
                return {"status": "ok", "bot_id": bot_id}

            # если был неактивен → реактивируем
            cur.execute("""
                UPDATE bots
                SET is_active=1
                WHERE id=?
            """, (bot_id,))

        else:
            # ========================
            # INSERT BOT (ONCE)
            # ========================
            cur.execute("""
                INSERT INTO bots (user_id, bot_token, created_at)
                VALUES (?, ?, ?)
            """, (
                user_id,
                bot_token,
                now
            ))

            bot_id = cur.lastrowid

        # ========================
        # BUILD WEBHOOK
        # ========================
        webhook_url = f"https://your-domain.com/webhook/{bot_id}"
        secret_token = secrets.token_urlsafe(32)

        # ========================
        # UPDATE BOT META
        # ========================
        cur.execute("""
            UPDATE bots
            SET webhook_url=?, webhook_secret=?
            WHERE id=?
        """, (webhook_url, secret_token, bot_id))

        # ========================
        # CREATE PROJECT (SAFE)
        # ========================
        cur.execute("""
            SELECT id FROM projects
            WHERE user_id=?
            ORDER BY created_at DESC
            LIMIT 1
        """, (user_id,))

        if not cur.fetchone():
            cur.execute("""
                INSERT INTO projects (user_id, name, domain, created_at)
                VALUES (?, ?, ?, ?)
            """, (
                user_id,
                f"Project_{bot_id}",
                domain,
                now
            ))

        # ========================
        # PROMPTS
        # ========================
        ensure_prompts_for_domain(
            cur,
            domain,
            call_smaipl,
            debug=False
        )

        # ========================
        # SET TELEGRAM WEBHOOK
        # ========================
        res = requests.post(
            f"https://api.telegram.org/bot{bot_token}/setWebhook",
            json={
                "url": webhook_url,
                "secret_token": secret_token
            },
            timeout=10
        ).json()

        # ========================
        # WEBHOOK FAIL → DEACTIVATE
        # ========================
        if not res.get("ok"):
            logging.error(f"[WEBHOOK_SETUP_FAILED] bot_id={bot_id} response={res}")

            cur.execute("""
                UPDATE bots
                SET is_active=0
                WHERE id=?
            """, (bot_id,))

            c.commit()

            return {
                "status": "error",
                "reason": "webhook setup failed"
            }

        # ========================
        # SUCCESS → COMMIT
        # ========================
        c.commit()

        return {
            "status": "ok",
            "bot_id": bot_id
        }

    except Exception as e:
        c.rollback()
        logging.exception("[REGISTER_BOT_ERROR]")
        raise

    finally:
        c.close()

# ========================
# API (FASTAPI)
# ========================

@app.post("/messages")
def ingest_message(payload: MessageIn):
    c = conn()
    try:
        cur = c.cursor()

	# ========================
	# API RATE LIMIT (IP/USER)
	# ========================
	key = payload.bot_token[:5]

	now = time.time()

	with API_RATE_LIMIT_LOCK:
    	    events = API_RATE_LIMIT.setdefault(key, [])
    	    events = [t for t in events if now - t < API_WINDOW]
    	    API_RATE_LIMIT[key] = events

    	if len(events) > API_MAX:
            raise HTTPException(status_code=429, detail="Too many requests")

    	events.append(now)


	# ========================
	# LIGHT RATE LIMIT (INGEST)
	# ========================
	global GLOBAL_RATE_LIMIT

	now = time.time()

	# чистим старые
	while GLOBAL_RATE_LIMIT and now - GLOBAL_RATE_LIMIT[0] > 5:
    	    GLOBAL_RATE_LIMIT.pop(0)

	if len(GLOBAL_RATE_LIMIT) > 100:
    	    logging.warning("[INGEST_RATE_LIMIT] overloaded")
    	    return {"status": "dropped"}

	GLOBAL_RATE_LIMIT.append(now)

        # ========================
        # VALIDATION
        # ========================
        tg_chat_id = payload.telegram_chat_id

	if tg_chat_id is None:
    	    raise HTTPException(status_code=400, detail="telegram_chat_id required")

	# ========================
	# AUTH (UNIFIED)
	# ========================
	bot = get_user_from_payload(cur, payload)

	bot_id = bot["bot_id"]
	user_id = bot["user_id"]

        if not user_id:
            raise HTTPException(status_code=403, detail="Invalid tenant")

        # ========================
        # MULTI-TENANT GUARD (CRITICAL)
        # ========================
        cur.execute("""
            SELECT 1
            FROM chats c
            JOIN bots b ON c.bot_id = b.id
            WHERE c.telegram_chat_id=?
            AND b.user_id != ?
            LIMIT 1
        """, (tg_chat_id, user_id))

        if cur.fetchone():
            raise HTTPException(status_code=403, detail="Chat belongs to another user")

        # ========================
        # QUEUE OVERFLOW PROTECTION
        # ========================
        MAX_QUEUE_SIZE = 10000

        if task_queue.qsize() >= MAX_QUEUE_SIZE:
            log_event(
    		"QUEUE_OVERFLOW",
    		action="drop_message"
	    )		
            return {
                "status": "error",
                "reason": "queue overloaded"
            }

	    log_event(
    		"API_INGEST",
    		chat_id=tg_chat_id,
    		bot_id=bot_id,
    		text_len=len(payload.text)
	    )

        # ========================
        # ENQUEUE TASK (ONLY ONCE!)
        # ========================
        task_queue.put(create_task(
            "message",
            {
                "bot_id": bot_id,
                "telegram_chat_id": tg_chat_id,
                "payload": payload.model_dump()
            }
        ))

        return {"status": "ok"}

    except HTTPException:
        raise

    except Exception as e:
        logging.exception("[INGEST_API_ERROR]")
        raise HTTPException(status_code=500, detail="internal error")

    finally:
        c.close()


# ========================
# VALIDATION (STRICT TYPES)
# ========================
VALID_CHAT_TYPES = {"team", "client"}
VALID_SUMMARY_TYPES = {"daily", "weekly", "custom"}

def validate_chat_type(value: str):
    if value not in VALID_CHAT_TYPES:
        raise HTTPException(status_code=400, detail="invalid chat_type")
    return value

def validate_summary_type(value: str):
    if value not in VALID_SUMMARY_TYPES:
        raise HTTPException(status_code=400, detail="invalid summary_type")
    return value


@app.post("/webhook/{bot_id}")
async def telegram_webhook(bot_id: int, request: Request):
    c = conn()
    try:
        cur = c.cursor()

	# ========================
	# WEBHOOK SECURITY (SECRET TOKEN)
	# ========================
	incoming_secret = request.headers.get("x-telegram-bot-api-secret-token")

	cur.execute("""
    	    SELECT webhook_secret
    	    FROM bots
    	    WHERE id=?
	""", (bot_id,))

	row = cur.fetchone()

	if not row or not row[0]:
    	    raise HTTPException(status_code=403, detail="Webhook not configured")

	expected_secret = row[0]

	if incoming_secret != expected_secret:
	    log_event(
    		"WEBHOOK_REJECTED",
    		bot_id=bot_id,
    		ip=request.client.host
	    )
    	    raise HTTPException(status_code=403, detail="Invalid webhook signature")

        # ========================
        # GET BOT (CRITICAL)
        # ========================
        cur.execute("""
            SELECT bot_token, is_active
            FROM bots
            WHERE id=?
        """, (bot_id,))

        row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Bot not found")

        bot_token, is_active = row

        bot = validate_bot_token(cur, bot_token)

	if not bot:
    	    return {"status": "ignored"}

        # ========================
        # PARSE UPDATE
        # ========================
        data = await request.json()

        message = data.get("message") or data.get("edited_message")

        if not message:
            return {"status": "ignored"}

        # ========================
        # FILTER CHAT TYPE
        # ========================
        chat = message.get("chat", {})
        chat_type = chat.get("type")

        if chat_type not in ["group", "supergroup"]:
            return {"status": "ignored"}

        text = message.get("text")

        if not text:
            return {"status": "ignored"}

        # ========================
        # EXTRACT FIELDS
        # ========================
        payload = {
            "bot_token": bot_token,
            "telegram_chat_id": chat.get("id"),
            "telegram_message_id": message.get("message_id"),
            "user_name": (
                message.get("from", {}).get("full_name")
                or message.get("from", {}).get("username", "")
            ),
            "text": text,
            "timestamp": datetime.utcfromtimestamp(
                message.get("date")
            ).isoformat()
        }

        # ========================
        # INTERNAL CALL → /messages
        # ========================
        task_queue.put(create_task(
            "message",
            {
                "bot_id": bot_id,
                "telegram_chat_id": payload["telegram_chat_id"],
                "payload": payload
            }
        ))

        return {"status": "ok"}

    except Exception as e:
        logging.exception("[WEBHOOK_ERROR]")
        return {"status": "error"}

    finally:
        c.close()

# ========================
# COMMANDS API
# ========================

@app.post("/commands/summary")
def run_summary(payload: SummaryRequest):
    c = conn()
    try:
        cur = c.cursor()

	import uuid
	trace_id = str(uuid.uuid4())[:8]

	logging.info(f"[API_TRACE_START] trace={trace_id}")

        # ========================
	# INPUT (Pydantic)
	# ========================
	bot_token = payload.bot_token
	chat_id = payload.chat_id
	period = payload.period

	# ========================
	# AUTH (UNIFIED)
	# ========================
	bot = get_user_from_payload(cur, payload.dict())

	bot_id = bot["bot_id"]
	user_id = bot["user_id"]	

	# ========================
	# API RATE LIMIT (summary)
	# ========================
	key = f"user:{bot_token[:10]}"
	now = time.time()

	with API_RATE_LIMIT_LOCK:
    	    events = API_RATE_LIMIT.setdefault(key, [])
    	    events = [t for t in events if now - t < API_WINDOW]
    	    API_RATE_LIMIT[key] = events

    	if len(events) > 20:
            raise HTTPException(status_code=429, detail="Too many summary requests")

    	events.append(now)

	# ========================
	# RATE LIMIT (CRITICAL)
	# ========================
	if not allow_summary_request(user_id):
    	    log_event(
    		"RATE_LIMIT",
    		user_id=user_id
	    )
    	    raise HTTPException(status_code=429, detail="Too many requests")

	# ========================
	# OWNERSHIP CHECK (CRITICAL)
	# ========================
	cur.execute("""
    	    SELECT 1
    	    FROM chats c
    	    JOIN bots b ON c.bot_id = b.id
    	    WHERE c.id=? AND b.user_id=?
	""", (chat_id, user_id))

	if not cur.fetchone():
    	    raise HTTPException(status_code=403, detail="Access denied")

        # ========================
        # CHECK CHAT EXISTS
        # ========================
        cur.execute("""
            SELECT c.id, b.bot_token, p.domain
            FROM chats c
            JOIN bots b ON c.bot_id = b.id
            JOIN projects p ON c.project_id = p.id
            WHERE c.id=? AND b.id=?
        """, (chat_id, bot_id))

        row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="chat not found")

        chat_id, bot_token, domain = row


        # ========================
        # ENQUEUE SUMMARY TASK
        # ========================
        task_queue.put(create_task(
            "summary",
            {
                "chat_id": chat_id,
                "bot_token": bot_token,
                "domain": domain,
                "period": period,
		"trace_id": trace_id
            }
        ))

        return {"status": "queued"}

    except HTTPException:
        raise

    except Exception:
        logging.exception("[SUMMARY_COMMAND_ERROR]")
        raise HTTPException(status_code=500, detail="internal error")

    finally:
        c.close()

def allow_summary_request(user_id):
    now = time.time()

    with rate_limit_lock:
	# ========================
	# CLEANUP OLD USERS (TTL)
	# ========================
	inactive_users = [
    	    uid for uid, events in RATE_LIMIT_USER.items()
    	    if not events or now - max(events) > USER_LIMIT_WINDOW
	]

	for uid in inactive_users:
    	    del RATE_LIMIT_USER[uid]


        # ========================
        # USER LIMIT
        # ========================
        user_events = RATE_LIMIT_USER.setdefault(user_id, [])

        user_events = [t for t in user_events if now - t < USER_LIMIT_WINDOW]
        RATE_LIMIT_USER[user_id] = user_events

        if len(user_events) >= USER_LIMIT_MAX:
            return False

        user_events.append(now)

        # ========================
        # GLOBAL LIMIT
        # ========================
        RATE_LIMIT_GLOBAL.append(now)

        while RATE_LIMIT_GLOBAL and now - RATE_LIMIT_GLOBAL[0] > GLOBAL_LIMIT_WINDOW:
            RATE_LIMIT_GLOBAL.pop(0)

        if len(RATE_LIMIT_GLOBAL) > GLOBAL_LIMIT_MAX:
            return False

    return True

# ========================
# READ API
# ========================
@app.get("/summaries/latest")
def get_latest_summary(chat_id: int, authorization: str = Header(None)):
    c = conn()
    try:
        cur = c.cursor()

        bot = get_user_from_auth(cur, authorization)
	user_id = bot["user_id"]

        # ========================
        # OWNERSHIP CHECK
        # ========================
        cur.execute("""
            SELECT 1
            FROM chats c
            JOIN bots b ON c.bot_id = b.id
            WHERE c.id=? AND b.user_id=?
            LIMIT 1
        """, (chat_id, user_id))

        if not cur.fetchone():
            raise HTTPException(status_code=403, detail="Access denied")

        # ========================
        # GET SUMMARY
        # ========================
        cur.execute("""
            SELECT  text, created_at
            FROM summaries
            WHERE chat_id=? AND status='done'
            ORDER BY created_at DESC
            LIMIT 1
        """, (chat_id,))

        row = cur.fetchone()

        if not row:
            return {"status": "empty"}

        return {
            "status": "success",
            "data": {
                "summary": row[0],
                "created_at": row[1]
            }
        }

    finally:
        c.close()


@app.get("/chats/get")
def get_chat(telegram_chat_id: str, authorization: str = Header(None)):
    c = conn()
    try:
        cur = c.cursor()

       if not telegram_chat_id:
    	   raise HTTPException(status_code=400, detail="telegram_chat_id required")

	bot = get_user_from_auth(cur, authorization)
	bot_id = bot["bot_id"]
	user_id = bot["user_id"]

        # ========================
        # MULTI-TENANT GUARD
        # ========================
        cur.execute("""
            SELECT 1
            FROM chats c
            JOIN bots b ON c.bot_id = b.id
            WHERE c.telegram_chat_id=?
            AND b.user_id != ?
            LIMIT 1
        """, (telegram_chat_id, user_id))

        if cur.fetchone():
            raise HTTPException(status_code=403, detail="Access denied")

        # ========================
        # GET CHAT
        # ========================
        cur.execute("""
            SELECT id
            FROM chats
            WHERE bot_id=? AND telegram_chat_id=?
        """, (bot_id, telegram_chat_id))

        row = cur.fetchone()

        if not row:
            return {"status": "not_found"}

        return {"chat_id": row[0]}

    except HTTPException:
        raise

    except Exception:
        logging.exception("[GET_CHAT_ERROR]")
        raise HTTPException(status_code=500, detail="internal error")

    finally:
        c.close()

# ========================
# START
# ========================
ensure_schema()

threading.Thread(target=processor, daemon=True).start()
threading.Thread(target=scheduler, daemon=True).start()
