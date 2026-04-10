# ========================
# IMPORTS
# ========================
import os
import logging
import asyncio
import httpx
import time
import random

from aiogram import Bot, Dispatcher
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command

BOT_TOKEN = os.getenv("BOT_TOKEN")
BACKEND_URL = os.getenv("BACKEND_URL")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ========================
# STATE
# ========================

# RATE LIMIT STORAGE

SUMMARY_COOLDOWN = 30        # сек на чат
GLOBAL_LIMIT_WINDOW = 10     # сек
GLOBAL_LIMIT_MAX = 10        # max команд за окно


# ========================
# UI STATE (CRITICAL)
# ========================
waiting_time_input = {}  # tg_id -> timestamp

TIMEOUT = 300  # 5 минут

def cleanup_state():
    now = time.time()

    expired = [
        k for k, v in waiting_time_input.items()
        if now - v > TIMEOUT
    ]

    for k in expired:
        del waiting_time_input[k]

# ========================
# HTTP (ASYNC + RETRY)
# ========================
async def request(method, url, payload=None, params=None, headers=None):
    async with httpx.AsyncClient(timeout=5) as client:
        try:
            if method == "POST":
                r = await client.post(url, json=payload, headers=headers)
            else:
                r = await client.get(url, params=params, headers=headers)

            if r.status_code != 200:
                logging.warning(f"[HTTP_ERROR] status={r.status_code} body={r.text}")
                return None

            return r.json()

        except Exception as e:
            logging.error(f"[HTTP_EXCEPTION] {e}")
            return None

async def send_message_with_retry(payload, retries=3):
    for attempt in range(retries):
        result = await request(
            "POST",
            f"{BACKEND_URL}/messages",
            payload=payload
        )

        res = decode_response(result)

	if res["ok"]:
    	    return result

        logging.warning(
            f"[INGEST_RETRY] attempt={attempt+1} chat={payload.get('telegram_chat_id')}"
        )

        delay = min(2 ** attempt, 5)
	jitter = random.uniform(0, 0.5)

	await asyncio.sleep(delay + jitter)

    logging.error("[INGEST_FAILED] message dropped")
    return None


async def get_summary_with_retry(chat_id, retries=3):
    for attempt in range(retries):
        result = await request(
            "GET",
            f"{BACKEND_URL}/summaries/latest",
            params={
    		"chat_id": chat_id
	    },
	    headers={
    		"Authorization": f"Bearer {BOT_TOKEN}"
	    }
        )

        if result:
            return result

        logging.warning(
            f"[SUMMARY_RETRY] attempt={attempt+1} chat={chat_id}"
        )

        delay = min(2 ** attempt, 5)
	jitter = random.uniform(0, 0.5)

	await asyncio.sleep(delay + jitter)

    logging.error(f"[SUMMARY_FAILED] chat={chat_id}")
    return None


async def request_with_retry(method, url, payload=None, params=None, headers=None, retries=3):
    for attempt in range(retries):
        result = await request(method, url, payload, params, headers)

        if result:
            return result

        delay = min(2 ** attempt, 5)
        jitter = random.uniform(0, 0.5)

        logging.warning(
            f"[HTTP_RETRY] attempt={attempt+1} delay={delay:.2f} jitter={jitter:.2f}"
        )

        await asyncio.sleep(delay + jitter)

    logging.error(f"[HTTP_FAILED] url={url}")
    return None


def decode_response(res):
    if not res:
        return {
            "ok": False,
            "type": "network_error",
            "status": None,
            "data": None
        }

    if not isinstance(res, dict):
        return {
            "ok": False,
            "type": "invalid_response",
            "status": None,
            "data": None
        }

    status = res.get("status")

    if not status:
        return {
            "ok": False,
            "type": "contract_error",
            "status": None,
            "data": None
        }

    result = res.get("result") or {}

    return {
        "ok": status == "success" and result.get("success") is True,
        "type": "ok" if status == "success" else "backend_error",
        "status": status,
        "data": result.get("data"),
        "raw": res
    }

# ========================
# BACKEND HELPERS
# ========================
async def get_chat_id(tg_id):
    r = await request_with_retry(
        "GET",
        f"{BACKEND_URL}/chats/get",
        params={
            "telegram_chat_id": tg_id
        },
        headers={
            "Authorization": f"Bearer {BOT_TOKEN}"
        }
    )

    res = decode_response(r)

    if not res["ok"]:
        logging.error(f"[CHAT_ID_ERROR] type={res['type']}")
        return None

    data = res["data"]

    if not data:
        return None

    return data.get("chat_id")

async def update_setting(endpoint, tg_chat_id, payload):
    logging.info(f"Settings update: {endpoint} chat={tg_chat_id}")

    r = await request(
        "POST",
        f"{BACKEND_URL}{endpoint}",
        payload={
            "bot_token": BOT_TOKEN,
            "telegram_chat_id": tg_chat_id,
            **payload
        }
    )

    res = decode_response(r)

    if not res["ok"]:
        logging.error(
            f"[SETTINGS_ERROR] endpoint={endpoint} chat={tg_chat_id} type={res['type']}"
        )

    return res

# ========================
# TEXT SPLIT
# ========================
def split_text(text, limit=4000):
    if not text:
        return [""]

    if not isinstance(text, str):
        text = str(text)

    return [text[i:i + limit] for i in range(0, len(text), limit)]


async def send_long(chat_id, text):
    if not text:
        await bot.send_message(chat_id, "❌ Нет данных для отображения")
        return

    if not isinstance(text, str):
        text = str(text)

    parts = split_text(text)
    
    for i, part in enumerate(parts):
        prefix = f"Часть {i+1}/{len(parts)}\n\n" if len(parts) > 1 else ""
        await bot.send_message(chat_id, prefix + part)

# ========================
# INGESTION
# ========================
async def handle_ingest(message: Message):
    if message.chat.type not in ["group", "supergroup"]:
        return

    if not message.text:
        return

    logging.info(f"Ingest message chat={message.chat.id}")

    await send_message_with_retry({
        "bot_token": BOT_TOKEN,
        "telegram_chat_id": message.chat.id,
        "telegram_message_id": message.message_id,
        "user_name": message.from_user.full_name if message.from_user else "",
        "text": message.text,
        "timestamp": message.date.isoformat()
    })


# ========================
# SUMMARY (PRODUCTION)
# ========================
@dp.message(Command("summary"))
async def summary(message: Message):

    # ========================
    # ONLY GROUPS
    # ========================
    if message.chat.type not in ["group", "supergroup"]:
        await message.answer("❌ Команда доступна только в группе")
        return

    tg_id = message.chat.id

    # ========================
    # GET chat_id FROM BACKEND
    # ========================
    chat_id = await get_chat_id(tg_id)

    if not chat_id:
        await message.answer("❌ Чат не зарегистрирован")
        return

    res = await request_with_retry(
        "POST",
        f"{BACKEND_URL}/commands/summary",
        payload={
            "bot_token": BOT_TOKEN,
            "chat_id": chat_id,
            "period": "day"
        }
    )

    if not res:
        await message.answer("❌ Не удалось запустить summary (backend недоступен)")
        return

    # ========================
    # GET SUMMARY (RETRY)
    # ========================
    result = await get_summary_with_retry(chat_id)

    res = decode_response(result)

    # ========================
    # NETWORK
    # ========================
    if res["type"] == "network_error":
        await message.answer("❌ Backend не ответил")
        return

    # ========================
    # STATUS
    # ========================
    status = res["status"]

    if status == "processing":
        await message.answer("⏳ Summary в процессе генерации")
        return

    if status == "empty":
        await message.answer("⚠️ Пока нет данных для summary")
        return

    if status != "success":
        await message.answer("❌ Ошибка получения summary")
        return

    # ========================
    # DATA
    # ========================
    text = res["data"]

    if not text:
        await message.answer("⚠️ Summary пустой")
        return

    await send_long(message.chat.id, text)

# ========================
# MESSAGE ROUTER (CRITICAL)
# ========================
@dp.message()
async def message_router(message: Message):
    tg_id = message.chat.id
 
    cleanup_state()

    # ========================
    # UI FLOW
    # ========================
    if tg_id in waiting_time_input:
        handled = await handle_time(message)
        if handled:
            return

    # ========================
    # INGESTION
    # ========================
    await handle_ingest(message)
# ========================
# UI
# ========================
def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Тип чата", callback_data="menu_type")],
        [InlineKeyboardButton(text="Расписание", callback_data="menu_schedule")],
        [InlineKeyboardButton(text="Время", callback_data="menu_time")],
        [InlineKeyboardButton(text="Период", callback_data="menu_period")],
        [InlineKeyboardButton(text="Timezone", callback_data="menu_timezone")]
    ])


def type_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Команда", callback_data="type_team")],
        [InlineKeyboardButton(text="Клиент", callback_data="type_client")]
    ])


def schedule_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Daily", callback_data="schedule_daily")],
        [InlineKeyboardButton(text="Weekly", callback_data="schedule_weekly")]
    ])


def period_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Day", callback_data="period_day")],
        [InlineKeyboardButton(text="Week", callback_data="period_week")]
    ])


def timezone_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="UTC", callback_data="tz_UTC")],
        [InlineKeyboardButton(text="London", callback_data="tz_Europe/London")],
        [InlineKeyboardButton(text="Moscow", callback_data="tz_Europe/Moscow")]
    ])


# ========================
# SETUP
# ========================
@dp.message(Command("setup"))
async def setup(message: Message):
    logging.info(f"Open settings chat={message.chat.id}")
    await message.answer("⚙️ Настройки:", reply_markup=main_menu())


# ========================
# CALLBACKS
# ========================
@dp.callback_query()
async def callbacks(callback: CallbackQuery):
    cleanup_state()
    data = callback.data
    tg_id = callback.message.chat.id

    logging.info(f"Callback {data} chat={tg_id}")

    if data == "menu_type":
        await callback.message.answer("Тип чата:", reply_markup=type_kb())

    elif data == "menu_schedule":
        await callback.message.answer("Расписание:", reply_markup=schedule_kb())

    elif data == "menu_time":
        waiting_time_input[tg_id] = time.time()
        await callback.message.answer("Введи время (HH:MM)")

    elif data == "menu_period":
        await callback.message.answer("Период:", reply_markup=period_kb())

    elif data == "menu_timezone":
        await callback.message.answer("Timezone:", reply_markup=timezone_kb())

    elif data.startswith("type_"):
        await update_setting("/chats/set_type", tg_id, {"type": data.split("_")[1]})
        await callback.message.answer("✅ Тип обновлён")

    elif data.startswith("schedule_"):
        await update_setting("/chats/set_schedule", tg_id, {"schedule": data.split("_")[1]})
        await callback.message.answer("✅ Расписание обновлено")

    elif data.startswith("period_"):
        await update_setting("/chats/set_period", tg_id, {"period": data.split("_")[1]})
        await callback.message.answer("✅ Период обновлён")

    elif data.startswith("tz_"):
        tz = data.replace("tz_", "")
        await update_setting("/chats/set_timezone", tg_id, {"timezone": tz})
        await callback.message.answer("✅ Timezone обновлён")

    await callback.answer()


# ========================
# TIME INPUT
# ========================
async def handle_time(message: Message):
    tg_id = message.chat.id

    if tg_id not in waiting_time_input:
        return False

    text = message.text.strip()

    if ":" not in text or len(text) != 5:
        await message.answer("❌ Формат: HH:MM")
        return

    logging.info(f"Time set chat={tg_id} value={text}")

    await update_setting("/chats/set_time", tg_id, {"time": text})

    waiting_time_input.pop(tg_id, None)

    await message.answer(f"✅ Время: {text}")

    return True
