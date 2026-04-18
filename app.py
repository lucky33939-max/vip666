import os
import re
import time
import json
import asyncio
import traceback
from io import BytesIO
from html import escape
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo
from urllib.parse import urlencode

import requests
import aiohttp
from PIL import Image, ImageDraw, ImageFont

from fastapi import FastAPI, Request, HTTPException, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from aiogram import Bot, Dispatcher, types
from aiogram.types import Update
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
    BufferedInputFile,
)
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv
import uvicorn

from db import (
    init_db,
    get_setting,
    set_setting,
    get_admin,
    add_admin,
    remove_admin,
    get_all_admins,
    save_group,
    save_member,
    get_groups,
    is_operator,
    get_members,
    add_transaction,
    get_last_transaction,
    add_wallet_check,
    get_wallet_checks_page,
    count_wallet_checks,
    undo_transaction,
    get_transactions,
    set_trial_code,
    get_trial_code,
    add_access_user,
    remove_access_user,
    has_access_user,
    get_access_users,
    has_claimed_free_trial,
    mark_claimed_free_trial,
    create_rental_order,
    get_rental_order,
    get_pending_rental_orders,
    get_rental_orders_by_status,
    mark_rental_order_paid,
    mark_rental_order_rejected,
    get_access_user_by_id,
    has_expiry_notice,
    add_expiry_notice,
    get_expired_access_users,
    count_access_users,
    count_active_access_users,
    count_expired_access_users,
    count_permanent_access_users,
    get_access_users_page,
    count_access_users_filtered,
    extend_access_user,
    set_access_user_permanent,
    approve_rental_order,
    get_db,
)

# ================= ENV =================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
PORT = int(os.getenv("PORT", "8080"))

BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID", "0") or 0)
SUPER_ADMIN_ID = int(os.getenv("SUPER_ADMIN_ID", "0") or 0)

WEB_TOKEN = os.getenv("WEB_TOKEN", "").strip()
TELEGRAM_SECRET_TOKEN = os.getenv("TELEGRAM_SECRET_TOKEN", "").strip()

WEB_ADMIN_NAME = os.getenv("WEB_ADMIN_NAME", "BOT 888").strip() or "BOT 888"

BOT_BASE_URL = (
    os.getenv("BOT_BASE_URL")
    or os.getenv("RENDER_EXTERNAL_URL")
    or os.getenv("BASE_URL")
    or ""
).rstrip("/")

WEB_BASE_URL = (
    os.getenv("WEB_BASE_URL")
    or BOT_BASE_URL
).rstrip("/")

TRONGRID_API_KEY = os.getenv("TRONGRID_API_KEY", "").strip()
TRONGRID_API_URL = "https://api.trongrid.io"

PAYMENT_ADDRESS = os.getenv("PAYMENT_ADDRESS", "TSPpLmYuFXLi6GU1W4uyG6NKGbdWPw886U").strip()
PAYMENT_SUPPORT = os.getenv("PAYMENT_SUPPORT", "/ZZB339").strip()

AUTO_PAY_INTERVAL = int(os.getenv("AUTO_PAY_INTERVAL", "15"))
AUTO_PAY_TX_LIMIT = int(os.getenv("AUTO_PAY_TX_LIMIT", "20"))
AUTO_PAY_TOLERANCE = float(os.getenv("AUTO_PAY_TOLERANCE", "0.0001"))

WELCOME_ENABLED = os.getenv("WELCOME_ENABLED", "1").strip() == "1"
WELCOME_TEXT = os.getenv("WELCOME_TEXT", "欢迎 {name} 加入本群。").strip()

ENV_MODE = os.getenv("ENV", "dev").lower()
IS_PRODUCTION = ENV_MODE == "prod"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing in environment variables")

if not WEB_TOKEN:
    raise RuntimeError("WEB_TOKEN is missing in environment variables")

# ================= GLOBALS =================
BEIJING_TZ = ZoneInfo("Asia/Shanghai")
USDT_TRC20_CONTRACT = "TXLAQ63Xg1NAzckPwKHvzw7CSEmLMEqcdj"
TRON_ADDR_RE = re.compile(r"\bT[1-9A-HJ-NP-Za-km-z]{33}\b")

BOT_USERNAME = None
HTTP_SESSION = None

RATE_CACHE = {"value": None, "ts": 0}
RATE_CACHE_TTL = 30
USDT_DAILY_UPDATE_KEY = "usdt_daily_update_date"

# ================= BOT =================
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(link_preview_is_disabled=True),
)
dp = Dispatcher(storage=MemoryStorage())
BOT_USERNAME = None

HTTP_SESSION = None  # dùng chung cho aiohttp

# ================= BACKGROUND TASKS =================
async def daily_usdt_update_loop():
    while True:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print("daily_usdt_update_loop error:", repr(e))
            traceback.print_exc()
            await asyncio.sleep(60)
            
async def expiry_warning_loop():
    while True:
        try:
            now_ts = int(time.time())
            rows = get_expired_access_users(now_ts)

            for user_id, username, expires_at in rows:
                notice_key = f"expired:{expires_at}"
                if has_expiry_notice(user_id, notice_key):
                    continue
                try:
                    await bot.send_message(
                        user_id,
                        "⛔ 您的使用权限已到期。\n如需继续使用，请联系管理员或自助续费。"
                    )
                except Exception as e:
                    print("expiry notify error:", repr(e))
                add_expiry_notice(user_id, notice_key)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            print("expiry_warning_loop error:", repr(e))
            traceback.print_exc()

        await asyncio.sleep(300)
        
async def activate_rental_order(order_code, granted_by=None):
    try:
        return approve_rental_order(order_code, granted_by=granted_by)
    except Exception as e:
        print("activate_rental_order error:", e)
        return None, None, str(e)

async def get_usdt_in_transactions(address, limit=AUTO_PAY_TX_LIMIT):
    data = await trongrid_get(
        f"/v1/accounts/{address}/transactions/trc20",
        params={"limit": limit, "only_confirmed": "true"},
    )
    return data.get("data", []) if data else []

def parse_usdt_tx(tx):
    try:
        return {
            "to": tx.get("to"),
            "amount": float(tx.get("value", 0)) / 1_000_000,
            "txid": tx.get("transaction_id"),
        }
    except Exception:
        return None

async def auto_check_payments():
    while True:
        try:
            orders = get_pending_rental_orders(limit=100)
            txs = await get_usdt_in_transactions(PAYMENT_ADDRESS)
            parsed = [p for p in (parse_usdt_tx(tx) for tx in txs) if p]

            used_txids = set()

            for order_code, user_id, username, full_name, category_title, plan_label, amount, created_at in orders:
                amount = float(amount)

                for tx in parsed:
                    txid = tx.get("txid")
                    if not txid or txid in used_txids:
                        continue

                    if tx.get("to") == PAYMENT_ADDRESS and abs(float(tx.get("amount", 0)) - amount) < AUTO_PAY_TOLERANCE:
                        _, new_expires_at, err = await activate_rental_order(order_code)

                        if not err:
                            used_txids.add(txid)
                            try:
                                await bot.send_message(
                                    user_id,
                                    (
                                        "✅ 自动到账\n"
                                        f"订单：<code>{order_code}</code>\n"
                                        f"金额：{amount}U\n"
                                        f"到期：{fmt_ts(new_expires_at)}"
                                    ),
                                    parse_mode="HTML",
                                )
                            except Exception as e:
                                print("notify auto paid error:", repr(e))

                            print("AUTO PAID:", order_code, txid)
                            break

        except asyncio.CancelledError:
            raise
        except Exception as e:
            print("AUTO PAY ERROR:", repr(e))
            traceback.print_exc()

        await asyncio.sleep(AUTO_PAY_INTERVAL)

# ================= APP LIFESPAN =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global BOT_USERNAME, HTTP_SESSION

    HTTP_SESSION = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=20)
    )

    init_db()

    try:
        me = await bot.get_me()
        BOT_USERNAME = me.username
        print("Bot username:", BOT_USERNAME)
    except Exception as e:
        print("get_me error:", repr(e))
        traceback.print_exc()

    if BOT_BASE_URL:
        webhook_url = f"{BOT_BASE_URL}/webhook"
        try:
            await bot.delete_webhook(drop_pending_updates=False)
        except Exception as e:
            print("delete_webhook before set error:", repr(e))

        try:
            await bot.set_webhook(
                url=webhook_url,
                secret_token=TELEGRAM_SECRET_TOKEN or None,
                drop_pending_updates=False,
            )
            print("Webhook set:", webhook_url)
        except Exception as e:
            print("set_webhook error:", repr(e))
            traceback.print_exc()
    else:
        print("BOT_BASE_URL not set, webhook not configured")

    tasks = [
        asyncio.create_task(daily_usdt_update_loop(), name="daily_usdt_update_loop"),
        asyncio.create_task(expiry_warning_loop(), name="expiry_warning_loop"),
        asyncio.create_task(auto_check_payments(), name="auto_check_payments"),
    ]

    try:
        yield
    finally:
        for task in tasks:
            task.cancel()

        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"task shutdown error {task.get_name()}:", repr(e))

        try:
            if HTTP_SESSION and not HTTP_SESSION.closed:
                await HTTP_SESSION.close()
        except Exception as e:
            print("HTTP_SESSION close error:", repr(e))

        try:
            await bot.session.close()
        except Exception as e:
            print("bot session close error:", repr(e))
            
app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"ok": True, "service": "vip666-1"}
    
@app.post("/webhook")
async def telegram_webhook(request: Request):
    try:
        if TELEGRAM_SECRET_TOKEN:
            got_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if got_secret != TELEGRAM_SECRET_TOKEN:
                print("WEBHOOK SECRET MISMATCH")
                return JSONResponse({"ok": False, "error": "secret mismatch"}, status_code=403)

        data = await request.json()
        print("WEBHOOK DATA:", json.dumps(data, ensure_ascii=False)[:2000])

        update = Update.model_validate(data)
        await dp.feed_update(bot, update)

        print("UPDATE FED OK")
        return JSONResponse({"ok": True})
    except Exception as e:
        print("WEBHOOK ERROR:", repr(e))
        traceback.print_exc()
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
        
# ================= STATES =================
class BroadcastFSM(StatesGroup):
    waiting_content = State()
    waiting_confirm = State()

class TrialFSM(StatesGroup):
    waiting_code = State()

class AdminFSM(StatesGroup):
    waiting_add_admin = State()
    waiting_del_admin = State()
    waiting_trial_code = State()

class AddressQueryFSM(StatesGroup):
    waiting_address = State()

# ================= BASIC HELPERS =================
@dp.message(lambda message: message.text and message.text.lower() == "ping")
async def ping_test(message: types.Message):
    print("PING TEST RECEIVED:", message.text)
    await message.answer("pong")
    
def is_cmd(message: types.Message, *cmds):
    if not message or not message.text:
        return False
    head = message.text.strip().split()[0].lower()
    head = head.split("@")[0]
    return head in [c.lower() for c in cmds]

def is_group_message(message: types.Message):
    return bool(message and message.chat and message.chat.type in ("group", "supergroup"))

def is_private(message: types.Message):
    return bool(message and message.chat and message.chat.type == "private")

def should_ignore_message(m: types.Message):
    return (not m or not m.from_user or m.from_user.is_bot or not m.text)

def fmt_num(x):
    if x is None:
        return "0"
    try:
        x = float(x)
        if abs(x - int(x)) < 1e-9:
            return str(int(x))
        return f"{x:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return str(x)

def fmt_ts(ts):
    if not ts:
        return "-"
    try:
        ts = int(ts)
        if ts > 10_000_000_000:
            ts = ts // 1000
        return datetime.fromtimestamp(ts, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "-"

def get_chat_setting(chat_id, key, default=None):
    v = get_setting(chat_id, key, None)
    if v is None and chat_id != -1:
        v = get_setting(-1, key, None)
    return v if v is not None else default

def set_chat_setting(chat_id, key, value):
    set_setting(chat_id, key, value)

def ensure_group(m: types.Message):
    if is_group_message(m):
        save_group(m.chat.id, m.chat.title or "Unnamed group")
        if m.from_user:
            save_member(
                m.chat.id,
                m.from_user.id,
                m.from_user.username or "",
                m.from_user.full_name or "",
            )

@app.head("/")
def home_head():
    return {"ok": True}
    
def get_rate(chat_id):
    try:
        return float(get_chat_setting(chat_id, "rate", "190"))
    except Exception:
        return 190.0

def get_fee(chat_id):
    try:
        return float(get_chat_setting(chat_id, "fee", "7"))
    except Exception:
        return 7.0

def get_enabled(chat_id):
    return str(get_chat_setting(chat_id, "enabled", "1")) == "1"

def is_bot_owner(user_id):
    return bool(BOT_OWNER_ID and int(user_id) == int(BOT_OWNER_ID))

def is_super_admin(user_id):
    return bool(SUPER_ADMIN_ID and int(user_id) == int(SUPER_ADMIN_ID))

def get_user_role(user_id):
    if is_bot_owner(user_id):
        return "owner"
    if is_super_admin(user_id):
        return "super"

    role = get_admin(user_id)
    if role == "super":
        return "super"
    if role == "admin":
        return "admin"
    return None

def can_use_manage_panel(user_id):
    return get_user_role(user_id) in ("owner", "super", "admin")

def can_use_bot_ops(user_id):
    return get_user_role(user_id) in ("owner", "super", "admin")

def can_manage_codes(user_id):
    return get_user_role(user_id) in ("owner", "super")

def can_manage_admins(user_id):
    return get_user_role(user_id) == "owner"

def deny_text():
    return "❌ 无权限"

def has_bot_access(user_id):
    return get_user_role(user_id) in ("owner", "super", "admin") or has_access_user(user_id)

def is_admin_or_operator(chat_id, user: types.User | None):
    if not user:
        return False
    if can_use_bot_ops(user.id):
        return True
    return is_operator(chat_id, user_id=user.id, username=user.username or "")

def is_tron_address(addr: str):
    if not addr:
        return False
    return bool(re.fullmatch(r"T[1-9A-HJ-NP-Za-km-z]{33}", addr.strip()))

def extract_tron_address(text: str):
    if not text:
        return None
    m = TRON_ADDR_RE.search(text.strip())
    return m.group(0) if m else None

async def send_long_text(chat_id, text, reply_markup=None, parse_mode="HTML"):
    text = text or ""
    max_len = 3500

    if len(text) <= max_len:
        return await bot.send_message(
            chat_id,
            text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
        )

    parts = []
    buf = ""
    for line in text.splitlines(True):
        if len(buf) + len(line) > max_len:
            if buf:
                parts.append(buf)
            buf = line
        else:
            buf += line

    if buf:
        parts.append(buf)

    for i, part in enumerate(parts):
        await bot.send_message(
            chat_id,
            part,
            reply_markup=reply_markup if i == len(parts) - 1 else None,
            parse_mode=parse_mode,
        )

def extract_username_only(text: str):
    text = (text or "").strip()
    if not text:
        return None
    if text.startswith("@"):
        text = text[1:].strip()
    if re.fullmatch(r"[A-Za-z0-9_]{4,}", text or ""):
        return text.lower()
    return None


def find_member_by_username(chat_id, username: str):
    username = (username or "").strip().lower()
    if not username:
        return None

    members = get_members(chat_id) or []
    for m in members:
        try:
            if isinstance(m, dict):
                mid = int(m.get("user_id") or 0)
                mun = (m.get("username") or "").strip().lower()
                mname = (m.get("full_name") or "").strip()
            else:
                mid = int(m[1])
                mun = (m[2] or "").strip().lower()
                mname = (m[3] or "").strip()

            if mun == username:
                return {
                    "user_id": mid,
                    "username": mun,
                    "full_name": mname,
                }
        except Exception:
            continue

    return None

def day_range(ts=None):
    dt = datetime.now(BEIJING_TZ) if ts is None else datetime.fromtimestamp(int(ts), BEIJING_TZ)
    start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1) - timedelta(seconds=1)
    return int(start.timestamp()), int(end.timestamp())

def month_range(offset_months=0):
    now = datetime.now(BEIJING_TZ)
    year = now.year
    month = now.month - offset_months

    while month <= 0:
        month += 12
        year -= 1

    start = datetime(year, month, 1, tzinfo=BEIJING_TZ)
    if month == 12:
        nxt = datetime(year + 1, 1, 1, tzinfo=BEIJING_TZ)
    else:
        nxt = datetime(year, month + 1, 1, tzinfo=BEIJING_TZ)

    end = nxt - timedelta(seconds=1)
    return int(start.timestamp()), int(end.timestamp())

def build_vip_welcome_text(display_name, username="", user_id=None, activator_name=None):
    safe_name = escape(display_name or "User")
    safe_username = f"@{escape(username)}" if username else "未设置"
    safe_user_id = str(user_id or "-")

    lines = [
        "╔══════════════════════╗",
        f"   💎 <b>VIP {safe_name}</b> 💎",
        "╚══════════════════════╝",
        "",
        f"👤 客户：{safe_username}",
        f"🆔 ID：<code>{safe_user_id}</code>",
        "🏆 等级：VIP",
        "⚡ 状态：已开通VIP",
    ]

    if activator_name:
        lines.append(f"🔐 开通人：<b>{escape(activator_name)}</b>")

    lines.extend([
        "",
        "━━━━━━━━━━━━━━━━━━",
        "✨ 服务已就绪",
        "请选择下方功能 👇",
    ])

    return "\n".join(lines)

def build_normal_welcome_text(display_name, username="", user_id=None):
    safe_name = escape(display_name or "User")
    safe_username = f"@{escape(username)}" if username else "未设置"
    safe_user_id = str(user_id or "-")

    lines = [
        "╔══════════════════════╗",
        f"   🌟 <b>普通用户 {safe_name}</b> 🌟",
        "╚══════════════════════╝",
        "",
        f"👤 客户：{safe_username}",
        f"🆔 ID：<code>{safe_user_id}</code>",
        "🏆 等级：普通用户",
        "⚡ 状态：未开通",
        "",
        "━━━━━━━━━━━━━━━━━━",
        "✨ 当前账号尚未激活",
        "请先申请试用或输入续费码开通 👇",
    ]

    return "\n".join(lines)

async def get_activator_name(granted_by):
    if not granted_by:
        return None

    try:
        granted_by = int(granted_by)
    except Exception:
        return None

    if BOT_OWNER_ID and granted_by == BOT_OWNER_ID:
        return "Bot Owner"

    if SUPER_ADMIN_ID and granted_by == SUPER_ADMIN_ID:
        return "Super Admin"

    admin_role = get_admin(granted_by)
    if admin_role:
        try:
            chat = await bot.get_chat(granted_by)
            return chat.full_name or chat.username or f"Admin {granted_by}"
        except Exception:
            return f"Admin {granted_by}"

    try:
        chat = await bot.get_chat(granted_by)
        return chat.full_name or chat.username or str(granted_by)
    except Exception:
        return str(granted_by)
        
# ================= AMOUNT PARSER =================
def parse_amount_expr(expr, chat_id, default_direct_unit=False):
    if not expr:
        return None

    expr = expr.strip().replace(" ", "")
    if not expr:
        return None

    body = expr[1:] if expr[0] in "+-" else expr
    body = body.strip()
    if not body:
        return None

    rate_default = get_rate(chat_id)
    fee_default = get_fee(chat_id)

    # Ví dụ: 777u
    if body.lower().endswith("u"):
        num = body[:-1]
        try:
            unit_amount = abs(float(num))
            return {
                "raw_amount": None,
                "unit_amount": unit_amount,
                "rate_used": rate_default,
                "fee_used": 0.0,
            }
        except Exception:
            return None

    # Ví dụ: 1000/7.8
    if "/" in body:
        try:
            raw_s, rate_s = body.split("/", 1)
            raw_amount = abs(float(raw_s))
            rate_used = float(rate_s)
            if rate_used == 0:
                return None

            fee_used = fee_default
            unit_amount = raw_amount / rate_used * (1 - fee_used / 100.0)

            return {
                "raw_amount": raw_amount,
                "unit_amount": unit_amount,
                "rate_used": rate_used,
                "fee_used": fee_used,
            }
        except Exception:
            return None

    # Ví dụ: 100*1.2
    if "*" in body:
        try:
            left_s, right_s = body.split("*", 1)
            raw_amount = abs(float(left_s))
            factor = float(right_s)
            unit_amount = raw_amount * factor

            return {
                "raw_amount": raw_amount,
                "unit_amount": unit_amount,
                "rate_used": factor,
                "fee_used": 0.0,
            }
        except Exception:
            return None

    # Ví dụ: 1000
    try:
        val = abs(float(body))
    except Exception:
        return None

    if default_direct_unit:
        return {
            "raw_amount": None,
            "unit_amount": val,
            "rate_used": rate_default,
            "fee_used": 0.0,
        }

    unit_amount = val / rate_default * (1 - fee_default / 100.0)
    return {
        "raw_amount": val,
        "unit_amount": unit_amount,
        "rate_used": rate_default,
        "fee_used": fee_default,
    }

# ================= UI =================
def menu_kb(user_id=None):
    keyboard = [
        [KeyboardButton(text="🔥 开始记账")],
        [
            KeyboardButton(text="💎 申请试用"),
            KeyboardButton(text="📝 使用说明"),
        ],
        [
            KeyboardButton(text="📈 实时U价"),
            KeyboardButton(text="🔍 地址查询"),
        ],
        [
            KeyboardButton(text="🔑 自助续费"),
            KeyboardButton(text="📜 交易历史"),
        ],
    ]

    if user_id is not None and can_use_manage_panel(user_id):
        keyboard.append([KeyboardButton(text="🛠 管理面板")])

    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
    )

def start_inline_kb(user_id=None):
    if BOT_USERNAME:
        add_url = f"https://t.me/{BOT_USERNAME}?startgroup=add"
    else:
        add_url = "https://t.me/"

    buttons = [
        [InlineKeyboardButton(text="➕ 添加机器人到群", url=add_url)],
        [InlineKeyboardButton(text="📝 使用说明", callback_data="menu:help")],
    ]

    return InlineKeyboardMarkup(inline_keyboard=buttons)

def copy_cmd_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 复制：开始", copy_text=CopyTextButton(text="开始")),
            InlineKeyboardButton(text="📋 复制：总账单", copy_text=CopyTextButton(text="总账单")),
        ],
        [
            InlineKeyboardButton(text="📋 复制：设置汇率190", copy_text=CopyTextButton(text="设置汇率190")),
            InlineKeyboardButton(text="📋 复制：设置费率7", copy_text=CopyTextButton(text="设置费率7")),
        ],
        [
            InlineKeyboardButton(text="📋 复制：地址查询", copy_text=CopyTextButton(text="地址查询")),
            InlineKeyboardButton(text="📋 复制：撤销", copy_text=CopyTextButton(text="撤销")),
        ],
        [
            InlineKeyboardButton(text="📋 复制：群发广播", copy_text=CopyTextButton(text="群发广播")),
            InlineKeyboardButton(text="📋 复制：使用说明", copy_text=CopyTextButton(text="使用说明")),
        ],
    ])

def begin_copy_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 开始", callback_data="copy:开始"),
            InlineKeyboardButton(text="📋 关闭记账", callback_data="copy:关闭记账"),
        ],
        [
            InlineKeyboardButton(text="📋 设置汇率190", callback_data="copy:设置汇率190"),
            InlineKeyboardButton(text="📋 设置费率7", callback_data="copy:设置费率7"),
        ],
        [
            InlineKeyboardButton(text="📋 +1000", callback_data="copy:+1000"),
            InlineKeyboardButton(text="📋 -1000", callback_data="copy:-1000"),
        ],
        [
            InlineKeyboardButton(text="📋 下发5000", callback_data="copy:下发5000"),
            InlineKeyboardButton(text="📋 P+2000", callback_data="copy:P+2000"),
        ],
        [
            InlineKeyboardButton(text="📋 总账单", callback_data="copy:总账单"),
            InlineKeyboardButton(text="📋 撤销", callback_data="copy:撤销"),
        ],
    ])

def manage_panel_kb(user_id):
    rows = []

    if can_manage_admins(user_id):
        rows.append([
            InlineKeyboardButton(text="➕ 添加管理员", callback_data="manage:add_admin"),
            InlineKeyboardButton(text="➖ 删除管理员", callback_data="manage:del_admin"),
        ])

    if can_use_manage_panel(user_id):
        rows.append([
            InlineKeyboardButton(text="📋 管理员列表", callback_data="manage:list_admin"),
        ])
        rows.append([
            InlineKeyboardButton(text="🧾 待支付订单", callback_data="order:list_pending"),
            InlineKeyboardButton(text="📦 订单历史", callback_data="order:history:all"),
        ])

    if can_manage_codes(user_id):
        rows.append([
            InlineKeyboardButton(text="🔑 创建续费码", callback_data="manage:create_code"),
            InlineKeyboardButton(text="🗑 回收续费码", callback_data="manage:revoke_code"),
        ])

    if not rows:
        rows = [[InlineKeyboardButton(text="❌ 无权限", callback_data="noop")]]

    return InlineKeyboardMarkup(inline_keyboard=rows)

def report_kb(chat_id):
    rows = []

    if WEB_BASE_URL:
        today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
        params = urlencode({"date": today})
        group_url = f"{WEB_BASE_URL}/group/{chat_id}?{params}"
        rows.append([InlineKeyboardButton(text="🧾 查看本群账单", url=group_url)])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def history_groups_kb():
    groups = get_groups()
    rows = []

    if not WEB_BASE_URL:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚠️ WEB_BASE_URL 未配置", callback_data="noop")]
        ])

    for chat_id, title in groups:
        today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
        params = urlencode({"date": today})
        url = f"{WEB_BASE_URL}/group/{chat_id}?{params}"
        rows.append([InlineKeyboardButton(text=f"📂 {title}", url=url)])

    if not rows:
        rows.append([InlineKeyboardButton(text="暂无群组", callback_data="noop")])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def order_history_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 全部订单", callback_data="order:history:all")],
        [
            InlineKeyboardButton(text="⏳ 待支付", callback_data="order:history:pending"),
            InlineKeyboardButton(text="✅ 已支付", callback_data="order:history:paid"),
        ],
        [InlineKeyboardButton(text="❌ 已拒绝", callback_data="order:history:rejected")],
    ])

def address_result_kb(address, page=1):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📜 链上交易记录", callback_data=f"addr:tx:{address}:{page}"),
        ],
        [
            InlineKeyboardButton(text="🔄 重新查询", callback_data="addr:again"),
            InlineKeyboardButton(text="⬅️ 返回菜单", callback_data="addr:back"),
        ],
    ])

def tx_history_kb(address, page=1):
    buttons = []
    if page > 1:
        buttons.append(
            InlineKeyboardButton(
                text="⬅️ 上一页",
                callback_data=f"addr:tx:{address}:{page-1}"
            )
        )

    buttons.append(
        InlineKeyboardButton(
            text=f"📄 第 {page} 页",
            callback_data="noop"
        )
    )

    buttons.append(
        InlineKeyboardButton(
            text="下一页 ➡️",
            callback_data=f"addr:tx:{address}:{page+1}"
        )
    )

    return InlineKeyboardMarkup(inline_keyboard=[buttons])

# ================= RENT UI =================
RENT_CATEGORIES = {
    "group_admin": {"title": "🤖 Bot quản trị nhóm"},
    "computer": {"title": "💻 Bot máy tính"},
    "translator": {"title": "🌐 Bot dịch thuật"},
}

RENT_PLANS = {
    "1m": {"label": "一个月", "amount": 100},
    "3m": {"label": "三个月", "amount": 230},
    "6m": {"label": "六个月", "amount": 400},
    "1y": {"label": "一年", "amount": 700},
}

def rent_main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 Bot quản trị nhóm", callback_data="rent:group_admin")],
        [InlineKeyboardButton(text="💻 Bot máy tính", callback_data="rent:computer")],
        [InlineKeyboardButton(text="🌐 Bot dịch thuật", callback_data="rent:translator")],
    ])

def rent_plan_kb(category_key):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="一个月 (100U)", callback_data=f"rent:plan:{category_key}:1m")],
        [InlineKeyboardButton(text="三个月 (230U)", callback_data=f"rent:plan:{category_key}:3m")],
        [InlineKeyboardButton(text="六个月 (400U)", callback_data=f"rent:plan:{category_key}:6m")],
        [InlineKeyboardButton(text="一年 (700U)", callback_data=f"rent:plan:{category_key}:1y")],
        [InlineKeyboardButton(text="⬅️ 返回", callback_data="rent:main")],
    ])

def rent_payment_text(category_key, plan_key, order_code):
    cat = RENT_CATEGORIES.get(category_key, {})
    plan = RENT_PLANS.get(plan_key, {})

    title = escape(cat.get("title", "套餐"))
    plan_label = escape(plan.get("label", ""))
    amount = plan.get("amount", 0)

    return (
        f"✅ <b>{title}</b>\n"
        f"📦 套餐：<b>{plan_label}</b>\n"
        f"🧾 订单号：<code>{escape(str(order_code))}</code>\n\n"
        f"🌿 <b>收款地址：TRC20-USDT</b>\n"
        f"├ 💰订单金额：<b>{amount} U</b>\n"
        f"└➤ <code>{escape(PAYMENT_ADDRESS)}</code>\n\n"
        f"请按指定金额转账，付款后等待管理员审核开通。\n"
        f"🗣️ 在线客服：<code>{escape(PAYMENT_SUPPORT)}</code>"
    )

def rent_payment_kb(amount):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 复制地址", callback_data=f"copy:{PAYMENT_ADDRESS}"),
            InlineKeyboardButton(text=f"📋 复制金额 {amount}U", callback_data=f"copy:{amount}"),
        ],
        [
            InlineKeyboardButton(text="⬅️ 返回套餐", callback_data="rent:main"),
            InlineKeyboardButton(text="🔄 重新选择", callback_data="rent:back"),
        ],
    ])

# ================= TEXTS =================
def help_text():
    return (
        "📚 记账机器人使用说明\n\n"
        "【基础功能】\n"
        "• 开始记账：开始 / 🔥 开始记账\n"
        "• 停止记账：关闭记账 / 停止记账\n"
        "• 打开发言：上课\n"
        "• 停止发言：下课\n\n"
        "【参数设置】\n"
        "• 设置汇率：设置汇率190\n"
        "• 设置费率：设置费率7\n\n"
        "【记账指令】\n"
        "• +1000 / -1000\n"
        "• +1000/7.8 / -1000/7.8\n"
        "• +7777u / -7777u\n"
        "• 下发5000 / 下发1000R\n"
        "• P+2000 / P-1000\n"
        "• +1000 备注\n\n"
        "【查看功能】\n"
        "• 总账单\n"
        "• 账单\n"
        "• /我\n"
        "• 撤销\n"
        "• 上个月总账单\n\n"
        "【试用与续费】\n"
        "• 首次可领取 24 小时免费试用权限\n"
        "• 到期后可输入管理员发放的续费码\n"
        "• 或使用自助续费菜单提交租用订单\n"
    )

def begin_help_text():
    return (
        "🔥 <b>开始记账</b>\n\n"
        "请先将机器人添加到群聊，并授予必要权限。\n\n"
        "<b>常用命令</b>\n"
        "• <code>开始</code>\n"
        "• <code>关闭记账</code>\n"
        "• <code>设置汇率190</code>\n"
        "• <code>设置费率7</code>\n"
        "• <code>+1000</code>\n"
        "• <code>-1000</code>\n"
        "• <code>下发5000</code>\n"
        "• <code>P+2000</code>\n"
        "• <code>总账单</code>\n"
        "• <code>撤销</code>\n"
    )

def address_query_text():
    return (
        "🔍 <b>地址查询</b>\n\n"
        "请直接发送 TRON 地址进行查询。\n\n"
        "<b>示例：</b>\n"
        "<code>TSPpLmYuFXLi6GU1W4uyG6NKGbdWPw886U</code>"
    )

def group_feature_text():
    return (
        "👥 <b>分组功能说明</b>\n\n"
        "支持以下用法：\n"
        "• 直接记账：<code>+1000</code>\n"
        "• 指定目标：<code>张三+1000</code>\n"
        "• 回复某人消息后输入：<code>+1000</code>\n"
        "• 下发：<code>下发5000</code>\n"
        "• 寄存：<code>P+2000</code>"
    )

@dp.message(AdminFSM.waiting_del_admin)
async def process_del_admin(message: types.Message, state: FSMContext):
    if should_ignore_message(message):
        return

    if not can_manage_admins(message.from_user.id):
        await message.answer("❌ 无权限")
        await state.clear()
        return

    ensure_group(message)

    username = extract_username_only(message.text or "")
    if not username:
        await message.answer(
            "请发送要删除的操作员用户名。\n\n"
            "格式：@username\n\n"
            "例如：@abc123"
        )
        return

    target = find_member_by_username(message.chat.id, username)
    if not target:
        await message.answer(
            "❌ 未找到该用户。\n\n"
            "请确认用户名正确，且对方曾在本群发言。"
        )
        return

    target_id = int(target["user_id"])
    remove_admin(target_id)
    await state.clear()

    await message.answer(
        f"✅ 已删除操作员\n用户名：@{escape(target.get('username') or username)}\nID：<code>{target_id}</code>",
        parse_mode="HTML",
    )

@dp.message(AdminFSM.waiting_add_admin)
async def process_add_admin(message: types.Message, state: FSMContext):
    if should_ignore_message(message):
        return

    if not can_manage_admins(message.from_user.id):
        await message.answer("❌ 无权限")
        await state.clear()
        return

    ensure_group(message)

    username = extract_username_only(message.text or "")
    if not username:
        await message.answer(
            "请发送要添加的操作员用户名。\n\n"
            "格式：@username\n\n"
            "例如：@abc123"
        )
        return

    target = find_member_by_username(message.chat.id, username)
    if not target:
        await message.answer(
            "❌ 未找到该用户。\n\n"
            "请确认：\n"
            "1. 对方已经在群里发过言\n"
            "2. 用户名输入正确\n"
            "3. 格式必须是 @username"
        )
        return

    target_id = int(target["user_id"])
    target_username = target.get("username") or ""
    target_name = target.get("full_name") or ""

    add_admin(target_id, "admin")
    await state.clear()

    await message.answer(
        "✅ 已添加操作员\n"
        f"用户名：@{escape(target_username)}\n"
        f"姓名：{escape(target_name) if target_name else '未设置'}\n"
        f"ID：<code>{target_id}</code>\n\n"
        "现在对方可以使用机器人的操作功能。",
        parse_mode="HTML",
    )
    
# ================= REPORT HELPERS =================
def split_target_prefix(text):
    t = (text or "").strip()
    markers = ["下发", "P+", "P-", "+", "-"]
    for mk in markers:
        pos = t.find(mk)
        if pos > 0:
            target = t[:pos].strip()
            body = t[pos:].strip()
            if target:
                return target, body
    return None, t

def format_tx_line(tx):
    (
        tx_id, chat_id, user_id, username, display_name, target_name, kind,
        raw_amount, unit_amount, rate_used, fee_used, note, original_text,
        created_at, undone
    ) = tx

    tm = datetime.fromtimestamp(created_at, BEIJING_TZ).strftime("%H:%M:%S")
    safe_target = escape(target_name) if target_name else ""
    safe_note = escape(note) if note else ""

    if kind == "reserve":
        line = f"{tm} {fmt_num(unit_amount)}U"
        if safe_target:
            line += f" {safe_target}"
        if safe_note:
            line += f" {safe_note}"
        return line.strip()

    if raw_amount is not None:
        if fee_used:
            line = f"{tm} {fmt_num(raw_amount)} / {fmt_num(rate_used)} * ({1 - fee_used/100:.2f}) = {fmt_num(unit_amount)}U"
        else:
            line = f"{tm} {fmt_num(raw_amount)} / {fmt_num(rate_used)} = {fmt_num(unit_amount)}U"
    else:
        line = f"{tm} {fmt_num(unit_amount)}U"

    extra = []
    if safe_target:
        extra.append(safe_target)
    if safe_note:
        extra.append(safe_note)

    if extra:
        line += " " + " ".join(extra)

    return line.strip()

def summarize_transactions(txs):
    income = [t for t in txs if t[6] == "income"]
    payout = [t for t in txs if t[6] == "payout"]
    reserve = [t for t in txs if t[6] == "reserve"]

    total_income_unit = sum((t[8] or 0) for t in income)
    total_payout_unit = sum((t[8] or 0) for t in payout)
    total_reserve_unit = sum((t[8] or 0) for t in reserve)

    due = total_income_unit + total_reserve_unit
    paid = total_payout_unit
    pending = due - paid

    total_raw_income = sum((abs(t[7]) or 0) for t in income if t[7] is not None)

    return {
        "income_count": len(income),
        "payout_count": len(payout),
        "reserve_count": len(reserve),
        "total_income_unit": total_income_unit,
        "total_payout_unit": total_payout_unit,
        "total_reserve_unit": total_reserve_unit,
        "due": due,
        "paid": paid,
        "pending": pending,
        "total_raw_income": total_raw_income,
    }

def report_text(chat_id, start_ts, end_ts, title="账单", user_id=None, display_name=None):
    txs = get_transactions(chat_id, start_ts=start_ts, end_ts=end_ts, user_id=user_id)
    stats = summarize_transactions(txs)

    income_txs = [t for t in txs if t[6] == "income"]
    payout_txs = [t for t in txs if t[6] == "payout"]
    reserve_txs = [t for t in txs if t[6] == "reserve"]

    lines = [f"📘 <b>{escape(title)}</b>"]
    if display_name:
        lines.append(f"👤 用户：{escape(display_name)}")

    lines.append("")
    lines.append(f"🟢 <b>入款（{len(income_txs)}笔）</b>")
    if income_txs:
        for tx in income_txs:
            lines.append(format_tx_line(tx))
    else:
        lines.append("暂无入款")

    lines.append("")
    lines.append(f"🔵 <b>下发（{len(payout_txs)}笔）</b>")
    if payout_txs:
        for tx in payout_txs:
            lines.append(format_tx_line(tx))
    else:
        lines.append("暂无下发")

    if reserve_txs:
        lines.append("")
        lines.append(f"🟣 <b>寄存（{len(reserve_txs)}笔）</b>")
        for tx in reserve_txs:
            lines.append(format_tx_line(tx))

    # Thêm lại phần 分组统计
    if user_id is None:
        lines.append("")
        lines.append("📂 <b>分组统计</b>")
        group_map = {}

        for tx in income_txs:
            key = escape(tx[5] or "未命名")
            group_map.setdefault(key, 0.0)
            group_map[key] += float(tx[8] or 0)

        if group_map:
            for k, v in group_map.items():
                lines.append(f"{k} 入:{fmt_num(v)}U")
        else:
            lines.append("暂无分组数据")

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━")
    lines.append(f"💰 总入款：{fmt_num(stats['total_raw_income'])} ({fmt_num(stats['total_income_unit'])}U)")
    lines.append(f"📈 汇率：{fmt_num(get_rate(chat_id))}")
    lines.append(f"📉 交易费率：{fmt_num(get_fee(chat_id))}%")
    lines.append("")
    lines.append(f"📦 应下发：{fmt_num(stats['due'])}U")
    lines.append(f"✅ 已下发：{fmt_num(stats['paid'])}U")
    lines.append(f"⏳ 未下发：{fmt_num(stats['pending'])}U")

    return "\n".join(lines)

# ================= TRON API =================
async def trongrid_get(path, params=None):
    headers = {
        "accept": "application/json",
        "user-agent": "Mozilla/5.0",
    }
    if TRONGRID_API_KEY:
        headers["TRON-PRO-API-KEY"] = TRONGRID_API_KEY

    url = path if path.startswith("http") else f"{TRONGRID_API_URL}{path}"

    if HTTP_SESSION is None:
        return {}

    async with HTTP_SESSION.get(url, params=params, headers=headers) as resp:
        if resp.status != 200:
            return {}
        return await resp.json()

def _pick_account(payload):
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get("data"), list) and payload["data"]:
        return payload["data"][0]
    if payload.get("address"):
        return payload
    if isinstance(payload.get("data"), dict):
        return payload["data"]
    return None

def _parse_trc20_usdt(account):
    if not isinstance(account, dict):
        return None

    candidates = [
        "trc20token_balances",
        "trc20",
        "tokenBalances",
        "tokens",
        "assetV2",
    ]

    for key in candidates:
        items = account.get(key)
        if not items:
            continue

        if isinstance(items, dict):
            items = [items]
        if not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, dict):
                continue

            sym = str(
                item.get("tokenAbbr")
                or item.get("symbol")
                or item.get("tokenName")
                or item.get("name")
                or ""
            ).upper()

            contract = str(
                item.get("contract_address")
                or item.get("tokenAddress")
                or item.get("tokenId")
                or item.get("contract")
                or ""
            )

            if sym == "USDT" or contract == USDT_TRC20_CONTRACT:
                raw = (
                    item.get("balance")
                    or item.get("value")
                    or item.get("amount")
                    or item.get("tokenValue")
                )
                if raw is None:
                    return 0.0

                try:
                    decimals = int(item.get("precision") or item.get("decimals") or 6)
                except Exception:
                    decimals = 6

                try:
                    return float(raw) / (10 ** decimals)
                except Exception:
                    try:
                        return float(raw)
                    except Exception:
                        return 0.0
    return None

async def check_tron_address(address: str):
    def _fetch():
        headers = {
            "accept": "application/json",
            "user-agent": "Mozilla/5.0",
        }
        if TRONGRID_API_KEY:
            headers["TRON-PRO-API-KEY"] = TRONGRID_API_KEY

        sources = [
            f"https://api.trongrid.io/v1/accounts/{address}",
            f"https://apilist.tronscanapi.com/api/account?address={address}",
        ]

        for url in sources:
            try:
                r = requests.get(url, timeout=8, headers=headers)
                if not r.ok:
                    continue
                payload = r.json()
                acc = _pick_account(payload)
                if acc:
                    source_name = "trongrid" if "trongrid" in url else "tronscan"
                    return {"source": source_name, "account": acc}
            except Exception as e:
                print("wallet api error:", url, e)

        return None

    result = await asyncio.to_thread(_fetch)
    if not result:
        return None

    acc = result["account"]

    trx_balance = None
    try:
        if acc.get("balance") is not None:
            trx_balance = float(acc.get("balance")) / 1_000_000
    except Exception as e:
        print("trx_balance parse error:", e)

    usdt_balance = _parse_trc20_usdt(acc)

    tx_count = (
        acc.get("transaction_count")
        or acc.get("txCount")
        or acc.get("transactionsCount")
        or acc.get("totalTransactionCount")
        or acc.get("trxCount")
        or None
    )
    try:
        tx_count = int(tx_count) if tx_count is not None else None
    except Exception:
        tx_count = None

    create_time = (
        acc.get("create_time")
        or acc.get("createTime")
        or acc.get("create_time_ms")
        or acc.get("createTimeMs")
    )
    latest_time = (
        acc.get("latest_opration_time")
        or acc.get("latestOperationTime")
        or acc.get("latest_operation_time")
        or acc.get("latest_tx_time")
    )

    return {
        "source": result["source"],
        "address": address,
        "trx_balance": trx_balance,
        "usdt_balance": usdt_balance,
        "tx_count": tx_count,
        "create_time": create_time,
        "latest_time": latest_time,
        "raw": acc,
    }


async def get_tron_transactions(address, page=1, page_size=10):
    offset = max(0, (page - 1) * page_size)

    tx_data = await trongrid_get(
        f"/v1/accounts/{address}/transactions",
        params={
            "limit": page_size,
            "only_confirmed": "true",
            "order_by": "block_timestamp,desc",
            "offset": offset,
        },
    )

    return tx_data.get("data", []) if tx_data else []


def format_tron_tx_row(tx):
    try:
        ts = tx.get("block_timestamp")
        dt = datetime.fromtimestamp(ts / 1000, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S") if ts else "-"
        txid = tx.get("txID", "-")
        contract = tx.get("raw_data", {}).get("contract", [])
        tx_type = "-"
        if contract:
            tx_type = contract[0].get("type", "-")

        return f"• {dt} | {escape(tx_type)}\n  <code>{escape(txid)}</code>"
    except Exception:
        return "• 无法解析交易"


def format_address_info_text(address, info, sender_name=None, user_send_count=None):
    if not info:
        return (
            f"🔎 <b>地址查询结果</b>\n\n"
            f"📌 地址：<code>{escape(address)}</code>\n"
            "⚠️ 无法获取链上数据，请稍后重试。"
        )

    trx_balance = info.get("trx_balance", 0)
    usdt_balance = info.get("usdt_balance", 0)
    tx_count = info.get("tx_count", 0)
    first_tx = info.get("create_time") or "-"
    last_active = info.get("latest_time") or "-"
    sig_status = "已签名地址" if (tx_count or 0) > 0 else "新钱包 / 未签名地址"

    lines = [
        "🔎 <b>TRON 地址查询</b>",
        "",
    ]

    if sender_name:
        lines.append(f"👤 查询人：<code>{escape(sender_name)}</code>")

    if user_send_count is not None:
        lines.append(f"📌 本群发送次数：<b>{user_send_count}</b> 次")

    lines.extend([
        f"📌 地址：<code>{escape(address)}</code>",
        f"💰 TRX：<b>{fmt_num(trx_balance)}</b>",
        f"💰 USDT：<b>{fmt_num(usdt_balance)}</b>",
        f"📊 交易次数：<b>{tx_count if tx_count is not None else 0}</b>",
        f"🔰 状态：<b>{sig_status}</b>",
        f"📡 数据来源：<b>{escape(str(info.get('source', '-')))}</b>",
        f"⏰ 首次交易：<b>{fmt_ts(first_tx)}</b>",
        f"🌟 最后活跃：<b>{fmt_ts(last_active)}</b>",
    ])

    return "\n".join(lines)

def make_wallet_card_image(
    address,
    sender_name,
    trx_balance=None,
    usdt_balance=None,
    tx_count=None,
    source="trongrid",
    create_time=None,
    latest_time=None,
):
    width, height = 1080, 1350

    top_green = (18, 185, 150)
    top_green2 = (16, 165, 138)
    body_bg = (20, 30, 44)
    panel_bg = (26, 40, 58)
    panel_bg2 = (30, 46, 66)
    white = (245, 248, 250)
    mute = (165, 180, 190)
    gold = (245, 198, 76)
    blue = (120, 185, 255)
    green = (100, 235, 160)
    red = (255, 120, 120)

    img = Image.new("RGB", (width, height), body_bg)
    draw = ImageDraw.Draw(img)

    for y in range(height):
        if y < 330:
            r = int(top_green[0] * (1 - y / 330) + top_green2[0] * (y / 330))
            g = int(top_green[1] * (1 - y / 330) + top_green2[1] * (y / 330))
            b = int(top_green[2] * (1 - y / 330) + top_green2[2] * (y / 330))
            draw.line([(0, y), (width, y)], fill=(r, g, b))
        else:
            draw.line([(0, y), (width, y)], fill=body_bg)

    font_candidates = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJKSC-Regular.otf",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]

    def load_font(size):
        for fp in font_candidates:
            try:
                return ImageFont.truetype(fp, size)
            except Exception:
                pass
        return ImageFont.load_default()

    font_title = load_font(54)
    font_sub = load_font(28)
    font_mid = load_font(32)

    def box(x1, y1, x2, y2, radius=26, fill=panel_bg, outline=None, width=2):
        draw.rounded_rectangle((x1, y1, x2, y2), radius=radius, fill=fill, outline=outline, width=width)

    def text(x, y, s, font, fill=white):
        draw.text((x, y), str(s), font=font, fill=fill)

    def center_text(y, s, font, fill=white):
        bbox = draw.textbbox((0, 0), str(s), font=font)
        w = bbox[2] - bbox[0]
        x = (width - w) // 2
        draw.text((x, y), str(s), font=font, fill=fill)

    def fmt_time_local(ts):
        if not ts:
            return "N/A"
        try:
            ts = int(ts)
            if ts > 10_000_000_000:
                ts = ts // 1000
            return datetime.fromtimestamp(ts, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return "N/A"

    box(40, 35, 1040, 300, radius=36, fill=top_green2, outline=(255, 255, 255, 40), width=2)
    box(90, 198, 990, 250, radius=18, fill=(60, 130, 108), outline=(220, 255, 240), width=2)
    center_text(70, "USDT防篡改验证核对", font_title, fill=white)
    center_text(144, "《请双方谨慎核对地址是否与图中一致，如有误停止付款》", font_sub, fill=(232, 247, 242))
    center_text(209, address, font_mid, fill=white)
    center_text(258, f"查询人: {sender_name}", font_sub, fill=(225, 245, 240))

    box(40, 330, 1040, 1140, radius=34, fill=panel_bg, outline=(42, 70, 90), width=2)
    text(70, 360, "🔎 查询地址：", font_mid, fill=white)
    text(250, 360, address, font_mid, fill=blue)

    box(60, 460, 1020, 1030, radius=28, fill=panel_bg2, outline=(55, 90, 110), width=2)
    tx_status = "已签名地址" if (tx_count or 0) > 0 else "未签名地址"
    tx_status_color = green if (tx_count or 0) > 0 else red

    rows = [
        ("💡 交易次数", str(tx_count if tx_count is not None else "N/A"), white),
        ("⏰ 首次交易", fmt_time_local(create_time), white),
        ("🌟 最后活跃", fmt_time_local(latest_time), white),
        ("🛡 签名状态", tx_status, tx_status_color),
        ("💰 USDT 余额", f"{fmt_num(usdt_balance)} USDT", gold),
        ("💰 TRX 余额", f"{fmt_num(trx_balance)} TRX", gold),
        ("📡 数据来源", str(source), mute),
    ]

    y = 500
    gap = 70
    for label, value, value_color in rows:
        text(85, y, f"{label}：", font_mid, fill=white)
        text(330, y, value, font_mid, fill=value_color)
        y += gap

    bio = BytesIO()
    img.save(bio, "PNG")
    bio.seek(0)
    return BufferedInputFile(bio.read(), filename="usdt_check_cn.png")
    
# ================= USDT RATE =================
async def fetch_usdt_rates():
    urls = [
        "https://open.er-api.com/v6/latest/USD",
        "https://api.exchangerate.host/latest?base=USD&symbols=CNY,VND",
    ]

    if HTTP_SESSION is None:
        return None

    for url in urls:
        try:
            async with HTTP_SESSION.get(url) as resp:
                data = await resp.json()

                if data.get("result") == "success" and "rates" in data:
                    rates = data["rates"]
                    return {
                        "usd_cny": float(rates.get("CNY")) if rates.get("CNY") else None,
                        "usd_vnd": float(rates.get("VND")) if rates.get("VND") else None,
                    }

                rates = data.get("rates", {})
                return {
                    "usd_cny": float(rates.get("CNY")) if rates.get("CNY") else None,
                    "usd_vnd": float(rates.get("VND")) if rates.get("VND") else None,
                }
        except Exception as e:
            print("fetch_usdt_rates error:", e)

    return None

def format_usdt_rate_text(rates):
    now_str = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    cny = rates.get("usd_cny") if rates else None
    vnd = rates.get("usd_vnd") if rates else None

    lines = ["📈 <b>实时U价</b>", ""]

    if cny:
        lines.append(f"🇨🇳 市场价：<code>{cny:.4f}</code> CNY / USDT")
        lines.append(f"• 1 CNY ≈ <code>{1/cny:.4f}</code> USDT")
    else:
        lines.append("🇨🇳 市场价：<i>获取失败</i>")

    if vnd:
        lines.append(f"🇻🇳 市场价：<code>{vnd:,.0f}</code> VND / USDT")
        lines.append(f"• 1 VND ≈ <code>{1/vnd:.8f}</code> USDT")
    else:
        lines.append("🇻🇳 市场价：<i>获取失败</i>")

    lines += ["", f"🕒 更新时间：<code>{now_str}</code>"]
    return "\n".join(lines)

def rate_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 刷新价格", callback_data="rate:refresh")],
        [InlineKeyboardButton(text="📝 使用说明", callback_data="menu:help")],
    ])

async def get_usdt_rates_cached(force=False):
    now = time.time()
    if not force and RATE_CACHE["value"] and (now - RATE_CACHE["ts"] < RATE_CACHE_TTL):
        return RATE_CACHE["value"]

    rates = await fetch_usdt_rates()
    if rates:
        RATE_CACHE["value"] = rates
        RATE_CACHE["ts"] = now
        return rates
    return RATE_CACHE["value"]

async def daily_usdt_update_loop():
    while True:
        try:
            now = datetime.now(BEIJING_TZ)
            today_key = now.strftime("%Y-%m-%d")
            target_time = now.replace(hour=8, minute=0, second=0, microsecond=0)
            last_update_date = get_setting(-1, USDT_DAILY_UPDATE_KEY, "")

            if now >= target_time and last_update_date != today_key:
                rates = await fetch_usdt_rates()
                if rates:
                    RATE_CACHE["value"] = rates
                    RATE_CACHE["ts"] = time.time()
                    set_setting(-1, USDT_DAILY_UPDATE_KEY, today_key)
                    print(f"[USDT] Updated at {now.strftime('%Y-%m-%d %H:%M:%S')} Beijing time")

            if now < target_time:
                sleep_seconds = (target_time - now).total_seconds()
                await asyncio.sleep(min(sleep_seconds, 60))
            else:
                await asyncio.sleep(60)
        except Exception as e:
            print("daily_usdt_update_loop error:", e)
            await asyncio.sleep(60)

# ================= RENEW / EXPIRY =================
def plan_duration_seconds(plan_key):
    if plan_key == "1m":
        return 30 * 24 * 60 * 60
    if plan_key == "3m":
        return 90 * 24 * 60 * 60
    if plan_key == "6m":
        return 180 * 24 * 60 * 60
    if plan_key == "1y":
        return 365 * 24 * 60 * 60
    return 30 * 24 * 60 * 60

def calc_renew_expire_at(user_id, plan_key):
    now_ts = int(time.time())
    duration = plan_duration_seconds(plan_key)

    access_row = get_access_user_by_id(user_id)
    current_exp = None
    if access_row and len(access_row) >= 5:
        current_exp = access_row[4]

    base_ts = now_ts
    if current_exp and int(current_exp) > now_ts:
        base_ts = int(current_exp)

    return base_ts + duration

async def activate_rental_order(order_code, granted_by=None):
    row = get_rental_order(order_code)
    if not row:
        return None, None, "订单不存在"

    (
        order_code, user_id, username, full_name, category_key, category_title,
        plan_key, plan_label, amount, status, created_at, paid_at, expires_at, note
    ) = row

    if status == "paid":
        return row, expires_at, "订单已支付"

    new_expires_at = calc_renew_expire_at(user_id, plan_key)

    mark_rental_order_paid(order_code, expires_at=new_expires_at)
    add_access_user(
        user_id=user_id,
        username=username or "",
        granted_by=granted_by,
        expires_at=new_expires_at,
    )

    return row, new_expires_at, None

async def expiry_warning_loop():
    while True:
        try:
            now_ts = int(time.time())
            rows = get_access_users()

            for row in rows:
                user_id, username, granted_by, granted_at, expires_at = row

                if not expires_at:
                    continue

                expires_at = int(expires_at)
                remain = expires_at - now_ts

                if remain <= 0:
                    notice_key = "expired"
                    if not has_expiry_notice(user_id, notice_key):
                        add_expiry_notice(user_id, notice_key)
                        try:
                            await bot.send_message(
                                user_id,
                                "⏳ 您的使用权限已到期，请尽快续费。",
                                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                    [InlineKeyboardButton(text="🔑 立即续费", callback_data="rent:main")]
                                ]),
                            )
                        except Exception as e:
                            print("expired notify failed:", e)
                    continue

                warning_map = [
                    (7 * 24 * 3600, "7d", "7 天"),
                    (3 * 24 * 3600, "3d", "3 天"),
                    (1 * 24 * 3600, "1d", "1 天"),
                    (1 * 3600, "1h", "1 小时"),
                ]

                for threshold, key, label in warning_map:
                    if remain <= threshold and remain > threshold - 3600:
                        notice_key = f"warn_{key}"
                        if not has_expiry_notice(user_id, notice_key):
                            add_expiry_notice(user_id, notice_key)
                            try:
                                expire_str = datetime.fromtimestamp(expires_at, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
                                await bot.send_message(
                                    user_id,
                                    (
                                        f"⚠️ 您的权限将在 <b>{label}</b> 后到期。\n\n"
                                        f"到期时间：<code>{expire_str}</code>\n"
                                        "请及时续费。"
                                    ),
                                    parse_mode="HTML",
                                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                        [InlineKeyboardButton(text="🔑 立即续费", callback_data="rent:main")]
                                    ]),
                                )
                            except Exception as e:
                                print("warn notify failed:", e)
                        break

        except Exception as e:
            print("expiry_warning_loop error:", e)

        await asyncio.sleep(300)

# ================= COMMON CALLBACKS =================
@dp.callback_query(lambda c: c.data == "noop")
async def noop_cb(c: types.CallbackQuery):
    await c.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("copy:"))
async def copy_cb(c: types.CallbackQuery):
    if not c.message:
        return await c.answer()

    text = c.data.split(":", 1)[1]
    await c.message.answer(f"📋 请复制：\n<code>{text}</code>", parse_mode="HTML")
    await c.answer("已发送可复制文本")

@dp.callback_query(lambda c: c.data == "menu:help")
async def menu_help_cb(c: types.CallbackQuery):
    if not c.message:
        return
    await c.message.answer(help_text(), parse_mode="HTML", reply_markup=menu_kb(c.from_user.id if c.from_user else None))
    await c.answer()

@dp.callback_query(lambda c: c.data == "menu:copy")
async def menu_copy_cb(c: types.CallbackQuery):
    if not c.message:
        return
    await c.answer()

# ================= START / PRIVATE MENU =================
@dp.message(lambda m: is_private(m) and m.text and is_cmd(m, "/start"))
async def start_cmd(m: types.Message):
    custom_text = get_setting(-1, "start_text")
    is_vip = has_bot_access(m.from_user.id)

    activator_name = None
    if is_vip:
        access_row = get_access_user_by_id(m.from_user.id)
        if access_row and len(access_row) >= 3:
            activator_name = await get_activator_name(access_row[2])

    if custom_text:
        text = custom_text
    elif is_vip:
        text = build_vip_welcome_text(
            display_name=m.from_user.full_name or "User",
            username=m.from_user.username or "",
            user_id=m.from_user.id,
            activator_name=activator_name,
        )
    else:
        text = build_normal_welcome_text(
            display_name=m.from_user.full_name or "User",
            username=m.from_user.username or "",
            user_id=m.from_user.id,
        )

    await m.answer(text, reply_markup=menu_kb(m.from_user.id), parse_mode="HTML")
    await m.answer("📋 常用命令复制区：", reply_markup=copy_cmd_kb())
    await m.answer("👇 你也可以从这里开始：", reply_markup=start_inline_kb(m.from_user.id))

    
@dp.message(lambda m: is_private(m) and m.text in ("🔥 开始记账", "开始记账", "开始"))
async def menu_begin(m: types.Message):
   await m.answer(begin_help_text(), parse_mode="HTML")

@dp.message(lambda m: is_private(m) and ((m.text in ("📝 使用说明", "使用说明")) or is_cmd(m, "/help")))
async def menu_help(m: types.Message):
    await m.reply(help_text(), reply_markup=menu_kb(m.from_user.id), parse_mode="HTML")

@dp.message(lambda m: is_private(m) and m.text in ("📋 复制命令", "复制命令"))
async def menu_copy(m: types.Message):
    await m.reply("📋 常用命令复制区：", reply_markup=copy_cmd_kb())

@dp.message(lambda m: is_private(m) and m.text in ("👥 分组功能", "分组功能"))
async def group_feature_menu(m: types.Message):
    await m.answer(group_feature_text(), parse_mode="HTML")

# ================= TRIAL / ACCESS =================
@dp.message(lambda m: is_private(m) and m.text in ("💎 申请试用", "申请试用"))
async def menu_trial(m: types.Message, state: FSMContext):
    if can_manage_codes(m.from_user.id):
        return await m.answer("🛠 管理员快捷面板", reply_markup=manage_panel_kb(m.from_user.id))

    if has_bot_access(m.from_user.id):
        return await m.reply("✅ 您已拥有使用权限。")

    if not has_claimed_free_trial(m.from_user.id):
        expires_at = int(time.time()) + 24 * 60 * 60
        add_access_user(
            user_id=m.from_user.id,
            username=m.from_user.username or "",
            granted_by=None,
            expires_at=expires_at,
        )
        mark_claimed_free_trial(m.from_user.id)
        return await m.reply(
            "✅ 您已获得 24 小时免费试用权限。\n"
            "到期后请输入管理员发放的续费码，或使用自助续费。"
        )

    await state.set_state(TrialFSM.waiting_code)
    await m.reply(
        "⏳ 您的免费试用已用过或已到期。\n\n"
        "请输入管理员发送的续费码继续使用。"
    )

@dp.message(TrialFSM.waiting_code)
async def receive_trial_redeem_code(m: types.Message, state: FSMContext):
    if not m.text:
        return

    code = m.text.strip()
    real_code = (get_trial_code() or "").strip()

    if not real_code:
        return await m.reply("❌ 当前未设置续费码，请联系管理员。")

    if code != real_code:
        return await m.reply("❌ 续费码错误，请重试。")

    add_access_user(
        user_id=m.from_user.id,
        username=m.from_user.username or "",
        granted_by=None,
        expires_at=None,
    )

    await state.clear()
    await m.reply("✅ 续费成功，您已获得长期使用权限。")

# ================= GROUP CONTROL =================
@dp.message(lambda m: is_group_message(m) and (m.text in ("开始", "开始记账", "开启记账", "🔥 开始记账")))
async def start_accounting(m: types.Message):
    ensure_group(m)
    if not can_use_bot_ops(m.from_user.id):
        return await m.reply(deny_text())

    set_chat_setting(m.chat.id, "enabled", "1")
    await m.reply("✅ 记账已开启！")

@dp.message(lambda m: is_group_message(m) and m.text in ("关闭记账", "停止记账"))
async def stop_accounting(m: types.Message):
    ensure_group(m)
    if not can_use_bot_ops(m.from_user.id):
        return await m.reply(deny_text())

    set_chat_setting(m.chat.id, "enabled", "0")
    await m.reply("⛔ 记账已关闭！")

@dp.message(lambda m: is_group_message(m) and m.text in ("上课", "下课"))
async def group_permission_cmd(m: types.Message):
    ensure_group(m)
    if not can_use_bot_ops(m.from_user.id):
        return await m.reply(deny_text())

    try:
        if m.text == "上课":
            await bot.set_chat_permissions(
                m.chat.id,
                permissions=types.ChatPermissions(can_send_messages=True),
            )
            await m.reply("✅ 已开启发言")
        else:
            await bot.set_chat_permissions(
                m.chat.id,
                permissions=types.ChatPermissions(can_send_messages=False),
            )
            await m.reply("✅ 已禁言")
    except Exception as e:
        await m.reply("❌ 机器人没有权限修改群权限")
        print("group_permission_cmd error:", e)

@dp.message(lambda m: is_group_message(m) and bool(re.match(r"^设置汇率\s*-?\d+(\.\d+)?$", (m.text or "").strip())))
async def set_rate_cmd(m: types.Message):
    ensure_group(m)
    if not can_use_bot_ops(m.from_user.id):
        return await m.reply(deny_text())

    num = re.findall(r"-?\d+(?:\.\d+)?", m.text or "")
    if not num:
        return await m.reply("❌ 格式错误")
    set_chat_setting(m.chat.id, "rate", num[0])
    await m.reply(f"✅ 汇率已设置为 {num[0]}")

@dp.message(lambda m: is_group_message(m) and bool(re.match(r"^设置费率\s*-?\d+(\.\d+)?$", (m.text or "").strip())))
async def set_fee_cmd(m: types.Message):
    ensure_group(m)
    if not can_use_bot_ops(m.from_user.id):
        return await m.reply(deny_text())

    num = re.findall(r"-?\d+(?:\.\d+)?", m.text or "")
    if not num:
        return await m.reply("❌ 格式错误")
    set_chat_setting(m.chat.id, "fee", num[0])
    await m.reply(f"✅ 费率已设置为 {num[0]}%")

@dp.message(lambda m: is_group_message(m) and m.text in ("总账单", "今日总账单"))
async def day_report_cmd(m: types.Message):
    ensure_group(m)
    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply(deny_text())

    start_ts, end_ts = day_range()
    await send_long_text(
        m.chat.id,
        report_text(m.chat.id, start_ts, end_ts, title="今日账单"),
        reply_markup=report_kb(m.chat.id),
    )

@dp.message(lambda m: is_group_message(m) and m.text in ("上个月总账单",))
async def prev_month_report_cmd(m: types.Message):
    ensure_group(m)
    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply(deny_text())

    start_ts, end_ts = month_range(offset_months=1)
    await send_long_text(
        m.chat.id,
        report_text(m.chat.id, start_ts, end_ts, title="上个月账单"),
        reply_markup=report_kb(m.chat.id),
    )

@dp.message(lambda m: is_group_message(m) and (m.text in ("账单",) or is_cmd(m, "/我")))
async def user_report_cmd(m: types.Message):
    ensure_group(m)

    if is_cmd(m, "/我"):
        user = m.from_user
    elif m.reply_to_message and m.reply_to_message.from_user:
        user = m.reply_to_message.from_user
    else:
        user = m.from_user

    start_ts, end_ts = day_range()
    text = report_text(
        m.chat.id,
        start_ts,
        end_ts,
        title="个人账单",
        user_id=user.id,
        display_name=user.full_name or (user.username or str(user.id)),
    )
    await send_long_text(m.chat.id, text, reply_markup=report_kb(m.chat.id))

@dp.message(lambda m: is_group_message(m) and m.text == "撤销")
async def undo_cmd(m: types.Message):
    ensure_group(m)
    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply(deny_text())

    tx = get_last_transaction(m.chat.id)
    if not tx:
        return await m.reply("暂无可撤销记录")

    undo_transaction(tx[0])
    start_ts, end_ts = day_range()
    await send_long_text(
        m.chat.id,
        "↩️ 已撤销上一笔记录\n\n" + report_text(m.chat.id, start_ts, end_ts, title="今日账单"),
        reply_markup=report_kb(m.chat.id),
    )

# ================= REALTIME RATE =================
@dp.message(lambda m: m.text in ("实时U价", "📈 实时U价"))
async def menu_rate(m: types.Message):
    if not can_use_bot_ops(m.from_user.id):
        return await m.reply(deny_text())

    rates = await get_usdt_rates_cached()
    await m.answer(format_usdt_rate_text(rates), reply_markup=rate_kb(), parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "rate:refresh")
async def rate_refresh_cb(c: types.CallbackQuery):
    if not c.message:
        return
    rates = await get_usdt_rates_cached(force=True)
    await c.message.answer(format_usdt_rate_text(rates), reply_markup=rate_kb(), parse_mode="HTML")
    await c.answer("✅ 已刷新")

# ================= ADDRESS QUERY =================
@dp.message(lambda m: is_private(m) and m.text in ("地址查询", "🔍 地址查询", "📍 地址查询"))
async def menu_address_query(m: types.Message, state: FSMContext):
    if not can_use_bot_ops(m.from_user.id) and not has_bot_access(m.from_user.id):
        return await m.reply("❌ 无权限")
    await state.set_state(AddressQueryFSM.waiting_address)
    await m.reply(address_query_text(), parse_mode="HTML")

@dp.message(AddressQueryFSM.waiting_address)
async def receive_address_query(m: types.Message, state: FSMContext):
    if not can_use_bot_ops(m.from_user.id) and not has_bot_access(m.from_user.id):
        return await m.reply("❌ 无权限")

    addr = (m.text or "").strip()
    if not is_tron_address(addr):
        return await m.reply(
            "❌ 地址格式不正确，请重新输入 TRON 地址。\n"
            "示例：<code>TSPpLmYuFXLi6GU1W4uyG6NKGbdWPw886U</code>",
            parse_mode="HTML",
        )

    await m.reply("⏳ 正在查询链上数据，请稍候...")

    try:
        info = await check_tron_address(addr)
        text = format_address_info_text(addr, info)
    except Exception as e:
        print("on-chain query error:", e)
        text = f"🔎 查询地址：<code>{addr}</code>\n\n⚠️ 查询失败，请稍后再试。"

    await state.clear()
    await m.reply(text, parse_mode="HTML", reply_markup=address_result_kb(addr, page=1))

@dp.callback_query(lambda c: c.data == "addr:again")
async def addr_again_cb(c: types.CallbackQuery, state: FSMContext):
    if not c.message:
        return
    await state.set_state(AddressQueryFSM.waiting_address)
    await c.message.answer(address_query_text(), parse_mode="HTML")
    await c.answer()

@dp.callback_query(lambda c: c.data == "addr:back")
async def addr_back_cb(c: types.CallbackQuery, state: FSMContext):
    if not c.message:
        return
    await state.clear()
    await c.message.answer("✅ 已返回主菜单")
    await c.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("addr:tx:"))
async def addr_tx_cb(c: types.CallbackQuery):
    if not c.message:
        return

    parts = c.data.split(":")
    address = parts[2]
    page = int(parts[3]) if len(parts) >= 4 and parts[3].isdigit() else 1

    await c.message.answer("⏳ 正在加载交易记录，请稍候...")

    try:
        txs = await get_tron_transactions(address, page=page, page_size=10)
        if not txs:
            await c.message.answer(f"🔎 查询地址：<code>{address}</code>\n📄 当前页无交易记录", parse_mode="HTML")
            return await c.answer()
    
        text = f"🔎 查询地址：<code>{address}</code>\n🗂 当前页码：第 {page} 页\n\n📄 交易记录：\n"
        for tx in txs:
            text += format_tron_tx_row(tx) + "\n\n"

        await c.message.answer(text, parse_mode="HTML", reply_markup=tx_history_kb(address, page))
    except Exception as e:
        print("addr tx cb error:", e)
        await c.message.answer("⚠️ 交易记录加载失败，请稍后再试。")

    await c.answer()

# ================= WALLET UI =================
def address_result_kb(address, page=1):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📜 链上交易记录", callback_data=f"addr:tx:{address}:{page}"),
        ],
        [
            InlineKeyboardButton(text="🔄 重新查询", callback_data="addr:again"),
            InlineKeyboardButton(text="⬅️ 返回菜单", callback_data="addr:back"),
        ],
    ])

def tx_history_kb(address, page=1):
    buttons = []
    if page > 1:
        buttons.append(
            InlineKeyboardButton(
                text="⬅️ 上一页",
                callback_data=f"addr:tx:{address}:{page-1}"
            )
        )

    buttons.append(
        InlineKeyboardButton(
            text=f"📄 第 {page} 页",
            callback_data="noop"
        )
    )

    buttons.append(
        InlineKeyboardButton(
            text="下一页 ➡️",
            callback_data=f"addr:tx:{address}:{page+1}"
        )
    )

    return InlineKeyboardMarkup(inline_keyboard=[buttons])

# ================= WALLET HELPERS =================
def get_user_wallet_send_count(user_id, chat_id=None):
    try:
        with get_db() as (_conn, cur):
            if chat_id is None:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM wallet_checks
                    WHERE user_id = %s
                    """,
                    (int(user_id),)
                )
            else:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM wallet_checks
                    WHERE user_id = %s AND chat_id = %s
                    """,
                    (int(user_id), int(chat_id))
                )

            row = cur.fetchone()
            return int(row[0] or 0) if row else 0
    except Exception as e:
        print("get_user_wallet_send_count error:", e)
        return 0

def wallet_risk_analysis(info):
    warnings = []
    score = 0

    tx_count = info.get("tx_count") or 0
    trx_balance = float(info.get("trx_balance") or 0)
    usdt_balance = float(info.get("usdt_balance") or 0)
    latest_time = info.get("latest_time")

    now_ts = int(time.time())

    if tx_count == 0:
        warnings.append(("🆕 新钱包", "该地址暂无交易记录，可能为新钱包地址，请结合实际用途继续核对。"))
    elif tx_count < 3:
        warnings.append(("⚠️ 注意", "该地址交易次数较少，建议进一步核实。"))
        score += 1

    if trx_balance < 1:
        warnings.append(("⚠️ 注意", "TRX余额较低，可能影响链上转账或能量消耗。"))
        score += 1

    if usdt_balance <= 0:
        warnings.append(("ℹ️ 提示", "当前USDT余额为0，请确认该地址用途是否正常。"))

    if latest_time:
        try:
            lt = int(latest_time)
            if lt > 10_000_000_000:
                lt = lt // 1000

            idle_days = (now_ts - lt) // 86400

            if idle_days >= 90:
                warnings.append(("🚨 高风险", f"该地址已超过 {idle_days} 天未活跃，请谨慎核对。"))
                score += 2
            elif idle_days >= 30:
                warnings.append(("⚠️ 注意", f"该地址已 {idle_days} 天未活跃。"))
                score += 1
        except Exception:
            pass

    if score >= 3:
        level = "🚨 高风险地址"
    elif score >= 1:
        level = "⚠️ 需谨慎核对"
    else:
        level = "✅ 基本正常"

    return level, warnings

def build_wallet_warning_html(info):
    level, warnings = wallet_risk_analysis(info)

    if not warnings:
        return "\n\n✅ <b>地址状态正常，未发现明显异常。</b>"

    lines = [f"\n\n🛡 <b>风险评估</b>", f"• <b>{escape(level)}</b>"]
    for tag, msg in warnings:
        lines.append(f"• <b>{escape(tag)}</b>：{escape(msg)}")
    return "\n".join(lines)

# ================= TRON API =================
async def trongrid_get(path, params=None):
    headers = {
        "accept": "application/json",
        "user-agent": "Mozilla/5.0",
    }
    if TRONGRID_API_KEY:
        headers["TRON-PRO-API-KEY"] = TRONGRID_API_KEY

    url = path if path.startswith("http") else f"{TRONGRID_API_URL}{path}"

    if HTTP_SESSION is None:
        return {}

    async with HTTP_SESSION.get(url, params=params, headers=headers) as resp:
        if resp.status != 200:
            return {}
        return await resp.json()

def _pick_account(payload):
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get("data"), list) and payload["data"]:
        return payload["data"][0]
    if payload.get("address"):
        return payload
    if isinstance(payload.get("data"), dict):
        return payload["data"]
    return None

def _parse_trc20_usdt(account):
    if not isinstance(account, dict):
        return None

    candidates = [
        "trc20token_balances",
        "trc20",
        "tokenBalances",
        "tokens",
        "assetV2",
    ]

    for key in candidates:
        items = account.get(key)
        if not items:
            continue

        if isinstance(items, dict):
            items = [items]
        if not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, dict):
                continue

            sym = str(
                item.get("tokenAbbr")
                or item.get("symbol")
                or item.get("tokenName")
                or item.get("name")
                or ""
            ).upper()

            contract = str(
                item.get("contract_address")
                or item.get("tokenAddress")
                or item.get("tokenId")
                or item.get("contract")
                or ""
            )

            if sym == "USDT" or contract == USDT_TRC20_CONTRACT:
                raw = (
                    item.get("balance")
                    or item.get("value")
                    or item.get("amount")
                    or item.get("tokenValue")
                )
                if raw is None:
                    return 0.0

                try:
                    decimals = int(item.get("precision") or item.get("decimals") or 6)
                except Exception:
                    decimals = 6

                try:
                    return float(raw) / (10 ** decimals)
                except Exception:
                    try:
                        return float(raw)
                    except Exception:
                        return 0.0
    return None

async def check_tron_address(address: str):
    def _fetch():
        headers = {
            "accept": "application/json",
            "user-agent": "Mozilla/5.0",
        }
        if TRONGRID_API_KEY:
            headers["TRON-PRO-API-KEY"] = TRONGRID_API_KEY

        sources = [
            f"https://api.trongrid.io/v1/accounts/{address}",
            f"https://apilist.tronscanapi.com/api/account?address={address}",
        ]

        for url in sources:
            try:
                r = requests.get(url, timeout=8, headers=headers)
                if not r.ok:
                    continue
                payload = r.json()
                acc = _pick_account(payload)
                if acc:
                    source_name = "trongrid" if "trongrid" in url else "tronscan"
                    return {"source": source_name, "account": acc}
            except Exception as e:
                print("wallet api error:", url, e)
        return None

    result = await asyncio.to_thread(_fetch)
    if not result:
        return None

    acc = result["account"]

    trx_balance = None
    try:
        if acc.get("balance") is not None:
            trx_balance = float(acc.get("balance")) / 1_000_000
    except Exception as e:
        print("trx_balance parse error:", e)

    usdt_balance = _parse_trc20_usdt(acc)

    tx_count = (
        acc.get("transaction_count")
        or acc.get("txCount")
        or acc.get("transactionsCount")
        or acc.get("totalTransactionCount")
        or acc.get("trxCount")
        or None
    )
    try:
        tx_count = int(tx_count) if tx_count is not None else None
    except Exception:
        tx_count = None

    create_time = (
        acc.get("create_time")
        or acc.get("createTime")
        or acc.get("create_time_ms")
        or acc.get("createTimeMs")
    )
    latest_time = (
        acc.get("latest_opration_time")
        or acc.get("latestOperationTime")
        or acc.get("latest_operation_time")
        or acc.get("latest_tx_time")
    )

    return {
        "source": result["source"],
        "address": address,
        "trx_balance": trx_balance,
        "usdt_balance": usdt_balance,
        "tx_count": tx_count,
        "create_time": create_time,
        "latest_time": latest_time,
        "raw": acc,
    }

async def get_tron_transactions(address, page=1, page_size=10):
    offset = max(0, (page - 1) * page_size)
    tx_data = await trongrid_get(
        f"/v1/accounts/{address}/transactions",
        params={
            "limit": page_size,
            "only_confirmed": "true",
            "order_by": "block_timestamp,desc",
            "offset": offset,
        },
    )
    return tx_data.get("data", []) if tx_data else []

def format_tron_tx_row(tx):
    try:
        ts = tx.get("block_timestamp")
        dt = datetime.fromtimestamp(ts / 1000, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S") if ts else "-"
        txid = tx.get("txID", "-")
        contract = tx.get("raw_data", {}).get("contract", [])
        tx_type = "-"
        if contract:
            tx_type = contract[0].get("type", "-")
        return f"• {dt} | {escape(tx_type)}\n  <code>{escape(txid)}</code>"
    except Exception:
        return "• 无法解析交易"

def format_address_info_text(address, info, sender_name=None, user_send_count=None):
    if not info:
        return (
            f"🔎 <b>地址查询结果</b>\n\n"
            f"📌 地址：<code>{escape(address)}</code>\n"
            "⚠️ 无法获取链上数据，请稍后重试。"
        )

    trx_balance = info.get("trx_balance", 0)
    usdt_balance = info.get("usdt_balance", 0)
    tx_count = info.get("tx_count", 0)
    first_tx = info.get("create_time") or "-"
    last_active = info.get("latest_time") or "-"
    sig_status = "已签名地址" if (tx_count or 0) > 0 else "新钱包 / 未签名地址"

    lines = [
        "🔎 <b>TRON 地址查询</b>",
        "",
    ]

    if sender_name:
        lines.append(f"👤 查询人：<code>{escape(sender_name)}</code>")

    if user_send_count is not None:
        lines.append(f"📌 本群发送次数：<b>{user_send_count}</b> 次")

    lines.extend([
        f"📌 地址：<code>{escape(address)}</code>",
        f"💰 TRX：<b>{fmt_num(trx_balance)}</b>",
        f"💰 USDT：<b>{fmt_num(usdt_balance)}</b>",
        f"📊 交易次数：<b>{tx_count if tx_count is not None else 0}</b>",
        f"🔰 状态：<b>{sig_status}</b>",
        f"📡 数据来源：<b>{escape(str(info.get('source', '-')))}</b>",
        f"⏰ 首次交易：<b>{fmt_ts(first_tx)}</b>",
        f"🌟 最后活跃：<b>{fmt_ts(last_active)}</b>",
    ])

    return "\n".join(lines)

def make_wallet_card_image(
    address,
    sender_name,
    user_send_count=0,
    trx_balance=None,
    usdt_balance=None,
    tx_count=None,
    source="trongrid",
    create_time=None,
    latest_time=None,
):
    width, height = 1080, 1350

    top_green = (18, 185, 150)
    top_green2 = (16, 165, 138)
    body_bg = (20, 30, 44)
    panel_bg = (26, 40, 58)
    panel_bg2 = (30, 46, 66)

    white = (245, 248, 250)
    mute = (165, 180, 190)
    gold = (245, 198, 76)
    blue = (120, 185, 255)
    green = (100, 235, 160)
    red = (255, 120, 120)
    yellow = (255, 210, 90)

    img = Image.new("RGB", (width, height), body_bg)
    draw = ImageDraw.Draw(img)

    # nền chuyển màu
    for y in range(height):
        if y < 330:
            r = int(top_green[0] * (1 - y / 330) + top_green2[0] * (y / 330))
            g = int(top_green[1] * (1 - y / 330) + top_green2[1] * (y / 330))
            b = int(top_green[2] * (1 - y / 330) + top_green2[2] * (y / 330))
            draw.line([(0, y), (width, y)], fill=(r, g, b))
        else:
            draw.line([(0, y), (width, y)], fill=body_bg)

    font_candidates = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJKSC-Regular.otf",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "arial.ttf",
    ]

    def load_font(size):
        for fp in font_candidates:
            try:
                return ImageFont.truetype(fp, size)
            except Exception:
                pass
        return ImageFont.load_default()

    font_title = load_font(54)
    font_sub = load_font(28)
    font_mid = load_font(32)

    def box(x1, y1, x2, y2, radius=26, fill=panel_bg, outline=None, width=2):
        draw.rounded_rectangle((x1, y1, x2, y2), radius=radius, fill=fill, outline=outline, width=width)

    def text(x, y, s, font, fill=white):
        draw.text((x, y), str(s), font=font, fill=fill)

    def center_text(y, s, font, fill=white):
        bbox = draw.textbbox((0, 0), str(s), font=font)
        w = bbox[2] - bbox[0]
        x = (width - w) // 2
        draw.text((x, y), str(s), font=font, fill=fill)

    def right_badge(x2, y1, label, fill_bg=(48, 78, 118), fill_text=white, pad_x=16, pad_y=10, radius=18):
        bbox = draw.textbbox((0, 0), str(label), font=font_sub)
        tw = bbox[2] - bbox[0]
        th = bbox[3] - bbox[1]
        x1 = x2 - tw - pad_x * 2
        y2 = y1 + th + pad_y * 2
        draw.rounded_rectangle((x1, y1, x2, y2), radius=radius, fill=fill_bg)
        draw.text((x1 + pad_x, y1 + pad_y - 1), str(label), font=font_sub, fill=fill_text)

    def fmt_time_local(ts):
        if not ts:
            return "N/A"
        try:
            ts = int(ts)
            if ts > 10_000_000_000:
                ts = ts // 1000
            return datetime.fromtimestamp(ts, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return "N/A"

    # phân tích risk
    risk_level, _warnings = wallet_risk_analysis({
        "tx_count": tx_count,
        "trx_balance": trx_balance,
        "usdt_balance": usdt_balance,
        "latest_time": latest_time,
    })

    risk_color = green
    if "高风险" in risk_level:
        risk_color = red
    elif "谨慎" in risk_level:
        risk_color = yellow

    tx_status = "已签名地址" if (tx_count or 0) > 0 else "新钱包 / 未签名地址"
    tx_status_color = green if (tx_count or 0) > 0 else yellow

    # header
    box(40, 35, 1040, 300, radius=36, fill=top_green2, outline=(255, 255, 255, 40), width=2)
    box(90, 198, 990, 250, radius=18, fill=(60, 130, 108), outline=(220, 255, 240), width=2)

    center_text(70, "USDT防篡改验证核对", font_title, fill=white)
    center_text(144, "《请双方谨慎核对地址是否与图中一致，如有误请立即停止付款》", font_sub, fill=(232, 247, 242))
    center_text(209, address, font_mid, fill=white)
    center_text(258, f"查询人: {sender_name}", font_sub, fill=(225, 245, 240))

    right_badge(
        1000,
        52,
        f"{sender_name} · 第 {user_send_count} 次",
        fill_bg=(40, 72, 108),
        fill_text=white,
    )

    # thân chính
    box(40, 330, 1040, 1140, radius=34, fill=panel_bg, outline=(42, 70, 90), width=2)
    text(70, 360, "🔎 查询地址：", font_mid, fill=white)
    text(250, 360, address, font_mid, fill=blue)

    box(60, 460, 1020, 1030, radius=28, fill=panel_bg2, outline=(55, 90, 110), width=2)

    rows = [
        ("🛡 风险等级", risk_level, risk_color),
        ("💡 交易次数", str(tx_count if tx_count is not None else "N/A"), white),
        ("⏰ 首次交易", fmt_time_local(create_time), white),
        ("🌟 最后活跃", fmt_time_local(latest_time), white),
        ("🔰 签名状态", tx_status, tx_status_color),
        ("💰 USDT 余额", f"{fmt_num(usdt_balance)} USDT", gold),
        ("💰 TRX 余额", f"{fmt_num(trx_balance)} TRX", gold),
        ("📡 数据来源", str(source), mute),
    ]

    y = 500
    gap = 64
    for label, value, value_color in rows:
        text(85, y, f"{label}：", font_mid, fill=white)
        text(330, y, value, font_mid, fill=value_color)
        y += gap

    bio = BytesIO()
    img.save(bio, "PNG")
    bio.seek(0)
    return BufferedInputFile(bio.read(), filename="usdt_check_cn.png")

# ================= ADDRESS QUERY =================
@dp.message(lambda m: is_private(m) and m.text in ("地址查询", "🔍 地址查询", "📍 地址查询"))
async def menu_address_query(m: types.Message, state: FSMContext):
    if not m.from_user:
        return

    if not can_use_bot_ops(m.from_user.id) and not has_bot_access(m.from_user.id):
        return await m.reply("❌ 无权限")

    await state.set_state(AddressQueryFSM.waiting_address)
    await m.reply(
        "🔍 <b>地址查询</b>\n\n请直接发送 TRON 地址进行查询。\n\n示例：\n<code>TSPpLmYuFXLi6GU1W4uyG6NKGbdWPw886U</code>",
        parse_mode="HTML",
    )

@dp.message(AddressQueryFSM.waiting_address)
async def receive_address_query(m: types.Message, state: FSMContext):
    if not m.from_user:
        return

    if not can_use_bot_ops(m.from_user.id) and not has_bot_access(m.from_user.id):
        return await m.reply("❌ 无权限")

    addr = (m.text or "").strip()
    if not is_tron_address(addr):
        return await m.reply(
            "❌ 地址格式不正确，请重新输入 TRON 地址。\n示例：<code>TSPpLmYuFXLi6GU1W4uyG6NKGbdWPw886U</code>",
            parse_mode="HTML",
        )

    wait_msg = await m.reply("⏳ 正在查询链上数据，请稍候...")

    try:
        info = await check_tron_address(addr)
        sender_name = m.from_user.full_name or (m.from_user.username or str(m.from_user.id))

        try:
            add_wallet_check(
                chat_id=m.chat.id,
                user_id=m.from_user.id,
                username=m.from_user.username or "",
                full_name=m.from_user.full_name or "",
                address=addr,
                trx_balance=info.get("trx_balance") if info else None,
                usdt_balance=info.get("usdt_balance") if info else None,
                tx_count=info.get("tx_count") if info else None,
            )
        except Exception as e:
            print("private add_wallet_check error:", e)

        user_send_count = get_user_wallet_send_count(m.from_user.id, None)

        text_html = format_address_info_text(
            addr,
            info,
            sender_name=sender_name,
            user_send_count=user_send_count,
        )

        if info:
            text_html += build_wallet_warning_html(info)

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Tronscan", url=f"https://tronscan.org/#/address/{addr}")],
            [InlineKeyboardButton(text="📄 最近链上交易", callback_data=f"addr:tx:{addr}:1")],
            [InlineKeyboardButton(text="🔄 重新查询", callback_data="addr:again")],
            [InlineKeyboardButton(text="⬅️ 返回菜单", callback_data="addr:back")],
        ])

        try:
            photo = make_wallet_card_image(
                address=addr,
                sender_name=sender_name,
                user_send_count=user_send_count,
                trx_balance=info.get("trx_balance") if info else None,
                usdt_balance=info.get("usdt_balance") if info else None,
                tx_count=info.get("tx_count") if info else None,
                source=info.get("source") if info else "unknown",
                create_time=info.get("create_time") if info else None,
                latest_time=info.get("latest_time") if info else None,
            )
            await m.answer_photo(photo=photo, caption=text_html, reply_markup=kb, parse_mode="HTML")
        except Exception as e:
            print("private send wallet photo error:", e)
            await m.reply(text_html, parse_mode="HTML", reply_markup=kb)

    except Exception as e:
        print("on-chain query error:", e)
        await m.reply(
            f"🔎 查询地址：<code>{escape(addr)}</code>\n\n⚠️ 查询失败，请稍后再试。",
            parse_mode="HTML",
        )

    await state.clear()

    try:
        await wait_msg.delete()
    except Exception:
        pass


@dp.callback_query(lambda c: c.data == "addr:again")
async def addr_again_cb(c: types.CallbackQuery, state: FSMContext):
    if not c.message:
        return
    await state.set_state(AddressQueryFSM.waiting_address)
    await c.message.answer("请重新发送 TRON 地址。")
    await c.answer()

@dp.callback_query(lambda c: c.data == "addr:back")
async def addr_back_cb(c: types.CallbackQuery, state: FSMContext):
    await state.clear()
    if c.message:
        await c.message.answer("✅ 已返回主菜单")
    await c.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("addr:tx:"))
async def addr_tx_cb(c: types.CallbackQuery):
    if not c.message or not c.data:
        return

    parts = c.data.split(":")
    if len(parts) < 4:
        return await c.answer("数据错误", show_alert=True)

    address = parts[2].strip()
    if not is_tron_address(address):
        return await c.answer("地址错误", show_alert=True)

    try:
        page = int(parts[3])
        if page < 1:
            page = 1
    except Exception:
        page = 1

    await c.message.answer("⏳ 正在加载交易记录，请稍候...")

    try:
        txs = await get_tron_transactions(address, page=page, page_size=10)
        if not txs:
            await c.message.answer(
                f"🔎 查询地址：<code>{escape(address)}</code>\n📄 当前页无交易记录",
                parse_mode="HTML"
            )
            return await c.answer()

        text = f"🔎 查询地址：<code>{escape(address)}</code>\n🗂 当前页码：第 {page} 页\n\n📄 交易记录：\n"
        for tx in txs:
            text += format_tron_tx_row(tx) + "\n\n"

        await c.message.answer(
            text,
            parse_mode="HTML",
            reply_markup=tx_history_kb(address, page)
        )
    except Exception as e:
        print("addr tx cb error:", e)
        await c.message.answer("⚠️ 交易记录加载失败，请稍后再试。")

    await c.answer()


# ================= WALLET AUTO CHECK IN GROUP =================
@dp.message(lambda m: is_group_message(m) and m.text and extract_tron_address(m.text) is not None)
async def tron_address_check_handler(m: types.Message):
    if should_ignore_message(m):
        return

    ensure_group(m)

    address = extract_tron_address(m.text)
    if not address:
        return

    status_msg = await m.reply("⏳ 正在查询地址，请稍候...")

    try:
        info = await check_tron_address(address)
        if not info:
            try:
                return await status_msg.edit_text("❌ 未能获取钱包数据，请稍后再试。")
            except Exception:
                return

        tx_count = info.get("tx_count")
        trx_balance = info.get("trx_balance")
        usdt_balance = info.get("usdt_balance")

        try:
            add_wallet_check(
                chat_id=m.chat.id,
                user_id=m.from_user.id,
                username=m.from_user.username or "",
                full_name=m.from_user.full_name or "",
                address=address,
                trx_balance=trx_balance,
                usdt_balance=usdt_balance,
                tx_count=tx_count,
            )
        except Exception as e:
            print("add_wallet_check error:", e)

        sender_name = m.from_user.full_name or (m.from_user.username or "Unknown")
        user_send_count = get_user_wallet_send_count(m.from_user.id, m.chat.id)

        caption = format_address_info_text(
            address,
            info,
            sender_name=sender_name,
            user_send_count=user_send_count,
        )
        caption += build_wallet_warning_html(info)

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Tronscan", url=f"https://tronscan.org/#/address/{address}")],
            [InlineKeyboardButton(text="📄 最近链上交易", callback_data=f"addr:tx:{address}:1")],
        ])

        try:
            photo = make_wallet_card_image(
                address=address,
                sender_name=sender_name,
                user_send_count=user_send_count,
                trx_balance=trx_balance,
                usdt_balance=usdt_balance,
                tx_count=tx_count,
                source=info.get("source"),
                create_time=info.get("create_time"),
                latest_time=info.get("latest_time"),
            )
            await m.answer_photo(
                photo=photo,
                caption=caption,
                reply_markup=kb,
                parse_mode="HTML",
            )
        except Exception as e:
            print("send wallet photo error:", e)
            await m.reply(
                caption,
                reply_markup=kb,
                parse_mode="HTML",
            )

    except Exception as e:
        print("tron_address_check_handler error:", e)
        try:
            await status_msg.edit_text("❌ 查询地址时发生错误。")
        except Exception:
            pass

    try:
        await status_msg.delete()
    except Exception:
        pass


# ================= WALLET CHECK LOGS =================
def build_wallet_logs_text(rows, page, total):
    text_lines = [
        "📄 <b>钱包查询记录</b>",
        f"📍 当前页码：第 <b>{page + 1}</b> 页",
        f"📊 总记录数：<b>{total}</b>",
        "",
    ]

    buttons = []

    for row in rows:
        _id, chat_id, user_id, username, full_name, address, trx_balance, usdt_balance, tx_count, created_at = row
        sender = full_name or username or str(user_id)
        tm = fmt_ts(created_at)
        status_text = "新钱包 / 未签名地址" if tx_count in (None, 0) else "已签名地址"

        text_lines.append(
            f"🕒 {tm}\n"
            f"👥 群组：<code>{chat_id}</code>\n"
            f"👤 用户：<code>{user_id}</code> {escape(sender)}\n"
            f"🏷 用户名：@{escape(username or '-')}\n"
            f"📌 地址：<code>{escape(address)}</code>\n"
            f"💰 TRX：<b>{fmt_num(trx_balance)}</b> | USDT：<b>{fmt_num(usdt_balance)}</b>\n"
            f"📊 交易次数：<b>{tx_count if tx_count is not None else 'N/A'}</b>\n"
            f"🔰 状态：<b>{status_text}</b>\n"
            f"{'—' * 26}"
        )

        buttons.append([
            InlineKeyboardButton(
                text=f"🔗 {address[:10]}...",
                url=f"https://tronscan.org/#/address/{address}",
            )
        ])

    return "\n\n".join(text_lines), buttons


@dp.message(lambda m: m.text == "交易记录")
async def wallet_logs_menu(m: types.Message):
    if not m.from_user:
        return
    if not can_use_manage_panel(m.from_user.id):
        return await m.reply("❌ 无权限")

    rows = get_wallet_checks_page(limit=10, offset=0)
    if not rows:
        return await m.reply("暂无历史记录。")

    total = count_wallet_checks()
    text, buttons = build_wallet_logs_text(rows, page=0, total=total)

    if total > 10:
        buttons.append([InlineKeyboardButton(text="下一页 ➡️", callback_data="wallet:recent:1")])

    await m.reply(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


@dp.callback_query(lambda c: c.data and c.data.startswith("wallet:recent:"))
async def wallet_logs_cb(c: types.CallbackQuery):
    if not c.message or not c.from_user:
        return

    if not can_use_manage_panel(c.from_user.id):
        return await c.answer("无权限", show_alert=True)

    try:
        page = int(c.data.split(":")[-1])
    except Exception:
        page = 0

    limit = 10
    offset = page * limit

    rows = get_wallet_checks_page(limit=limit, offset=offset)
    if not rows:
        await c.message.edit_text("暂无历史记录。")
        return await c.answer()

    total = count_wallet_checks()
    has_prev = page > 0
    has_next = offset + limit < total

    text, buttons = build_wallet_logs_text(rows, page=page, total=total)

    nav = []
    if has_prev:
        nav.append(InlineKeyboardButton(text="⬅️ 上一页", callback_data=f"wallet:recent:{page - 1}"))
    if has_next:
        nav.append(InlineKeyboardButton(text="下一页 ➡️", callback_data=f"wallet:recent:{page + 1}"))
    if nav:
        buttons.append(nav)

    await c.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )
    await c.answer()

# ================= MANAGE PANEL =================
@dp.message(lambda m: m.text in ("管理面板", "管理员快捷面板", "续费管理面板", "🛠 管理面板"))
async def manage_panel_cmd(m: types.Message):
    if not can_use_manage_panel(m.from_user.id):
        return await m.reply(deny_text())

    await m.reply(
        "🛠 <b>管理面板</b>\n\n点击下方按钮执行操作。",
        reply_markup=manage_panel_kb(m.from_user.id),
        parse_mode="HTML",
    )

@dp.callback_query(lambda c: c.data == "manage:list_admin")
async def manage_list_admin_cb(c: types.CallbackQuery):
    if not c.from_user or not can_use_manage_panel(c.from_user.id):
        return await c.answer("无权限", show_alert=True)

    rows = get_all_admins()
    lines = ["📋 <b>管理员列表</b>", ""]

    if BOT_OWNER_ID:
        lines.append(f"• <code>{BOT_OWNER_ID}</code> — owner")
    if SUPER_ADMIN_ID and SUPER_ADMIN_ID != BOT_OWNER_ID:
        lines.append(f"• <code>{SUPER_ADMIN_ID}</code> — super(env)")

    for uid, role in rows:
        if uid in (BOT_OWNER_ID, SUPER_ADMIN_ID):
            continue
        lines.append(f"• <code>{uid}</code> — {role}")

    await c.message.answer("\n".join(lines), parse_mode="HTML")
    await c.answer()

@dp.callback_query(lambda c: c.data == "manage:add_admin")
async def manage_add_admin_cb(c: types.CallbackQuery, state: FSMContext):
    if not c.from_user or not can_manage_admins(c.from_user.id):
        return await c.answer("无权限", show_alert=True)

    await state.set_state(AdminFSM.waiting_add_admin)
    await c.message.answer("➕ <b>添加管理员</b>\n\n请回复目标用户消息，或直接发送用户ID。", parse_mode="HTML")
    await c.answer()

@dp.message(AdminFSM.waiting_add_admin)
async def receive_add_admin(m: types.Message, state: FSMContext):
    if not can_manage_admins(m.from_user.id):
        return await m.reply(deny_text())

    uid = None
    if m.reply_to_message and m.reply_to_message.from_user:
        uid = m.reply_to_message.from_user.id
    elif m.text and m.text.strip().isdigit():
        uid = int(m.text.strip())

    if not uid:
        return await m.reply("❌ 格式错误，请回复某人消息或发送用户ID。")

    add_admin(uid, "admin")
    await state.clear()
    await m.reply(f"✅ 已添加管理员：<code>{uid}</code>", parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "manage:del_admin")
async def manage_del_admin_cb(c: types.CallbackQuery, state: FSMContext):
    if not c.from_user or not can_manage_admins(c.from_user.id):
        return await c.answer("无权限", show_alert=True)

    await state.set_state(AdminFSM.waiting_del_admin)
    await c.message.answer("➖ <b>删除管理员</b>\n\n请回复目标用户消息，或直接发送用户ID。", parse_mode="HTML")
    await c.answer()

@dp.message(AdminFSM.waiting_del_admin)
async def receive_del_admin(m: types.Message, state: FSMContext):
    if not can_manage_admins(m.from_user.id):
        return await m.reply(deny_text())

    uid = None
    if m.reply_to_message and m.reply_to_message.from_user:
        uid = m.reply_to_message.from_user.id
    elif m.text and m.text.strip().isdigit():
        uid = int(m.text.strip())

    if not uid:
        return await m.reply("❌ 格式错误，请回复某人消息或发送用户ID。")

    remove_admin(uid)
    await state.clear()
    await m.reply(f"✅ 已删除管理员：<code>{uid}</code>", parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "manage:create_code")
async def manage_create_code_cb(c: types.CallbackQuery, state: FSMContext):
    if not c.from_user or not can_manage_codes(c.from_user.id):
        return await c.answer("无权限", show_alert=True)

    await state.set_state(AdminFSM.waiting_trial_code)
    await c.message.answer("🔑 <b>创建续费码</b>\n\n请发送新的续费码，例如：<code>ABC123</code>", parse_mode="HTML")
    await c.answer()

@dp.message(AdminFSM.waiting_trial_code)
async def receive_manage_trial_code(m: types.Message, state: FSMContext):
    if not can_manage_codes(m.from_user.id):
        return await m.reply(deny_text())

    code = (m.text or "").strip()
    if not code:
        return await m.reply("❌ 请输入有效续费码。")

    set_trial_code(code)
    await state.clear()
    await m.reply(f"✅ 已设置续费码：<code>{code}</code>", parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "manage:revoke_code")
async def manage_revoke_code_cb(c: types.CallbackQuery):
    if not c.from_user or not can_manage_codes(c.from_user.id):
        return await c.answer("无权限", show_alert=True)

    set_trial_code("")
    await c.message.answer("🗑 <b>续费码已回收</b>", parse_mode="HTML")
    await c.answer()

# ================= RENT MENU =================
@dp.message(lambda m: m.text in ("🔑 自助续费", "自助续费", "续费/租用"))
async def menu_rent(m: types.Message):
    await m.answer("🔑 <b>请选择要租用的机器人类型</b>", reply_markup=rent_main_kb(), parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "rent:main")
async def rent_main_cb(c: types.CallbackQuery):
    if not c.message:
        return
    await c.message.answer("🔑 <b>请选择要租用的机器人类型</b>", reply_markup=rent_main_kb(), parse_mode="HTML")
    await c.answer()

@dp.callback_query(lambda c: c.data == "rent:back")
async def rent_back_cb(c: types.CallbackQuery):
    if not c.message:
        return
    await c.message.answer("🔑 <b>请选择要租用的机器人类型</b>", reply_markup=rent_main_kb(), parse_mode="HTML")
    await c.answer()

@dp.callback_query(lambda c: c.data in ("rent:group_admin", "rent:computer", "rent:translator"))
async def rent_category_cb(c: types.CallbackQuery):
    if not c.message:
        return
    category_key = c.data.split(":")[1]
    title = RENT_CATEGORIES.get(category_key, {}).get("title", "套餐")
    await c.message.answer(f"📦 <b>{title}</b>\n\n请选择租用时长：", reply_markup=rent_plan_kb(category_key), parse_mode="HTML")
    await c.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("rent:plan:"))
async def rent_plan_cb(c: types.CallbackQuery):
    if not c.message or not c.from_user:
        return

    _, _, category_key, plan_key = c.data.split(":", 3)
    cat = RENT_CATEGORIES.get(category_key)
    plan = RENT_PLANS.get(plan_key)

    if not cat or not plan:
        return await c.answer("套餐不存在", show_alert=True)

    category_title = cat["title"]
    plan_label = plan["label"]
    amount = plan["amount"]

    order_code = create_rental_order(
        user_id=c.from_user.id,
        username=c.from_user.username or "",
        full_name=c.from_user.full_name or "",
        category_key=category_key,
        category_title=category_title,
        plan_key=plan_key,
        plan_label=plan_label,
        amount=amount,
        note="rent_order",
    )

    text = rent_payment_text(category_key, plan_key, order_code)
    await c.message.answer(text, reply_markup=rent_payment_kb(amount), parse_mode="HTML")
    await c.answer("✅ 已生成订单")

# ================= ORDER MANAGEMENT =================
@dp.callback_query(lambda c: c.data == "order:list_pending")
async def order_list_pending_cb(c: types.CallbackQuery):
    if not c.message or not c.from_user:
        return

    if not can_use_manage_panel(c.from_user.id):
        return await c.answer("无权限", show_alert=True)

    rows = get_pending_rental_orders(limit=10)
    if not rows:
        await c.message.answer("暂无待支付订单")
        return await c.answer()

    buttons = []
    for order_code, user_id, username, full_name, category_title, plan_label, amount, created_at in rows:
        buttons.append([
            InlineKeyboardButton(
                text=f"🧾 {order_code} | {plan_label} | {amount}U",
                callback_data=f"order:view:{order_code}",
            )
        ])

    await c.message.answer("🧾 <b>待支付订单</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await c.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("order:view:"))
async def view_order_cb(c: types.CallbackQuery):
    if not c.message or not c.from_user:
        return

    if not can_use_manage_panel(c.from_user.id):
        return await c.answer("无权限", show_alert=True)

    order_code = c.data.split(":", 2)[2]
    row = get_rental_order(order_code)
    if not row:
        return await c.answer("订单不存在", show_alert=True)

    (
        order_code, user_id, username, full_name, category_key, category_title,
        plan_key, plan_label, amount, status, created_at, paid_at, expires_at, note
    ) = row

    created_str = datetime.fromtimestamp(created_at, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    paid_str = "-" if not paid_at else datetime.fromtimestamp(paid_at, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    expire_str = "-" if not expires_at else datetime.fromtimestamp(expires_at, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")

    text = (
        f"🧾 <b>订单详情</b>\n\n"
        f"订单号：<code>{order_code}</code>\n"
        f"用户：<code>{user_id}</code> @{username or '-'}\n"
        f"姓名：{full_name or '-'}\n"
        f"类型：{category_title}\n"
        f"套餐：{plan_label}\n"
        f"金额：<b>{amount} U</b>\n"
        f"状态：<b>{status}</b>\n"
        f"创建时间：{created_str}\n"
        f"支付时间：{paid_str}\n"
        f"到期时间：{expire_str}\n"
    )

    rows = []
    if status == "pending":
        rows.append([
            InlineKeyboardButton(text="✅ 确认已付款", callback_data=f"order:approve:{order_code}"),
            InlineKeyboardButton(text="❌ 拒绝", callback_data=f"order:reject:{order_code}"),
        ])
    rows.append([InlineKeyboardButton(text="⬅️ 返回订单列表", callback_data="order:list_pending")])

    await c.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")
    await c.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("order:approve:"))
async def order_approve_cb(c: types.CallbackQuery):
    if not c.message or not c.from_user:
        return

    if not can_use_manage_panel(c.from_user.id):
        return await c.answer("无权限", show_alert=True)

    order_code = c.data.split(":", 2)[2]
    row = get_rental_order(order_code)
    if not row:
        return await c.answer("订单不存在", show_alert=True)

    (
        order_code, user_id, username, full_name, category_key, category_title,
        plan_key, plan_label, amount, status, created_at, paid_at, expires_at, note
    ) = row

    if status == "paid":
        return await c.answer("订单已支付", show_alert=True)

    row2, new_expires_at, err = await activate_rental_order(order_code, granted_by=c.from_user.id)
    if err:
        return await c.answer(err, show_alert=True)

    expire_str = datetime.fromtimestamp(new_expires_at, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")

    try:
        await bot.send_message(
            user_id,
            (
                "✅ <b>续费/租用成功</b>\n\n"
                f"订单号：<code>{order_code}</code>\n"
                f"类型：{category_title}\n"
                f"套餐：{plan_label}\n"
                f"到期时间：<b>{expire_str}</b>\n\n"
                "权限已自动开通/续期。"
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        print("notify paid user failed:", e)

    await c.message.answer(
        (
            f"✅ <b>已确认付款</b>\n\n"
            f"订单号：<code>{order_code}</code>\n"
            f"用户：<code>{user_id}</code>\n"
            f"到期时间：<b>{expire_str}</b>\n"
            "权限已开通/已续期。"
        ),
        parse_mode="HTML",
    )
    await c.answer("✅ 已开通/续期")

@dp.callback_query(lambda c: c.data and c.data.startswith("order:reject:"))
async def order_reject_cb(c: types.CallbackQuery):
    if not c.message or not c.from_user:
        return

    if not can_use_manage_panel(c.from_user.id):
        return await c.answer("无权限", show_alert=True)

    order_code = c.data.split(":", 2)[2]
    row = get_rental_order(order_code)
    if not row:
        return await c.answer("订单不存在", show_alert=True)

    (
        order_code, user_id, username, full_name, category_key, category_title,
        plan_key, plan_label, amount, status, created_at, paid_at, expires_at, note
    ) = row

    if status == "paid":
        return await c.answer("订单已支付", show_alert=True)

    mark_rental_order_rejected(order_code)

    await c.message.answer(
        (
            f"❌ <b>订单已拒绝</b>\n\n"
            f"订单号：<code>{order_code}</code>\n"
            f"用户：<code>{user_id}</code>\n"
            f"套餐：{plan_label}\n"
            f"金额：<b>{amount} U</b>\n"
            f"状态：<b>rejected</b>"
        ),
        parse_mode="HTML",
    )

    try:
        await bot.send_message(
            user_id,
            (
                "❌ <b>您的订单未通过</b>\n\n"
                f"订单号：<code>{order_code}</code>\n"
                f"套餐：{plan_label}\n"
                "如有疑问，请联系管理员。"
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        print("notify reject user failed:", e)

    await c.answer("✅ 已拒绝")

@dp.message(lambda m: m.text in ("订单历史", "租用历史", "历史订单"))
async def order_history_cmd(m: types.Message):
    if not can_use_manage_panel(m.from_user.id):
        return await m.reply("❌ 无权限")
    await m.reply("🧾 <b>订单历史</b>\n\n请选择查看类型：", reply_markup=order_history_kb(), parse_mode="HTML")

@dp.callback_query(lambda c: c.data and c.data.startswith("order:history:"))
async def order_history_cb(c: types.CallbackQuery):
    if not c.message or not c.from_user:
        return

    if not can_use_manage_panel(c.from_user.id):
        return await c.answer("无权限", show_alert=True)

    status = c.data.split(":")[2]
    if status == "all":
        rows = get_rental_orders_by_status(None, limit=20)
        title = "📦 全部订单"
    else:
        rows = get_rental_orders_by_status(status, limit=20)
        title = f"📦 {status}"

    if not rows:
        await c.message.answer(f"{title}\n\n暂无记录")
        return await c.answer()

    text = f"{title}\n\n"
    for row in rows:
        order_code, user_id, username, full_name, category_title, plan_label, amount, st, created_at, paid_at, expires_at = row
        created_str = datetime.fromtimestamp(created_at, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
        paid_str = "-" if not paid_at else datetime.fromtimestamp(paid_at, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
        expire_str = "-" if not expires_at else datetime.fromtimestamp(expires_at, BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")

        text += (
            f"• <code>{order_code}</code>\n"
            f"  {category_title} | {plan_label} | {amount}U | {st}\n"
            f"  用户：<code>{user_id}</code> @{username or '-'}\n"
            f"  创建：{created_str}\n"
            f"  支付：{paid_str}\n"
            f"  到期：{expire_str}\n\n"
        )

    await send_long_text(c.message.chat.id, text, parse_mode="HTML")
    await c.answer()

# ================= BROADCAST =================
@dp.message(lambda m: m.text in ("📣 群发广播", "群发广播"))
async def menu_broadcast(m: types.Message, state: FSMContext):
    if is_private(m):
        if get_user_role(m.from_user.id) not in ("owner", "super"):
            return await m.answer("❌ 只有超级管理员可在私聊里全局群发。")
        scope = "all"
        target_chat_id = -1
    else:
        ensure_group(m)
        if not can_use_manage_panel(m.from_user.id):
            return await m.reply("❌ 无权限")
        scope = "current"
        target_chat_id = m.chat.id

    await state.set_state(BroadcastFSM.waiting_content)
    await state.update_data(scope=scope, target_chat_id=target_chat_id, creator_id=m.from_user.id)
    await m.reply("📢 请发送要广播的内容。")

@dp.message(BroadcastFSM.waiting_content)
async def broadcast_receive_content(m: types.Message, state: FSMContext):
    data = await state.get_data()
    creator_id = data.get("creator_id")

    if creator_id and m.from_user and m.from_user.id != creator_id:
        return

    scope = data.get("scope", "current")
    target_chat_id = data.get("target_chat_id", m.chat.id)

    await state.update_data(
        source_chat_id=m.chat.id,
        source_message_id=m.message_id,
        scope=scope,
        target_chat_id=target_chat_id,
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="确认群发(普通)", callback_data="bc:copy"),
            InlineKeyboardButton(text="确认群发(转发)", callback_data="bc:fwd"),
        ],
        [InlineKeyboardButton(text="取消群发", callback_data="bc:cancel")],
    ])

    await m.reply("请确认广播方式：", reply_markup=kb)
    await state.set_state(BroadcastFSM.waiting_confirm)

@dp.callback_query(lambda c: c.data and c.data.startswith("bc:"))
async def broadcast_callback(c: types.CallbackQuery, state: FSMContext):
    if not c.from_user:
        return

    data = await state.get_data()
    creator_id = data.get("creator_id")

    if creator_id and c.from_user.id != creator_id:
        return await c.answer("❌ 无权限", show_alert=True)

    scope = data.get("scope", "current")
    source_chat_id = data.get("source_chat_id")
    source_message_id = data.get("source_message_id")

    if c.data == "bc:cancel":
        await state.clear()
        if c.message:
            await c.message.edit_text("✅ 已取消群发")
        return await c.answer()

    if c.data not in ("bc:copy", "bc:fwd"):
        return await c.answer()

    if scope == "all":
        targets = [g[0] for g in get_groups()]
    else:
        target_chat_id = data.get("target_chat_id")
        targets = [target_chat_id] if target_chat_id is not None else []

    if not source_chat_id or not source_message_id:
        await state.clear()
        if c.message:
            await c.message.edit_text("❌ 广播内容已失效，请重新发送。")
        return await c.answer()

    ok = 0
    fail = 0
    for chat_id in targets:
        try:
            if c.data == "bc:copy":
                await bot.copy_message(chat_id=chat_id, from_chat_id=source_chat_id, message_id=source_message_id)
            else:
                await bot.forward_message(chat_id=chat_id, from_chat_id=source_chat_id, message_id=source_message_id)
            ok += 1
        except Exception as e:
            fail += 1
            print("broadcast error:", e)

    await state.clear()
    if c.message:
        await c.message.edit_text(f"✅ 群发完成\n成功：{ok}\n失败：{fail}")
    await c.answer()

# ================= TRANSACTION HISTORY WEB =================
@dp.message(lambda m: m.text in ("交易历史", "📜 交易历史"))
async def menu_history(m: types.Message):
    if not can_use_bot_ops(m.from_user.id):
        return await m.reply(deny_text())

    await m.reply(
        "📜 <b>交易历史</b>\n\n请选择一个群组，点击后将打开网页历史记录。",
        reply_markup=history_groups_kb(),
        parse_mode="HTML",
    )

@dp.callback_query(lambda c: c.data == "report:full")
async def report_full_cb(c: types.CallbackQuery):
    if not c.message or not c.from_user:
        return
    if not is_group_message(c.message):
        return
    if not is_admin_or_operator(c.message.chat.id, c.from_user):
        return await c.answer("无权限", show_alert=True)

    start_ts, end_ts = day_range()
    await c.message.reply(
        report_text(c.message.chat.id, start_ts, end_ts, title="今日账单"),
        reply_markup=report_kb(c.message.chat.id),
    )
    await c.answer()

# ================= LEDGER HANDLER =================
@dp.message()
async def ledger_handler(m: types.Message):
    if should_ignore_message(m):
        return
    if not is_group_message(m):
        return
    if not m.text:
        return
    if m.text.startswith("/"):
        return

    ensure_group(m)

    if not get_enabled(m.chat.id):
        return

    txt = m.text.strip()

    if txt in ("+0", "-0", "0"):
        start_ts, end_ts = day_range()
        await send_long_text(
            m.chat.id,
            report_text(m.chat.id, start_ts, end_ts, title="今日账单"),
            reply_markup=report_kb(m.chat.id),
        )
        return

    if txt.startswith("P+") or txt.startswith("P-"):
        if not is_admin_or_operator(m.chat.id, m.from_user):
            return await m.reply(deny_text())

        parsed = parse_amount_expr(txt[1:], m.chat.id, default_direct_unit=True)
        if not parsed:
            return await m.reply("❌ 格式错误")

        target = None
        if m.reply_to_message and m.reply_to_message.from_user:
            target = m.reply_to_message.from_user.full_name

        add_transaction(
            chat_id=m.chat.id,
            user_id=m.from_user.id,
            username=m.from_user.username or "",
            display_name=m.from_user.full_name or "",
            target_name=target,
            kind="reserve",
            raw_amount=parsed["raw_amount"],
            unit_amount=parsed["unit_amount"],
            rate_used=parsed["rate_used"],
            fee_used=parsed["fee_used"],
            note="寄存",
            original_text=txt,
        )

        start_ts, end_ts = day_range()
        await send_long_text(
            m.chat.id,
            report_text(m.chat.id, start_ts, end_ts, title="今日账单"),
            reply_markup=report_kb(m.chat.id),
        )
        return

    if txt.startswith("下发"):
        if not is_admin_or_operator(m.chat.id, m.from_user):
            return await m.reply(deny_text())

        body = txt[len("下发"):].strip()
        if not body:
            return await m.reply("格式：下发5000 / 下发1000R / 下发1000/7.8")

        has_conversion = ("R" in body) or ("r" in body) or ("/" in body) or ("*" in body)
        expr = body.replace("R", "").replace("r", "")
        parsed = parse_amount_expr(expr, m.chat.id, default_direct_unit=not has_conversion)
        if not parsed:
            return await m.reply("❌ 下发格式错误")

        target = None
        if m.reply_to_message and m.reply_to_message.from_user:
            target = m.reply_to_message.from_user.full_name

        add_transaction(
            chat_id=m.chat.id,
            user_id=m.from_user.id,
            username=m.from_user.username or "",
            display_name=m.from_user.full_name or "",
            target_name=target,
            kind="payout",
            raw_amount=parsed["raw_amount"],
            unit_amount=parsed["unit_amount"],
            rate_used=parsed["rate_used"],
            fee_used=parsed["fee_used"],
            note="下发",
            original_text=txt,
        )

        start_ts, end_ts = day_range()
        await send_long_text(
            m.chat.id,
            report_text(m.chat.id, start_ts, end_ts, title="今日账单"),
            reply_markup=report_kb(m.chat.id),
        )
        return

    target_name, body = split_target_prefix(txt)

    if not body or body[0] not in ("+", "-"):
        return

    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply(deny_text())

    note = ""
    if " " in body:
        first_part, note = body.split(" ", 1)
        amount_expr = first_part.strip()
        note = note.strip()
    else:
        amount_expr = body.strip()

    parsed = parse_amount_expr(amount_expr, m.chat.id, default_direct_unit=False)
    if not parsed:
        return await m.reply("❌ 记账格式错误")

    kind = "income" if amount_expr.startswith("+") else "payout"

    if not target_name:
        if m.reply_to_message and m.reply_to_message.from_user:
            target_name = m.reply_to_message.from_user.full_name
        else:
            target_name = ""

    add_transaction(
        chat_id=m.chat.id,
        user_id=m.from_user.id,
        username=m.from_user.username or "",
        display_name=m.from_user.full_name or "",
        target_name=target_name,
        kind=kind,
        raw_amount=parsed["raw_amount"],
        unit_amount=parsed["unit_amount"],
        rate_used=parsed["rate_used"],
        fee_used=parsed["fee_used"],
        note=note,
        original_text=txt,
    )

    start_ts, end_ts = day_range()
    await send_long_text(
        m.chat.id,
        report_text(m.chat.id, start_ts, end_ts, title="今日账单"),
        reply_markup=report_kb(m.chat.id),
    )

# ================= USER / BOT JOIN =================
@dp.message(lambda m: bool(m.new_chat_members))
async def new_members(m: types.Message):
    ensure_group(m)

    if not WELCOME_ENABLED:
        return

    try:
        names = ", ".join(u.full_name for u in m.new_chat_members if not u.is_bot)
        if names:
            await m.reply(WELCOME_TEXT.format(name=names))
    except Exception as e:
        print("new_members error:", e)

@dp.my_chat_member()
async def on_bot_member_update(e: types.ChatMemberUpdated):
    try:
        if e.new_chat_member.status in ("member", "administrator") and e.old_chat_member.status == "left":
            save_group(e.chat.id, e.chat.title or "Unnamed group")
            await bot.send_message(e.chat.id, "✅ 记账机器人已加入本群。")
    except Exception as ex:
        print("on_bot_member_update error:", ex)

# ================= WEBHOOK / HEALTH =================
@app.post("/webhook")
async def webhook(req: Request):
    if TELEGRAM_SECRET_TOKEN:
        secret = req.headers.get("x-telegram-bot-api-secret-token", "")
        if secret != TELEGRAM_SECRET_TOKEN:
            print("webhook secret mismatch")
            raise HTTPException(status_code=401, detail="Unauthorized")

    data = await req.json()
    update = types.Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}


# ================= WEB AUTH HELPERS =================
def get_web_admin_name():
    return WEB_ADMIN_NAME or "BOT 888"

def is_web_logged_in(request: Request):
    session = request.cookies.get("god_session", "")
    return session == WEB_TOKEN

def guard(request: Request):
    if not is_web_logged_in(request):
        return RedirectResponse(url="/login", status_code=302)
    return None

def simple_page(title: str, subtitle: str, body: str = ""):
    return f"""
<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(title)}</title>
<style>
body {{
    margin: 0;
    font-family: Inter, Arial, sans-serif;
    background: linear-gradient(135deg, #060913 0%, #0a1020 45%, #070b17 100%);
    color: #eaf2ff;
    padding: 30px;
}}
.wrap {{
    max-width: 1380px;
    margin: 0 auto;
}}
.top {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 24px;
    flex-wrap: wrap;
    gap: 12px;
}}
.title {{
    font-size: 36px;
    font-weight: 900;
}}
.sub {{
    color: #8da2c0;
    margin-top: 8px;
}}
.back {{
    color: white;
    text-decoration: none;
    padding: 12px 16px;
    border-radius: 14px;
    background: rgba(255,255,255,.05);
    border: 1px solid rgba(255,255,255,.08);
}}
.quick-links {{
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
    margin-bottom: 18px;
}}
.quick-links a {{
    text-decoration: none;
    color: white;
    padding: 10px 14px;
    border-radius: 12px;
    background: rgba(255,255,255,.05);
    border: 1px solid rgba(255,255,255,.08);
}}
.card {{
    background: rgba(17,25,40,.72);
    border: 1px solid rgba(255,255,255,.08);
    border-radius: 24px;
    padding: 24px;
    margin-bottom: 20px;
}}
table {{
    width: 100%;
    border-collapse: collapse;
    margin-top: 18px;
}}
th, td {{
    padding: 14px 12px;
    border-bottom: 1px solid rgba(255,255,255,.08);
    text-align: left;
    vertical-align: top;
}}
th {{
    color: #8da2c0;
    font-size: 13px;
    text-transform: uppercase;
}}
.badge {{
    display: inline-block;
    padding: 6px 12px;
    border-radius: 999px;
    background: rgba(34,227,142,.12);
    border: 1px solid rgba(34,227,142,.2);
    color: #98f3c5;
    font-size: 12px;
    font-weight: 700;
}}
.badge.red {{
    background: rgba(255,93,115,.12);
    border-color: rgba(255,93,115,.20);
    color: #ff9baa;
}}
.badge.yellow {{
    background: rgba(255,204,51,.12);
    border-color: rgba(255,204,51,.20);
    color: #ffe38b;
}}
.mono {{
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    word-break: break-all;
}}
.grid {{
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 14px;
}}
.stat {{
    background: rgba(255,255,255,.03);
    border: 1px solid rgba(255,255,255,.06);
    border-radius: 18px;
    padding: 16px;
}}
.stat-label {{
    color: #8da2c0;
    font-size: 12px;
    margin-bottom: 8px;
}}
.stat-value {{
    font-size: 28px;
    font-weight: 800;
}}
pre {{
    white-space: pre-wrap;
    word-break: break-word;
    color: #eaf2ff;
}}
@media (max-width: 980px) {{
    .grid {{
        grid-template-columns: 1fr 1fr;
    }}
}}
@media (max-width: 640px) {{
    .grid {{
        grid-template-columns: 1fr;
    }}
}}
</style>
</head>
<body>
<div class="wrap">
    <div class="top">
        <div>
            <div class="title">{escape(title)}</div>
            <div class="sub">{escape(subtitle)}</div>
        </div>
        <a class="back" href="/dashboard">← Về Dashboard</a>
    </div>

    <div class="quick-links">
        <a href="/dashboard">🏠 Dashboard</a>
        <a href="/groups">👥 Groups</a>
        <a href="/transactions">💸 Transactions</a>
        <a href="/users">👑 Users</a>
        <a href="/orders">📦 Orders</a>
        <a href="/admins">🛡 Admins</a>
        <a href="/wallet-checks">🔎 Wallet Logs</a>
        <a href="/wallet-summary">📊 Wallet Summary</a>
    </div>

    {body}
</div>
</body>
</html>
"""

# ================= DASHBOARD DATA =================
def dashboard_stats():
    try:
        stats = {
            "vip_users": 0,
            "groups": 0,
            "today_tx": 0,
            "today_amount": 0.0,
            "pending_orders": 0,
            "all_orders": 0,
            "wallet_checks": 0,
            "wallet_users": 0,
        }

        with get_db() as (_conn, cur):
            try:
                cur.execute("SELECT COUNT(*) FROM access_users")
                stats["vip_users"] = int(cur.fetchone()[0] or 0)
            except Exception as e:
                print("dashboard vip_users error:", e)

            try:
                cur.execute("SELECT COUNT(*) FROM groups")
                stats["groups"] = int(cur.fetchone()[0] or 0)
            except Exception as e:
                print("dashboard groups error:", e)

            try:
                start_ts, end_ts = day_range()
                cur.execute(
                    '''
                    SELECT COUNT(*), COALESCE(SUM(unit_amount), 0)
                    FROM transactions
                    WHERE created_at >= %s
                      AND created_at <= %s
                      AND COALESCE(undone, FALSE) = FALSE
                    ''',
                    (start_ts, end_ts)
                )
                row = cur.fetchone() or (0, 0)
                stats["today_tx"] = int(row[0] or 0)
                stats["today_amount"] = float(row[1] or 0)
            except Exception as e:
                print("dashboard today error:", e)

            try:
                cur.execute("SELECT COUNT(*) FROM rental_orders WHERE status = 'pending'")
                stats["pending_orders"] = int(cur.fetchone()[0] or 0)
            except Exception as e:
                print("dashboard pending_orders error:", e)

            try:
                cur.execute("SELECT COUNT(*) FROM rental_orders")
                stats["all_orders"] = int(cur.fetchone()[0] or 0)
            except Exception as e:
                print("dashboard all_orders error:", e)

            try:
                cur.execute("SELECT COUNT(*) FROM wallet_checks")
                stats["wallet_checks"] = int(cur.fetchone()[0] or 0)
            except Exception as e:
                print("dashboard wallet_checks error:", e)

            try:
                cur.execute("SELECT COUNT(DISTINCT user_id) FROM wallet_checks")
                stats["wallet_users"] = int(cur.fetchone()[0] or 0)
            except Exception as e:
                print("dashboard wallet_users error:", e)

        return stats

    except Exception as e:
        print("dashboard_stats error:", e)
        return {
            "vip_users": 0,
            "groups": 0,
            "today_tx": 0,
            "today_amount": 0,
            "pending_orders": 0,
            "all_orders": 0,
            "wallet_checks": 0,
            "wallet_users": 0,
        }

def dashboard_chart():
    try:
        labels = []
        values = []

        with get_db() as (_conn, cur):
            for i in range(6, -1, -1):
                d = datetime.now(BEIJING_TZ) - timedelta(days=i)
                start = d.replace(hour=0, minute=0, second=0, microsecond=0)
                end = start + timedelta(days=1)

                cur.execute(
                    '''
                    SELECT COALESCE(SUM(unit_amount), 0)
                    FROM transactions
                    WHERE created_at >= %s
                      AND created_at < %s
                      AND COALESCE(undone, FALSE) = FALSE
                    ''',
                    (int(start.timestamp()), int(end.timestamp()))
                )

                amount = cur.fetchone()[0] or 0
                labels.append(d.strftime("%m-%d"))
                values.append(float(amount))

        return labels, values

    except Exception as e:
        print("dashboard_chart error:", e)
        return [], []

# ================= PREMIUM LOGIN =================
def premium_login_html(error_msg=""):
    error_block = f'<div class="error-box">{escape(error_msg)}</div>' if error_msg else ""

    return f"""
<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>GOD Login</title>
<style>
:root {{
    --bg: #070b17;
    --panel: rgba(17, 25, 40, 0.78);
    --line: rgba(255,255,255,0.08);
    --text: #eaf2ff;
    --muted: #8da2c0;
}}
* {{
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}}
body {{
    min-height: 100vh;
    font-family: Inter, Arial, sans-serif;
    color: var(--text);
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 24px;
    background:
        radial-gradient(circle at top left, rgba(58,184,255,.18), transparent 28%),
        radial-gradient(circle at top right, rgba(139,92,246,.16), transparent 24%),
        radial-gradient(circle at bottom center, rgba(34,227,142,.10), transparent 28%),
        linear-gradient(135deg, #060913 0%, #0a1020 45%, #070b17 100%);
}}
.wrap {{
    width: 100%;
    max-width: 1120px;
    display: grid;
    grid-template-columns: 1.15fr 0.85fr;
    gap: 26px;
}}
.hero {{
    padding: 42px;
    border-radius: 30px;
    background: rgba(12, 19, 34, 0.78);
    backdrop-filter: blur(18px);
    border: 1px solid var(--line);
    box-shadow: 0 25px 70px rgba(0,0,0,.42);
}}
.hero-badge {{
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 9px 14px;
    border-radius: 999px;
    background: rgba(58,184,255,.12);
    border: 1px solid rgba(58,184,255,.25);
    color: #9addff;
    font-size: 13px;
    margin-bottom: 22px;
}}
.hero-title {{
    font-size: clamp(34px, 6vw, 62px);
    font-weight: 900;
    line-height: 1.02;
    letter-spacing: -.03em;
    margin-bottom: 16px;
    background: linear-gradient(90deg, #8fe8ff 0%, #3ab8ff 30%, #a78bfa 65%, #22e38e 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}}
.hero-sub {{
    color: #8da2c0;
    font-size: 15px;
    line-height: 1.7;
    max-width: 560px;
    margin-bottom: 28px;
}}
.feature {{
    margin-bottom: 14px;
    background: rgba(255,255,255,.03);
    border: 1px solid rgba(255,255,255,.06);
    border-radius: 18px;
    padding: 16px;
}}
.feature-title {{
    font-size: 15px;
    font-weight: 700;
    margin-bottom: 5px;
}}
.feature-text {{
    font-size: 13px;
    color: #8da2c0;
    line-height: 1.6;
}}
.login-card {{
    padding: 34px;
    border-radius: 30px;
    background: rgba(14, 22, 38, 0.86);
    backdrop-filter: blur(18px);
    border: 1px solid var(--line);
    box-shadow: 0 25px 70px rgba(0,0,0,.45);
    display: flex;
    flex-direction: column;
    justify-content: center;
}}
.login-title {{
    font-size: 34px;
    font-weight: 900;
    margin-bottom: 8px;
}}
.login-sub {{
    color: #8da2c0;
    font-size: 14px;
    line-height: 1.6;
    margin-bottom: 24px;
}}
.label {{
    display: block;
    font-size: 13px;
    color: #9bb0cc;
    margin-bottom: 10px;
    text-transform: uppercase;
    letter-spacing: .10em;
}}
.input {{
    width: 100%;
    padding: 16px 18px;
    border-radius: 18px;
    border: 1px solid rgba(255,255,255,.08);
    background: rgba(8, 13, 25, 0.88);
    color: white;
    font-size: 15px;
    outline: none;
    margin-bottom: 18px;
}}
.btn {{
    width: 100%;
    border: none;
    padding: 16px 18px;
    border-radius: 18px;
    background: linear-gradient(90deg, #38bdf8 0%, #3b82f6 38%, #22c55e 100%);
    color: white;
    font-size: 16px;
    font-weight: 800;
    cursor: pointer;
}}
.error-box {{
    margin-bottom: 16px;
    padding: 14px 16px;
    border-radius: 16px;
    background: rgba(255,93,115,.10);
    border: 1px solid rgba(255,93,115,.20);
    color: #ff9baa;
    font-size: 14px;
}}
.note {{
    margin-top: 16px;
    color: #6f87a8;
    font-size: 13px;
    line-height: 1.6;
    text-align: center;
}}
.footer-badge {{
    margin-top: 18px;
    display: flex;
    justify-content: center;
    gap: 10px;
    flex-wrap: wrap;
}}
.footer-pill {{
    padding: 8px 12px;
    border-radius: 999px;
    font-size: 12px;
    color: #b9cae2;
    background: rgba(255,255,255,.04);
    border: 1px solid rgba(255,255,255,.06);
}}
@media (max-width: 980px) {{
    .wrap {{
        grid-template-columns: 1fr;
    }}
}}
</style>
</head>
<body>
    <div class="wrap">
        <div class="hero">
            <div class="hero-badge">⚡ PREMIUM CONTROL ACCESS</div>
            <div class="hero-title">GOD BOT<br>LOGIN PANEL</div>
            <div class="hero-sub">
                Đăng nhập để truy cập hệ thống dashboard premium, theo dõi bot Telegram,
                giao dịch trong ngày, trạng thái đơn hàng và toàn bộ thông tin vận hành.
            </div>

            <div class="feature">
                <div class="feature-title">📈 Real-time Dashboard</div>
                <div class="feature-text">Xem thống kê bot, volume 7 ngày, user VIP, nhóm và đơn hàng.</div>
            </div>
            <div class="feature">
                <div class="feature-title">🛡️ Secure Access</div>
                <div class="feature-text">Chỉ admin có mật khẩu mới vào được khu vực quản trị.</div>
            </div>
            <div class="feature">
                <div class="feature-title">🚀 Premium Interface</div>
                <div class="feature-text">Thiết kế dark glass đồng bộ hoàn toàn với GOD BOT Dashboard.</div>
            </div>
        </div>

        <div class="login-card">
            <div class="login-title">🔐 Đăng nhập</div>
            <div class="login-sub">Nhập mật khẩu quản trị để tiếp tục vào dashboard.</div>

            {error_block}

            <form method="post" action="/login">
                <label class="label">Admin Password</label>
                <input class="input" type="password" name="password" placeholder="Nhập mật khẩu web..." required>
                <button class="btn" type="submit">VÀO DASHBOARD</button>
            </form>

            <div class="note">
                Mật khẩu đăng nhập là giá trị <b>WEB_TOKEN</b> trong file <b>.env</b>.
            </div>

            <div class="footer-badge">
                <div class="footer-pill">Cloudflare SSL</div>
                <div class="footer-pill">FastAPI</div>
                <div class="footer-pill">Telegram Webhook</div>
            </div>
        </div>
    </div>
</body>
</html>
"""

# ================= ROOT / LOGIN =================
@app.get("/", response_class=HTMLResponse)
def home():
    return RedirectResponse(url="/login", status_code=302)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if is_web_logged_in(request):
        return RedirectResponse(url="/dashboard", status_code=302)
    return premium_login_html()

@app.post("/login")
async def login_submit(password: str = Form(...)):
    if password != WEB_TOKEN:
        return HTMLResponse(premium_login_html("❌ Sai mật khẩu đăng nhập"), status_code=401)

    resp = RedirectResponse(url="/dashboard", status_code=303)
    resp.set_cookie(
        key="god_session",
        value=WEB_TOKEN,
        httponly=True,
        samesite="lax",
        secure=IS_PRODUCTION,
        max_age=7 * 24 * 3600,
    )
    return resp

@app.get("/logout")
async def logout():
    resp = RedirectResponse(url="/login", status_code=302)
    resp.delete_cookie("god_session")
    return resp

# ================= DASHBOARD =================
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    auth = guard(request)
    if auth:
        return auth

    stats = dashboard_stats()
    labels, values = dashboard_chart()

    safe_bot_username = escape(BOT_USERNAME or "-")
    safe_webhook = escape(f"{BOT_BASE_URL}/webhook" if BOT_BASE_URL else "Not configured")
    now_text = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    admin_name = escape(get_web_admin_name())

    return f"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>GOD BOT Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
:root {{
    --text: #eaf2ff;
    --muted: #8da2c0;
    --blue: #3ab8ff;
    --green: #22e38e;
    --yellow: #ffcc33;
    --red: #ff5d73;
    --purple: #b38cff;
    --shadow: 0 20px 50px rgba(0,0,0,.35);
}}
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
    font-family: Inter, Arial, sans-serif;
    color: var(--text);
    background:
        radial-gradient(circle at top left, rgba(58,184,255,.18), transparent 30%),
        radial-gradient(circle at top right, rgba(139,92,246,.14), transparent 25%),
        radial-gradient(circle at bottom center, rgba(34,227,142,.12), transparent 30%),
        linear-gradient(135deg, #060913 0%, #0a1020 45%, #070b17 100%);
    padding: 28px;
}}
.container {{ max-width: 1480px; margin: 0 auto; }}
.hero {{
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 20px;
    margin-bottom: 24px;
    flex-wrap: wrap;
}}
.badge {{
    display: inline-flex;
    align-items: center;
    gap: 8px;
    width: fit-content;
    padding: 8px 14px;
    border-radius: 999px;
    background: rgba(58,184,255,.12);
    border: 1px solid rgba(58,184,255,.25);
    color: #8ed9ff;
    font-size: 13px;
    margin-bottom: 10px;
}}
.title {{
    font-size: clamp(32px, 5vw, 64px);
    font-weight: 900;
    letter-spacing: -.03em;
    line-height: 1;
    background: linear-gradient(90deg, #8fe8ff 0%, #3ab8ff 30%, #a78bfa 65%, #22e38e 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}}
.subtitle {{
    color: var(--muted);
    font-size: 15px;
    margin-top: 10px;
}}
.hero-right {{
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    justify-content: flex-end;
    align-items: flex-start;
}}
.pill {{
    padding: 12px 18px;
    border-radius: 999px;
    background: rgba(17, 25, 40, 0.78);
    border: 1px solid rgba(255,255,255,.08);
    box-shadow: var(--shadow);
    color: var(--text);
    font-size: 14px;
}}
.pill.online {{
    color: #9ff3c8;
}}
.pill a {{
    color: white;
    text-decoration: none;
}}
.welcome-box {{
    width: 100%;
    max-width: 420px;
    background: rgba(17, 25, 40, 0.78);
    border: 1px solid rgba(255,255,255,.08);
    box-shadow: var(--shadow);
    border-radius: 24px;
    padding: 18px 20px;
}}
.welcome-line-1 {{
    font-size: 20px;
    font-weight: 800;
    color: #7ee7ff;
    margin-bottom: 6px;
}}
.welcome-line-2 {{
    font-size: 15px;
    color: #b4c6df;
    margin-bottom: 8px;
}}
.welcome-line-3 {{
    font-size: 14px;
    color: #8ef0b9;
}}
.quick-nav {{
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
    margin: 18px 0 26px;
}}
.quick-btn {{
    text-decoration: none;
    color: white;
    padding: 12px 18px;
    border-radius: 14px;
    background: rgba(255,255,255,.05);
    border: 1px solid rgba(255,255,255,.08);
    transition: .2s ease;
    font-weight: 700;
}}
.quick-btn:hover {{
    transform: translateY(-2px);
    background: rgba(58,184,255,.12);
    border-color: rgba(58,184,255,.3);
}}
.grid {{
    display: grid;
    grid-template-columns: repeat(12, 1fr);
    gap: 18px;
}}
.card {{
    background: rgba(17, 25, 40, 0.72);
    border: 1px solid rgba(255,255,255,.08);
    border-radius: 24px;
    box-shadow: var(--shadow);
}}
.card-link {{
    text-decoration: none;
    color: inherit;
    display: block;
}}
.stat {{
    grid-column: span 3;
    padding: 22px;
    min-height: 150px;
    cursor: pointer;
    transition: .2s ease;
}}
.stat:hover {{
    transform: translateY(-4px);
    border-color: rgba(58,184,255,.28);
}}
.stat-top {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 22px;
}}
.icon {{
    width: 48px;
    height: 48px;
    border-radius: 16px;
    display: grid;
    place-items: center;
    font-size: 22px;
    background: rgba(255,255,255,.06);
    border: 1px solid rgba(255,255,255,.08);
}}
.stat-label {{
    color: var(--muted);
    font-size: 13px;
    text-transform: uppercase;
    letter-spacing: .12em;
}}
.stat-value {{
    font-size: 42px;
    font-weight: 800;
    line-height: 1;
    margin-bottom: 12px;
}}
.stat-sub {{
    color: var(--muted);
    font-size: 13px;
}}
.chart-card {{
    grid-column: span 8;
    padding: 24px;
    min-height: 420px;
}}
.side-card {{
    grid-column: span 4;
    padding: 24px;
}}
.section-title {{
    font-size: 20px;
    font-weight: 700;
    margin-bottom: 8px;
}}
.section-sub {{
    color: var(--muted);
    font-size: 14px;
    margin-bottom: 20px;
}}
.kv {{
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 18px;
    padding: 16px 0;
    border-bottom: 1px solid rgba(255,255,255,.06);
}}
.kv:last-child {{ border-bottom: none; }}
.kv-key {{
    color: var(--muted);
    font-size: 13px;
    text-transform: uppercase;
    letter-spacing: .08em;
}}
.kv-val {{
    text-align: right;
    font-size: 14px;
    color: var(--text);
    word-break: break-all;
    max-width: 70%;
}}
.footer {{
    margin-top: 22px;
    text-align: center;
    color: var(--muted);
    font-size: 13px;
}}
@media (max-width: 1180px) {{
    .stat {{ grid-column: span 6; }}
    .chart-card {{ grid-column: span 12; }}
    .side-card {{ grid-column: span 12; }}
}}
@media (max-width: 720px) {{
    .stat {{ grid-column: span 12; }}
}}
</style>
</head>
<body>
<div class="container">
    <div class="hero">
        <div>
            <div class="badge">⚡ PREMIUM CONTROL PANEL</div>
            <div class="title">GOD BOT DASHBOARD</div>
            <div class="subtitle">Real-time Telegram bot analytics • dark premium interface • admin control panel</div>
        </div>

        <div class="hero-right">
            <div class="pill">🕒 {now_text}</div>
            <div class="pill online">● ONLINE</div>
            <div class="pill"><a href="/logout">🚪 LOGOUT</a></div>

            <div class="welcome-box">
                <div class="welcome-line-1">Welcome Admin</div>
                <div class="welcome-line-2">Xin chào Owner</div>
                <div class="welcome-line-3">Logged in as {admin_name}</div>
            </div>
        </div>
    </div>

    <div class="quick-nav">
        <a class="quick-btn" href="/dashboard">🏠 Dashboard</a>
        <a class="quick-btn" href="/groups">👥 Groups</a>
        <a class="quick-btn" href="/transactions">💸 Transactions</a>
        <a class="quick-btn" href="/bots">🤖 Bots</a>
        <a class="quick-btn" href="/admins">🛡 Admins</a>
        <a class="quick-btn" href="/orders">📦 Orders</a>
        <a class="quick-btn" href="/users">👑 Users</a>
        <a class="quick-btn" href="/wallet-checks">🔎 Wallet Logs</a>
        <a class="quick-btn" href="/wallet-summary">📊 Wallet Summary</a>
    </div>

    <div class="grid">
        <a class="card card-link" href="/users">
            <div class="stat">
                <div class="stat-top"><div><div class="stat-label">VIP USERS</div></div><div class="icon">👑</div></div>
                <div class="stat-value" style="color:#22e38e;">{stats["vip_users"]}</div>
                <div class="stat-sub">Premium access accounts</div>
            </div>
        </a>

        <a class="card card-link" href="/groups">
            <div class="stat">
                <div class="stat-top"><div><div class="stat-label">GROUPS</div></div><div class="icon">👥</div></div>
                <div class="stat-value" style="color:#3ab8ff;">{stats["groups"]}</div>
                <div class="stat-sub">Connected Telegram groups</div>
            </div>
        </a>

        <a class="card card-link" href="/transactions">
            <div class="stat">
                <div class="stat-top"><div><div class="stat-label">TODAY TX</div></div><div class="icon">📊</div></div>
                <div class="stat-value" style="color:#ffcc33;">{stats["today_tx"]}</div>
                <div class="stat-sub">Transactions recorded today</div>
            </div>
        </a>

        <a class="card card-link" href="/transactions">
            <div class="stat">
                <div class="stat-top"><div><div class="stat-label">TODAY U</div></div><div class="icon">💸</div></div>
                <div class="stat-value" style="color:#22e38e;">{float(stats["today_amount"]):.2f}</div>
                <div class="stat-sub">Total volume today</div>
            </div>
        </a>

        <a class="card card-link" href="/orders">
            <div class="stat">
                <div class="stat-top"><div><div class="stat-label">PENDING ORDERS</div></div><div class="icon">⏳</div></div>
                <div class="stat-value" style="color:#ff5d73;">{stats["pending_orders"]}</div>
                <div class="stat-sub">Orders waiting for approval</div>
            </div>
        </a>

        <a class="card card-link" href="/orders">
            <div class="stat">
                <div class="stat-top"><div><div class="stat-label">ALL ORDERS</div></div><div class="icon">📦</div></div>
                <div class="stat-value" style="color:#b38cff;">{stats["all_orders"]}</div>
                <div class="stat-sub">Rental / renew history</div>
            </div>
        </a>

        <a class="card card-link" href="/wallet-checks">
            <div class="stat">
                <div class="stat-top"><div><div class="stat-label">WALLET CHECKS</div></div><div class="icon">🔎</div></div>
                <div class="stat-value" style="color:#3ab8ff;">{stats["wallet_checks"]}</div>
                <div class="stat-sub">Wallet query logs</div>
            </div>
        </a>

        <a class="card card-link" href="/wallet-summary">
            <div class="stat">
                <div class="stat-top"><div><div class="stat-label">WALLET USERS</div></div><div class="icon">👤</div></div>
                <div class="stat-value" style="color:#22e38e;">{stats["wallet_users"]}</div>
                <div class="stat-sub">Users sent wallet addresses</div>
            </div>
        </a>

        <div class="card chart-card">
            <div class="section-title">📈 7 Day Volume</div>
            <div class="section-sub">Transaction volume trend for the last 7 days</div>
            <canvas id="myChart" height="120"></canvas>
        </div>

        <div class="card side-card">
            <div class="section-title">🛰 System Overview</div>
            <div class="section-sub">Core runtime information and public endpoints</div>

            <div class="kv"><div class="kv-key">Bot Username</div><div class="kv-val">@{safe_bot_username}</div></div>
            <div class="kv"><div class="kv-key">Webhook</div><div class="kv-val">{safe_webhook}</div></div>
            <div class="kv"><div class="kv-key">Payment Wallet</div><div class="kv-val">{escape(PAYMENT_ADDRESS)}</div></div>
            <div class="kv"><div class="kv-key">Logged In As</div><div class="kv-val">{admin_name}</div></div>
        </div>
    </div>

    <div class="footer">GOD MODE • Auto refresh every 20 seconds</div>
</div>

<script>
const labels = {json.dumps(labels, ensure_ascii=False)};
const values = {json.dumps(values)};

const ctx = document.getElementById('myChart').getContext('2d');
const gradient = ctx.createLinearGradient(0, 0, 0, 320);
gradient.addColorStop(0, 'rgba(58,184,255,0.38)');
gradient.addColorStop(1, 'rgba(58,184,255,0.02)');

new Chart(ctx, {{
    type: 'line',
    data: {{
        labels: labels,
        datasets: [{{
            label: '7 Day Volume',
            data: values,
            borderColor: '#43c6ff',
            backgroundColor: gradient,
            fill: true,
            borderWidth: 3,
            tension: 0.38
        }}]
    }},
    options: {{
        responsive: true,
        maintainAspectRatio: false,
        plugins: {{
            legend: {{
                labels: {{
                    color: '#d8e6ff'
                }}
            }}
        }},
        scales: {{
            x: {{
                ticks: {{ color: '#9fb3d1' }},
                grid: {{ color: 'rgba(255,255,255,0.05)' }}
            }},
            y: {{
                ticks: {{ color: '#9fb3d1' }},
                grid: {{ color: 'rgba(255,255,255,0.05)' }}
            }}
        }}
    }}
}});
setTimeout(() => location.reload(), 20000);
</script>
</body>
</html>
"""

# ================= WEB PAGES =================
@app.get("/bots", response_class=HTMLResponse)
async def bots_page(request: Request):
    auth = guard(request)
    if auth:
        return auth

    body = f"""
    <div class="card">
        <table>
            <thead>
                <tr>
                    <th>Bot</th>
                    <th>Username</th>
                    <th>Status</th>
                    <th>Webhook</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>God Bot</td>
                    <td>@{escape(BOT_USERNAME or "-")}</td>
                    <td><span class="badge">ONLINE</span></td>
                    <td class="mono">{escape(f"{BOT_BASE_URL}/webhook" if BOT_BASE_URL else "Not configured")}</td>
                </tr>
            </tbody>
        </table>
    </div>
    """
    return HTMLResponse(simple_page("🤖 Bot Management", "Quản lý bot đang hoạt động", body))

@app.get("/admins", response_class=HTMLResponse)
async def admins_page(request: Request):
    auth = guard(request)
    if auth:
        return auth

    rows = get_all_admins()
    html_rows = []

    if BOT_OWNER_ID:
        html_rows.append(f"<tr><td>{BOT_OWNER_ID}</td><td>owner</td><td><span class='badge'>ACTIVE</span></td></tr>")
    if SUPER_ADMIN_ID and SUPER_ADMIN_ID != BOT_OWNER_ID:
        html_rows.append(f"<tr><td>{SUPER_ADMIN_ID}</td><td>super(env)</td><td><span class='badge'>ACTIVE</span></td></tr>")

    for uid, role in rows:
        if uid in (BOT_OWNER_ID, SUPER_ADMIN_ID):
            continue
        html_rows.append(
            f"<tr><td>{uid}</td><td>{escape(role)}</td><td><span class='badge'>ACTIVE</span></td></tr>"
        )

    body = f"""
    <div class="card">
        <table>
            <thead><tr><th>User ID</th><th>Role</th><th>Status</th></tr></thead>
            <tbody>{''.join(html_rows) if html_rows else '<tr><td colspan="3">No admins</td></tr>'}</tbody>
        </table>
    </div>
    """
    return HTMLResponse(simple_page("🛡 Admin Management", "Quản lý admin web", body))

@app.get("/orders", response_class=HTMLResponse)
async def orders_page(request: Request):
    auth = guard(request)
    if auth:
        return auth

    rows = get_rental_orders_by_status(None, limit=100)
    trs = []
    for row in rows:
        order_code, user_id, username, full_name, category_title, plan_label, amount, st, created_at, paid_at, expires_at = row
        badge_cls = "badge"
        if st == "rejected":
            badge_cls = "badge red"
        elif st == "pending":
            badge_cls = "badge yellow"

        trs.append(
            f"<tr>"
            f"<td>{escape(order_code)}</td>"
            f"<td>{user_id}</td>"
            f"<td>@{escape(username or '-')}</td>"
            f"<td>{escape(category_title or '-')}</td>"
            f"<td>{escape(plan_label or '-')}</td>"
            f"<td>{fmt_num(amount)}U</td>"
            f"<td><span class='{badge_cls}'>{escape(st)}</span></td>"
            f"<td>{fmt_ts(created_at)}</td>"
            f"</tr>"
        )

    body = f"""
    <div class="card">
        <table>
            <thead>
                <tr>
                    <th>Order</th>
                    <th>User ID</th>
                    <th>Username</th>
                    <th>Category</th>
                    <th>Plan</th>
                    <th>Amount</th>
                    <th>Status</th>
                    <th>Created</th>
                </tr>
            </thead>
            <tbody>{''.join(trs) if trs else '<tr><td colspan="8">No orders</td></tr>'}</tbody>
        </table>
    </div>
    """
    return HTMLResponse(simple_page("📦 Orders", "Quản lý đơn hàng / gia hạn", body))

@app.get("/users", response_class=HTMLResponse)
async def users_page(request: Request, page: int = 1, keyword: str = "", status: str = ""):
    auth = guard(request)
    if auth:
        return auth

    limit = 20
    offset = (max(page, 1) - 1) * limit
    rows = get_access_users_page(limit=limit, offset=offset, keyword=keyword or None, status=status or None)
    total = count_access_users_filtered(keyword=keyword or None, status=status or None)
    total_pages = max(1, (total + limit - 1) // limit)

    trs = []
    for user_id, username, granted_by, granted_at, expires_at in rows:
        role = "VIP"
        exp = "Permanent" if expires_at is None else fmt_ts(expires_at)
        trs.append(
            f"<tr>"
            f"<td>{user_id}</td>"
            f"<td>@{escape(username or '-')}</td>"
            f"<td>{granted_by or '-'}</td>"
            f"<td>{fmt_ts(granted_at)}</td>"
            f"<td>{exp}</td>"
            f"<td><span class='badge'>{role}</span></td>"
            f"</tr>"
        )

    body = f"""
    <div class="card">
        <div class="grid">
            <div class="stat"><div class="stat-label">Page</div><div class="stat-value">{page}</div></div>
            <div class="stat"><div class="stat-label">Total Pages</div><div class="stat-value">{total_pages}</div></div>
            <div class="stat"><div class="stat-label">Total Users</div><div class="stat-value">{total}</div></div>
            <div class="stat"><div class="stat-label">Rows</div><div class="stat-value">{len(rows)}</div></div>
        </div>
    </div>

    <div class="card">
        <table>
            <thead>
                <tr>
                    <th>User ID</th>
                    <th>Username</th>
                    <th>Granted By</th>
                    <th>Granted At</th>
                    <th>Expire At</th>
                    <th>Role</th>
                </tr>
            </thead>
            <tbody>{''.join(trs) if trs else '<tr><td colspan="6">No users</td></tr>'}</tbody>
        </table>
    </div>
    """
    return HTMLResponse(simple_page("👑 Users", "Quản lý user VIP", body))

@app.get("/transactions", response_class=HTMLResponse)
async def transactions_page(request: Request, date: str | None = None):
    auth = guard(request)
    if auth:
        return auth

    try:
        if date:
            dt = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=BEIJING_TZ)
            start_ts = int(dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
            end_ts = int((dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1) - timedelta(seconds=1)).timestamp())
        else:
            start_ts, end_ts = day_range()
    except Exception:
        start_ts, end_ts = day_range()

    trs = []
    with get_db() as (_conn, cur):
        cur.execute(
            '''
            SELECT id, chat_id, user_id, username, display_name, target_name, kind,
                   raw_amount, unit_amount, rate_used, fee_used, note, original_text,
                   created_at, undone
            FROM transactions
            WHERE created_at >= %s
              AND created_at <= %s
              AND COALESCE(undone, FALSE) = FALSE
            ORDER BY created_at DESC, id DESC
            LIMIT 200
            ''',
            (start_ts, end_ts)
        )
        rows = cur.fetchall()

    for tx in rows:
        tx_id, chat_id, user_id, username, display_name, target_name, kind, raw_amount, unit_amount, rate_used, fee_used, note, original_text, created_at, undone = tx
        trs.append(
            f"<tr>"
            f"<td>{fmt_ts(created_at)}</td>"
            f"<td>{chat_id}</td>"
            f"<td>{escape(kind or '-')}</td>"
            f"<td>{fmt_num(raw_amount)}</td>"
            f"<td>{fmt_num(unit_amount)}U</td>"
            f"<td>@{escape(username or '-')}</td>"
            f"<td>{escape(target_name or '-')}</td>"
            f"<td>{escape(note or '-')}</td>"
            f"</tr>"
        )

    body = f"""
    <div class="card">
        <table>
            <thead>
                <tr>
                    <th>Time</th>
                    <th>Chat ID</th>
                    <th>Type</th>
                    <th>Raw</th>
                    <th>U</th>
                    <th>Username</th>
                    <th>Target</th>
                    <th>Note</th>
                </tr>
            </thead>
            <tbody>{''.join(trs) if trs else '<tr><td colspan="8">No transactions</td></tr>'}</tbody>
        </table>
    </div>
    """
    return HTMLResponse(simple_page("💸 Transaction History", "Lịch sử giao dịch toàn hệ thống", body))

@app.get("/groups", response_class=HTMLResponse)
async def groups_page(request: Request):
    auth = guard(request)
    if auth:
        return auth

    rows = get_groups()
    trs = []
    for chat_id, title in rows:
        trs.append(
            f"<tr>"
            f"<td>{escape(title or '-')}</td>"
            f"<td>{chat_id}</td>"
            f"<td><span class='badge'>ACTIVE</span></td>"
            f"<td><a class='back' href='/group/{chat_id}'>History</a></td>"
            f"</tr>"
        )

    body = f"""
    <div class="card">
        <table>
            <thead><tr><th>Group</th><th>Chat ID</th><th>Status</th><th>Action</th></tr></thead>
            <tbody>{''.join(trs) if trs else '<tr><td colspan="4">No groups</td></tr>'}</tbody>
        </table>
    </div>
    """
    return HTMLResponse(simple_page("👥 Group Management", "Quản lý nhóm Telegram", body))

@app.get("/group/{chat_id}", response_class=HTMLResponse)
async def group_history_page(chat_id: int, request: Request, date: str | None = None):
    auth = guard(request)
    if auth:
        return auth

    try:
        if date:
            dt = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=BEIJING_TZ)
            start_ts = int(dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
            end_ts = int((dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1) - timedelta(seconds=1)).timestamp())
        else:
            start_ts, end_ts = day_range()
    except Exception:
        start_ts, end_ts = day_range()

    txs = get_transactions(chat_id, start_ts=start_ts, end_ts=end_ts)
    stats = summarize_transactions(txs)

    trs = []
    for tx in txs:
        tx_id, chat_id, user_id, username, display_name, target_name, kind, raw_amount, unit_amount, rate_used, fee_used, note, original_text, created_at, undone = tx
        trs.append(
            f"<tr>"
            f"<td>{fmt_ts(created_at)}</td>"
            f"<td>{escape(kind or '-')}</td>"
            f"<td>{fmt_num(raw_amount)}</td>"
            f"<td>{fmt_num(unit_amount)}U</td>"
            f"<td>{fmt_num(rate_used)}</td>"
            f"<td>{fmt_num(fee_used)}%</td>"
            f"<td>{escape(display_name or '-')}</td>"
            f"<td>{escape(target_name or '-')}</td>"
            f"<td>@{escape(username or '-')}</td>"
            f"<td>{escape(note or '-')}</td>"
            f"</tr>"
        )

    body = f"""
    <div class="card">
        <div class="grid">
            <div class="stat"><div class="stat-label">总记录数</div><div class="stat-value">{len(txs)}</div></div>
            <div class="stat"><div class="stat-label">正常入款</div><div class="stat-value">{fmt_num(stats['total_income_unit'])}U</div></div>
            <div class="stat"><div class="stat-label">已下发</div><div class="stat-value">{fmt_num(stats['paid'])}U</div></div>
            <div class="stat"><div class="stat-label">待下发</div><div class="stat-value">{fmt_num(stats['pending'])}U</div></div>
        </div>
    </div>

    <div class="card">
        <table>
            <thead>
                <tr>
                    <th>Time</th>
                    <th>Type</th>
                    <th>Raw</th>
                    <th>U</th>
                    <th>Rate</th>
                    <th>Fee</th>
                    <th>By</th>
                    <th>Target</th>
                    <th>Username</th>
                    <th>Note</th>
                </tr>
            </thead>
            <tbody>{''.join(trs) if trs else '<tr><td colspan="10">No transactions</td></tr>'}</tbody>
        </table>
    </div>
    """
    return HTMLResponse(simple_page(f"📘 Group {chat_id}", "Lịch sử giao dịch nhóm", body))

@app.get("/wallet-checks", response_class=HTMLResponse)
async def wallet_checks_page(request: Request, page: int = 1):
    auth = guard(request)
    if auth:
        return auth

    limit = 30
    offset = (max(page, 1) - 1) * limit
    rows = get_wallet_checks_page(limit=limit, offset=offset)
    total = count_wallet_checks()
    total_pages = max(1, (total + limit - 1) // limit)

    trs = []
    for row in rows:
        _id, chat_id, user_id, username, full_name, address, trx_balance, usdt_balance, tx_count, created_at = row
        sender = full_name or username or str(user_id)
        trs.append(
            f"<tr>"
            f"<td>{fmt_ts(created_at)}</td>"
            f"<td>{chat_id}</td>"
            f"<td>{user_id}</td>"
            f"<td>{escape(sender)}</td>"
            f"<td>@{escape(username or '-')}</td>"
            f"<td class='mono'>{escape(address or '-')}</td>"
            f"<td>{fmt_num(trx_balance)}</td>"
            f"<td>{fmt_num(usdt_balance)}</td>"
            f"<td>{tx_count if tx_count is not None else 'N/A'}</td>"
            f"</tr>"
        )

    body = f"""
    <div class="card">
        <div class="grid">
            <div class="stat"><div class="stat-label">Total Wallet Checks</div><div class="stat-value">{total}</div></div>
            <div class="stat"><div class="stat-label">Current Page Rows</div><div class="stat-value">{len(rows)}</div></div>
            <div class="stat"><div class="stat-label">Page</div><div class="stat-value">{page}</div></div>
            <div class="stat"><div class="stat-label">Total Pages</div><div class="stat-value">{total_pages}</div></div>
        </div>
    </div>

    <div class="card">
        <table>
            <thead>
                <tr>
                    <th>Time</th>
                    <th>Chat ID</th>
                    <th>User ID</th>
                    <th>Sender</th>
                    <th>Username</th>
                    <th>Address</th>
                    <th>TRX</th>
                    <th>USDT</th>
                    <th>TX Count</th>
                </tr>
            </thead>
            <tbody>{''.join(trs) if trs else '<tr><td colspan="9">No wallet logs</td></tr>'}</tbody>
        </table>
    </div>
    """
    return HTMLResponse(simple_page("🔎 Wallet Check Logs", "Tất cả log user gửi ví", body))

@app.get("/wallet-summary", response_class=HTMLResponse)
async def wallet_summary_page(request: Request):
    auth = guard(request)
    if auth:
        return auth

    total_checks = 0
    distinct_users = 0
    distinct_groups = 0
    user_rows = []
    group_rows = []

    try:
        with get_db() as (_conn, cur):
            cur.execute("SELECT COUNT(*) FROM wallet_checks")
            total_checks = int(cur.fetchone()[0] or 0)

            cur.execute("SELECT COUNT(DISTINCT user_id) FROM wallet_checks")
            distinct_users = int(cur.fetchone()[0] or 0)

            cur.execute("SELECT COUNT(DISTINCT chat_id) FROM wallet_checks")
            distinct_groups = int(cur.fetchone()[0] or 0)

            cur.execute(
                '''
                SELECT
                    user_id,
                    COALESCE(NULLIF(full_name, ''), NULLIF(username, ''), CAST(user_id AS TEXT)) AS sender,
                    username,
                    COUNT(*) AS total_times,
                    MAX(created_at) AS last_time
                FROM wallet_checks
                GROUP BY user_id, sender, username
                ORDER BY total_times DESC, last_time DESC
                LIMIT 50
                '''
            )
            user_rows = cur.fetchall()

            cur.execute(
                '''
                SELECT
                    chat_id,
                    COUNT(*) AS total_times,
                    COUNT(DISTINCT user_id) AS distinct_users_count,
                    MAX(created_at) AS last_time
                FROM wallet_checks
                GROUP BY chat_id
                ORDER BY total_times DESC, last_time DESC
                LIMIT 50
                '''
            )
            group_rows = cur.fetchall()

    except Exception as e:
        print("wallet_summary_page error:", e)

    user_trs = []
    for user_id, sender, username, total_times, last_time in user_rows:
        user_trs.append(
            f"<tr>"
            f"<td>{user_id}</td>"
            f"<td>{escape(sender or '-')}</td>"
            f"<td>@{escape(username or '-')}</td>"
            f"<td><span class='badge'>{total_times} 次</span></td>"
            f"<td>{fmt_ts(last_time)}</td>"
            f"</tr>"
        )

    group_trs = []
    for chat_id, total_times, d_users, last_time in group_rows:
        group_trs.append(
            f"<tr>"
            f"<td>{chat_id}</td>"
            f"<td><span class='badge'>{total_times} 次</span></td>"
            f"<td>{d_users}</td>"
            f"<td>{fmt_ts(last_time)}</td>"
            f"</tr>"
        )

    body = f"""
    <div class="card">
        <div class="grid">
            <div class="stat"><div class="stat-label">Total Wallet Checks</div><div class="stat-value">{total_checks}</div></div>
            <div class="stat"><div class="stat-label">Distinct Users</div><div class="stat-value">{distinct_users}</div></div>
            <div class="stat"><div class="stat-label">Distinct Groups</div><div class="stat-value">{distinct_groups}</div></div>
            <div class="stat"><div class="stat-label">Status</div><div class="stat-value">LIVE</div></div>
        </div>
    </div>

    <div class="card">
        <div style="font-size:22px;font-weight:800;margin-bottom:10px;">👤 User gửi ví bao nhiêu lần</div>
        <table>
            <thead><tr><th>User ID</th><th>Sender</th><th>Username</th><th>Total Times</th><th>Last Time</th></tr></thead>
            <tbody>{''.join(user_trs) if user_trs else '<tr><td colspan="5">No user summary</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <div style="font-size:22px;font-weight:800;margin-bottom:10px;">👥 Thống kê theo nhóm</div>
        <table>
            <thead><tr><th>Chat ID</th><th>Total Checks</th><th>Distinct Users</th><th>Last Time</th></tr></thead>
            <tbody>{''.join(group_trs) if group_trs else '<tr><td colspan="4">No group summary</td></tr>'}</tbody>
        </table>
    </div>
    """
    return HTMLResponse(simple_page("📊 Wallet Summary", "Thống kê user gửi ví và theo nhóm", body))

# ================= HEALTH =================
@app.get("/healthz")
async def healthz():
    return {
        "ok": True,
        "bot_username": BOT_USERNAME,
        "time": datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S"),
    }

# ================= RUN =================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
