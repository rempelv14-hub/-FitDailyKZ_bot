
import asyncio
import csv
import logging
import os
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    FSInputFile,
)

# ======================================
# OPTIONAL .env LOADER
# ======================================
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ======================================
# CONFIG
# ======================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("Укажите BOT_TOKEN в .env")

# Только один админ
ADMIN_TELEGRAM_ID = int(os.getenv("ADMIN_TELEGRAM_ID", "6954213997"))
DATABASE_PATH = os.getenv("DATABASE_PATH", "fitdaily.db")

BRAND_NAME = "FitDaily"
MANAGER_WHATSAPP = "https://wa.me/77712841932"
MANAGER_CONTACT_TEXT = "Менеджер FitDaily"

DELIVERY_TIME_SLOTS = [
    "06:00–08:00",
    "08:00–10:00",
    "10:00–12:00",
    "12:00–14:00",
    "14:00–16:00",
    "16:00–18:00",
    "18:00–20:00",
]

ABOUT_TEXT = (
    "<b>FitDaily</b> — сервис доставки правильного питания.\n\n"
    "🥗 Программы питания\n"
    "🍽 Актуальное меню\n"
    "⛔ Стоп-лист\n"
    "🎁 Промокоды\n"
    "💳 Выбор способа оплаты\n"
    "🚚 Доставка в выбранный интервал\n"
    "⭐ Отзывы\n"
    "✨ Простое оформление заказа в Telegram"
)

DELIVERY_INFO = (
    "<b>Доставка FitDaily</b>\n\n"
    "Мы доставляем заказы по выбранному адресу.\n"
    "Вы сами выбираете удобную дату и время доставки.\n\n"
    "После оформления менеджер подтверждает детали вручную."
)

PROGRAMS = {
    "slim": {
        "title": "🥬 Slim Start",
        "description": "Лёгкая программа для снижения калорийности.",
        "price_per_day": 6500,
        "calories": "1200–1400 ккал",
        "goal": "Снижение веса и лёгкость каждый день",
        "includes": [
            "5 приёмов пищи",
            "Контроль калорий",
            "Больше овощей и белка",
            "Лёгкое меню без перегруза",
        ],
        "sample_menu": [
            "Овсянка с ягодами",
            "Йогурт и яблоко",
            "Курица, булгур и овощи",
            "Творожный десерт",
            "Рыба и салат",
        ],
    },
    "balance": {
        "title": "⚖️ Balance Daily",
        "description": "Универсальный рацион на каждый день.",
        "price_per_day": 7200,
        "calories": "1500–1800 ккал",
        "goal": "Поддержание формы и энергии",
        "includes": [
            "5 полноценных приёмов пищи",
            "Баланс БЖУ",
            "Разнообразное меню",
            "Подходит для работы и учёбы",
        ],
        "sample_menu": [
            "Сырники с соусом",
            "Фруктовый салат",
            "Говядина с рисом",
            "Ореховый батончик",
            "Паста с курицей",
        ],
    },
    "protein": {
        "title": "💪 Protein Power",
        "description": "Белковая программа для активных людей.",
        "price_per_day": 7900,
        "calories": "1700–2200 ккал",
        "goal": "Спорт, сытость и восстановление",
        "includes": [
            "Повышенный белок",
            "Плотные порции",
            "Сложные углеводы",
            "Подходит для тренировок",
        ],
        "sample_menu": [
            "Омлет с индейкой",
            "Протеиновый десерт",
            "Куриный стейк и киноа",
            "Творог с орехами",
            "Лосось и овощи",
        ],
    },
    "detox": {
        "title": "🍏 Detox Light",
        "description": "Лёгкая программа для разгрузочных дней.",
        "price_per_day": 5900,
        "calories": "1000–1200 ккал",
        "goal": "Лёгкость и аккуратная разгрузка",
        "includes": [
            "4–5 лёгких приёмов пищи",
            "Смузи, супы и боулы",
            "Сниженная калорийность",
            "Больше зелени и овощей",
        ],
        "sample_menu": [
            "Смузи боул",
            "Цитрусовый перекус",
            "Крем-суп и салат",
            "Ягодный смузи",
            "Овощной боул с курицей",
        ],
    },
}

CURRENT_MENU = [
    {"name": "Овсянка с ягодами", "category": "Завтрак", "calories": "280 ккал"},
    {"name": "Сырники с йогуртовым соусом", "category": "Завтрак", "calories": "340 ккал"},
    {"name": "Курица с булгуром и овощами", "category": "Обед", "calories": "460 ккал"},
    {"name": "Говядина с рисом", "category": "Обед", "calories": "510 ккал"},
    {"name": "Творожный десерт", "category": "Перекус", "calories": "190 ккал"},
    {"name": "Лосось с овощами", "category": "Ужин", "calories": "430 ккал"},
]

STOP_LIST = [
    "Протеиновый десерт",
    "Паста с курицей",
]

FAQ = {
    "Как оформить заказ?": "Нажмите «🛒 Оформить заказ», выберите программу, срок, данные доставки и способ оплаты.",
    "Как проходит доставка?": "Вы выбираете адрес и интервал, менеджер подтверждает детали вручную.",
    "Можно ли выбрать время доставки?": "Да, доступные интервалы предлагаются при оформлении.",
    "Как связаться с менеджером?": f"Через WhatsApp: {MANAGER_WHATSAPP}",
    "Можно ли изменить заказ?": "Да, менеджер сможет уточнить детали после оформления.",
}

PAYMENT_METHODS = {
    "kaspi_demo": "🟡 Kaspi (демо)",
    "card_demo": "💳 Карта (демо)",
    "cash": "💵 Наличными курьеру",
}

PROMO_CODES = {
    "FIT10": {"discount_percent": 10, "active": 1, "description": "Скидка 10%"},
    "WELCOME15": {"discount_percent": 15, "active": 1, "description": "Скидка 15% для новых клиентов"},
    "PROTEIN5": {"discount_percent": 5, "active": 1, "description": "Скидка 5%"},
}

DURATIONS = {"1": 1, "5": 5, "7": 7, "14": 14, "30": 30}
STATUSES = ["Новая", "Ожидает оплату", "Подтверждена", "Готовится", "Передана в доставку", "Доставлена", "Отменена"]

# ======================================
# LOGGING
# ======================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ======================================
# FSM STATES
# ======================================
class OrderForm(StatesGroup):
    choosing_program = State()
    choosing_duration = State()
    entering_promo = State()
    entering_name = State()
    entering_phone = State()
    entering_address = State()
    entering_delivery_date = State()
    choosing_delivery_time = State()
    entering_comment = State()
    choosing_payment = State()
    confirming_order = State()

class ReviewForm(StatesGroup):
    entering_review = State()

# ======================================
# DATABASE
# ======================================
def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with closing(get_connection()) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT UNIQUE NOT NULL,
                created_at TEXT NOT NULL,
                telegram_user_id INTEGER NOT NULL,
                telegram_username TEXT,
                full_name TEXT NOT NULL,
                phone TEXT NOT NULL,
                address TEXT NOT NULL,
                delivery_date TEXT NOT NULL,
                delivery_time TEXT,
                program_key TEXT NOT NULL,
                program_title TEXT NOT NULL,
                days INTEGER NOT NULL,
                price_per_day INTEGER NOT NULL,
                total_price INTEGER NOT NULL,
                discount_percent INTEGER NOT NULL DEFAULT 0,
                discount_amount INTEGER NOT NULL DEFAULT 0,
                promo_code TEXT,
                payment_method TEXT,
                payment_status TEXT NOT NULL DEFAULT 'Не требуется',
                comment TEXT,
                status TEXT NOT NULL DEFAULT 'Новая'
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_user_id INTEGER UNIQUE NOT NULL,
                telegram_username TEXT,
                full_name TEXT NOT NULL,
                phone TEXT NOT NULL,
                address TEXT,
                favorite_program TEXT,
                first_order_at TEXT,
                last_order_at TEXT,
                total_orders INTEGER NOT NULL DEFAULT 0,
                total_spent INTEGER NOT NULL DEFAULT 0,
                last_order_id TEXT,
                last_program_title TEXT
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS client_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT NOT NULL,
                note_text TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT NOT NULL,
                telegram_user_id INTEGER NOT NULL,
                full_name TEXT,
                rating INTEGER NOT NULL,
                review_text TEXT,
                created_at TEXT NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS blacklisted_clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT UNIQUE NOT NULL,
                reason TEXT,
                created_at TEXT NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS promo_codes (
                code TEXT PRIMARY KEY,
                discount_percent INTEGER NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                description TEXT
            )
            """
        )

        columns = [row["name"] for row in conn.execute("PRAGMA table_info(orders)").fetchall()]
        add_columns = {
            "delivery_time": "ALTER TABLE orders ADD COLUMN delivery_time TEXT",
            "discount_percent": "ALTER TABLE orders ADD COLUMN discount_percent INTEGER NOT NULL DEFAULT 0",
            "discount_amount": "ALTER TABLE orders ADD COLUMN discount_amount INTEGER NOT NULL DEFAULT 0",
            "promo_code": "ALTER TABLE orders ADD COLUMN promo_code TEXT",
            "payment_method": "ALTER TABLE orders ADD COLUMN payment_method TEXT",
            "payment_status": "ALTER TABLE orders ADD COLUMN payment_status TEXT NOT NULL DEFAULT 'Не требуется'",
        }
        for col, sql in add_columns.items():
            if col not in columns:
                conn.execute(sql)

        for code, data in PROMO_CODES.items():
            conn.execute(
                """
                INSERT OR IGNORE INTO promo_codes (code, discount_percent, active, description)
                VALUES (?, ?, ?, ?)
                """,
                (code, data["discount_percent"], data["active"], data["description"]),
            )

        conn.commit()

def save_order_to_db(order_data: Dict[str, Any], telegram_user) -> None:
    with closing(get_connection()) as conn:
        conn.execute(
            """
            INSERT INTO orders (
                order_id, created_at, telegram_user_id, telegram_username, full_name,
                phone, address, delivery_date, delivery_time, program_key, program_title, days,
                price_per_day, total_price, discount_percent, discount_amount, promo_code,
                payment_method, payment_status, comment, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_data["order_id"],
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                telegram_user.id,
                telegram_user.username or "",
                order_data["name"],
                order_data["phone"],
                order_data["address"],
                order_data["delivery_date"],
                order_data["delivery_time"],
                order_data["program_key"],
                order_data["program_title"],
                order_data["days"],
                order_data["price_per_day"],
                order_data["total_price"],
                order_data.get("discount_percent", 0),
                order_data.get("discount_amount", 0),
                order_data.get("promo_code"),
                order_data.get("payment_method"),
                order_data.get("payment_status", "Не требуется"),
                order_data["comment"],
                order_data.get("status", "Новая"),
            ),
        )
        conn.commit()

def get_favorite_program_for_user(user_id: int, fallback: Optional[str] = None) -> Optional[str]:
    with closing(get_connection()) as conn:
        rows = conn.execute(
            "SELECT program_title, COUNT(*) as cnt FROM orders WHERE telegram_user_id = ? GROUP BY program_title ORDER BY cnt DESC",
            (user_id,),
        ).fetchall()
        if rows:
            return rows[0]["program_title"]
        return fallback

def upsert_client(order_data: Dict[str, Any], telegram_user) -> None:
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with closing(get_connection()) as conn:
        existing = conn.execute(
            "SELECT * FROM clients WHERE telegram_user_id = ?",
            (telegram_user.id,),
        ).fetchone()

        favorite_program = get_favorite_program_for_user(telegram_user.id, fallback=order_data["program_title"])

        if existing:
            conn.execute(
                """
                UPDATE clients
                SET telegram_username = ?,
                    full_name = ?,
                    phone = ?,
                    address = ?,
                    favorite_program = ?,
                    last_order_at = ?,
                    total_orders = total_orders + 1,
                    total_spent = total_spent + ?,
                    last_order_id = ?,
                    last_program_title = ?
                WHERE telegram_user_id = ?
                """,
                (
                    telegram_user.username or "",
                    order_data["name"],
                    order_data["phone"],
                    order_data["address"],
                    favorite_program,
                    now_str,
                    order_data["total_price"],
                    order_data["order_id"],
                    order_data["program_title"],
                    telegram_user.id,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO clients (
                    telegram_user_id, telegram_username, full_name, phone, address, favorite_program,
                    first_order_at, last_order_at, total_orders, total_spent, last_order_id, last_program_title
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    telegram_user.id,
                    telegram_user.username or "",
                    order_data["name"],
                    order_data["phone"],
                    order_data["address"],
                    order_data["program_title"],
                    now_str,
                    now_str,
                    1,
                    order_data["total_price"],
                    order_data["order_id"],
                    order_data["program_title"],
                ),
            )
        conn.commit()

def update_order_status(order_id: str, new_status: str) -> bool:
    with closing(get_connection()) as conn:
        cursor = conn.execute(
            "UPDATE orders SET status = ? WHERE order_id = ?",
            (new_status, order_id),
        )
        conn.commit()
        return cursor.rowcount > 0

def get_order_by_order_id(order_id: str) -> Optional[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone()

def get_orders_between(start_dt: datetime, end_dt: datetime) -> List[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute(
            "SELECT * FROM orders WHERE created_at >= ? AND created_at < ? ORDER BY created_at DESC",
            (start_dt.strftime("%Y-%m-%d %H:%M:%S"), end_dt.strftime("%Y-%m-%d %H:%M:%S")),
        ).fetchall()

def get_recent_orders(limit: int = 10) -> List[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute("SELECT * FROM orders ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()

def get_recent_clients(limit: int = 20) -> List[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute("SELECT * FROM clients ORDER BY last_order_at DESC LIMIT ?", (limit,)).fetchall()

def get_reviews(limit: int = 20) -> List[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute("SELECT * FROM reviews ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()

def get_last_order_by_user(user_id: int) -> Optional[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute(
            "SELECT * FROM orders WHERE telegram_user_id = ? ORDER BY created_at DESC LIMIT 1",
            (user_id,),
        ).fetchone()

def get_orders_by_status(status: str, limit: int = 20) -> List[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute(
            "SELECT * FROM orders WHERE status = ? ORDER BY created_at DESC LIMIT ?",
            (status, limit),
        ).fetchall()

def get_orders_by_delivery_date(date_text: str) -> List[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute(
            "SELECT * FROM orders WHERE delivery_date = ? ORDER BY delivery_time ASC, created_at DESC",
            (date_text,),
        ).fetchall()

def normalize_phone(phone: str) -> str:
    return re.sub(r"\D", "", phone or "")

def get_clients_by_phone(phone: str) -> List[sqlite3.Row]:
    digits = normalize_phone(phone)
    if not digits:
        return []
    with closing(get_connection()) as conn:
        rows = conn.execute("SELECT * FROM clients ORDER BY last_order_at DESC").fetchall()
        return [row for row in rows if normalize_phone(row["phone"]).endswith(digits[-10:])]

def get_client_notes(phone: str) -> List[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute(
            "SELECT * FROM client_notes WHERE phone = ? ORDER BY created_at DESC",
            (phone,),
        ).fetchall()

def add_client_note(phone: str, note_text: str):
    with closing(get_connection()) as conn:
        conn.execute(
            "INSERT INTO client_notes (phone, note_text, created_at) VALUES (?, ?, ?)",
            (phone, note_text, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        conn.commit()

def add_to_blacklist(phone: str, reason: str):
    with closing(get_connection()) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO blacklisted_clients (phone, reason, created_at) VALUES (?, ?, ?)",
            (phone, reason, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        conn.commit()

def is_blacklisted(phone: str) -> Optional[sqlite3.Row]:
    normalized = normalize_phone(phone)
    if not normalized:
        return None
    with closing(get_connection()) as conn:
        rows = conn.execute("SELECT * FROM blacklisted_clients").fetchall()
        for row in rows:
            if normalize_phone(row["phone"]).endswith(normalized[-10:]):
                return row
        return None

def get_promo(code: str) -> Optional[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute(
            "SELECT * FROM promo_codes WHERE code = ? AND active = 1",
            (code.upper(),),
        ).fetchone()

def add_review(order_id: str, telegram_user_id: int, full_name: str, rating: int, review_text: str):
    with closing(get_connection()) as conn:
        conn.execute(
            """
            INSERT INTO reviews (order_id, telegram_user_id, full_name, rating, review_text, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (order_id, telegram_user_id, full_name, rating, review_text, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        conn.commit()

# ======================================
# KEYBOARDS
# ======================================
def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🛒 Оформить заказ"), KeyboardButton(text="🔁 Повторить заказ")],
            [KeyboardButton(text="🥗 Программы питания"), KeyboardButton(text="🍽 Актуальное меню")],
            [KeyboardButton(text="⛔ Стоп-лист"), KeyboardButton(text="🎁 Промокоды")],
            [KeyboardButton(text="💸 Цены"), KeyboardButton(text="🚚 Доставка")],
            [KeyboardButton(text="❓ Вопросы"), KeyboardButton(text="⭐ Оставить отзыв")],
            [KeyboardButton(text="ℹ️ О нас"), KeyboardButton(text="💬 Менеджер")],
            [KeyboardButton(text="👑 Админ панель")],
        ],
        resize_keyboard=True,
    )

def programs_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🥬 Slim", callback_data="program_view:slim"),
                InlineKeyboardButton(text="⚖️ Balance", callback_data="program_view:balance"),
            ],
            [
                InlineKeyboardButton(text="💪 Protein", callback_data="program_view:protein"),
                InlineKeyboardButton(text="🍏 Detox", callback_data="program_view:detox"),
            ],
        ]
    )

def durations_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="1 день", callback_data="duration:1"),
                InlineKeyboardButton(text="5 дней", callback_data="duration:5"),
            ],
            [
                InlineKeyboardButton(text="7 дней", callback_data="duration:7"),
                InlineKeyboardButton(text="14 дней", callback_data="duration:14"),
            ],
            [InlineKeyboardButton(text="30 дней", callback_data="duration:30")],
        ]
    )

def promo_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎁 Ввести промокод", callback_data="promo:enter")],
            [InlineKeyboardButton(text="⏭ Пропустить", callback_data="promo:skip")],
        ]
    )

def delivery_time_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for slot in DELIVERY_TIME_SLOTS:
        rows.append([InlineKeyboardButton(text=slot, callback_data=f"delivery_time:{slot}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def payment_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🟡 Kaspi (демо)", callback_data="payment:kaspi_demo")],
            [InlineKeyboardButton(text="💳 Карта (демо)", callback_data="payment:card_demo")],
            [InlineKeyboardButton(text="💵 Наличными курьеру", callback_data="payment:cash")],
        ]
    )

def confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить заказ", callback_data="confirm:yes")],
            [InlineKeyboardButton(text="✏️ Заполнить заново", callback_data="confirm:restart")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data="confirm:cancel")],
        ]
    )

def program_details_keyboard(program_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🛒 Выбрать программу", callback_data=f"program_select:{program_key}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="programs:back")],
        ]
    )

def admin_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📊 Сегодня", callback_data="admin:today"),
                InlineKeyboardButton(text="📅 7 дней", callback_data="admin:week"),
            ],
            [
                InlineKeyboardButton(text="🧾 Последние заказы", callback_data="admin:recent"),
                InlineKeyboardButton(text="👥 Клиенты", callback_data="admin:clients"),
            ],
            [
                InlineKeyboardButton(text="🚚 Сегодня доставки", callback_data="admin:deliveries_today"),
                InlineKeyboardButton(text="🗓 Завтра доставки", callback_data="admin:deliveries_tomorrow"),
            ],
            [
                InlineKeyboardButton(text="📌 Ожидает оплату", callback_data="admin:status:Ожидает оплату"),
                InlineKeyboardButton(text="🍳 Готовится", callback_data="admin:status:Готовится"),
            ],
            [
                InlineKeyboardButton(text="🚚 В доставке", callback_data="admin:status:Передана в доставку"),
                InlineKeyboardButton(text="⭐ Отзывы", callback_data="admin:reviews"),
            ],
            [
                InlineKeyboardButton(text="📈 Сегменты", callback_data="admin:segments"),
            ],
        ]
    )

def admin_status_keyboard(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="💰 Ожидает оплату", callback_data=f"status:{order_id}:Ожидает оплату"),
                InlineKeyboardButton(text="✅ Подтверждена", callback_data=f"status:{order_id}:Подтверждена"),
            ],
            [
                InlineKeyboardButton(text="👨‍🍳 Готовится", callback_data=f"status:{order_id}:Готовится"),
                InlineKeyboardButton(text="🚚 В доставке", callback_data=f"status:{order_id}:Передана в доставку"),
            ],
            [
                InlineKeyboardButton(text="🎉 Доставлена", callback_data=f"status:{order_id}:Доставлена"),
                InlineKeyboardButton(text="❌ Отменена", callback_data=f"status:{order_id}:Отменена"),
            ],
        ]
    )

def review_rating_keyboard(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="1", callback_data=f"review:{order_id}:1"),
            InlineKeyboardButton(text="2", callback_data=f"review:{order_id}:2"),
            InlineKeyboardButton(text="3", callback_data=f"review:{order_id}:3"),
            InlineKeyboardButton(text="4", callback_data=f"review:{order_id}:4"),
            InlineKeyboardButton(text="5", callback_data=f"review:{order_id}:5"),
        ]]
    )

# ======================================
# HELPERS
# ======================================
def format_currency(value: int) -> str:
    return f"{value:,} ₸".replace(",", " ")

def generate_order_id(user_id: int) -> str:
    return f"FD-{user_id}-{datetime.now().strftime('%d%m%H%M%S')}"

def validate_phone(phone: str) -> bool:
    digits = normalize_phone(phone)
    return 10 <= len(digits) <= 15

def validate_delivery_date(date_text: str) -> bool:
    try:
        entered = datetime.strptime(date_text, "%d.%m.%Y")
        return entered.date() >= datetime.now().date()
    except ValueError:
        return False

def calculate_discount(total_price: int, promo_code: Optional[str]) -> Tuple[int, int]:
    if not promo_code:
        return 0, 0
    promo = get_promo(promo_code)
    if not promo:
        return 0, 0
    discount_percent = int(promo["discount_percent"])
    discount_amount = total_price * discount_percent // 100
    return discount_percent, discount_amount

def build_prices_text() -> str:
    lines = ["<b>💸 Цены на программы</b>\n"]
    for item in PROGRAMS.values():
        lines.append(f"• {item['title']} — <b>{format_currency(item['price_per_day'])}</b> / день")
    lines.append("\nИтоговая стоимость рассчитывается автоматически.")
    return "\n".join(lines)

def build_promos_text() -> str:
    lines = ["<b>🎁 Доступные промокоды</b>\n"]
    with closing(get_connection()) as conn:
        promos = conn.execute("SELECT * FROM promo_codes WHERE active = 1 ORDER BY discount_percent DESC").fetchall()
    for promo in promos:
        lines.append(f"• <b>{promo['code']}</b> — {promo['discount_percent']}% ({promo['description'] or 'без описания'})")
    return "\n".join(lines)

def build_current_menu_text() -> str:
    lines = ["<b>🍽 Актуальное меню FitDaily</b>\n"]
    for item in CURRENT_MENU:
        lines.append(
            f"• <b>{item['name']}</b>\n"
            f"  Раздел: {item['category']}\n"
            f"  Калорийность: {item['calories']}\n"
        )
    lines.append("Меню может обновляться ежедневно.")
    return "\n".join(lines)

def build_stop_list_text() -> str:
    lines = ["<b>⛔ Стоп-лист</b>\n"]
    if not STOP_LIST:
        lines.append("Сегодня стоп-листа нет ✅")
    else:
        for item in STOP_LIST:
            lines.append(f"• {item}")
    return "\n".join(lines)

def build_faq_text() -> str:
    lines = ["<b>❓ Частые вопросы</b>\n"]
    for question, answer in FAQ.items():
        lines.append(f"<b>{question}</b>\n{answer}\n")
    return "\n".join(lines)

def build_kaspi_demo_text(order_data: Dict[str, Any]) -> str:
    return (
        "<b>🟡 Оплата Kaspi (демо)</b>\n\n"
        "Это выдуманный демонстрационный способ оплаты для примера интерфейса.\n"
        "Настоящая интеграция с Kaspi здесь не подключена.\n\n"
        f"Сумма к оплате: <b>{format_currency(order_data['total_price'])}</b>\n"
        "Статус оплаты будет сохранён как <b>Ожидает оплаты</b>."
    )

def build_order_summary(data: Dict[str, Any]) -> str:
    promo_line = ""
    if data.get("promo_code"):
        promo_line = (
            f"🎁 Промокод: <b>{data['promo_code']}</b>\n"
            f"📉 Скидка: <b>{data.get('discount_percent', 0)}%</b> ({format_currency(data.get('discount_amount', 0))})\n"
        )

    return (
        "<b>✨ Проверьте ваш заказ</b>\n\n"
        f"🆔 Номер: <b>{data['order_id']}</b>\n"
        f"🥗 Программа: <b>{data['program_title']}</b>\n"
        f"📆 Срок: <b>{data['days']} дн.</b>\n"
        f"💰 Цена в день: <b>{format_currency(data['price_per_day'])}</b>\n"
        f"{promo_line}"
        f"💳 Итого: <b>{format_currency(data['total_price'])}</b>\n"
        f"💸 Оплата: <b>{PAYMENT_METHODS.get(data['payment_method'], data['payment_method'])}</b>\n"
        f"📌 Статус оплаты: <b>{data.get('payment_status', '—')}</b>\n\n"
        f"👤 Имя: {data['name']}\n"
        f"📞 Телефон: {data['phone']}\n"
        f"📍 Адрес: {data['address']}\n"
        f"🗓 Дата доставки: {data['delivery_date']}\n"
        f"⏰ Время доставки: {data['delivery_time']}\n"
        f"📝 Комментарий: {data['comment']}"
    )

def build_admin_order_text(data: Dict[str, Any], user) -> str:
    username = f"@{user.username}" if user.username else "не указан"
    return (
        "<b>🆕 Новая заявка FitDaily</b>\n\n"
        f"🆔 Заказ: <b>{data['order_id']}</b>\n"
        f"📌 Статус: <b>{data.get('status', 'Новая')}</b>\n"
        f"📌 Статус оплаты: <b>{data.get('payment_status', '—')}</b>\n"
        f"💸 Способ оплаты: <b>{PAYMENT_METHODS.get(data.get('payment_method'), data.get('payment_method'))}</b>\n"
        f"👤 Клиент: {data['name']}\n"
        f"📞 Телефон: {data['phone']}\n"
        f"📍 Адрес: {data['address']}\n"
        f"🗓 Дата: {data['delivery_date']}\n"
        f"⏰ Время: {data['delivery_time']}\n"
        f"🥗 Программа: {data['program_title']}\n"
        f"📆 Срок: {data['days']} дн.\n"
        f"🎁 Промокод: {data.get('promo_code') or 'нет'}\n"
        f"💰 Цена до скидки: {format_currency(data['price_per_day'] * data['days'])}\n"
        f"📉 Скидка: {format_currency(data.get('discount_amount', 0))}\n"
        f"💳 Итого: {format_currency(data['total_price'])}\n"
        f"📝 Комментарий: {data['comment']}\n\n"
        f"Telegram ID: <code>{user.id}</code>\n"
        f"Username: {username}\n\n"
        "Выберите статус ниже:"
    )

def build_stats_text(title: str, orders: List[sqlite3.Row]) -> str:
    total_orders = len(orders)
    total_revenue = sum(order["total_price"] for order in orders)

    by_status: Dict[str, int] = {}
    by_program: Dict[str, int] = {}

    for order in orders:
        by_status[order["status"]] = by_status.get(order["status"], 0) + 1
        by_program[order["program_title"]] = by_program.get(order["program_title"], 0) + 1

    lines = [
        f"<b>{title}</b>",
        "",
        f"🧾 Заказов: <b>{total_orders}</b>",
        f"💰 Выручка: <b>{format_currency(total_revenue)}</b>",
        "",
        "<b>По статусам:</b>",
    ]
    if by_status:
        for status, count in by_status.items():
            lines.append(f"• {status}: {count}")
    else:
        lines.append("• Нет данных")

    lines.append("")
    lines.append("<b>По программам:</b>")
    if by_program:
        for program, count in by_program.items():
            lines.append(f"• {program}: {count}")
    else:
        lines.append("• Нет данных")

    return "\n".join(lines)

def build_clients_text(clients: List[sqlite3.Row]) -> str:
    lines = ["<b>👥 Последние клиенты</b>\n"]
    for client in clients:
        lines.append(
            f"<b>{client['full_name']}</b>\n"
            f"📞 {client['phone']}\n"
            f"📦 Заказов: {client['total_orders']}\n"
            f"💰 Потрачено: {format_currency(client['total_spent'])}\n"
            f"🥗 Любимая программа: {client['favorite_program'] or '—'}\n"
            f"🕒 Последний заказ: {client['last_order_at']}\n"
        )
    return "\n".join(lines)

def build_orders_text(title: str, orders: List[sqlite3.Row]) -> str:
    if not orders:
        return f"<b>{title}</b>\n\nНет данных."
    lines = [f"<b>{title}</b>\n"]
    for order in orders:
        lines.append(
            f"<b>{order['order_id']}</b>\n"
            f"👤 {order['full_name']}\n"
            f"📞 {order['phone']}\n"
            f"🥗 {order['program_title']}\n"
            f"🗓 {order['delivery_date']} {order['delivery_time'] or ''}\n"
            f"📌 {order['status']}\n"
            f"💳 {format_currency(order['total_price'])}\n"
        )
    text = "\n".join(lines)
    return text[:3900] + "\n\n..." if len(text) > 3900 else text

def build_reviews_text(reviews: List[sqlite3.Row]) -> str:
    if not reviews:
        return "<b>⭐ Отзывы</b>\n\nПока отзывов нет."
    lines = ["<b>⭐ Последние отзывы</b>\n"]
    for review in reviews:
        lines.append(
            f"<b>{review['full_name'] or 'Клиент'}</b> — {review['rating']}/5\n"
            f"🆔 {review['order_id']}\n"
            f"💬 {review['review_text'] or 'Без текста'}\n"
            f"🕒 {review['created_at']}\n"
        )
    return "\n".join(lines)

def client_segment(client: sqlite3.Row) -> str:
    total_orders = client["total_orders"]
    last_order_at = client["last_order_at"]
    if total_orders == 1:
        return "Новый"
    if total_orders >= 5:
        try:
            dt = datetime.strptime(last_order_at, "%Y-%m-%d %H:%M:%S")
            if dt >= datetime.now() - timedelta(days=30):
                return "Постоянный"
        except Exception:
            pass
    try:
        dt = datetime.strptime(last_order_at, "%Y-%m-%d %H:%M:%S")
        if dt < datetime.now() - timedelta(days=30):
            return "Спящий"
    except Exception:
        pass
    return "Активный"

def build_segments_text() -> str:
    clients = get_recent_clients(1000)
    stats = {"Новый": 0, "Постоянный": 0, "Активный": 0, "Спящий": 0}
    for client in clients:
        stats[client_segment(client)] += 1
    return (
        "<b>📊 Сегменты клиентов</b>\n\n"
        f"🆕 Новые: <b>{stats['Новый']}</b>\n"
        f"🔥 Активные: <b>{stats['Активный']}</b>\n"
        f"💎 Постоянные: <b>{stats['Постоянный']}</b>\n"
        f"😴 Спящие: <b>{stats['Спящий']}</b>"
    )

def admin_only(user_id: int) -> bool:
    return user_id == ADMIN_TELEGRAM_ID

async def notify_user_about_status(bot: Bot, order: sqlite3.Row, new_status: str):
    try:
        text = (
            f"<b>Обновление по заказу {order['order_id']}</b>\n\n"
            f"📌 Новый статус: <b>{new_status}</b>\n"
            f"🥗 Программа: {order['program_title']}\n"
            f"💳 Сумма: {format_currency(order['total_price'])}"
        )
        await bot.send_message(order["telegram_user_id"], text)
        if new_status == "Передана в доставку":
            await bot.send_message(order["telegram_user_id"], "🚚 Курьер выехал. Ожидайте доставку.")
        if new_status == "Доставлена":
            await bot.send_message(
                order["telegram_user_id"],
                "⭐ Спасибо за заказ! Оцените, пожалуйста, ваш опыт:",
                reply_markup=review_rating_keyboard(order["order_id"]),
            )
    except Exception as e:
        logger.warning("Не удалось уведомить клиента: %s", e)

def export_clients_to_csv(path: str):
    clients = get_recent_clients(10000)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow([
            "full_name", "phone", "telegram_username", "address", "favorite_program",
            "first_order_at", "last_order_at", "total_orders", "total_spent", "segment"
        ])
        for client in clients:
            writer.writerow([
                client["full_name"],
                client["phone"],
                client["telegram_username"],
                client["address"],
                client["favorite_program"],
                client["first_order_at"],
                client["last_order_at"],
                client["total_orders"],
                client["total_spent"],
                client_segment(client),
            ])

# ======================================
# USER HANDLERS
# ======================================
async def start_handler(message: Message, state: FSMContext):
    await state.clear()
    text = (
        f"Здравствуйте! Добро пожаловать в <b>{BRAND_NAME}</b> 🥗\n\n"
        "Здесь можно оформить заказ, посмотреть меню, стоп-лист, промокоды и выбрать способ оплаты."
    )
    await message.answer(text, reply_markup=main_menu_keyboard())

async def programs_handler(message: Message):
    await message.answer(
        "<b>🥗 Программы питания FitDaily</b>\n\nНажмите на программу, чтобы посмотреть подробности.",
        reply_markup=programs_keyboard(),
    )

async def current_menu_handler(message: Message):
    await message.answer(build_current_menu_text())

async def stop_list_handler(message: Message):
    await message.answer(build_stop_list_text())

async def promo_list_handler(message: Message):
    await message.answer(build_promos_text())

async def faq_handler(message: Message):
    await message.answer(build_faq_text())

async def prices_handler(message: Message):
    await message.answer(build_prices_text())

async def delivery_handler(message: Message):
    await message.answer(DELIVERY_INFO)

async def about_handler(message: Message):
    await message.answer(ABOUT_TEXT)

async def manager_handler(message: Message):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💬 Написать в WhatsApp", url=MANAGER_WHATSAPP)]
        ]
    )
    await message.answer(
        "<b>💬 Связь с менеджером</b>\n\n"
        f"{MANAGER_CONTACT_TEXT}\n\n"
        "Нажмите кнопку ниже, чтобы открыть WhatsApp.",
        reply_markup=kb,
    )

async def repeat_order_handler(message: Message, state: FSMContext):
    last_order = get_last_order_by_user(message.from_user.id)
    if not last_order:
        await message.answer("У вас пока нет прошлых заказов.")
        return

    await state.clear()
    await state.update_data(
        program_key=last_order["program_key"],
        program_title=last_order["program_title"],
        price_per_day=last_order["price_per_day"],
        days=last_order["days"],
        promo_code=last_order["promo_code"],
        discount_percent=last_order["discount_percent"],
        discount_amount=last_order["discount_amount"],
        total_price=last_order["total_price"],
        name=last_order["full_name"],
        phone=last_order["phone"],
        address=last_order["address"],
        comment=last_order["comment"] or "Нет",
    )
    await state.set_state(OrderForm.entering_delivery_date)
    await message.answer(
        "<b>🔁 Повтор заказа</b>\n\n"
        f"Мы подставили ваш прошлый заказ: {last_order['program_title']} на {last_order['days']} дн.\n"
        "Введите новую дату первой доставки в формате ДД.ММ.ГГГГ:"
    )

async def review_start_handler(message: Message):
    last_order = get_last_order_by_user(message.from_user.id)
    if not last_order:
        await message.answer("Сначала оформите хотя бы один заказ.")
        return
    await message.answer("Оцените последний заказ:", reply_markup=review_rating_keyboard(last_order["order_id"]))

async def order_start_handler(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(OrderForm.choosing_program)
    await message.answer(
        "<b>🛒 Оформление заказа</b>\n\nВыберите программу питания:",
        reply_markup=programs_keyboard(),
    )

async def program_view_handler(callback: CallbackQuery):
    program_key = callback.data.split(":", 1)[1]
    program = PROGRAMS.get(program_key)
    if not program:
        await callback.answer("Программа не найдена", show_alert=True)
        return

    includes_text = "\n".join([f"• {item}" for item in program["includes"]])
    menu_text = "\n".join([f"• {item}" for item in program["sample_menu"]])

    await callback.message.edit_text(
        f"<b>{program['title']}</b>\n\n"
        f"{program['description']}\n\n"
        f"<b>Калорийность:</b> {program['calories']}\n"
        f"<b>Подходит для:</b> {program['goal']}\n"
        f"<b>Цена:</b> {format_currency(program['price_per_day'])} / день\n\n"
        f"<b>Что входит:</b>\n{includes_text}\n\n"
        f"<b>Пример меню:</b>\n{menu_text}\n\n"
        "Нажмите кнопку ниже, чтобы выбрать программу.",
        reply_markup=program_details_keyboard(program_key),
    )
    await callback.answer()

async def programs_back_handler(callback: CallbackQuery):
    await callback.message.edit_text(
        "<b>🥗 Программы питания FitDaily</b>\n\nНажмите на программу, чтобы посмотреть подробности.",
        reply_markup=programs_keyboard(),
    )
    await callback.answer()

async def choose_program_handler(callback: CallbackQuery, state: FSMContext):
    program_key = callback.data.split(":", 1)[1]
    program = PROGRAMS.get(program_key)
    if not program:
        await callback.answer("Программа не найдена", show_alert=True)
        return

    await state.update_data(
        program_key=program_key,
        program_title=program["title"],
        price_per_day=program["price_per_day"],
    )
    await state.set_state(OrderForm.choosing_duration)

    await callback.message.edit_text(
        f"<b>{program['title']}</b>\n"
        f"{program['description']}\n"
        f"Калорийность: {program['calories']}\n\n"
        "Теперь выберите продолжительность программы:",
        reply_markup=durations_keyboard(),
    )
    await callback.answer()

async def choose_duration_handler(callback: CallbackQuery, state: FSMContext):
    duration_key = callback.data.split(":", 1)[1]
    days = DURATIONS[duration_key]
    data = await state.get_data()
    total_price = data["price_per_day"] * days

    await state.update_data(days=days, total_price=total_price, discount_percent=0, discount_amount=0, promo_code=None)
    await state.set_state(OrderForm.entering_promo)

    await callback.message.edit_text(
        f"Вы выбрали программу на <b>{days} дн.</b>\n"
        f"Стоимость без скидки: <b>{format_currency(total_price)}</b>\n\n"
        "Хотите применить промокод?",
        reply_markup=promo_keyboard(),
    )
    await callback.answer()

async def promo_callback_handler(callback: CallbackQuery, state: FSMContext):
    action = callback.data.split(":", 1)[1]
    if action == "skip":
        await state.set_state(OrderForm.entering_name)
        await callback.message.edit_text("Промокод пропущен.\n\nВведите ваше имя:")
    else:
        await state.set_state(OrderForm.entering_promo)
        await callback.message.edit_text("Введите промокод текстом:")
    await callback.answer()

async def promo_text_handler(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    data = await state.get_data()
    promo = get_promo(code)
    if not promo:
        await message.answer("Промокод не найден или неактивен. Введите другой или напишите Пропустить.")
        return

    base_total = data["price_per_day"] * data["days"]
    discount_percent, discount_amount = calculate_discount(base_total, code)
    total_price = max(base_total - discount_amount, 0)

    await state.update_data(
        promo_code=code,
        discount_percent=discount_percent,
        discount_amount=discount_amount,
        total_price=total_price,
    )
    await state.set_state(OrderForm.entering_name)
    await message.answer(
        f"Промокод <b>{code}</b> применён.\n"
        f"Скидка: <b>{discount_percent}%</b>\n"
        f"Итого: <b>{format_currency(total_price)}</b>\n\n"
        "Введите ваше имя:"
    )

async def name_handler(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) < 2:
        await message.answer("Введите корректное имя.")
        return
    await state.update_data(name=name)
    await state.set_state(OrderForm.entering_phone)
    await message.answer("Введите номер телефона:")

async def phone_handler(message: Message, state: FSMContext):
    phone = message.text.strip()
    if not validate_phone(phone):
        await message.answer("Введите корректный номер телефона. Пример: +77011234567")
        return

    blacklist = is_blacklisted(phone)
    if blacklist:
        await message.answer("К сожалению, оформление недоступно. Свяжитесь с менеджером.")
        return

    await state.update_data(phone=phone)
    await state.set_state(OrderForm.entering_address)
    await message.answer("Введите адрес доставки:")

async def address_handler(message: Message, state: FSMContext):
    address = message.text.strip()
    if len(address) < 5:
        await message.answer("Введите более подробный адрес доставки.")
        return
    await state.update_data(address=address)
    await state.set_state(OrderForm.entering_delivery_date)
    await message.answer("Введите дату первой доставки в формате ДД.ММ.ГГГГ, например: 22.04.2026")

async def delivery_date_handler(message: Message, state: FSMContext):
    date_text = message.text.strip()
    if not validate_delivery_date(date_text):
        await message.answer("Введите корректную дату не раньше сегодняшней. Пример: 22.04.2026")
        return
    await state.update_data(delivery_date=date_text)
    await state.set_state(OrderForm.choosing_delivery_time)
    await message.answer("Выберите удобное время доставки:", reply_markup=delivery_time_keyboard())

async def delivery_time_handler(callback: CallbackQuery, state: FSMContext):
    slot = callback.data.split(":", 1)[1]
    await state.update_data(delivery_time=slot)
    await state.set_state(OrderForm.entering_comment)
    await callback.message.edit_text(
        f"Время доставки выбрано: <b>{slot}</b>\n\n"
        "Введите комментарий к заказу. Если комментария нет, напишите: Нет"
    )
    await callback.answer()

async def comment_handler(message: Message, state: FSMContext):
    comment = message.text.strip()
    await state.update_data(comment=comment)
    await state.set_state(OrderForm.choosing_payment)
    await message.answer("Выберите способ оплаты:", reply_markup=payment_keyboard())

async def payment_handler(callback: CallbackQuery, state: FSMContext):
    payment_method = callback.data.split(":", 1)[1]
    order_id = generate_order_id(callback.from_user.id)

    payment_status = "Не требуется"
    status = "Новая"
    if payment_method in ("kaspi_demo", "card_demo"):
        payment_status = "Ожидает оплаты"
        status = "Ожидает оплату"

    await state.update_data(
        payment_method=payment_method,
        payment_status=payment_status,
        status=status,
        order_id=order_id,
    )
    data = await state.get_data()

    text = build_order_summary(data)
    if payment_method == "kaspi_demo":
        text += "\n\n" + build_kaspi_demo_text(data)
    elif payment_method == "card_demo":
        text += (
            "\n\n<b>💳 Оплата картой (демо)</b>\n\n"
            "Это демонстрационный сценарий без реального процессинга.\n"
            "Статус оплаты будет сохранён как <b>Ожидает оплаты</b>."
        )

    await state.set_state(OrderForm.confirming_order)
    await callback.message.edit_text(text, reply_markup=confirm_keyboard())
    await callback.answer()

async def confirm_handler(callback: CallbackQuery, state: FSMContext, bot: Bot):
    action = callback.data.split(":", 1)[1]

    if action == "cancel":
        await state.clear()
        await callback.message.edit_text("Заказ отменён.")
        await bot.send_message(callback.from_user.id, "Вы вернулись в главное меню.", reply_markup=main_menu_keyboard())
        await callback.answer()
        return

    if action == "restart":
        await state.clear()
        await state.set_state(OrderForm.choosing_program)
        await callback.message.edit_text("Заполним заявку заново. Выберите программу питания:", reply_markup=programs_keyboard())
        await callback.answer()
        return

    data = await state.get_data()

    db_status = "✅ Заявка и клиент сохранены в базе данных."
    try:
        save_order_to_db(data, callback.from_user)
        upsert_client(data, callback.from_user)
    except Exception as error:
        logger.exception("DB save error: %s", error)
        db_status = "⚠️ Не удалось сохранить заявку в базе данных."

    admin_status = "⚠️ Не удалось отправить заявку администратору."
    try:
        await bot.send_message(
            ADMIN_TELEGRAM_ID,
            build_admin_order_text(data, callback.from_user),
            reply_markup=admin_status_keyboard(data["order_id"]),
        )
        admin_status = "✅ Заявка отправлена администратору."
    except Exception as error:
        logger.exception("Admin notify error: %s", error)

    manager_kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="💬 Написать менеджеру в WhatsApp", url=MANAGER_WHATSAPP)]]
    )

    await callback.message.edit_text(
        "<b>Спасибо! Ваш заказ принят.</b>\n\n"
        f"Номер заказа: <b>{data['order_id']}</b>\n"
        f"Статус заказа: <b>{data.get('status', 'Новая')}</b>\n"
        f"Статус оплаты: <b>{data.get('payment_status', '—')}</b>\n\n"
        f"{admin_status}\n{db_status}"
    )
    await bot.send_message(callback.from_user.id, "Для быстрой связи можете сразу написать менеджеру:", reply_markup=manager_kb)
    await bot.send_message(callback.from_user.id, "Главное меню открыто снова.", reply_markup=main_menu_keyboard())
    await state.clear()
    await callback.answer()

async def review_callback_handler(callback: CallbackQuery, state: FSMContext):
    _, order_id, rating = callback.data.split(":")
    await state.set_state(ReviewForm.entering_review)
    await state.update_data(review_order_id=order_id, review_rating=int(rating))
    await callback.message.edit_text(
        f"Спасибо! Вы поставили оценку <b>{rating}/5</b>.\n\nТеперь напишите короткий отзыв. Если не хотите, напишите: Нет"
    )
    await callback.answer()

async def review_text_handler(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    order_id = data.get("review_order_id")
    rating = data.get("review_rating")
    review_text = message.text.strip()
    order = get_order_by_order_id(order_id) if order_id else None
    full_name = order["full_name"] if order else (message.from_user.full_name or "Клиент")
    add_review(order_id, message.from_user.id, full_name, rating, review_text if review_text.lower() != "нет" else "")

    await message.answer("Спасибо за отзыв! 💚", reply_markup=main_menu_keyboard())
    try:
        await bot.send_message(
            ADMIN_TELEGRAM_ID,
            f"<b>⭐ Новый отзыв</b>\n\nЗаказ: <b>{order_id}</b>\nКлиент: {full_name}\nОценка: <b>{rating}/5</b>\nОтзыв: {review_text}"
        )
    except Exception:
        pass
    await state.clear()

# ======================================
# ADMIN HANDLERS
# ======================================
async def admin_panel_handler(message: Message):
    if not admin_only(message.from_user.id):
        await message.answer("У вас нет доступа к админ панели.")
        return
    await message.answer(
        "<b>👑 Админ панель FitDaily</b>\n\nВыберите нужное действие ниже:",
        reply_markup=admin_main_keyboard(),
    )

async def admin_help_handler(message: Message):
    if not admin_only(message.from_user.id):
        return
    text = (
        "<b>Команды администратора</b>\n\n"
        "/orders_today — статистика за сегодня\n"
        "/orders_week — статистика за 7 дней\n"
        "/recent_orders — последние 10 заказов\n"
        "/clients — последние клиенты\n"
        "/segments — сегменты клиентов\n"
        "/reviews — последние отзывы\n"
        "/deliveries_today — доставки на сегодня\n"
        "/deliveries_tomorrow — доставки на завтра\n"
        "/find_client +7701... — поиск клиента по номеру\n"
        "/export_clients — выгрузка клиентов в CSV\n"
        "/note_phone +7701... текст — заметка по клиенту\n"
        "/blacklist_phone +7701... причина — чёрный список\n"
        "/order FD-... — подробности заказа\n"
        "/set_status FD-... Доставлена — сменить статус вручную"
    )
    await message.answer(text)

async def orders_today_handler(message: Message):
    if not admin_only(message.from_user.id):
        return
    start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    orders = get_orders_between(start, end)
    await message.answer(build_stats_text("📊 Статистика за сегодня", orders))

async def orders_week_handler(message: Message):
    if not admin_only(message.from_user.id):
        return
    end = datetime.now() + timedelta(seconds=1)
    start = datetime.now() - timedelta(days=7)
    orders = get_orders_between(start, end)
    await message.answer(build_stats_text("📅 Статистика за 7 дней", orders))

async def recent_orders_handler(message: Message):
    if not admin_only(message.from_user.id):
        return
    await message.answer(build_orders_text("🧾 Последние 10 заказов", get_recent_orders(10)))

async def clients_handler(message: Message):
    if not admin_only(message.from_user.id):
        return
    clients = get_recent_clients(20)
    if not clients:
        await message.answer("Клиентов пока нет.")
        return
    await message.answer(build_clients_text(clients))

async def segments_handler(message: Message):
    if not admin_only(message.from_user.id):
        return
    await message.answer(build_segments_text())

async def reviews_handler(message: Message):
    if not admin_only(message.from_user.id):
        return
    await message.answer(build_reviews_text(get_reviews(20)))

async def deliveries_today_handler(message: Message):
    if not admin_only(message.from_user.id):
        return
    date_text = datetime.now().strftime("%d.%m.%Y")
    await message.answer(build_orders_text(f"🚚 Доставки на {date_text}", get_orders_by_delivery_date(date_text)))

async def deliveries_tomorrow_handler(message: Message):
    if not admin_only(message.from_user.id):
        return
    date_text = (datetime.now() + timedelta(days=1)).strftime("%d.%m.%Y")
    await message.answer(build_orders_text(f"🗓 Доставки на {date_text}", get_orders_by_delivery_date(date_text)))

async def export_clients_handler(message: Message):
    if not admin_only(message.from_user.id):
        return
    path = "clients_export.csv"
    export_clients_to_csv(path)
    await message.answer_document(FSInputFile(path), caption="Экспорт клиентской базы")

async def find_client_handler(message: Message):
    if not admin_only(message.from_user.id):
        return
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /find_client +77011234567")
        return
    phone = parts[1].strip()
    clients = get_clients_by_phone(phone)
    if not clients:
        await message.answer("Клиент не найден.")
        return

    lines = ["<b>🔎 Результаты поиска</b>\n"]
    for client in clients:
        lines.append(
            f"<b>{client['full_name']}</b>\n"
            f"📞 {client['phone']}\n"
            f"📦 Заказов: {client['total_orders']}\n"
            f"💰 Потрачено: {format_currency(client['total_spent'])}\n"
            f"🥗 Любимая программа: {client['favorite_program'] or '—'}\n"
        )
        notes = get_client_notes(client["phone"])[:3]
        if notes:
            lines.append("📝 Заметки:")
            for note in notes:
                lines.append(f"• {note['note_text']} ({note['created_at']})")
        lines.append("")
    await message.answer("\n".join(lines))

async def note_phone_handler(message: Message):
    if not admin_only(message.from_user.id):
        return
    parts = message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Использование: /note_phone +77011234567 Текст заметки")
        return
    phone, note_text = parts[1], parts[2]
    add_client_note(phone, note_text)
    await message.answer("Заметка сохранена.")

async def blacklist_phone_handler(message: Message):
    if not admin_only(message.from_user.id):
        return
    parts = message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Использование: /blacklist_phone +77011234567 Причина")
        return
    phone, reason = parts[1], parts[2]
    add_to_blacklist(phone, reason)
    await message.answer("Клиент добавлен в чёрный список.")

async def order_detail_handler(message: Message):
    if not admin_only(message.from_user.id):
        return
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /order FD-123")
        return

    order_id = parts[1].strip()
    order = get_order_by_order_id(order_id)
    if not order:
        await message.answer("Заказ не найден.")
        return

    text = (
        f"<b>Заказ {order['order_id']}</b>\n\n"
        f"📌 Статус: <b>{order['status']}</b>\n"
        f"📌 Оплата: <b>{order['payment_status']}</b>\n"
        f"💳 Способ оплаты: <b>{PAYMENT_METHODS.get(order['payment_method'], order['payment_method'])}</b>\n"
        f"👤 Клиент: {order['full_name']}\n"
        f"📞 Телефон: {order['phone']}\n"
        f"📍 Адрес: {order['address']}\n"
        f"🗓 Дата доставки: {order['delivery_date']}\n"
        f"⏰ Время доставки: {order['delivery_time'] or 'не указано'}\n"
        f"🥗 Программа: {order['program_title']}\n"
        f"📆 Срок: {order['days']} дн.\n"
        f"🎁 Промокод: {order['promo_code'] or 'нет'}\n"
        f"📉 Скидка: {format_currency(order['discount_amount'] or 0)}\n"
        f"💳 Итого: {format_currency(order['total_price'])}\n"
        f"📝 Комментарий: {order['comment']}\n"
        f"🕒 Создан: {order['created_at']}"
    )
    await message.answer(text, reply_markup=admin_status_keyboard(order['order_id']))

async def set_status_handler(message: Message, bot: Bot):
    if not admin_only(message.from_user.id):
        return

    parts = message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Использование: /set_status FD-123 Доставлена")
        return

    order_id = parts[1].strip()
    new_status = parts[2].strip()

    if new_status not in STATUSES:
        await message.answer("Недопустимый статус.")
        return

    updated = update_order_status(order_id, new_status)
    if not updated:
        await message.answer("Заказ не найден.")
        return

    order = get_order_by_order_id(order_id)
    if order:
        await notify_user_about_status(bot, order, new_status)

    await message.answer(f"Статус заказа <b>{order_id}</b> обновлён: <b>{new_status}</b>")

async def admin_callback_handler(callback: CallbackQuery):
    if not admin_only(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    parts = callback.data.split(":")
    if len(parts) >= 3 and parts[1] == "status":
        status = ":".join(parts[2:])
        await callback.message.edit_text(
            build_orders_text(f"📌 Заказы со статусом: {status}", get_orders_by_status(status, 20)),
            reply_markup=admin_main_keyboard(),
        )
        await callback.answer()
        return

    action = parts[1]

    if action == "today":
        start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        orders = get_orders_between(start, end)
        await callback.message.edit_text(build_stats_text("📊 Статистика за сегодня", orders), reply_markup=admin_main_keyboard())

    elif action == "week":
        end = datetime.now() + timedelta(seconds=1)
        start = datetime.now() - timedelta(days=7)
        orders = get_orders_between(start, end)
        await callback.message.edit_text(build_stats_text("📅 Статистика за 7 дней", orders), reply_markup=admin_main_keyboard())

    elif action == "recent":
        await callback.message.edit_text(build_orders_text("🧾 Последние заказы", get_recent_orders(10)), reply_markup=admin_main_keyboard())

    elif action == "clients":
        clients = get_recent_clients(20)
        text = "Клиентов пока нет." if not clients else build_clients_text(clients)
        if len(text) > 3900:
            text = text[:3900] + "\n\n..."
        await callback.message.edit_text(text, reply_markup=admin_main_keyboard())

    elif action == "deliveries_today":
        date_text = datetime.now().strftime("%d.%m.%Y")
        await callback.message.edit_text(build_orders_text(f"🚚 Доставки на {date_text}", get_orders_by_delivery_date(date_text)), reply_markup=admin_main_keyboard())

    elif action == "deliveries_tomorrow":
        date_text = (datetime.now() + timedelta(days=1)).strftime("%d.%m.%Y")
        await callback.message.edit_text(build_orders_text(f"🗓 Доставки на {date_text}", get_orders_by_delivery_date(date_text)), reply_markup=admin_main_keyboard())

    elif action == "reviews":
        await callback.message.edit_text(build_reviews_text(get_reviews(20)), reply_markup=admin_main_keyboard())

    elif action == "segments":
        await callback.message.edit_text(build_segments_text(), reply_markup=admin_main_keyboard())

    await callback.answer()

async def status_callback_handler(callback: CallbackQuery, bot: Bot):
    if not admin_only(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    _, order_id, new_status = callback.data.split(":", 2)
    if new_status not in STATUSES:
        await callback.answer("Некорректный статус", show_alert=True)
        return

    updated = update_order_status(order_id, new_status)
    if not updated:
        await callback.answer("Заказ не найден", show_alert=True)
        return

    order = get_order_by_order_id(order_id)
    if order:
        text = (
            f"<b>Заказ {order['order_id']}</b>\n\n"
            f"📌 Статус: <b>{order['status']}</b>\n"
            f"📌 Оплата: <b>{order['payment_status']}</b>\n"
            f"👤 Клиент: {order['full_name']}\n"
            f"📞 Телефон: {order['phone']}\n"
            f"📍 Адрес: {order['address']}\n"
            f"🗓 Дата доставки: {order['delivery_date']}\n"
            f"⏰ Время доставки: {order['delivery_time'] or 'не указано'}\n"
            f"🥗 Программа: {order['program_title']}\n"
            f"📆 Срок: {order['days']} дн.\n"
            f"💳 Итого: {format_currency(order['total_price'])}\n"
            f"📝 Комментарий: {order['comment']}"
        )
        await callback.message.edit_text(text, reply_markup=admin_status_keyboard(order_id))
        await notify_user_about_status(bot, order, new_status)

    await callback.answer(f"Статус обновлён: {new_status}")

# ======================================
# FALLBACK
# ======================================
async def fallback_handler(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state == OrderForm.entering_promo.state and message.text.strip().lower() == "пропустить":
        await state.set_state(OrderForm.entering_name)
        await message.answer("Промокод пропущен.\n\nВведите ваше имя:")
        return

    if current_state is None:
        await message.answer("Я не понял сообщение. Выберите нужный раздел через меню ниже.", reply_markup=main_menu_keyboard())

# ======================================
# MAIN
# ======================================
async def main():
    init_db()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())

    # User
    dp.message.register(start_handler, CommandStart())
    dp.message.register(order_start_handler, F.text == "🛒 Оформить заказ")
    dp.message.register(repeat_order_handler, F.text == "🔁 Повторить заказ")
    dp.message.register(programs_handler, F.text == "🥗 Программы питания")
    dp.message.register(current_menu_handler, F.text == "🍽 Актуальное меню")
    dp.message.register(stop_list_handler, F.text == "⛔ Стоп-лист")
    dp.message.register(promo_list_handler, F.text == "🎁 Промокоды")
    dp.message.register(prices_handler, F.text == "💸 Цены")
    dp.message.register(delivery_handler, F.text == "🚚 Доставка")
    dp.message.register(faq_handler, F.text == "❓ Вопросы")
    dp.message.register(review_start_handler, F.text == "⭐ Оставить отзыв")
    dp.message.register(about_handler, F.text == "ℹ️ О нас")
    dp.message.register(manager_handler, F.text == "💬 Менеджер")
    dp.message.register(admin_panel_handler, F.text == "👑 Админ панель")

    # Callbacks
    dp.callback_query.register(program_view_handler, F.data.startswith("program_view:"))
    dp.callback_query.register(programs_back_handler, F.data == "programs:back")
    dp.callback_query.register(choose_program_handler, F.data.startswith("program_select:"))
    dp.callback_query.register(choose_duration_handler, OrderForm.choosing_duration, F.data.startswith("duration:"))
    dp.callback_query.register(promo_callback_handler, F.data.startswith("promo:"))
    dp.callback_query.register(delivery_time_handler, OrderForm.choosing_delivery_time, F.data.startswith("delivery_time:"))
    dp.callback_query.register(payment_handler, OrderForm.choosing_payment, F.data.startswith("payment:"))
    dp.callback_query.register(confirm_handler, OrderForm.confirming_order, F.data.startswith("confirm:"))
    dp.callback_query.register(admin_callback_handler, F.data.startswith("admin:"))
    dp.callback_query.register(status_callback_handler, F.data.startswith("status:"))
    dp.callback_query.register(review_callback_handler, F.data.startswith("review:"))

    # FSM messages
    dp.message.register(promo_text_handler, OrderForm.entering_promo)
    dp.message.register(name_handler, OrderForm.entering_name)
    dp.message.register(phone_handler, OrderForm.entering_phone)
    dp.message.register(address_handler, OrderForm.entering_address)
    dp.message.register(delivery_date_handler, OrderForm.entering_delivery_date)
    dp.message.register(comment_handler, OrderForm.entering_comment)
    dp.message.register(review_text_handler, ReviewForm.entering_review)

    # Admin commands
    dp.message.register(admin_help_handler, Command("help_admin"))
    dp.message.register(orders_today_handler, Command("orders_today"))
    dp.message.register(orders_week_handler, Command("orders_week"))
    dp.message.register(recent_orders_handler, Command("recent_orders"))
    dp.message.register(clients_handler, Command("clients"))
    dp.message.register(segments_handler, Command("segments"))
    dp.message.register(reviews_handler, Command("reviews"))
    dp.message.register(deliveries_today_handler, Command("deliveries_today"))
    dp.message.register(deliveries_tomorrow_handler, Command("deliveries_tomorrow"))
    dp.message.register(export_clients_handler, Command("export_clients"))
    dp.message.register(find_client_handler, Command("find_client"))
    dp.message.register(note_phone_handler, Command("note_phone"))
    dp.message.register(blacklist_phone_handler, Command("blacklist_phone"))
    dp.message.register(order_detail_handler, Command("order"))
    dp.message.register(set_status_handler, Command("set_status"))

    dp.message.register(fallback_handler)

    logger.info("Bot started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
