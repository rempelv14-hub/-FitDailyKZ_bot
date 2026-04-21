"""
FitDaily Telegram Bot
Сервис доставки правильного питания
"""

import asyncio
import csv
import logging
import os
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from html import escape
from typing import Any, Dict, List, Optional, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ──────────────────────────────────────────────────────────────
# КОНФИГУРАЦИЯ
# ──────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("Укажите BOT_TOKEN в .env файле")

ADMIN_TELEGRAM_ID = int(os.getenv("ADMIN_TELEGRAM_ID", "6954213997"))
DATABASE_PATH = os.getenv("DATABASE_PATH", "fitdaily.db")

BRAND_NAME = "FitDaily"
MANAGER_WHATSAPP = "https://wa.me/77712841932"
MANAGER_CONTACT_TEXT = "Менеджер FitDaily"
MAX_DELIVERY_DAYS_AHEAD = 365

DELIVERY_TIME_SLOTS = [
    "06:00-08:00",
    "08:00-10:00",
    "10:00-12:00",
    "12:00-14:00",
    "14:00-16:00",
    "16:00-18:00",
    "18:00-20:00",
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
        "calories": "1200-1400 ккал",
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
        "calories": "1500-1800 ккал",
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
        "calories": "1700-2200 ккал",
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
        "calories": "1000-1200 ккал",
        "goal": "Лёгкость и аккуратная разгрузка",
        "includes": [
            "4-5 лёгких приёмов пищи",
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
    {"name": "Овсянка с ягодами",          "category": "Завтрак",  "calories": "280 ккал"},
    {"name": "Сырники с йогуртовым соусом", "category": "Завтрак",  "calories": "340 ккал"},
    {"name": "Курица с булгуром и овощами", "category": "Обед",     "calories": "460 ккал"},
    {"name": "Говядина с рисом",            "category": "Обед",     "calories": "510 ккал"},
    {"name": "Творожный десерт",            "category": "Перекус",  "calories": "190 ккал"},
    {"name": "Лосось с овощами",            "category": "Ужин",     "calories": "430 ккал"},
]

STOP_LIST = [
    "Протеиновый десерт",
    "Паста с курицей",
]

FAQ = {
    "Как оформить заказ?":           "Нажмите «🛒 Оформить заказ», выберите программу, срок, данные доставки и способ оплаты.",
    "Как проходит доставка?":        "Вы выбираете адрес и интервал, менеджер подтверждает детали вручную.",
    "Можно ли выбрать время?":       "Да, доступные интервалы предлагаются при оформлении.",
    "Как связаться с менеджером?":   f"Через WhatsApp: {MANAGER_WHATSAPP}",
    "Можно ли изменить заказ?":      "Да, менеджер уточнит детали после оформления.",
    "Когда начнётся первая доставка?": "В выбранную вами дату. Менеджер подтвердит это после оформления.",
    "Есть ли пробный день?":         "Уточняйте у менеджера — для некоторых программ возможен тестовый заказ на 1 день.",
}

PAYMENT_METHODS = {
    "kaspi_demo": "🟡 Kaspi (демо)",
    "card_demo":  "💳 Карта (демо)",
    "cash":       "💵 Наличными курьеру",
}

PROMO_CODES = {
    "FIT10":     {"discount_percent": 10, "active": 1, "description": "Скидка 10%"},
    "WELCOME15": {"discount_percent": 15, "active": 1, "description": "Скидка 15% для новых клиентов"},
    "PROTEIN5":  {"discount_percent":  5, "active": 1, "description": "Скидка 5%"},
}

DURATIONS = {"1": 1, "5": 5, "7": 7, "14": 14, "30": 30}

STATUSES = [
    "Новая",
    "Ожидает оплату",
    "Подтверждена",
    "Готовится",
    "Передана в доставку",
    "Доставлена",
    "Отменена",
]

# Короткие коды статусов для callback_data (лимит Telegram — 64 байта)
STATUS_CODES = {
    "s0": "Новая",
    "s1": "Ожидает оплату",
    "s2": "Подтверждена",
    "s3": "Готовится",
    "s4": "Передана в доставку",
    "s5": "Доставлена",
    "s6": "Отменена",
}

# ──────────────────────────────────────────────────────────────
# ЛОГИРОВАНИЕ
# ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# HTML-ЭКРАНИРОВАНИЕ
# ──────────────────────────────────────────────────────────────
def h(value: Any) -> str:
    """Безопасная вставка произвольных данных в HTML-сообщение."""
    return escape(str(value or ""))


# ──────────────────────────────────────────────────────────────
# FSM СОСТОЯНИЯ
# ──────────────────────────────────────────────────────────────
class OrderForm(StatesGroup):
    choosing_program    = State()
    choosing_duration   = State()
    entering_promo      = State()
    entering_name       = State()
    entering_phone      = State()
    entering_address    = State()
    entering_delivery_date  = State()
    choosing_delivery_time  = State()
    entering_comment    = State()
    choosing_payment    = State()
    confirming_order    = State()


class ReviewForm(StatesGroup):
    entering_review = State()


class BroadcastForm(StatesGroup):
    entering_text = State()


# ──────────────────────────────────────────────────────────────
# БАЗА ДАННЫХ
# ──────────────────────────────────────────────────────────────
def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with closing(get_connection()) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id         TEXT UNIQUE NOT NULL,
                created_at       TEXT NOT NULL,
                telegram_user_id INTEGER NOT NULL,
                telegram_username TEXT,
                full_name        TEXT NOT NULL,
                phone            TEXT NOT NULL,
                address          TEXT NOT NULL,
                delivery_date    TEXT NOT NULL,
                delivery_time    TEXT,
                program_key      TEXT NOT NULL,
                program_title    TEXT NOT NULL,
                days             INTEGER NOT NULL,
                price_per_day    INTEGER NOT NULL,
                total_price      INTEGER NOT NULL,
                discount_percent INTEGER NOT NULL DEFAULT 0,
                discount_amount  INTEGER NOT NULL DEFAULT 0,
                promo_code       TEXT,
                payment_method   TEXT,
                payment_status   TEXT NOT NULL DEFAULT 'Не требуется',
                comment          TEXT,
                status           TEXT NOT NULL DEFAULT 'Новая'
            );

            CREATE TABLE IF NOT EXISTS clients (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_user_id INTEGER UNIQUE NOT NULL,
                telegram_username TEXT,
                full_name        TEXT NOT NULL,
                phone            TEXT NOT NULL,
                address          TEXT,
                favorite_program TEXT,
                first_order_at   TEXT,
                last_order_at    TEXT,
                total_orders     INTEGER NOT NULL DEFAULT 0,
                total_spent      INTEGER NOT NULL DEFAULT 0,
                last_order_id    TEXT,
                last_program_title TEXT
            );

            CREATE TABLE IF NOT EXISTS client_notes (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                phone      TEXT NOT NULL,
                note_text  TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS reviews (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id         TEXT NOT NULL,
                telegram_user_id INTEGER NOT NULL,
                full_name        TEXT,
                rating           INTEGER NOT NULL,
                review_text      TEXT,
                created_at       TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS blacklisted_clients (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                phone      TEXT UNIQUE NOT NULL,
                reason     TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS promo_codes (
                code             TEXT PRIMARY KEY,
                discount_percent INTEGER NOT NULL,
                active           INTEGER NOT NULL DEFAULT 1,
                description      TEXT,
                uses_count       INTEGER NOT NULL DEFAULT 0
            );
            """
        )

        # Миграция: добавляем колонки, которых может не быть в старой БД
        _migrate(conn, "orders", {
            "delivery_time":    "ALTER TABLE orders ADD COLUMN delivery_time TEXT",
            "discount_percent": "ALTER TABLE orders ADD COLUMN discount_percent INTEGER NOT NULL DEFAULT 0",
            "discount_amount":  "ALTER TABLE orders ADD COLUMN discount_amount  INTEGER NOT NULL DEFAULT 0",
            "promo_code":       "ALTER TABLE orders ADD COLUMN promo_code TEXT",
            "payment_method":   "ALTER TABLE orders ADD COLUMN payment_method TEXT",
            "payment_status":   "ALTER TABLE orders ADD COLUMN payment_status TEXT NOT NULL DEFAULT 'Не требуется'",
        })
        _migrate(conn, "promo_codes", {
            "uses_count": "ALTER TABLE promo_codes ADD COLUMN uses_count INTEGER NOT NULL DEFAULT 0",
        })

        for code, data in PROMO_CODES.items():
            conn.execute(
                "INSERT OR IGNORE INTO promo_codes (code, discount_percent, active, description)"
                " VALUES (?, ?, ?, ?)",
                (code, data["discount_percent"], data["active"], data["description"]),
            )
        conn.commit()


def _migrate(conn: sqlite3.Connection, table: str, columns: Dict[str, str]) -> None:
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for col, sql in columns.items():
        if col not in existing:
            conn.execute(sql)


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def save_order_to_db(order_data: Dict[str, Any], telegram_user) -> None:
    with closing(get_connection()) as conn:
        conn.execute(
            """
            INSERT INTO orders (
                order_id, created_at, telegram_user_id, telegram_username, full_name,
                phone, address, delivery_date, delivery_time, program_key, program_title,
                days, price_per_day, total_price, discount_percent, discount_amount,
                promo_code, payment_method, payment_status, comment, status
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                order_data["order_id"], _now(),
                telegram_user.id, telegram_user.username or "",
                order_data["name"], order_data["phone"], order_data["address"],
                order_data["delivery_date"], order_data.get("delivery_time", ""),
                order_data["program_key"], order_data["program_title"],
                order_data["days"], order_data["price_per_day"], order_data["total_price"],
                order_data.get("discount_percent", 0), order_data.get("discount_amount", 0),
                order_data.get("promo_code"), order_data.get("payment_method"),
                order_data.get("payment_status", "Не требуется"),
                order_data.get("comment", ""), order_data.get("status", "Новая"),
            ),
        )
        # Увеличиваем счётчик использования промокода
        if order_data.get("promo_code"):
            conn.execute(
                "UPDATE promo_codes SET uses_count = uses_count + 1 WHERE code = ?",
                (order_data["promo_code"],),
            )
        conn.commit()


def get_favorite_program_for_user(user_id: int, fallback: Optional[str] = None) -> Optional[str]:
    with closing(get_connection()) as conn:
        row = conn.execute(
            "SELECT program_title FROM orders WHERE telegram_user_id = ?"
            " GROUP BY program_title ORDER BY COUNT(*) DESC LIMIT 1",
            (user_id,),
        ).fetchone()
        return row["program_title"] if row else fallback


def upsert_client(order_data: Dict[str, Any], telegram_user) -> None:
    # Вызывается ПОСЛЕ save_order_to_db, поэтому новый заказ уже учтён в статистике
    favorite_program = get_favorite_program_for_user(
        telegram_user.id, fallback=order_data["program_title"]
    )
    now = _now()
    with closing(get_connection()) as conn:
        existing = conn.execute(
            "SELECT id FROM clients WHERE telegram_user_id = ?", (telegram_user.id,)
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE clients
                SET telegram_username = ?, full_name = ?, phone = ?, address = ?,
                    favorite_program = ?, last_order_at = ?,
                    total_orders = total_orders + 1,
                    total_spent  = total_spent  + ?,
                    last_order_id = ?, last_program_title = ?
                WHERE telegram_user_id = ?
                """,
                (
                    telegram_user.username or "", order_data["name"],
                    order_data["phone"], order_data["address"],
                    favorite_program, now, order_data["total_price"],
                    order_data["order_id"], order_data["program_title"],
                    telegram_user.id,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO clients (
                    telegram_user_id, telegram_username, full_name, phone, address,
                    favorite_program, first_order_at, last_order_at,
                    total_orders, total_spent, last_order_id, last_program_title
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    telegram_user.id, telegram_user.username or "",
                    order_data["name"], order_data["phone"], order_data["address"],
                    favorite_program, now, now, 1, order_data["total_price"],
                    order_data["order_id"], order_data["program_title"],
                ),
            )
        conn.commit()


def update_order_status(order_id: str, new_status: str) -> bool:
    with closing(get_connection()) as conn:
        cur = conn.execute(
            "UPDATE orders SET status = ? WHERE order_id = ?", (new_status, order_id)
        )
        conn.commit()
        return cur.rowcount > 0


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
        return conn.execute(
            "SELECT * FROM orders ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()


def get_recent_clients(limit: int = 20) -> List[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute(
            "SELECT * FROM clients ORDER BY last_order_at DESC LIMIT ?", (limit,)
        ).fetchall()


def get_all_client_ids() -> List[int]:
    with closing(get_connection()) as conn:
        rows = conn.execute("SELECT telegram_user_id FROM clients").fetchall()
        return [r["telegram_user_id"] for r in rows]


def get_reviews(limit: int = 20) -> List[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute(
            "SELECT * FROM reviews ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()


def get_last_order_by_user(user_id: int) -> Optional[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute(
            "SELECT * FROM orders WHERE telegram_user_id = ? ORDER BY created_at DESC LIMIT 1",
            (user_id,),
        ).fetchone()


def get_user_orders(user_id: int, limit: int = 5) -> List[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute(
            "SELECT * FROM orders WHERE telegram_user_id = ? ORDER BY created_at DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()


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
        return [r for r in rows if normalize_phone(r["phone"]).endswith(digits[-10:])]


def get_client_notes(phone: str) -> List[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute(
            "SELECT * FROM client_notes WHERE phone = ? ORDER BY created_at DESC", (phone,)
        ).fetchall()


def add_client_note(phone: str, note_text: str) -> None:
    with closing(get_connection()) as conn:
        conn.execute(
            "INSERT INTO client_notes (phone, note_text, created_at) VALUES (?,?,?)",
            (phone, note_text, _now()),
        )
        conn.commit()


def add_to_blacklist(phone: str, reason: str) -> None:
    with closing(get_connection()) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO blacklisted_clients (phone, reason, created_at) VALUES (?,?,?)",
            (phone, reason, _now()),
        )
        conn.commit()


def remove_from_blacklist(phone: str) -> bool:
    normalized = normalize_phone(phone)
    with closing(get_connection()) as conn:
        # находим все совпадения и удаляем
        rows = conn.execute("SELECT phone FROM blacklisted_clients").fetchall()
        deleted = 0
        for row in rows:
            if normalize_phone(row["phone"]).endswith(normalized[-10:]):
                conn.execute("DELETE FROM blacklisted_clients WHERE phone = ?", (row["phone"],))
                deleted += 1
        conn.commit()
        return deleted > 0


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
            "SELECT * FROM promo_codes WHERE code = ? AND active = 1", (code.upper(),)
        ).fetchone()


def add_promo_code(code: str, percent: int, description: str) -> bool:
    try:
        with closing(get_connection()) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO promo_codes (code, discount_percent, active, description, uses_count)"
                " VALUES (?,?,1,?,0)",
                (code.upper(), percent, description),
            )
            conn.commit()
        return True
    except Exception:
        return False


def deactivate_promo_code(code: str) -> bool:
    with closing(get_connection()) as conn:
        cur = conn.execute(
            "UPDATE promo_codes SET active = 0 WHERE code = ?", (code.upper(),)
        )
        conn.commit()
        return cur.rowcount > 0


def add_review(order_id: str, telegram_user_id: int, full_name: str, rating: int, review_text: str) -> None:
    with closing(get_connection()) as conn:
        conn.execute(
            "INSERT INTO reviews (order_id, telegram_user_id, full_name, rating, review_text, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (order_id, telegram_user_id, full_name, rating, review_text, _now()),
        )
        conn.commit()


def get_global_stats() -> Dict[str, Any]:
    with closing(get_connection()) as conn:
        total_orders   = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        total_revenue  = conn.execute("SELECT COALESCE(SUM(total_price),0) FROM orders").fetchone()[0]
        total_clients  = conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0]
        avg_rating_row = conn.execute("SELECT ROUND(AVG(rating),1) FROM reviews").fetchone()
        avg_rating     = avg_rating_row[0] if avg_rating_row[0] else "—"
        return {
            "total_orders":  total_orders,
            "total_revenue": total_revenue,
            "total_clients": total_clients,
            "avg_rating":    avg_rating,
        }


# ──────────────────────────────────────────────────────────────
# КЛАВИАТУРЫ
# ──────────────────────────────────────────────────────────────
def main_menu_keyboard(is_admin: bool = False) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="🛒 Оформить заказ"),   KeyboardButton(text="🔁 Повторить заказ")],
        [KeyboardButton(text="📋 Мои заказы"),        KeyboardButton(text="🥗 Программы питания")],
        [KeyboardButton(text="🍽 Актуальное меню"),   KeyboardButton(text="⛔ Стоп-лист")],
        [KeyboardButton(text="🎁 Промокоды"),         KeyboardButton(text="💸 Цены")],
        [KeyboardButton(text="🚚 Доставка"),          KeyboardButton(text="❓ Вопросы")],
        [KeyboardButton(text="⭐ Оставить отзыв"),    KeyboardButton(text="💬 Менеджер")],
        [KeyboardButton(text="ℹ️ О нас")],
    ]
    if is_admin:
        rows.append([KeyboardButton(text="👑 Админ панель")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def cancel_keyboard() -> InlineKeyboardMarkup:
    """Кнопка отмены, доступная на каждом шаге оформления заказа."""
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="❌ Отменить заказ", callback_data="order:cancel_flow")]]
    )


def programs_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🥬 Slim",    callback_data="program_view:slim"),
                InlineKeyboardButton(text="⚖️ Balance", callback_data="program_view:balance"),
            ],
            [
                InlineKeyboardButton(text="💪 Protein", callback_data="program_view:protein"),
                InlineKeyboardButton(text="🍏 Detox",   callback_data="program_view:detox"),
            ],
        ]
    )


def durations_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="1 день",   callback_data="duration:1"),
                InlineKeyboardButton(text="5 дней",   callback_data="duration:5"),
            ],
            [
                InlineKeyboardButton(text="7 дней",   callback_data="duration:7"),
                InlineKeyboardButton(text="14 дней",  callback_data="duration:14"),
            ],
            [
                InlineKeyboardButton(text="30 дней",  callback_data="duration:30"),
            ],
            [InlineKeyboardButton(text="❌ Отменить", callback_data="order:cancel_flow")],
        ]
    )


def promo_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎁 Ввести промокод", callback_data="promo:enter")],
            [InlineKeyboardButton(text="⏭ Пропустить",       callback_data="promo:skip")],
            [InlineKeyboardButton(text="❌ Отменить",         callback_data="order:cancel_flow")],
        ]
    )


def delivery_time_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=slot, callback_data=f"dtime:{slot}")]
        for slot in DELIVERY_TIME_SLOTS
    ]
    rows.append([InlineKeyboardButton(text="❌ Отменить", callback_data="order:cancel_flow")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def payment_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🟡 Kaspi (демо)",      callback_data="payment:kaspi_demo")],
            [InlineKeyboardButton(text="💳 Карта (демо)",       callback_data="payment:card_demo")],
            [InlineKeyboardButton(text="💵 Наличными курьеру",  callback_data="payment:cash")],
            [InlineKeyboardButton(text="❌ Отменить",           callback_data="order:cancel_flow")],
        ]
    )


def confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить заказ",  callback_data="confirm:yes")],
            [InlineKeyboardButton(text="✏️ Заполнить заново",   callback_data="confirm:restart")],
            [InlineKeyboardButton(text="❌ Отменить",            callback_data="confirm:cancel")],
        ]
    )


def program_details_keyboard(program_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🛒 Выбрать программу", callback_data=f"program_select:{program_key}")],
            [InlineKeyboardButton(text="⬅️ Назад",             callback_data="programs:back")],
        ]
    )


def admin_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📊 Сегодня",        callback_data="admin:today"),
                InlineKeyboardButton(text="📅 7 дней",          callback_data="admin:week"),
            ],
            [
                InlineKeyboardButton(text="🧾 Последние заказы", callback_data="admin:recent"),
                InlineKeyboardButton(text="👥 Клиенты",          callback_data="admin:clients"),
            ],
            [
                InlineKeyboardButton(text="🚚 Сегодня доставки",  callback_data="admin:del_today"),
                InlineKeyboardButton(text="🗓 Завтра доставки",   callback_data="admin:del_tomorrow"),
            ],
            [
                InlineKeyboardButton(text="📌 Ожидает оплату",  callback_data="admin:sf:s1"),
                InlineKeyboardButton(text="🍳 Готовится",        callback_data="admin:sf:s3"),
            ],
            [
                InlineKeyboardButton(text="🚚 В доставке",      callback_data="admin:sf:s4"),
                InlineKeyboardButton(text="⭐ Отзывы",           callback_data="admin:reviews"),
            ],
            [
                InlineKeyboardButton(text="📈 Сегменты",         callback_data="admin:segments"),
                InlineKeyboardButton(text="🎁 Промокоды",        callback_data="admin:promos"),
            ],
        ]
    )


def admin_status_keyboard(order_id: str) -> InlineKeyboardMarkup:
    def cb(code: str) -> str:
        return f"ss:{order_id}:{code}"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="💰 Ожидает оплату", callback_data=cb("s1")),
                InlineKeyboardButton(text="✅ Подтверждена",    callback_data=cb("s2")),
            ],
            [
                InlineKeyboardButton(text="👨‍🍳 Готовится",    callback_data=cb("s3")),
                InlineKeyboardButton(text="🚚 В доставке",      callback_data=cb("s4")),
            ],
            [
                InlineKeyboardButton(text="🎉 Доставлена",      callback_data=cb("s5")),
                InlineKeyboardButton(text="❌ Отменена",         callback_data=cb("s6")),
            ],
        ]
    )


def review_rating_keyboard(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text=f"{i}⭐", callback_data=f"rv:{order_id}:{i}")
            for i in range(1, 6)
        ]]
    )


# ──────────────────────────────────────────────────────────────
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ──────────────────────────────────────────────────────────────
def admin_only(user_id: int) -> bool:
    return user_id == ADMIN_TELEGRAM_ID


def format_currency(value: int) -> str:
    return f"{value:,} ₸".replace(",", " ")


def generate_order_id(user_id: int) -> str:
    return f"FD-{user_id}-{datetime.now().strftime('%d%m%H%M%S')}"


def validate_phone(phone: str) -> bool:
    digits = normalize_phone(phone)
    return 10 <= len(digits) <= 15


def validate_delivery_date(date_text: str) -> bool:
    try:
        entered = datetime.strptime(date_text, "%d.%m.%Y").date()
        today   = datetime.now().date()
        return today <= entered <= today + timedelta(days=MAX_DELIVERY_DAYS_AHEAD)
    except ValueError:
        return False


def calculate_discount(total_price: int, promo_code: Optional[str]) -> Tuple[int, int]:
    if not promo_code:
        return 0, 0
    promo = get_promo(promo_code)
    if not promo:
        return 0, 0
    pct = int(promo["discount_percent"])
    amount = total_price * pct // 100
    return pct, amount


def client_segment(client: sqlite3.Row) -> str:
    total  = client["total_orders"]
    last   = client["last_order_at"]
    if total == 1:
        return "Новый"
    if total >= 5:
        try:
            if datetime.strptime(last, "%Y-%m-%d %H:%M:%S") >= datetime.now() - timedelta(days=30):
                return "Постоянный"
        except Exception:
            pass
    try:
        if datetime.strptime(last, "%Y-%m-%d %H:%M:%S") < datetime.now() - timedelta(days=30):
            return "Спящий"
    except Exception:
        pass
    return "Активный"


# ──────────────────────────────────────────────────────────────
# ТЕКСТОВЫЕ КОНСТРУКТОРЫ
# ──────────────────────────────────────────────────────────────
def build_prices_text() -> str:
    lines = ["<b>💸 Цены на программы</b>\n"]
    for item in PROGRAMS.values():
        lines.append(f"• {item['title']} — <b>{format_currency(item['price_per_day'])}</b> / день")
    lines.append("\nИтоговая стоимость рассчитывается автоматически при оформлении.")
    return "\n".join(lines)


def build_promos_text() -> str:
    lines = ["<b>🎁 Доступные промокоды</b>\n"]
    with closing(get_connection()) as conn:
        promos = conn.execute(
            "SELECT * FROM promo_codes WHERE active = 1 ORDER BY discount_percent DESC"
        ).fetchall()
    if not promos:
        lines.append("Активных промокодов сейчас нет.")
    for p in promos:
        lines.append(f"• <b>{h(p['code'])}</b> — {p['discount_percent']}% ({h(p['description'] or 'без описания')})")
    return "\n".join(lines)


def build_admin_promos_text() -> str:
    lines = ["<b>🎁 Все промокоды</b>\n"]
    with closing(get_connection()) as conn:
        promos = conn.execute("SELECT * FROM promo_codes ORDER BY active DESC, discount_percent DESC").fetchall()
    if not promos:
        lines.append("Промокодов нет.")
        return "\n".join(lines)
    for p in promos:
        status = "✅ активен" if p["active"] else "❌ отключён"
        lines.append(
            f"• <b>{h(p['code'])}</b> — {p['discount_percent']}%"
            f" | {status} | использований: {p['uses_count']}"
            f"\n  {h(p['description'] or '—')}"
        )
    return "\n".join(lines)


def build_current_menu_text() -> str:
    lines = ["<b>🍽 Актуальное меню FitDaily</b>\n"]
    for item in CURRENT_MENU:
        lines.append(
            f"• <b>{item['name']}</b>\n"
            f"  Раздел: {item['category']}  |  {item['calories']}\n"
        )
    lines.append("Меню обновляется ежедневно.")
    return "\n".join(lines)


def build_stop_list_text() -> str:
    lines = ["<b>⛔ Стоп-лист</b>\n"]
    if not STOP_LIST:
        lines.append("Сегодня стоп-листа нет ✅")
    else:
        lines.extend(f"• {item}" for item in STOP_LIST)
    return "\n".join(lines)


def build_faq_text() -> str:
    lines = ["<b>❓ Частые вопросы</b>\n"]
    for q, a in FAQ.items():
        lines.append(f"<b>{q}</b>\n{a}\n")
    return "\n".join(lines)


def build_kaspi_demo_text(order_data: Dict[str, Any]) -> str:
    return (
        "<b>🟡 Оплата Kaspi (демо)</b>\n\n"
        "Демонстрационный способ оплаты. Настоящая интеграция не подключена.\n\n"
        f"Сумма к оплате: <b>{format_currency(order_data['total_price'])}</b>\n"
        "Статус оплаты: <b>Ожидает оплаты</b>."
    )


def build_order_summary(data: Dict[str, Any]) -> str:
    promo_line = ""
    if data.get("promo_code"):
        promo_line = (
            f"🎁 Промокод: <b>{h(data['promo_code'])}</b>\n"
            f"📉 Скидка: <b>{data.get('discount_percent', 0)}%</b>"
            f" ({format_currency(data.get('discount_amount', 0))})\n"
        )
    payment_label = PAYMENT_METHODS.get(data.get("payment_method", ""), data.get("payment_method", "—"))
    return (
        "<b>✨ Проверьте ваш заказ</b>\n\n"
        f"🆔 Номер: <b>{h(data['order_id'])}</b>\n"
        f"🥗 Программа: <b>{h(data['program_title'])}</b>\n"
        f"📆 Срок: <b>{data['days']} дн.</b>\n"
        f"💰 Цена в день: <b>{format_currency(data['price_per_day'])}</b>\n"
        f"{promo_line}"
        f"💳 Итого: <b>{format_currency(data['total_price'])}</b>\n"
        f"💸 Оплата: <b>{h(payment_label)}</b>\n"
        f"📌 Статус оплаты: <b>{h(data.get('payment_status', '—'))}</b>\n\n"
        f"👤 Имя: {h(data['name'])}\n"
        f"📞 Телефон: {h(data['phone'])}\n"
        f"📍 Адрес: {h(data['address'])}\n"
        f"🗓 Дата доставки: {h(data['delivery_date'])}\n"
        f"⏰ Время доставки: {h(data.get('delivery_time', '—'))}\n"
        f"📝 Комментарий: {h(data.get('comment', '—'))}"
    )


def build_admin_order_text(data: Dict[str, Any], user) -> str:
    username = f"@{h(user.username)}" if user.username else "не указан"
    payment_label = PAYMENT_METHODS.get(data.get("payment_method", ""), data.get("payment_method", "—"))
    return (
        "<b>🆕 Новая заявка FitDaily</b>\n\n"
        f"🆔 Заказ: <b>{h(data['order_id'])}</b>\n"
        f"📌 Статус: <b>{h(data.get('status', 'Новая'))}</b>\n"
        f"📌 Статус оплаты: <b>{h(data.get('payment_status', '—'))}</b>\n"
        f"💸 Способ оплаты: <b>{h(payment_label)}</b>\n"
        f"👤 Клиент: {h(data['name'])}\n"
        f"📞 Телефон: {h(data['phone'])}\n"
        f"📍 Адрес: {h(data['address'])}\n"
        f"🗓 Дата: {h(data['delivery_date'])}\n"
        f"⏰ Время: {h(data.get('delivery_time', '—'))}\n"
        f"🥗 Программа: {h(data['program_title'])}\n"
        f"📆 Срок: {data['days']} дн.\n"
        f"🎁 Промокод: {h(data.get('promo_code') or 'нет')}\n"
        f"💰 Цена до скидки: {format_currency(data['price_per_day'] * data['days'])}\n"
        f"📉 Скидка: {format_currency(data.get('discount_amount', 0))}\n"
        f"💳 Итого: {format_currency(data['total_price'])}\n"
        f"📝 Комментарий: {h(data.get('comment', '—'))}\n\n"
        f"Telegram ID: <code>{user.id}</code>\n"
        f"Username: {username}\n\n"
        "Выберите статус:"
    )


def build_stats_text(title: str, orders: List[sqlite3.Row]) -> str:
    total_revenue = sum(o["total_price"] for o in orders)
    by_status: Dict[str, int] = {}
    by_program: Dict[str, int] = {}
    for o in orders:
        by_status[o["status"]]       = by_status.get(o["status"], 0) + 1
        by_program[o["program_title"]] = by_program.get(o["program_title"], 0) + 1

    lines = [
        f"<b>{h(title)}</b>", "",
        f"🧾 Заказов: <b>{len(orders)}</b>",
        f"💰 Выручка: <b>{format_currency(total_revenue)}</b>",
        "", "<b>По статусам:</b>",
    ]
    for s, c in (by_status.items() or [("—", 0)]):
        lines.append(f"• {h(s)}: {c}")
    lines += ["", "<b>По программам:</b>"]
    for p, c in (by_program.items() or [("—", 0)]):
        lines.append(f"• {h(p)}: {c}")
    return "\n".join(lines)


def build_clients_text(clients: List[sqlite3.Row]) -> str:
    lines = ["<b>👥 Последние клиенты</b>\n"]
    for c in clients:
        lines.append(
            f"<b>{h(c['full_name'])}</b> | 📞 {h(c['phone'])}\n"
            f"📦 Заказов: {c['total_orders']} | 💰 {format_currency(c['total_spent'])}\n"
            f"🥗 {h(c['favorite_program'] or '—')} | 🕒 {h(c['last_order_at'])}\n"
        )
    return "\n".join(lines)


def build_orders_text(title: str, orders: List[sqlite3.Row]) -> str:
    if not orders:
        return f"<b>{h(title)}</b>\n\nНет данных."
    lines = [f"<b>{h(title)}</b>\n"]
    for o in orders:
        lines.append(
            f"<b>{h(o['order_id'])}</b>\n"
            f"👤 {h(o['full_name'])} | 📞 {h(o['phone'])}\n"
            f"🥗 {h(o['program_title'])}\n"
            f"🗓 {h(o['delivery_date'])} {h(o['delivery_time'] or '')}\n"
            f"📌 {h(o['status'])} | 💳 {format_currency(o['total_price'])}\n"
        )
    text = "\n".join(lines)
    return text[:3900] + "\n\n..." if len(text) > 3900 else text


def build_my_orders_text(orders: List[sqlite3.Row]) -> str:
    if not orders:
        return "У вас пока нет заказов. Нажмите «🛒 Оформить заказ»."
    lines = ["<b>📋 Ваши последние заказы</b>\n"]
    for o in orders:
        lines.append(
            f"<b>{h(o['order_id'])}</b>\n"
            f"🥗 {h(o['program_title'])} — {o['days']} дн.\n"
            f"🗓 {h(o['delivery_date'])}\n"
            f"📌 <i>{h(o['status'])}</i>\n"
            f"💳 {format_currency(o['total_price'])}\n"
        )
    return "\n".join(lines)


def build_reviews_text(reviews: List[sqlite3.Row]) -> str:
    if not reviews:
        return "<b>⭐ Отзывы</b>\n\nПока отзывов нет."
    lines = ["<b>⭐ Последние отзывы</b>\n"]
    for r in reviews:
        stars = "⭐" * r["rating"]
        lines.append(
            f"<b>{h(r['full_name'] or 'Клиент')}</b> {stars}\n"
            f"🆔 {h(r['order_id'])}\n"
            f"💬 {h(r['review_text'] or 'Без текста')}\n"
            f"🕒 {h(r['created_at'])}\n"
        )
    return "\n".join(lines)


def build_segments_text() -> str:
    clients = get_recent_clients(10000)
    stats: Dict[str, int] = {"Новый": 0, "Постоянный": 0, "Активный": 0, "Спящий": 0}
    for c in clients:
        seg = client_segment(c)
        stats[seg] = stats.get(seg, 0) + 1
    return (
        "<b>📊 Сегменты клиентов</b>\n\n"
        f"🆕 Новые: <b>{stats['Новый']}</b>\n"
        f"🔥 Активные: <b>{stats['Активный']}</b>\n"
        f"💎 Постоянные: <b>{stats['Постоянный']}</b>\n"
        f"😴 Спящие: <b>{stats['Спящий']}</b>"
    )


async def notify_user_about_status(bot: Bot, order: sqlite3.Row, new_status: str) -> None:
    try:
        await bot.send_message(
            order["telegram_user_id"],
            f"<b>Обновление по заказу {h(order['order_id'])}</b>\n\n"
            f"📌 Новый статус: <b>{h(new_status)}</b>\n"
            f"🥗 Программа: {h(order['program_title'])}\n"
            f"💳 Сумма: {format_currency(order['total_price'])}",
        )
        if new_status == "Передана в доставку":
            await bot.send_message(order["telegram_user_id"], "🚚 Курьер выехал. Ожидайте доставку!")
        if new_status == "Доставлена":
            await bot.send_message(
                order["telegram_user_id"],
                "⭐ Спасибо за заказ! Оцените, пожалуйста, ваш опыт:",
                reply_markup=review_rating_keyboard(order["order_id"]),
            )
    except Exception as e:
        logger.warning("Не удалось уведомить клиента %s: %s", order["telegram_user_id"], e)


def _export_to_csv(path: str, rows: List[sqlite3.Row], headers: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(headers)
        for row in rows:
            writer.writerow([row[col] if col in row.keys() else "" for col in headers])


def export_clients_to_csv(path: str) -> None:
    clients = get_recent_clients(10000)
    headers = [
        "full_name", "phone", "telegram_username", "address", "favorite_program",
        "first_order_at", "last_order_at", "total_orders", "total_spent",
    ]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(headers + ["segment"])
        for c in clients:
            writer.writerow([c[h_] for h_ in headers] + [client_segment(c)])


def export_orders_to_csv(path: str) -> None:
    orders = get_recent_orders(10000)
    headers = [
        "order_id", "created_at", "full_name", "phone", "address",
        "delivery_date", "delivery_time", "program_title", "days",
        "price_per_day", "discount_percent", "discount_amount",
        "promo_code", "total_price", "payment_method", "payment_status",
        "status", "comment",
    ]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(headers)
        for o in orders:
            writer.writerow([o[col] if col in o.keys() else "" for col in headers])


async def _send_and_delete_csv(message: Message, path: str, caption: str) -> None:
    try:
        await message.answer_document(FSInputFile(path), caption=caption)
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


# ──────────────────────────────────────────────────────────────
# ОБРАБОТЧИКИ — ПОЛЬЗОВАТЕЛЬ
# ──────────────────────────────────────────────────────────────
async def start_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer(
        f"Здравствуйте! Добро пожаловать в <b>{BRAND_NAME}</b> 🥗\n\n"
        "Оформляйте заказ, смотрите меню, стоп-лист и промокоды прямо здесь.",
        reply_markup=main_menu_keyboard(is_admin=admin_only(message.from_user.id)),
    )


async def cancel_command_handler(message: Message, state: FSMContext) -> None:
    """Отмена текущего действия через /cancel."""
    current = await state.get_state()
    await state.clear()
    if current:
        await message.answer(
            "Действие отменено. Вы вернулись в главное меню.",
            reply_markup=main_menu_keyboard(is_admin=admin_only(message.from_user.id)),
        )
    else:
        await message.answer(
            "Нет активного действия для отмены.",
            reply_markup=main_menu_keyboard(is_admin=admin_only(message.from_user.id)),
        )


async def cancel_flow_callback(callback: CallbackQuery, state: FSMContext) -> None:
    """Отмена через инлайн-кнопку во время оформления."""
    await state.clear()
    await callback.message.edit_text("Оформление заказа отменено.")
    await callback.message.answer(
        "Вы вернулись в главное меню.",
        reply_markup=main_menu_keyboard(is_admin=admin_only(callback.from_user.id)),
    )
    await callback.answer()


async def my_orders_handler(message: Message) -> None:
    orders = get_user_orders(message.from_user.id, limit=5)
    await message.answer(build_my_orders_text(orders))


async def programs_handler(message: Message) -> None:
    await message.answer(
        "<b>🥗 Программы питания FitDaily</b>\n\nНажмите на программу, чтобы посмотреть подробности.",
        reply_markup=programs_keyboard(),
    )


async def current_menu_handler(message: Message) -> None:
    await message.answer(build_current_menu_text())


async def stop_list_handler(message: Message) -> None:
    await message.answer(build_stop_list_text())


async def promo_list_handler(message: Message) -> None:
    await message.answer(build_promos_text())


async def faq_handler(message: Message) -> None:
    await message.answer(build_faq_text())


async def prices_handler(message: Message) -> None:
    await message.answer(build_prices_text())


async def delivery_handler(message: Message) -> None:
    await message.answer(DELIVERY_INFO)


async def about_handler(message: Message) -> None:
    await message.answer(ABOUT_TEXT)


async def manager_handler(message: Message) -> None:
    await message.answer(
        "<b>💬 Связь с менеджером</b>\n\n"
        f"{MANAGER_CONTACT_TEXT}\n\n"
        "Нажмите кнопку ниже, чтобы открыть WhatsApp.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="💬 Написать в WhatsApp", url=MANAGER_WHATSAPP)
        ]]),
    )


async def repeat_order_handler(message: Message, state: FSMContext) -> None:
    last_order = get_last_order_by_user(message.from_user.id)
    if not last_order:
        await message.answer("У вас пока нет прошлых заказов.")
        return

    await state.clear()

    base_total = last_order["price_per_day"] * last_order["days"]
    promo_code = last_order["promo_code"]
    if promo_code and get_promo(promo_code):
        discount_percent, discount_amount = calculate_discount(base_total, promo_code)
        total_price = max(base_total - discount_amount, 0)
    else:
        promo_code, discount_percent, discount_amount = None, 0, 0
        total_price = base_total

    promo_note = ""
    if last_order["promo_code"] and not promo_code:
        promo_note = "\n⚠️ Промокод из прошлого заказа больше не активен."

    await state.update_data(
        program_key=last_order["program_key"],
        program_title=last_order["program_title"],
        price_per_day=last_order["price_per_day"],
        days=last_order["days"],
        promo_code=promo_code,
        discount_percent=discount_percent,
        discount_amount=discount_amount,
        total_price=total_price,
        name=last_order["full_name"],
        phone=last_order["phone"],
        address=last_order["address"],
        comment=last_order["comment"] or "Нет",
    )
    await state.set_state(OrderForm.entering_delivery_date)
    await message.answer(
        "<b>🔁 Повтор заказа</b>\n\n"
        f"Программа: {h(last_order['program_title'])} на {last_order['days']} дн.\n"
        f"Итого: <b>{format_currency(total_price)}</b>{promo_note}\n\n"
        "Введите новую дату доставки (ДД.ММ.ГГГГ):",
        reply_markup=cancel_keyboard(),
    )


async def review_start_handler(message: Message) -> None:
    last_order = get_last_order_by_user(message.from_user.id)
    if not last_order:
        await message.answer("Сначала оформите хотя бы один заказ.")
        return
    await message.answer(
        "Оцените последний заказ:",
        reply_markup=review_rating_keyboard(last_order["order_id"]),
    )


# ──────────────────────────────────────────────────────────────
# ОФОРМЛЕНИЕ ЗАКАЗА
# ──────────────────────────────────────────────────────────────
async def order_start_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(OrderForm.choosing_program)
    await message.answer(
        "<b>🛒 Оформление заказа</b>\n\nВыберите программу питания:",
        reply_markup=programs_keyboard(),
    )


async def program_view_handler(callback: CallbackQuery) -> None:
    program_key = callback.data.split(":", 1)[1]
    program = PROGRAMS.get(program_key)
    if not program:
        await callback.answer("Программа не найдена", show_alert=True)
        return
    includes_text = "\n".join(f"• {i}" for i in program["includes"])
    menu_text     = "\n".join(f"• {i}" for i in program["sample_menu"])
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


async def programs_back_handler(callback: CallbackQuery) -> None:
    await callback.message.edit_text(
        "<b>🥗 Программы питания FitDaily</b>\n\nНажмите на программу, чтобы посмотреть подробности.",
        reply_markup=programs_keyboard(),
    )
    await callback.answer()


async def choose_program_handler(callback: CallbackQuery, state: FSMContext) -> None:
    program_key = callback.data.split(":", 1)[1]
    program = PROGRAMS.get(program_key)
    if not program:
        await callback.answer("Программа не найдена", show_alert=True)
        return

    current_state = await state.get_state()
    if current_state is None:
        await state.set_state(OrderForm.choosing_program)
    elif current_state != OrderForm.choosing_program.state:
        await callback.answer("Нажмите «🛒 Оформить заказ» в меню.", show_alert=True)
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
        "Выберите продолжительность программы:",
        reply_markup=durations_keyboard(),
    )
    await callback.answer()


async def choose_duration_handler(callback: CallbackQuery, state: FSMContext) -> None:
    days = DURATIONS.get(callback.data.split(":", 1)[1])
    if days is None:
        await callback.answer("Неверный выбор", show_alert=True)
        return
    data = await state.get_data()
    total_price = data["price_per_day"] * days
    await state.update_data(days=days, total_price=total_price, discount_percent=0, discount_amount=0, promo_code=None)
    await state.set_state(OrderForm.entering_promo)
    await callback.message.edit_text(
        f"Программа на <b>{days} дн.</b>\n"
        f"Стоимость: <b>{format_currency(total_price)}</b>\n\n"
        "Хотите применить промокод?",
        reply_markup=promo_keyboard(),
    )
    await callback.answer()


async def promo_callback_handler(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.data.split(":", 1)[1] == "skip":
        await state.set_state(OrderForm.entering_name)
        await callback.message.edit_text("Промокод пропущен.\n\nВведите ваше имя:")
    else:
        await state.set_state(OrderForm.entering_promo)
        await callback.message.edit_text("Введите промокод или напишите «Пропустить»:")
    await callback.answer()


async def promo_text_handler(message: Message, state: FSMContext) -> None:
    text = message.text.strip()
    if text.lower() == "пропустить":
        await state.set_state(OrderForm.entering_name)
        await message.answer("Промокод пропущен.\n\nВведите ваше имя:")
        return
    code = text.upper()
    data = await state.get_data()
    if not get_promo(code):
        await message.answer("Промокод не найден или неактивен.\nВведите другой или напишите «Пропустить».")
        return
    base_total = data["price_per_day"] * data["days"]
    discount_percent, discount_amount = calculate_discount(base_total, code)
    total_price = max(base_total - discount_amount, 0)
    await state.update_data(promo_code=code, discount_percent=discount_percent, discount_amount=discount_amount, total_price=total_price)
    await state.set_state(OrderForm.entering_name)
    await message.answer(
        f"Промокод <b>{h(code)}</b> применён.\n"
        f"Скидка: <b>{discount_percent}%</b> | Итого: <b>{format_currency(total_price)}</b>\n\n"
        "Введите ваше имя:"
    )


async def name_handler(message: Message, state: FSMContext) -> None:
    name = message.text.strip()
    if len(name) < 2:
        await message.answer("Введите корректное имя (минимум 2 символа).")
        return
    await state.update_data(name=name)
    await state.set_state(OrderForm.entering_phone)
    await message.answer("Введите номер телефона:", reply_markup=cancel_keyboard())


async def phone_handler(message: Message, state: FSMContext) -> None:
    phone = message.text.strip()
    if not validate_phone(phone):
        await message.answer("Введите корректный номер телефона.\nПример: +77011234567")
        return
    if is_blacklisted(phone):
        await message.answer("К сожалению, оформление недоступно. Свяжитесь с менеджером.")
        return
    await state.update_data(phone=phone)
    await state.set_state(OrderForm.entering_address)
    await message.answer("Введите адрес доставки:", reply_markup=cancel_keyboard())


async def address_handler(message: Message, state: FSMContext) -> None:
    address = message.text.strip()
    if len(address) < 5:
        await message.answer("Введите более подробный адрес доставки.")
        return
    await state.update_data(address=address)
    await state.set_state(OrderForm.entering_delivery_date)
    await message.answer(
        "Введите дату первой доставки в формате ДД.ММ.ГГГГ\nНапример: 22.04.2026",
        reply_markup=cancel_keyboard(),
    )


async def delivery_date_handler(message: Message, state: FSMContext) -> None:
    date_text = message.text.strip()
    if not validate_delivery_date(date_text):
        await message.answer(
            f"Введите корректную дату (сегодня или не более {MAX_DELIVERY_DAYS_AHEAD} дней вперёд).\n"
            "Пример: 22.04.2026"
        )
        return
    await state.update_data(delivery_date=date_text)
    await state.set_state(OrderForm.choosing_delivery_time)
    await message.answer("Выберите удобное время доставки:", reply_markup=delivery_time_keyboard())


async def delivery_time_handler(callback: CallbackQuery, state: FSMContext) -> None:
    slot = callback.data.split(":", 1)[1]
    await state.update_data(delivery_time=slot)
    await state.set_state(OrderForm.entering_comment)
    await callback.message.edit_text(
        f"Время доставки: <b>{h(slot)}</b>\n\n"
        "Введите комментарий к заказу.\nЕсли комментария нет — напишите: Нет"
    )
    await callback.answer()


async def comment_handler(message: Message, state: FSMContext) -> None:
    await state.update_data(comment=message.text.strip())
    await state.set_state(OrderForm.choosing_payment)
    await message.answer("Выберите способ оплаты:", reply_markup=payment_keyboard())


async def payment_handler(callback: CallbackQuery, state: FSMContext) -> None:
    payment_method = callback.data.split(":", 1)[1]
    order_id = generate_order_id(callback.from_user.id)
    payment_status = "Не требуется"
    status = "Новая"
    if payment_method in ("kaspi_demo", "card_demo"):
        payment_status = "Ожидает оплаты"
        status = "Ожидает оплату"
    await state.update_data(payment_method=payment_method, payment_status=payment_status, status=status, order_id=order_id)
    data = await state.get_data()
    text = build_order_summary(data)
    if payment_method == "kaspi_demo":
        text += "\n\n" + build_kaspi_demo_text(data)
    elif payment_method == "card_demo":
        text += (
            "\n\n<b>💳 Оплата картой (демо)</b>\n\n"
            "Демонстрационный сценарий. Статус оплаты: <b>Ожидает оплаты</b>."
        )
    await state.set_state(OrderForm.confirming_order)
    await callback.message.edit_text(text, reply_markup=confirm_keyboard())
    await callback.answer()


async def confirm_handler(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    action = callback.data.split(":", 1)[1]
    is_admin = admin_only(callback.from_user.id)

    if action == "cancel":
        await state.clear()
        await callback.message.edit_text("Заказ отменён.")
        await bot.send_message(
            callback.from_user.id, "Вы вернулись в главное меню.",
            reply_markup=main_menu_keyboard(is_admin=is_admin),
        )
        await callback.answer()
        return

    if action == "restart":
        await state.clear()
        await state.set_state(OrderForm.choosing_program)
        await callback.message.edit_text(
            "Заполним заявку заново. Выберите программу питания:",
            reply_markup=programs_keyboard(),
        )
        await callback.answer()
        return

    data = await state.get_data()
    required = ["order_id", "program_key", "program_title", "price_per_day",
                "days", "total_price", "name", "phone", "address", "delivery_date", "payment_method"]
    missing = [f for f in required if not data.get(f)]
    if missing:
        logger.error("Отсутствуют поля заказа: %s", missing)
        await state.clear()
        await callback.answer("Ошибка данных. Начните заново.", show_alert=True)
        await bot.send_message(
            callback.from_user.id, "Произошла ошибка. Оформите заказ заново.",
            reply_markup=main_menu_keyboard(is_admin=is_admin),
        )
        return

    db_status = "✅ Заявка сохранена."
    try:
        save_order_to_db(data, callback.from_user)
        upsert_client(data, callback.from_user)
    except Exception as e:
        logger.exception("Ошибка сохранения заказа: %s", e)
        db_status = "⚠️ Не удалось сохранить заявку."

    admin_status = "⚠️ Не удалось уведомить администратора."
    try:
        await bot.send_message(
            ADMIN_TELEGRAM_ID,
            build_admin_order_text(data, callback.from_user),
            reply_markup=admin_status_keyboard(data["order_id"]),
        )
        admin_status = "✅ Заявка отправлена администратору."
    except Exception as e:
        logger.exception("Ошибка уведомления администратора: %s", e)

    await callback.message.edit_text(
        "<b>Спасибо! Ваш заказ принят.</b>\n\n"
        f"Номер заказа: <b>{h(data['order_id'])}</b>\n"
        f"Статус: <b>{h(data.get('status', 'Новая'))}</b>\n"
        f"Статус оплаты: <b>{h(data.get('payment_status', '—'))}</b>\n\n"
        f"{admin_status}\n{db_status}"
    )
    await bot.send_message(
        callback.from_user.id,
        "Если есть вопросы — напишите менеджеру:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="💬 WhatsApp менеджер", url=MANAGER_WHATSAPP)
        ]]),
    )
    await bot.send_message(
        callback.from_user.id, "Главное меню:",
        reply_markup=main_menu_keyboard(is_admin=is_admin),
    )
    await state.clear()
    await callback.answer()


async def review_callback_handler(callback: CallbackQuery, state: FSMContext) -> None:
    parts = callback.data.split(":", 2)
    if len(parts) != 3:
        await callback.answer("Ошибка данных", show_alert=True)
        return
    _, order_id, rating_str = parts
    try:
        rating = int(rating_str)
        if not 1 <= rating <= 5:
            raise ValueError
    except ValueError:
        await callback.answer("Некорректная оценка", show_alert=True)
        return
    await state.set_state(ReviewForm.entering_review)
    await state.update_data(review_order_id=order_id, review_rating=rating)
    await callback.message.edit_text(
        f"Спасибо! Оценка <b>{rating}/5</b>.\n\nНапишите короткий отзыв (или «Нет»):"
    )
    await callback.answer()


async def review_text_handler(message: Message, state: FSMContext, bot: Bot) -> None:
    data = await state.get_data()
    order_id    = data.get("review_order_id")
    rating      = data.get("review_rating")
    review_text = message.text.strip()
    order = get_order_by_order_id(order_id) if order_id else None
    full_name = order["full_name"] if order else (message.from_user.full_name or "Клиент")

    add_review(order_id, message.from_user.id, full_name, rating,
               "" if review_text.lower() == "нет" else review_text)

    await message.answer(
        "Спасибо за отзыв! 💚",
        reply_markup=main_menu_keyboard(is_admin=admin_only(message.from_user.id)),
    )
    try:
        await bot.send_message(
            ADMIN_TELEGRAM_ID,
            f"<b>⭐ Новый отзыв</b>\n\n"
            f"Заказ: <b>{h(order_id)}</b>\n"
            f"Клиент: {h(full_name)}\n"
            f"Оценка: <b>{'⭐' * rating} ({rating}/5)</b>\n"
            f"Отзыв: {h(review_text)}",
        )
    except Exception:
        pass
    await state.clear()


# ──────────────────────────────────────────────────────────────
# ОБРАБОТЧИКИ — АДМИНИСТРАТОР
# ──────────────────────────────────────────────────────────────
async def admin_panel_handler(message: Message) -> None:
    if not admin_only(message.from_user.id):
        await message.answer("У вас нет доступа к этому разделу.")
        return
    stats = get_global_stats()
    await message.answer(
        "<b>👑 Админ панель FitDaily</b>\n\n"
        f"📦 Всего заказов: <b>{stats['total_orders']}</b>\n"
        f"💰 Общая выручка: <b>{format_currency(stats['total_revenue'])}</b>\n"
        f"👥 Клиентов: <b>{stats['total_clients']}</b>\n"
        f"⭐ Средняя оценка: <b>{stats['avg_rating']}</b>\n\n"
        "Выберите действие:",
        reply_markup=admin_main_keyboard(),
    )


async def admin_help_handler(message: Message) -> None:
    if not admin_only(message.from_user.id):
        return
    await message.answer(
        "<b>Команды администратора</b>\n\n"
        "/orders_today — статистика за сегодня\n"
        "/orders_week — статистика за 7 дней\n"
        "/recent_orders — последние 10 заказов\n"
        "/clients — последние клиенты\n"
        "/segments — сегменты клиентов\n"
        "/reviews — последние отзывы\n"
        "/deliveries_today — доставки на сегодня\n"
        "/deliveries_tomorrow — доставки на завтра\n"
        "/find_client +7701... — поиск по номеру\n"
        "/export_clients — CSV клиентов\n"
        "/export_orders — CSV заказов\n"
        "/note_phone +7701... текст — заметка\n"
        "/blacklist_phone +7701... причина — ЧС\n"
        "/unblacklist_phone +7701... — убрать из ЧС\n"
        "/order FD-... — подробности заказа\n"
        "/set_status FD-... Статус — сменить статус\n"
        "/add_promo КОД 15 Описание — добавить промокод\n"
        "/deactivate_promo КОД — отключить промокод\n"
        "/broadcast Текст — рассылка всем клиентам"
    )


async def orders_today_handler(message: Message) -> None:
    if not admin_only(message.from_user.id):
        return
    start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    await message.answer(build_stats_text("📊 За сегодня", get_orders_between(start, start + timedelta(days=1))))


async def orders_week_handler(message: Message) -> None:
    if not admin_only(message.from_user.id):
        return
    end = datetime.now() + timedelta(seconds=1)
    await message.answer(build_stats_text("📅 За 7 дней", get_orders_between(end - timedelta(days=7), end)))


async def recent_orders_handler(message: Message) -> None:
    if not admin_only(message.from_user.id):
        return
    await message.answer(build_orders_text("🧾 Последние 10 заказов", get_recent_orders(10)))


async def clients_handler(message: Message) -> None:
    if not admin_only(message.from_user.id):
        return
    clients = get_recent_clients(20)
    if not clients:
        await message.answer("Клиентов пока нет.")
        return
    text = build_clients_text(clients)
    await message.answer(text[:4000] + ("\n\n..." if len(text) > 4000 else ""))


async def segments_handler(message: Message) -> None:
    if not admin_only(message.from_user.id):
        return
    await message.answer(build_segments_text())


async def reviews_handler(message: Message) -> None:
    if not admin_only(message.from_user.id):
        return
    await message.answer(build_reviews_text(get_reviews(20)))


async def deliveries_today_handler(message: Message) -> None:
    if not admin_only(message.from_user.id):
        return
    date_text = datetime.now().strftime("%d.%m.%Y")
    await message.answer(build_orders_text(f"🚚 Доставки на {date_text}", get_orders_by_delivery_date(date_text)))


async def deliveries_tomorrow_handler(message: Message) -> None:
    if not admin_only(message.from_user.id):
        return
    date_text = (datetime.now() + timedelta(days=1)).strftime("%d.%m.%Y")
    await message.answer(build_orders_text(f"🗓 Доставки на {date_text}", get_orders_by_delivery_date(date_text)))


async def export_clients_handler(message: Message) -> None:
    if not admin_only(message.from_user.id):
        return
    path = "clients_export.csv"
    export_clients_to_csv(path)
    await _send_and_delete_csv(message, path, "Клиентская база")


async def export_orders_handler(message: Message) -> None:
    """Новая функция: выгрузка всех заказов в CSV."""
    if not admin_only(message.from_user.id):
        return
    path = "orders_export.csv"
    export_orders_to_csv(path)
    await _send_and_delete_csv(message, path, "Выгрузка заказов")


async def find_client_handler(message: Message) -> None:
    if not admin_only(message.from_user.id):
        return
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /find_client +77011234567")
        return
    clients = get_clients_by_phone(parts[1].strip())
    if not clients:
        await message.answer("Клиент не найден.")
        return
    lines = ["<b>🔎 Результаты поиска</b>\n"]
    for c in clients:
        lines.append(
            f"<b>{h(c['full_name'])}</b> | 📞 {h(c['phone'])}\n"
            f"📦 Заказов: {c['total_orders']} | 💰 {format_currency(c['total_spent'])}\n"
            f"🥗 {h(c['favorite_program'] or '—')} | Сегмент: {client_segment(c)}\n"
        )
        for note in get_client_notes(c["phone"])[:3]:
            lines.append(f"📝 {h(note['note_text'])} ({h(note['created_at'])})")
        lines.append("")
    await message.answer("\n".join(lines))


async def note_phone_handler(message: Message) -> None:
    if not admin_only(message.from_user.id):
        return
    parts = message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Использование: /note_phone +77011234567 Текст заметки")
        return
    add_client_note(parts[1], parts[2])
    await message.answer("Заметка сохранена.")


async def blacklist_phone_handler(message: Message) -> None:
    if not admin_only(message.from_user.id):
        return
    parts = message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Использование: /blacklist_phone +77011234567 Причина")
        return
    add_to_blacklist(parts[1], parts[2])
    await message.answer(f"Номер {h(parts[1])} добавлен в чёрный список.")


async def unblacklist_phone_handler(message: Message) -> None:
    """Новая функция: убрать номер из чёрного списка."""
    if not admin_only(message.from_user.id):
        return
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /unblacklist_phone +77011234567")
        return
    if remove_from_blacklist(parts[1].strip()):
        await message.answer(f"Номер {h(parts[1])} удалён из чёрного списка.")
    else:
        await message.answer("Номер не найден в чёрном списке.")


async def order_detail_handler(message: Message) -> None:
    if not admin_only(message.from_user.id):
        return
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /order FD-123")
        return
    order = get_order_by_order_id(parts[1].strip())
    if not order:
        await message.answer("Заказ не найден.")
        return
    payment_label = PAYMENT_METHODS.get(order["payment_method"], order["payment_method"] or "—")
    await message.answer(
        f"<b>Заказ {h(order['order_id'])}</b>\n\n"
        f"📌 Статус: <b>{h(order['status'])}</b>\n"
        f"📌 Оплата: <b>{h(order['payment_status'])}</b>\n"
        f"💳 Способ: <b>{h(payment_label)}</b>\n"
        f"👤 {h(order['full_name'])} | 📞 {h(order['phone'])}\n"
        f"📍 {h(order['address'])}\n"
        f"🗓 {h(order['delivery_date'])} {h(order['delivery_time'] or '')}\n"
        f"🥗 {h(order['program_title'])} — {order['days']} дн.\n"
        f"🎁 Промокод: {h(order['promo_code'] or 'нет')}\n"
        f"📉 Скидка: {format_currency(order['discount_amount'] or 0)}\n"
        f"💳 Итого: {format_currency(order['total_price'])}\n"
        f"📝 {h(order['comment'] or '—')}\n"
        f"🕒 Создан: {h(order['created_at'])}",
        reply_markup=admin_status_keyboard(order["order_id"]),
    )


async def set_status_handler(message: Message, bot: Bot) -> None:
    if not admin_only(message.from_user.id):
        return
    parts = message.text.strip().split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Использование: /set_status FD-123 Доставлена")
        return
    order_id, new_status = parts[1].strip(), parts[2].strip()
    if new_status not in STATUSES:
        await message.answer(f"Недопустимый статус.\nДопустимые: {', '.join(STATUSES)}")
        return
    if not update_order_status(order_id, new_status):
        await message.answer("Заказ не найден.")
        return
    order = get_order_by_order_id(order_id)
    if order:
        await notify_user_about_status(bot, order, new_status)
    await message.answer(f"Статус <b>{h(order_id)}</b> → <b>{h(new_status)}</b>")


async def add_promo_handler(message: Message) -> None:
    """Новая функция: добавление нового промокода."""
    if not admin_only(message.from_user.id):
        return
    parts = message.text.strip().split(maxsplit=3)
    if len(parts) < 4:
        await message.answer("Использование: /add_promo КОД 15 Описание")
        return
    code, percent_str, description = parts[1], parts[2], parts[3]
    try:
        percent = int(percent_str)
        if not 1 <= percent <= 100:
            raise ValueError
    except ValueError:
        await message.answer("Процент скидки должен быть числом от 1 до 100.")
        return
    if add_promo_code(code, percent, description):
        await message.answer(f"Промокод <b>{h(code.upper())}</b> ({percent}%) добавлен.")
    else:
        await message.answer("Не удалось добавить промокод.")


async def deactivate_promo_handler(message: Message) -> None:
    """Новая функция: отключение промокода."""
    if not admin_only(message.from_user.id):
        return
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /deactivate_promo КОД")
        return
    if deactivate_promo_code(parts[1].strip()):
        await message.answer(f"Промокод <b>{h(parts[1].upper())}</b> отключён.")
    else:
        await message.answer("Промокод не найден.")


async def broadcast_start_handler(message: Message, state: FSMContext) -> None:
    """Новая функция: рассылка сообщения всем клиентам."""
    if not admin_only(message.from_user.id):
        return
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /broadcast Текст сообщения")
        return
    broadcast_text = parts[1].strip()
    client_ids = get_all_client_ids()
    if not client_ids:
        await message.answer("Нет клиентов для рассылки.")
        return
    await message.answer(f"Начинаю рассылку для {len(client_ids)} клиентов...")
    sent, failed = 0, 0
    bot: Bot = message.bot
    for uid in client_ids:
        try:
            await bot.send_message(uid, f"📢 <b>Сообщение от {BRAND_NAME}</b>\n\n{broadcast_text}")
            sent += 1
            await asyncio.sleep(0.05)  # соблюдаем лимиты Telegram
        except Exception:
            failed += 1
    await message.answer(f"Рассылка завершена.\n✅ Отправлено: {sent}\n❌ Не доставлено: {failed}")


async def admin_callback_handler(callback: CallbackQuery) -> None:
    if not admin_only(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    parts  = callback.data.split(":", 2)
    action = parts[1] if len(parts) > 1 else ""

    if action == "sf":
        code   = parts[2] if len(parts) > 2 else ""
        status = STATUS_CODES.get(code, "")
        await callback.message.edit_text(
            build_orders_text(f"📌 Заказы: {status}", get_orders_by_status(status, 20)),
            reply_markup=admin_main_keyboard(),
        )
        await callback.answer()
        return

    if action == "today":
        start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        await callback.message.edit_text(
            build_stats_text("📊 За сегодня", get_orders_between(start, start + timedelta(days=1))),
            reply_markup=admin_main_keyboard(),
        )
        await callback.answer()
        return

    if action == "week":
        end = datetime.now() + timedelta(seconds=1)
        await callback.message.edit_text(
            build_stats_text("📅 За 7 дней", get_orders_between(end - timedelta(days=7), end)),
            reply_markup=admin_main_keyboard(),
        )
        await callback.answer()
        return

    if action == "recent":
        await callback.message.edit_text(
            build_orders_text("🧾 Последние заказы", get_recent_orders(10)),
            reply_markup=admin_main_keyboard(),
        )
        await callback.answer()
        return

    if action == "clients":
        clients = get_recent_clients(20)
        text = build_clients_text(clients) if clients else "Клиентов пока нет."
        await callback.message.edit_text(
            text[:3900] + ("\n\n..." if len(text) > 3900 else ""),
            reply_markup=admin_main_keyboard(),
        )
        await callback.answer()
        return

    if action == "del_today":
        date_text = datetime.now().strftime("%d.%m.%Y")
        await callback.message.edit_text(
            build_orders_text(f"🚚 Доставки на {date_text}", get_orders_by_delivery_date(date_text)),
            reply_markup=admin_main_keyboard(),
        )
        await callback.answer()
        return

    if action == "del_tomorrow":
        date_text = (datetime.now() + timedelta(days=1)).strftime("%d.%m.%Y")
        await callback.message.edit_text(
            build_orders_text(f"🗓 Доставки на {date_text}", get_orders_by_delivery_date(date_text)),
            reply_markup=admin_main_keyboard(),
        )
        await callback.answer()
        return

    if action == "reviews":
        await callback.message.edit_text(
            build_reviews_text(get_reviews(20)),
            reply_markup=admin_main_keyboard(),
        )
        await callback.answer()
        return

    if action == "segments":
        await callback.message.edit_text(
            build_segments_text(),
            reply_markup=admin_main_keyboard(),
        )
        await callback.answer()
        return

    if action == "promos":
        await callback.message.edit_text(
            build_admin_promos_text(),
            reply_markup=admin_main_keyboard(),
        )
        await callback.answer()
        return

    await callback.answer()


async def setstatus_callback_handler(callback: CallbackQuery, bot: Bot) -> None:
    if not admin_only(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    parts = callback.data.split(":", 2)
    if len(parts) != 3:
        await callback.answer("Некорректный формат", show_alert=True)
        return
    _, order_id, status_code = parts
    new_status = STATUS_CODES.get(status_code)
    if not new_status:
        await callback.answer("Некорректный статус", show_alert=True)
        return
    if not update_order_status(order_id, new_status):
        await callback.answer("Заказ не найден", show_alert=True)
        return
    order = get_order_by_order_id(order_id)
    if not order:
        await callback.answer(f"Статус: {new_status}")
        return
    payment_label = PAYMENT_METHODS.get(order["payment_method"], order["payment_method"] or "—")
    await callback.message.edit_text(
        f"<b>Заказ {h(order['order_id'])}</b>\n\n"
        f"📌 Статус: <b>{h(order['status'])}</b>\n"
        f"📌 Оплата: <b>{h(order['payment_status'])}</b>\n"
        f"💸 {h(payment_label)}\n"
        f"👤 {h(order['full_name'])} | 📞 {h(order['phone'])}\n"
        f"📍 {h(order['address'])}\n"
        f"🗓 {h(order['delivery_date'])} {h(order['delivery_time'] or '')}\n"
        f"🥗 {h(order['program_title'])} — {order['days']} дн.\n"
        f"💳 {format_currency(order['total_price'])}\n"
        f"📝 {h(order['comment'] or '—')}",
        reply_markup=admin_status_keyboard(order_id),
    )
    await notify_user_about_status(bot, order, new_status)
    await callback.answer(f"Статус: {new_status}")


# ──────────────────────────────────────────────────────────────
# FALLBACK
# ──────────────────────────────────────────────────────────────
async def fallback_handler(message: Message, state: FSMContext) -> None:
    current_state = await state.get_state()
    if current_state is None:
        await message.answer(
            "Я не понял сообщение. Воспользуйтесь меню ниже.",
            reply_markup=main_menu_keyboard(is_admin=admin_only(message.from_user.id)),
        )
    # Если FSM активен — молча ждём нужного ввода


# ──────────────────────────────────────────────────────────────
# ЗАПУСК
# ──────────────────────────────────────────────────────────────
async def main() -> None:
    init_db()
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp  = Dispatcher(storage=MemoryStorage())

    # ── Кнопки главного меню ──────────────────────────────────
    dp.message.register(start_handler,          CommandStart())
    dp.message.register(order_start_handler,    F.text == "🛒 Оформить заказ")
    dp.message.register(repeat_order_handler,   F.text == "🔁 Повторить заказ")
    dp.message.register(my_orders_handler,      F.text == "📋 Мои заказы")
    dp.message.register(programs_handler,        F.text == "🥗 Программы питания")
    dp.message.register(current_menu_handler,   F.text == "🍽 Актуальное меню")
    dp.message.register(stop_list_handler,       F.text == "⛔ Стоп-лист")
    dp.message.register(promo_list_handler,      F.text == "🎁 Промокоды")
    dp.message.register(prices_handler,          F.text == "💸 Цены")
    dp.message.register(delivery_handler,        F.text == "🚚 Доставка")
    dp.message.register(faq_handler,             F.text == "❓ Вопросы")
    dp.message.register(review_start_handler,   F.text == "⭐ Оставить отзыв")
    dp.message.register(about_handler,           F.text == "ℹ️ О нас")
    dp.message.register(manager_handler,         F.text == "💬 Менеджер")
    dp.message.register(admin_panel_handler,    F.text == "👑 Админ панель")

    # ── FSM шаги оформления заказа ────────────────────────────
    dp.message.register(promo_text_handler,     OrderForm.entering_promo)
    dp.message.register(name_handler,           OrderForm.entering_name)
    dp.message.register(phone_handler,          OrderForm.entering_phone)
    dp.message.register(address_handler,        OrderForm.entering_address)
    dp.message.register(delivery_date_handler,  OrderForm.entering_delivery_date)
    dp.message.register(comment_handler,        OrderForm.entering_comment)
    dp.message.register(review_text_handler,    ReviewForm.entering_review)

    # ── Команды администратора ────────────────────────────────
    dp.message.register(cancel_command_handler,     Command("cancel"))
    dp.message.register(admin_help_handler,          Command("help_admin"))
    dp.message.register(orders_today_handler,        Command("orders_today"))
    dp.message.register(orders_week_handler,         Command("orders_week"))
    dp.message.register(recent_orders_handler,       Command("recent_orders"))
    dp.message.register(clients_handler,             Command("clients"))
    dp.message.register(segments_handler,            Command("segments"))
    dp.message.register(reviews_handler,             Command("reviews"))
    dp.message.register(deliveries_today_handler,    Command("deliveries_today"))
    dp.message.register(deliveries_tomorrow_handler, Command("deliveries_tomorrow"))
    dp.message.register(export_clients_handler,      Command("export_clients"))
    dp.message.register(export_orders_handler,       Command("export_orders"))
    dp.message.register(find_client_handler,         Command("find_client"))
    dp.message.register(note_phone_handler,          Command("note_phone"))
    dp.message.register(blacklist_phone_handler,     Command("blacklist_phone"))
    dp.message.register(unblacklist_phone_handler,   Command("unblacklist_phone"))
    dp.message.register(order_detail_handler,        Command("order"))
    dp.message.register(set_status_handler,          Command("set_status"))
    dp.message.register(add_promo_handler,           Command("add_promo"))
    dp.message.register(deactivate_promo_handler,    Command("deactivate_promo"))
    dp.message.register(broadcast_start_handler,     Command("broadcast"))

    # ── Callback-хендлеры ─────────────────────────────────────
    dp.callback_query.register(cancel_flow_callback,      F.data == "order:cancel_flow")
    dp.callback_query.register(program_view_handler,       F.data.startswith("program_view:"))
    dp.callback_query.register(programs_back_handler,      F.data == "programs:back")
    dp.callback_query.register(choose_program_handler,     F.data.startswith("program_select:"))
    dp.callback_query.register(choose_duration_handler,    OrderForm.choosing_duration, F.data.startswith("duration:"))
    dp.callback_query.register(promo_callback_handler,     F.data.startswith("promo:"))
    dp.callback_query.register(delivery_time_handler,      OrderForm.choosing_delivery_time, F.data.startswith("dtime:"))
    dp.callback_query.register(payment_handler,            OrderForm.choosing_payment, F.data.startswith("payment:"))
    dp.callback_query.register(confirm_handler,            OrderForm.confirming_order, F.data.startswith("confirm:"))
    dp.callback_query.register(admin_callback_handler,     F.data.startswith("admin:"))
    dp.callback_query.register(setstatus_callback_handler, F.data.startswith("ss:"))
    dp.callback_query.register(review_callback_handler,    F.data.startswith("rv:"))

    # ── Fallback (последний!) ─────────────────────────────────
    dp.message.register(fallback_handler)

    logger.info("Бот запущен. Admin ID: %d", ADMIN_TELEGRAM_ID)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
