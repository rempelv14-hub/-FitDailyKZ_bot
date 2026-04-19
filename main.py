
import asyncio
import logging
import os
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

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
BOT_TOKEN = os.getenv("BOT_TOKEN", "8638726144:AAEomAz3f9LVonnSl15zjKYYxfAKgh9Fq2E")
ADMIN_TELEGRAM_ID = int(os.getenv("ADMIN_TELEGRAM_ID", "6954213997"))
DATABASE_PATH = os.getenv("DATABASE_PATH", "fitdaily.db")

BRAND_NAME = "FitDaily"
MANAGER_PHONE = "+77712841932"
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
    "<b>FitDaily</b> — стильный сервис доставки правильного питания.\n\n"
    "🥗 Удобный выбор программы\n"
    "🚚 Доставка в выбранный интервал\n"
    "💬 Быстрая связь с менеджером\n"
    "✨ Красивое и понятное оформление заказа прямо в Telegram"
)

DELIVERY_INFO = (
    "<b>Доставка FitDaily</b>\n\n"
    "Мы доставляем заказы по выбранному вами адресу.\n"
    "Вы сами выбираете удобное время доставки при оформлении.\n\n"
    "После создания заказа менеджер подтверждает детали вручную."
)

PROGRAMS = {
    "slim": {
        "title": "🥬 Slim Start",
        "description": "Лёгкая программа для комфортного снижения калорийности и мягкого входа в режим.",
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
        "description": "Универсальный рацион на каждый день для стабильного и вкусного питания.",
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
        "description": "Белковая программа для активных людей и тренировочных дней.",
        "price_per_day": 7900,
        "calories": "1700–2200 ккал",
        "goal": "Спорт, сытость и восстановление",
        "includes": [
            "Повышенный белок",
            "Плотные и сытные порции",
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
        "description": "Лёгкая выдуманная программа для разгрузочных и свежих дней.",
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

DURATIONS = {"1": 1, "5": 5, "7": 7, "14": 14, "30": 30}
STATUSES = ["Новая", "Подтверждена", "Готовится", "Передана в доставку", "Доставлена", "Отменена"]

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
    entering_name = State()
    entering_phone = State()
    entering_address = State()
    entering_delivery_date = State()
    choosing_delivery_time = State()
    entering_comment = State()
    confirming_order = State()


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
                comment TEXT,
                status TEXT NOT NULL DEFAULT 'Новая'
            )
            """
        )

        columns = [row["name"] for row in conn.execute("PRAGMA table_info(orders)").fetchall()]
        if "delivery_time" not in columns:
            conn.execute("ALTER TABLE orders ADD COLUMN delivery_time TEXT")

        conn.commit()


def save_order_to_db(order_data: Dict[str, Any], telegram_user) -> None:
    with closing(get_connection()) as conn:
        conn.execute(
            """
            INSERT INTO orders (
                order_id, created_at, telegram_user_id, telegram_username, full_name,
                phone, address, delivery_date, delivery_time, program_key, program_title, days,
                price_per_day, total_price, comment, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                order_data["comment"],
                "Новая",
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
        return conn.execute(
            "SELECT * FROM orders WHERE order_id = ?",
            (order_id,),
        ).fetchone()


def get_orders_between(start_dt: datetime, end_dt: datetime) -> List[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute(
            "SELECT * FROM orders WHERE created_at >= ? AND created_at < ? ORDER BY created_at DESC",
            (start_dt.strftime("%Y-%m-%d %H:%M:%S"), end_dt.strftime("%Y-%m-%d %H:%M:%S")),
        ).fetchall()


def get_recent_orders(limit: int = 10) -> List[sqlite3.Row]:
    with closing(get_connection()) as conn:
        return conn.execute(
            "SELECT * FROM orders ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()


# ======================================
# KEYBOARDS
# ======================================
def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🛒 Оформить заказ")],
            [KeyboardButton(text="🥗 Программы питания"), KeyboardButton(text="💸 Цены")],
            [KeyboardButton(text="🚚 Доставка"), KeyboardButton(text="ℹ️ О нас")],
            [KeyboardButton(text="💬 Менеджер"), KeyboardButton(text="👑 Админ панель")],
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


def delivery_time_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for slot in DELIVERY_TIME_SLOTS:
        rows.append([InlineKeyboardButton(text=slot, callback_data=f"delivery_time:{slot}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


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
            [InlineKeyboardButton(text="🧾 Последние 10 заказов", callback_data="admin:recent")],
            [InlineKeyboardButton(text="💬 Менеджер WhatsApp", url=MANAGER_WHATSAPP)],
        ]
    )


def admin_status_keyboard(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Подтверждена", callback_data=f"status:{order_id}:Подтверждена"),
                InlineKeyboardButton(text="👨‍🍳 Готовится", callback_data=f"status:{order_id}:Готовится"),
            ],
            [
                InlineKeyboardButton(text="🚚 В доставке", callback_data=f"status:{order_id}:Передана в доставку"),
                InlineKeyboardButton(text="🎉 Доставлена", callback_data=f"status:{order_id}:Доставлена"),
            ],
            [InlineKeyboardButton(text="❌ Отменена", callback_data=f"status:{order_id}:Отменена")],
            [InlineKeyboardButton(text="💬 Менеджер WhatsApp", url=MANAGER_WHATSAPP)],
        ]
    )


# ======================================
# HELPERS
# ======================================
def format_currency(value: int) -> str:
    return f"{value:,} ₸".replace(",", " ")


def generate_order_id(user_id: int) -> str:
    return f"FD-{user_id}-{datetime.now().strftime('%d%m%H%M%S')}"


def validate_phone(phone: str) -> bool:
    cleaned = re.sub(r"[^\d+]", "", phone)
    digits = re.sub(r"\D", "", cleaned)
    return 10 <= len(digits) <= 15


def validate_delivery_date(date_text: str) -> bool:
    try:
        entered = datetime.strptime(date_text, "%d.%m.%Y")
        return entered.date() >= datetime.now().date()
    except ValueError:
        return False


def build_prices_text() -> str:
    lines = ["<b>💸 Цены на программы</b>\n"]
    for item in PROGRAMS.values():
        lines.append(f"• {item['title']} — <b>{format_currency(item['price_per_day'])}</b> / день")
    lines.append("\nИтоговая стоимость рассчитывается автоматически.")
    return "\n".join(lines)


def build_order_summary(data: Dict[str, Any]) -> str:
    return (
        "<b>✨ Проверьте ваш заказ</b>\n\n"
        f"🆔 Номер: <b>{data['order_id']}</b>\n"
        f"🥗 Программа: <b>{data['program_title']}</b>\n"
        f"📆 Срок: <b>{data['days']} дн.</b>\n"
        f"💰 Цена в день: <b>{format_currency(data['price_per_day'])}</b>\n"
        f"💳 Итого: <b>{format_currency(data['total_price'])}</b>\n\n"
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
        f"📌 Статус: <b>Новая</b>\n"
        f"👤 Клиент: {data['name']}\n"
        f"📞 Телефон: {data['phone']}\n"
        f"📍 Адрес: {data['address']}\n"
        f"🗓 Дата: {data['delivery_date']}\n"
        f"⏰ Время: {data['delivery_time']}\n"
        f"🥗 Программа: {data['program_title']}\n"
        f"📆 Срок: {data['days']} дн.\n"
        f"💰 Цена в день: {format_currency(data['price_per_day'])}\n"
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


def admin_only(user_id: int) -> bool:
    return user_id == ADMIN_TELEGRAM_ID


# ======================================
# USER HANDLERS
# ======================================
async def start_handler(message: Message, state: FSMContext):
    await state.clear()
    text = (
        f"Здравствуйте! Добро пожаловать в <b>{BRAND_NAME}</b> 🥗\n\n"
        "Выберите нужный раздел в меню ниже.\n"
        "Оформление заказа сделано красиво и максимально просто ✨"
    )
    await message.answer(text, reply_markup=main_menu_keyboard())


async def programs_handler(message: Message):
    await message.answer(
        "<b>🥗 Программы питания FitDaily</b>\n\n"
        "Нажмите на программу, чтобы посмотреть подробности.",
        reply_markup=programs_keyboard(),
    )


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
        f"{MANAGER_CONTACT_TEXT}\n"
        f"Номер: <code>{MANAGER_PHONE}</code>\n\n"
        "Нажмите кнопку ниже, чтобы открыть WhatsApp.",
        reply_markup=kb,
    )


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
        "<b>🥗 Программы питания FitDaily</b>\n\n"
        "Нажмите на программу, чтобы посмотреть подробности.",
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

    await state.update_data(days=days, total_price=total_price)
    await state.set_state(OrderForm.entering_name)

    await callback.message.edit_text(
        f"Вы выбрали программу на <b>{days} дн.</b>\n"
        f"Предварительная стоимость: <b>{format_currency(total_price)}</b>\n\n"
        "Введите ваше имя:"
    )
    await callback.answer()


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
    await message.answer(
        "Выберите удобное время доставки:",
        reply_markup=delivery_time_keyboard(),
    )


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
    order_id = generate_order_id(message.from_user.id)
    await state.update_data(comment=comment, order_id=order_id)
    updated_data = await state.get_data()

    await state.set_state(OrderForm.confirming_order)
    await message.answer(build_order_summary(updated_data), reply_markup=confirm_keyboard())


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
        await callback.message.edit_text(
            "Заполним заявку заново. Выберите программу питания:",
            reply_markup=programs_keyboard(),
        )
        await callback.answer()
        return

    data = await state.get_data()

    db_status = "✅ Заявка сохранена в базе данных."
    try:
        save_order_to_db(data, callback.from_user)
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
        inline_keyboard=[
            [InlineKeyboardButton(text="💬 Написать менеджеру в WhatsApp", url=MANAGER_WHATSAPP)]
        ]
    )

    await callback.message.edit_text(
        "<b>Спасибо! Ваш заказ принят.</b>\n\n"
        f"Номер заказа: <b>{data['order_id']}</b>\n"
        "Менеджер свяжется с вами для подтверждения деталей.\n\n"
        f"{admin_status}\n{db_status}"
    )
    await bot.send_message(
        callback.from_user.id,
        "Для быстрой связи можете сразу написать менеджеру:",
        reply_markup=manager_kb,
    )
    await bot.send_message(callback.from_user.id, "Главное меню открыто снова.", reply_markup=main_menu_keyboard())
    await state.clear()
    await callback.answer()


# ======================================
# ADMIN PANEL
# ======================================
async def admin_panel_handler(message: Message):
    if not admin_only(message.from_user.id):
        await message.answer("У вас нет доступа к админ панели.")
        return

    await message.answer(
        "<b>👑 Админ панель FitDaily</b>\n\n"
        "Добро пожаловать в панель управления.\n"
        "Выберите нужное действие ниже:",
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

    orders = get_recent_orders(10)
    if not orders:
        await message.answer("Заказов пока нет.")
        return

    lines = ["<b>🧾 Последние 10 заказов</b>"]
    for order in orders:
        lines.append(
            f"\n<b>{order['order_id']}</b>\n"
            f"👤 {order['full_name']}\n"
            f"🥗 {order['program_title']}\n"
            f"📌 {order['status']}\n"
            f"💳 {format_currency(order['total_price'])}\n"
            f"🕒 {order['created_at']}"
        )
    await message.answer("\n".join(lines))


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
        f"👤 Клиент: {order['full_name']}\n"
        f"📞 Телефон: {order['phone']}\n"
        f"📍 Адрес: {order['address']}\n"
        f"🗓 Дата доставки: {order['delivery_date']}\n"
        f"⏰ Время доставки: {order['delivery_time'] or 'не указано'}\n"
        f"🥗 Программа: {order['program_title']}\n"
        f"📆 Срок: {order['days']} дн.\n"
        f"💳 Итого: {format_currency(order['total_price'])}\n"
        f"📝 Комментарий: {order['comment']}\n"
        f"🕒 Создан: {order['created_at']}"
    )
    await message.answer(text, reply_markup=admin_status_keyboard(order['order_id']))


async def set_status_handler(message: Message):
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

    await message.answer(f"Статус заказа <b>{order_id}</b> обновлён: <b>{new_status}</b>")


async def admin_callback_handler(callback: CallbackQuery):
    if not admin_only(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    action = callback.data.split(":", 1)[1]

    if action == "today":
        start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        orders = get_orders_between(start, end)
        await callback.message.edit_text(
            build_stats_text("📊 Статистика за сегодня", orders),
            reply_markup=admin_main_keyboard(),
        )

    elif action == "week":
        end = datetime.now() + timedelta(seconds=1)
        start = datetime.now() - timedelta(days=7)
        orders = get_orders_between(start, end)
        await callback.message.edit_text(
            build_stats_text("📅 Статистика за 7 дней", orders),
            reply_markup=admin_main_keyboard(),
        )

    elif action == "recent":
        orders = get_recent_orders(10)
        if not orders:
            await callback.message.edit_text("Заказов пока нет.", reply_markup=admin_main_keyboard())
        else:
            lines = ["<b>🧾 Последние 10 заказов</b>\n"]
            for order in orders:
                lines.append(
                    f"\n<b>{order['order_id']}</b>\n"
                    f"👤 {order['full_name']}\n"
                    f"🥗 {order['program_title']}\n"
                    f"📌 {order['status']}\n"
                    f"💳 {format_currency(order['total_price'])}\n"
                    f"🕒 {order['created_at']}"
                )
            text = "\n".join(lines)
            if len(text) > 3900:
                text = text[:3900] + "\n\n..."
            await callback.message.edit_text(text, reply_markup=admin_main_keyboard())

    await callback.answer()


async def status_callback_handler(callback: CallbackQuery):
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

    await callback.answer(f"Статус обновлён: {new_status}")


# ======================================
# FALLBACK
# ======================================
async def fallback_handler(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.answer(
            "Я не понял сообщение. Выберите нужный раздел через меню ниже.",
            reply_markup=main_menu_keyboard(),
        )


# ======================================
# MAIN
# ======================================
async def main():
    if BOT_TOKEN == "PASTE_YOUR_BOT_TOKEN":
        raise ValueError("Укажите BOT_TOKEN в .env или в конфиге.")

    init_db()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())

    # User
    dp.message.register(start_handler, CommandStart())
    dp.message.register(order_start_handler, F.text == "🛒 Оформить заказ")
    dp.message.register(programs_handler, F.text == "🥗 Программы питания")
    dp.message.register(prices_handler, F.text == "💸 Цены")
    dp.message.register(delivery_handler, F.text == "🚚 Доставка")
    dp.message.register(about_handler, F.text == "ℹ️ О нас")
    dp.message.register(manager_handler, F.text == "💬 Менеджер")
    dp.message.register(admin_panel_handler, F.text == "👑 Админ панель")

    dp.callback_query.register(program_view_handler, F.data.startswith("program_view:"))
    dp.callback_query.register(programs_back_handler, F.data == "programs:back")
    dp.callback_query.register(choose_program_handler, F.data.startswith("program_select:"))
    dp.callback_query.register(choose_duration_handler, OrderForm.choosing_duration, F.data.startswith("duration:"))
    dp.callback_query.register(delivery_time_handler, OrderForm.choosing_delivery_time, F.data.startswith("delivery_time:"))
    dp.callback_query.register(confirm_handler, OrderForm.confirming_order, F.data.startswith("confirm:"))
    dp.callback_query.register(admin_callback_handler, F.data.startswith("admin:"))
    dp.callback_query.register(status_callback_handler, F.data.startswith("status:"))

    dp.message.register(name_handler, OrderForm.entering_name)
    dp.message.register(phone_handler, OrderForm.entering_phone)
    dp.message.register(address_handler, OrderForm.entering_address)
    dp.message.register(delivery_date_handler, OrderForm.entering_delivery_date)
    dp.message.register(comment_handler, OrderForm.entering_comment)

    # Admin
    dp.message.register(admin_help_handler, Command("help_admin"))
    dp.message.register(orders_today_handler, Command("orders_today"))
    dp.message.register(orders_week_handler, Command("orders_week"))
    dp.message.register(recent_orders_handler, Command("recent_orders"))
    dp.message.register(order_detail_handler, Command("order"))
    dp.message.register(set_status_handler, Command("set_status"))

    dp.message.register(fallback_handler)

    logger.info("Bot started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
