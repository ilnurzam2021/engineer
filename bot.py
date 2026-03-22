#!/usr/bin/env python3
"""
Бот для управления задачами инженеров в мессенджере MAX.
Руководитель может добавлять инженеров по username и назначать задачи.
С кнопками-меню.
"""

import os
import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List, Tuple

from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import pytz

# Импорты из библиотеки maxapi
from maxapi import Bot, Dispatcher
from maxapi.types import MessageCreated, BotStarted, Command
from maxapi.types import InlineKeyboardMarkup, InlineKeyboardButton

# ==================== КОНФИГУРАЦИЯ ====================
load_dotenv()
BOT_TOKEN = os.getenv("MAX_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
TIMEZONE = pytz.timezone("Europe/Moscow")

if not BOT_TOKEN:
    raise ValueError("Не задан MAX_BOT_TOKEN в .env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=TIMEZONE)

# ==================== КЛАВИАТУРА ====================
def get_main_keyboard():
    """Возвращает инлайн-клавиатуру с основными командами"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 Мои задачи", callback_data="my_tasks"),
            InlineKeyboardButton(text="✅ Выполнить задачу", callback_data="done")
        ],
        [
            InlineKeyboardButton(text="➕ Добавить инженера", callback_data="add_engineer"),
            InlineKeyboardButton(text="📝 Назначить задачу", callback_data="assign")
        ],
        [
            InlineKeyboardButton(text="📢 Массовая рассылка", callback_data="broadcast"),
            InlineKeyboardButton(text="👥 Список инженеров", callback_data="list_engineers")
        ]
    ])

# ==================== БАЗА ДАННЫХ ====================
def init_db():
    conn = sqlite3.connect("engineers.db")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS engineers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER UNIQUE NOT NULL,
            username TEXT,
            full_name TEXT,
            registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            assigned_to INTEGER NOT NULL,
            due_date TIMESTAMP NOT NULL,
            created_by INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'active',
            reminder_24h_sent INTEGER DEFAULT 0,
            reminder_1h_sent INTEGER DEFAULT 0,
            reminder_5min_sent INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()
    logger.info("База данных инициализирована")

def register_engineer(user_id: int, username: str = None, full_name: str = None):
    conn = sqlite3.connect("engineers.db")
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO engineers (user_id, username, full_name)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username = excluded.username,
            full_name = excluded.full_name
    """, (user_id, username, full_name))
    conn.commit()
    conn.close()

def get_engineer_by_user_id(user_id: int) -> Optional[Tuple]:
    conn = sqlite3.connect("engineers.db")
    cur = conn.cursor()
    cur.execute("SELECT user_id, username, full_name FROM engineers WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row

def get_all_engineers() -> List[Tuple]:
    conn = sqlite3.connect("engineers.db")
    cur = conn.cursor()
    cur.execute("SELECT user_id, username, full_name FROM engineers ORDER BY full_name")
    rows = cur.fetchall()
    conn.close()
    return rows

def add_task(title: str, description: str, assigned_to: int, due_date: datetime, created_by: int) -> int:
    conn = sqlite3.connect("engineers.db")
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO tasks (title, description, assigned_to, due_date, created_by)
        VALUES (?, ?, ?, ?, ?)
    """, (title, description, assigned_to, due_date.isoformat(), created_by))
    task_id = cur.lastrowid
    conn.commit()
    conn.close()
    return task_id

def get_user_tasks(user_id: int, status: str = "active") -> List[Tuple]:
    conn = sqlite3.connect("engineers.db")
    cur = conn.cursor()
    cur.execute("""
        SELECT id, title, description, due_date, status
        FROM tasks
        WHERE assigned_to = ? AND status = ?
        ORDER BY due_date ASC
    """, (user_id, status))
    rows = cur.fetchall()
    conn.close()
    return rows

def get_all_active_tasks() -> List[Tuple]:
    conn = sqlite3.connect("engineers.db")
    cur = conn.cursor()
    cur.execute("""
        SELECT t.id, t.title, t.description, t.due_date, t.assigned_to,
               e.username, e.full_name, t.reminder_24h_sent, t.reminder_1h_sent, t.reminder_5min_sent
        FROM tasks t
        JOIN engineers e ON t.assigned_to = e.user_id
        WHERE t.status = 'active'
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

def complete_task(task_id: int, user_id: int) -> bool:
    conn = sqlite3.connect("engineers.db")
    cur = conn.cursor()
    cur.execute("""
        UPDATE tasks SET status = 'done'
        WHERE id = ? AND assigned_to = ? AND status = 'active'
    """, (task_id, user_id))
    updated = cur.rowcount > 0
    conn.commit()
    conn.close()
    return updated

def update_reminder_flag(task_id: int, field: str):
    conn = sqlite3.connect("engineers.db")
    cur = conn.cursor()
    cur.execute(f"UPDATE tasks SET {field} = 1 WHERE id = ?", (task_id,))
    conn.commit()
    conn.close()

# ==================== ОБРАБОТЧИКИ СОБЫТИЙ ====================

@dp.bot_started()
async def on_bot_started(event: BotStarted):
    user = event.user
    user_id = user.user_id
    username = getattr(user, 'username', None)
    full_name = getattr(user, 'first_name', '') + ' ' + getattr(user, 'last_name', '')
    full_name = full_name.strip()
    register_engineer(user_id, username, full_name)

    text = (
        "👋 Здравствуйте! Вы зарегистрированы в системе задач.\n\n"
        "Используйте кнопки ниже для управления."
    )
    await bot.send_message(
        chat_id=event.chat_id,
        text=text,
        reply_markup=get_main_keyboard()
    )

@dp.message_created(Command('start'))
async def cmd_start(event: MessageCreated):
    # Аналогично bot_started, но для ручного вызова
    user_id = event.message.sender.user_id
    username = getattr(event.message.sender, 'username', None)
    full_name = getattr(event.message.sender, 'first_name', '') + ' ' + getattr(event.message.sender, 'last_name', '')
    full_name = full_name.strip()
    register_engineer(user_id, username, full_name)

    text = (
        "👋 Здравствуйте! Вы зарегистрированы в системе задач.\n\n"
        "Используйте кнопки ниже для управления."
    )
    await bot.send_message(
        chat_id=event.chat_id,
        text=text,
        reply_markup=get_main_keyboard()
    )

@dp.message_created(Command('help'))
async def cmd_help(event: MessageCreated):
    text = (
        "📌 *Доступные команды:*\n"
        "Используйте кнопки под полем ввода.\n"
        "Также доступны текстовые команды:\n"
        "/start – приветствие и меню\n"
        "/add_engineer @username [Имя] – добавить инженера\n"
        "/list_engineers – список инженеров\n"
        "/assign @username Заголовок | Описание | ДД.ММ.ГГГГ ЧЧ:ММ\n"
        "/broadcast текст – сообщение всем\n"
        "/my_tasks – мои задачи\n"
        "/done N – отметить задачу выполненной\n"
        "/help – эта справка\n\n"
        "📅 *Формат даты:* ДД.ММ.ГГГГ ЧЧ:ММ (например, 31.12.2025 18:00)"
    )
    await event.message.answer(text, parse_mode="Markdown")

@dp.message_created(Command('add_engineer'))
async def cmd_add_engineer(event: MessageCreated):
    """Добавить инженера по username (только для руководителя)"""
    if event.message.sender.user_id != ADMIN_ID:
        await event.message.answer("⛔ Только руководитель может добавлять инженеров.")
        return

    text = event.message.body.text.replace("/add_engineer", "", 1).strip()
    if not text:
        await event.message.answer("❌ Использование: /add_engineer @username [Имя Фамилия]")
        return

    parts = text.split(maxsplit=1)
    username = parts[0].lstrip('@')
    full_name = parts[1] if len(parts) > 1 else username

    try:
        user_info = await bot.resolve_username(username)
        if not user_info:
            await event.message.answer(f"❌ Пользователь @{username} не найден в Max.")
            return
        user_id = user_info.user_id
    except Exception as e:
        await event.message.answer(f"❌ Ошибка при поиске пользователя: {e}")
        return

    register_engineer(user_id, username, full_name)

    try:
        await bot.send_message(
            chat_id=user_id,
            text=f"👋 Вас добавил руководитель в бот задач. Теперь вы будете получать уведомления.\n"
                 f"Используйте /my_tasks для просмотра, /done N для завершения задачи."
        )
    except Exception as e:
        logger.warning(f"Не удалось отправить приветствие {user_id}: {e}")

    await event.message.answer(f"✅ Инженер @{username} ({full_name}) добавлен. Уведомление отправлено.")

@dp.message_created(Command('list_engineers'))
async def cmd_list_engineers(event: MessageCreated):
    if event.message.sender.user_id != ADMIN_ID:
        await event.message.answer("⛔ Только руководитель.")
        return

    engineers = get_all_engineers()
    if not engineers:
        await event.message.answer("Нет зарегистрированных инженеров.")
        return

    answer = "📋 *Список инженеров:*\n\n"
    for user_id, username, full_name in engineers:
        answer += f"• {full_name} (@{username or 'нет username'}) — ID: {user_id}\n"
    await event.message.answer(answer)

@dp.message_created(Command('my_tasks'))
async def cmd_my_tasks(event: MessageCreated):
    user_id = event.message.sender.user_id
    tasks = get_user_tasks(user_id)

    if not tasks:
        await event.message.answer("✅ У вас нет активных задач.")
        return

    answer = "📋 *Ваши активные задачи:*\n\n"
    for task_id, title, desc, due_date_str, status in tasks:
        due_date = datetime.fromisoformat(due_date_str)
        due_fmt = due_date.strftime("%d.%m.%Y %H:%M")
        answer += f"*{task_id}.* {title}\n   📝 {desc}\n   ⏰ Срок: {due_fmt}\n\n"
    await event.message.answer(answer)

@dp.message_created(Command('done'))
async def cmd_done(event: MessageCreated):
    user_id = event.message.sender.user_id
    text = event.message.body.text
    parts = text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        await event.message.answer("❌ Использование: /done номер_задачи")
        return

    task_id = int(parts[1].strip())
    if complete_task(task_id, user_id):
        await event.message.answer(f"✅ Задача #{task_id} отмечена выполненной!")
    else:
        await event.message.answer(f"❌ Задача #{task_id} не найдена или уже выполнена.")

@dp.message_created(Command('broadcast'))
async def cmd_broadcast(event: MessageCreated):
    if event.message.sender.user_id != ADMIN_ID:
        await event.message.answer("⛔ У вас нет прав для этой команды.")
        return

    text = event.message.body.text.replace("/broadcast", "", 1).strip()
    if not text:
        await event.message.answer("❌ Использование: /broadcast сообщение для всех")
        return

    engineers = get_all_engineers()
    if not engineers:
        await event.message.answer("Нет зарегистрированных инженеров.")
        return

    success = 0
    for user_id, username, full_name in engineers:
        try:
            await bot.send_message(
                chat_id=user_id,
                text=f"📢 *Массовое уведомление:*\n\n{text}"
            )
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение {user_id}: {e}")

    await event.message.answer(f"✅ Рассылка завершена. Отправлено {success} из {len(engineers)} инженерам.")

@dp.message_created(Command('assign'))
async def cmd_assign(event: MessageCreated):
    if event.message.sender.user_id != ADMIN_ID:
        await event.message.answer("⛔ Только руководитель может создавать задачи.")
        return

    text = event.message.body.text.replace("/assign", "", 1).strip()
    if not text:
        await event.message.answer("❌ Использование: /assign @username Заголовок | Описание | ДД.ММ.ГГГГ ЧЧ:ММ")
        return

    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await event.message.answer("❌ Укажите username и параметры задачи.")
        return

    username_raw = parts[0].lstrip('@')
    rest = parts[1]

    task_parts = [p.strip() for p in rest.split("|")]
    if len(task_parts) < 3:
        await event.message.answer("❌ Формат: /assign @username Заголовок | Описание | ДД.ММ.ГГГГ ЧЧ:ММ")
        return

    title, description, due_str = task_parts[0], task_parts[1], task_parts[2]

    conn = sqlite3.connect("engineers.db")
    cur = conn.cursor()
    cur.execute("SELECT user_id, username, full_name FROM engineers WHERE username LIKE ?", (f"%{username_raw}%",))
    engineer = cur.fetchone()
    conn.close()

    if not engineer:
        await event.message.answer(f"❌ Инженер с username '{username_raw}' не найден. Сначала добавьте его через /add_engineer.")
        return

    assigned_to, username, full_name = engineer

    try:
        due_date = datetime.strptime(due_str, "%d.%m.%Y %H:%M")
        due_date = TIMEZONE.localize(due_date)
    except ValueError:
        await event.message.answer("❌ Неверный формат даты. Используйте: ДД.ММ.ГГГГ ЧЧ:ММ")
        return

    if due_date < datetime.now(TIMEZONE):
        await event.message.answer("⚠️ Срок выполнения уже прошёл.")
        return

    task_id = add_task(title, description, assigned_to, due_date, event.message.sender.user_id)
    due_fmt = due_date.strftime("%d.%m.%Y %H:%M")

    await event.message.answer(
        f"✅ Задача #{task_id} создана!\n"
        f"👷 Инженер: @{username}\n"
        f"📌 {title}\n"
        f"📝 {description or '—'}\n"
        f"⏰ Срок: {due_fmt}"
    )

    try:
        await bot.send_message(
            chat_id=assigned_to,
            text=f"🔔 Новая задача #{task_id}!\n\n"
                 f"*{title}*\n{description}\n\n"
                 f"⏰ Срок: {due_fmt}\n\n"
                 f"/my_tasks — список ваших задач."
        )
    except Exception as e:
        logger.error(f"Ошибка уведомления: {e}")

# ==================== ОБРАБОТКА НАЖАТИЙ КНОПОК ====================

@dp.callback_query_handler()
async def handle_callback(event):
    user_id = event.user.user_id
    callback_data = event.callback_data

    # Подтверждаем получение (чтобы убрать "часики")
    await bot.answer_callback_query(event.callback_query_id)

    if callback_data == "my_tasks":
        tasks = get_user_tasks(user_id)
        if not tasks:
            await bot.send_message(chat_id=user_id, text="✅ У вас нет активных задач.")
        else:
            answer = "📋 *Ваши активные задачи:*\n\n"
            for task_id, title, desc, due_date_str, status in tasks:
                due_date = datetime.fromisoformat(due_date_str)
                due_fmt = due_date.strftime("%d.%m.%Y %H:%M")
                answer += f"*{task_id}.* {title}\n   📝 {desc}\n   ⏰ Срок: {due_fmt}\n\n"
            await bot.send_message(chat_id=user_id, text=answer)

    elif callback_data == "done":
        await bot.send_message(chat_id=user_id, text="Введите номер задачи для выполнения: /done <номер>")

    elif callback_data == "add_engineer":
        if user_id != ADMIN_ID:
            await bot.send_message(chat_id=user_id, text="⛔ Только руководитель может добавлять инженеров.")
        else:
            await bot.send_message(chat_id=user_id, text="Введите команду: /add_engineer @username Имя Фамилия")

    elif callback_data == "assign":
        if user_id != ADMIN_ID:
            await bot.send_message(chat_id=user_id, text="⛔ Только руководитель может назначать задачи.")
        else:
            await bot.send_message(chat_id=user_id, text="Введите команду: /assign @username Заголовок | Описание | ДД.ММ.ГГГГ ЧЧ:ММ")

    elif callback_data == "broadcast":
        if user_id != ADMIN_ID:
            await bot.send_message(chat_id=user_id, text="⛔ Только руководитель может делать рассылку.")
        else:
            await bot.send_message(chat_id=user_id, text="Введите текст для массовой рассылки: /broadcast сообщение")

    elif callback_data == "list_engineers":
        if user_id != ADMIN_ID:
            await bot.send_message(chat_id=user_id, text="⛔ Только руководитель.")
        else:
            engineers = get_all_engineers()
            if not engineers:
                await bot.send_message(chat_id=user_id, text="Нет зарегистрированных инженеров.")
            else:
                answer = "📋 *Список инженеров:*\n\n"
                for uid, username, full_name in engineers:
                    answer += f"• {full_name} (@{username or 'нет username'}) — ID: {uid}\n"
                await bot.send_message(chat_id=user_id, text=answer)

# ==================== ФОНОВЫЕ НАПОМИНАНИЯ ====================

async def check_reminders():
    tasks = get_all_active_tasks()
    now = datetime.now(TIMEZONE)

    for task in tasks:
        (task_id, title, desc, due_date_str, assigned_to,
         username, full_name, rem_24h, rem_1h, rem_5min) = task
        due_date = datetime.fromisoformat(due_date_str)

        if due_date < now:
            conn = sqlite3.connect("engineers.db")
            cur = conn.cursor()
            cur.execute("UPDATE tasks SET status = 'expired' WHERE id = ?", (task_id,))
            conn.commit()
            conn.close()
            continue

        delta = due_date - now

        if not rem_24h and delta <= timedelta(hours=24):
            await bot.send_message(
                chat_id=assigned_to,
                text=f"⏰ *Напоминание о задаче #{task_id}*\n\n"
                     f"*{title}*\n{desc or ''}\n\n"
                     f"⏳ Осталось менее 24 часов. Срок: {due_date.strftime('%d.%m.%Y %H:%M')}"
            )
            update_reminder_flag(task_id, "reminder_24h_sent")

        elif not rem_1h and delta <= timedelta(hours=1):
            await bot.send_message(
                chat_id=assigned_to,
                text=f"⚠️ *Срочное напоминание!*\n\n"
                     f"Задача #{task_id} «{title}»\n"
                     f"Срок выполнения через час: {due_date.strftime('%d.%m.%Y %H:%M')}"
            )
            update_reminder_flag(task_id, "reminder_1h_sent")

        elif not rem_5min and delta <= timedelta(minutes=5):
            await bot.send_message(
                chat_id=assigned_to,
                text=f"🚨 *Задача #{task_id} должна быть выполнена через 5 минут!*\n\n"
                     f"«{title}» — {desc or ''}\n"
                     f"Срок: {due_date.strftime('%d.%m.%Y %H:%M')}"
            )
            update_reminder_flag(task_id, "reminder_5min_sent")

# ==================== ЗАПУСК ====================

async def main():
    init_db()
    try:
        await bot.delete_webhook()
    except Exception as e:
        logger.warning(f"Ошибка удаления webhook: {e}")

    scheduler.add_job(check_reminders, IntervalTrigger(seconds=60))
    scheduler.start()

    logger.info("Бот запущен и ожидает сообщений...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
