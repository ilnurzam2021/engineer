#!/usr/bin/env python3
"""
Бот для управления задачами инженеров в MAX.
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

from maxapi import Bot, Dispatcher
from maxapi.types import MessageCreated, BotStarted, Command

# ==================== КОНФИГУРАЦИЯ ====================
load_dotenv()
BOT_TOKEN = os.getenv("MAX_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
TIMEZONE = pytz.timezone("Europe/Moscow")

if not BOT_TOKEN:
    raise ValueError("Не задан MAX_BOT_TOKEN")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=TIMEZONE)

# ==================== БД ====================
def get_conn():
    conn = sqlite3.connect("engineers.db")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS engineers (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            description TEXT,
            assigned_to INTEGER,
            due_date TEXT,
            status TEXT DEFAULT 'active',
            reminder_24h_sent INTEGER DEFAULT 0,
            reminder_1h_sent INTEGER DEFAULT 0,
            reminder_5min_sent INTEGER DEFAULT 0,
            FOREIGN KEY(assigned_to) REFERENCES engineers(user_id) ON DELETE CASCADE
        )
    """)

    conn.commit()
    conn.close()

def register_engineer(user_id, username, full_name):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO engineers(user_id, username, full_name)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            full_name=excluded.full_name
    """, (user_id, username, full_name))
    conn.commit()
    conn.close()

def get_all_engineers():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id, username, full_name FROM engineers")
    rows = cur.fetchall()
    conn.close()
    return rows

def add_task(title, desc, user_id, due_date):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO tasks(title, description, assigned_to, due_date)
        VALUES (?, ?, ?, ?)
    """, (title, desc, user_id, due_date.isoformat()))
    task_id = cur.lastrowid
    conn.commit()
    conn.close()
    return task_id

def get_user_tasks(user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, title, description, due_date
        FROM tasks WHERE assigned_to=? AND status='active'
        ORDER BY due_date
    """, (user_id,))
    rows = cur.fetchall()
    conn.close()
    return rows

def complete_task(task_id, user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE tasks SET status='done'
        WHERE id=? AND assigned_to=?
    """, (task_id, user_id))
    ok = cur.rowcount > 0
    conn.commit()
    conn.close()
    return ok

def get_all_tasks_grouped():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT t.id, t.title, t.description, t.due_date, t.status,
               e.full_name, e.username, t.assigned_to
        FROM tasks t
        JOIN engineers e ON t.assigned_to = e.user_id
        WHERE t.status IN ('active','expired')
        ORDER BY e.full_name, t.due_date
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

# ==================== КОМАНДЫ ====================
@dp.bot_started()
async def on_bot_started(event: BotStarted):
    user = event.user

    register_engineer(
        user.user_id,
        getattr(user, 'username', None),
        f"{getattr(user,'first_name','')} {getattr(user,'last_name','')}".strip()
    )

    await bot.send_message(
        chat_id=event.chat_id,
        text="👋 Бот запущен! Напиши /help"
    )

@dp.message_created(Command('start'))
async def start(event: MessageCreated):
    u = event.message.sender
    register_engineer(u.user_id, getattr(u, 'username', None),
                      f"{getattr(u,'first_name','')} {getattr(u,'last_name','')}".strip())

    await event.message.answer("Бот задач запущен. /help")

@dp.message_created(Command('help'))
async def help_cmd(event: MessageCreated):
    if event.message.sender.user_id == ADMIN_ID:
        await event.message.answer(
            "/assign ID Заголовок | Описание | ДД.ММ.ГГГГ ЧЧ:ММ\n"
            "/all_tasks — все задачи\n"
            "/list_tasks — список всех активных задач (кратко)\n"
            "/broadcast текст\n"
            "/my_tasks\n"
            "/done N"
        )
    else:
        await event.message.answer("/my_tasks\n/done N")

@dp.message_created(Command('assign'))
async def assign(event: MessageCreated):
    if event.message.sender.user_id != ADMIN_ID:
        return

    text = event.message.body.text.replace("/assign", "").strip()
    parts = text.split(maxsplit=1)

    try:
        user_id = int(parts[0])
    except ValueError:
        await event.message.answer("❌ ID должен быть числом.")
        return

    if len(parts) < 2:
        await event.message.answer("❌ Укажите параметры: Заголовок | Описание | ДД.ММ.ГГГГ ЧЧ:ММ")
        return

    task_data = parts[1].split("|")
    if len(task_data) < 3:
        await event.message.answer("❌ Неверный формат. Используйте: Заголовок | Описание | ДД.ММ.ГГГГ ЧЧ:ММ")
        return

    title = task_data[0].strip()
    description = task_data[1].strip()
    due_str = task_data[2].strip()

    try:
        due_date = datetime.strptime(due_str, "%d.%m.%Y %H:%M")
        due_date = TIMEZONE.localize(due_date)
    except ValueError:
        await event.message.answer("❌ Неверный формат даты. Используйте: ДД.ММ.ГГГГ ЧЧ:ММ")
        return

    if due_date < datetime.now(TIMEZONE):
        await event.message.answer("⚠️ Срок выполнения уже прошёл.")
        return

    # Проверяем, существует ли инженер в БД
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id, full_name FROM engineers WHERE user_id=?", (user_id,))
    engineer = cur.fetchone()
    conn.close()

    if not engineer:
        await event.message.answer(f"❌ Инженер с ID {user_id} не зарегистрирован. Попросите его написать /start.")
        return

    task_id = add_task(title, description, user_id, due_date)

    await event.message.answer(f"✅ Задача #{task_id} создана для инженера {engineer[1]} (ID: {user_id})")

    # Отправляем уведомление инженеру
    try:
        await bot.send_message(user_id, f"🔔 Новая задача #{task_id}:\n{title}\n{description}\nСрок: {due_date.strftime('%d.%m.%Y %H:%M')}")
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление {user_id}: {e}")

@dp.message_created(Command('my_tasks'))
async def my_tasks(event: MessageCreated):
    user_id = event.message.sender.user_id
    tasks = get_user_tasks(user_id)

    if not tasks:
        await event.message.answer("✅ У вас нет активных задач.")
        return

    answer = "📋 Ваши активные задачи:\n\n"
    for t in tasks:
        task_id, title, desc, due_str = t
        due_date = datetime.fromisoformat(due_str)
        if due_date.tzinfo is None:
            due_date = TIMEZONE.localize(due_date)
        due_fmt = due_date.strftime("%d.%m.%Y %H:%M")
        answer += f"{task_id}. {title}\n   📝 {desc}\n   ⏰ Срок: {due_fmt}\n\n"

    await event.message.answer(answer)

@dp.message_created(Command('done'))
async def done(event: MessageCreated):
    user_id = event.message.sender.user_id
    text = event.message.body.text
    parts = text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        await event.message.answer("❌ Использование: /done номер_задачи")
        return

    task_id = int(parts[1].strip())
    if complete_task(task_id, user_id):
        await event.message.answer(f"✅ Задача #{task_id} выполнена!")
    else:
        await event.message.answer(f"❌ Задача #{task_id} не найдена или уже выполнена.")

@dp.message_created(Command('all_tasks'))
async def all_tasks(event: MessageCreated):
    if event.message.sender.user_id != ADMIN_ID:
        return

    tasks = get_all_tasks_grouped()
    now = datetime.now(TIMEZONE)

    if not tasks:
        await event.message.answer("Нет задач.")
        return

    text = "Все задачи:\n\n"
    current = None

    for t in tasks:
        task_id, title, desc, due_str, status, full_name, username, uid = t

        due = datetime.fromisoformat(due_str)
        if due.tzinfo is None:
            due = TIMEZONE.localize(due)

        if current != uid:
            current = uid
            text += f"\n👷 {full_name} (@{username or '—'})\n"

        status_icon = "❌" if due < now else "🟢"

        text += f"{task_id}. {title} ({due.strftime('%d.%m %H:%M')}) {status_icon}\n"

    await event.message.answer(text)

@dp.message_created(Command('list_tasks'))
async def list_tasks(event: MessageCreated):
    if event.message.sender.user_id != ADMIN_ID:
        return

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT t.id, t.title, t.description, t.due_date, t.status,
               e.full_name, e.username, t.assigned_to
        FROM tasks t
        JOIN engineers e ON t.assigned_to = e.user_id
        WHERE t.status='active'
        ORDER BY t.due_date
    """)
    tasks = cur.fetchall()
    conn.close()

    if not tasks:
        await event.message.answer("Нет активных задач.")
        return

    answer = "📋 Активные задачи:\n\n"
    for task in tasks:
        task_id, title, desc, due_str, status, full_name, username, uid = task
        due = datetime.fromisoformat(due_str)
        if due.tzinfo is None:
            due = TIMEZONE.localize(due)
        due_fmt = due.strftime("%d.%m.%Y %H:%M")
        answer += f"#{task_id}: {title}\n   👷 {full_name} (ID: {uid})\n   ⏰ {due_fmt}\n   📝 {desc or '—'}\n\n"

    await event.message.answer(answer)

@dp.message_created(Command('broadcast'))
async def broadcast(event: MessageCreated):
    if event.message.sender.user_id != ADMIN_ID:
        return

    msg = event.message.body.text.replace("/broadcast", "").strip()
    if not msg:
        await event.message.answer("❌ Использование: /broadcast текст")
        return

    users = get_all_engineers()

    success = 0
    for u in users:
        try:
            await bot.send_message(u[0], f"📢 Сообщение от руководителя:\n\n{msg}")
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение {u[0]}: {e}")

    await event.message.answer(f"✅ Рассылка завершена. Отправлено {success} из {len(users)} инженерам.")

# ==================== НАПОМИНАНИЯ ====================
async def check_reminders():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id, title, due_date, assigned_to, reminder_24h_sent, reminder_1h_sent, reminder_5min_sent FROM tasks WHERE status='active'")
    tasks = cur.fetchall()
    conn.close()

    now = datetime.now(TIMEZONE)

    for t in tasks:
        task_id, title, due_str, user_id, rem_24, rem_1, rem_5 = t
        due = datetime.fromisoformat(due_str)
        if due.tzinfo is None:
            due = TIMEZONE.localize(due)

        delta = due - now

        if due < now:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("UPDATE tasks SET status='expired' WHERE id=?", (task_id,))
            conn.commit()
            conn.close()
            await bot.send_message(user_id, f"⚠️ Задача #{task_id} «{title}» просрочена.")
            continue

        if not rem_24 and delta <= timedelta(hours=24):
            await bot.send_message(user_id, f"⏰ Напоминание о задаче #{task_id} «{title}». Осталось менее 24 часов. Срок: {due.strftime('%d.%m.%Y %H:%M')}")
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("UPDATE tasks SET reminder_24h_sent=1 WHERE id=?", (task_id,))
            conn.commit()
            conn.close()

        elif not rem_1 and delta <= timedelta(hours=1):
            await bot.send_message(user_id, f"⚠️ Срочное напоминание! Задача #{task_id} «{title}» должна быть выполнена через час. Срок: {due.strftime('%d.%m.%Y %H:%M')}")
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("UPDATE tasks SET reminder_1h_sent=1 WHERE id=?", (task_id,))
            conn.commit()
            conn.close()

        elif not rem_5 and delta <= timedelta(minutes=5):
            await bot.send_message(user_id, f"🚨 Задача #{task_id} «{title}» должна быть выполнена через 5 минут! Срок: {due.strftime('%d.%m.%Y %H:%M')}")
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("UPDATE tasks SET reminder_5min_sent=1 WHERE id=?", (task_id,))
            conn.commit()
            conn.close()

# ==================== ЗАПУСК ====================
async def main():
    init_db()

    scheduler.add_job(check_reminders, IntervalTrigger(seconds=60))
    scheduler.start()

    logger.info("Бот запущен")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
