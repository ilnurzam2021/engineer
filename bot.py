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

    user_id = int(parts[0])
    title, desc, due = [x.strip() for x in parts[1].split("|")]

    due_date = TIMEZONE.localize(datetime.strptime(due, "%d.%m.%Y %H:%M"))

    task_id = add_task(title, desc, user_id, due_date)

    await bot.send_message(user_id, f"Новая задача #{task_id}: {title}")

    await event.message.answer(f"Создана задача #{task_id}")

@dp.message_created(Command('my_tasks'))
async def my_tasks(event: MessageCreated):
    tasks = get_user_tasks(event.message.sender.user_id)

    if not tasks:
        await event.message.answer("Нет задач")
        return

    text = ""
    for t in tasks:
        due = datetime.fromisoformat(t[3])
        if due.tzinfo is None:
            due = TIMEZONE.localize(due)

        text += f"{t[0]}. {t[1]} до {due.strftime('%d.%m %H:%M')}\n"

    await event.message.answer(text)

@dp.message_created(Command('done'))
async def done(event: MessageCreated):
    task_id = int(event.message.body.text.split()[1])
    if complete_task(task_id, event.message.sender.user_id):
        await event.message.answer("Готово")
    else:
        await event.message.answer("Ошибка")

@dp.message_created(Command('all_tasks'))
async def all_tasks(event: MessageCreated):
    if event.message.sender.user_id != ADMIN_ID:
        return

    tasks = get_all_tasks_grouped()
    now = datetime.now(TIMEZONE)

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

@dp.message_created(Command('broadcast'))
async def broadcast(event: MessageCreated):
    if event.message.sender.user_id != ADMIN_ID:
        return

    msg = event.message.body.text.replace("/broadcast", "").strip()
    users = get_all_engineers()

    for u in users:
        try:
            await bot.send_message(u[0], msg)
            await asyncio.sleep(0.05)
        except:
            pass

    await event.message.answer("Рассылка завершена")

# ==================== НАПОМИНАНИЯ ====================

async def check_reminders():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id, title, due_date, assigned_to FROM tasks WHERE status='active'")
    tasks = cur.fetchall()

    now = datetime.now(TIMEZONE)

    for t in tasks:
        task_id, title, due_str, user_id = t
        due = datetime.fromisoformat(due_str)

        if due.tzinfo is None:
            due = TIMEZONE.localize(due)

        if due < now:
            cur.execute("UPDATE tasks SET status='expired' WHERE id=?", (task_id,))
            await bot.send_message(user_id, f"Задача {title} просрочена")

    conn.commit()
    conn.close()

# ==================== ЗАПУСК ====================

async def main():
    init_db()

    scheduler.add_job(check_reminders, IntervalTrigger(seconds=60))
    scheduler.start()

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
