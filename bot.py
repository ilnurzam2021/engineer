#!/usr/bin/env python3
"""
Бот для управления задачами инженеров в MAX.
Добавлены функции и смайлики для улучшения интерфейса.
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
from apscheduler.triggers.cron import CronTrigger
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
    DB_PATH = "/app/data/engineers.db"
    conn = sqlite3.connect(DB_PATH)
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

def get_all_active_tasks_for_reminder():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT t.id, t.title, t.due_date, t.assigned_to,
               e.full_name, e.username
        FROM tasks t
        JOIN engineers e ON t.assigned_to = e.user_id
        WHERE t.status = 'active'
        ORDER BY t.due_date
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

def get_engineer_by_id(user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id, username, full_name FROM engineers WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row

def delete_engineer(user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM engineers WHERE user_id=?", (user_id,))
    ok = cur.rowcount > 0
    conn.commit()
    conn.close()
    return ok

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
        text="🤖 Бот запущен! Напиши /help"
    )

@dp.message_created(Command('start'))
async def start(event: MessageCreated):
    u = event.message.sender
    register_engineer(u.user_id, getattr(u, 'username', None),
                      f"{getattr(u,'first_name','')} {getattr(u,'last_name','')}".strip())
    await event.message.answer("👋 Бот задач запущен. /help")

@dp.message_created(Command('help'))
async def help_cmd(event: MessageCreated):
    if event.message.sender.user_id == ADMIN_ID:
        await event.message.answer(
            "📋 *Команды руководителя:*\n"
            "/assign ID Заголовок | Описание | ДД.ММ.ГГГГ ЧЧ:ММ — создать задачу\n"
            "/all_tasks — все задачи (группировка)\n"
            "/list_engineers — список всех инженеров\n"
            "/add_engineer <ID> <Имя> — ➕ добавить инженера\n"
            "/remove_user <ID> — ❌ удалить инженера\n"
            "/broadcast текст — рассылка всем\n"
            "/remind_all_now — ⏰ напомнить всем о задачах\n"
            "/stats — 📊 статистика\n"
            "/task_info <ID> — ℹ️ информация о задаче\n"
            "/check_user <ID> — 🔍 проверить наличие инженера\n"
            "/my_tasks — мои задачи\n"
            "/done N — ✅ отметить выполненной\n"
            "/help — 💬 эта справка"
        )
    else:
        await event.message.answer(
            "📋 *Команды инженера:*\n"
            "/my_tasks — мои задачи\n"
            "/done N — ✅ отметить выполненной\n"
            "/help — 💬 справка"
        )

@dp.message_created(Command('assign'))
async def assign(event: MessageCreated):
    if event.message.sender.user_id != ADMIN_ID:
        return

    text = event.message.body.text.replace("/assign", "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await event.message.answer("❌ Формат: /assign ID Заголовок | Описание | ДД.ММ.ГГГГ ЧЧ:ММ")
        return

    try:
        user_id = int(parts[0])
    except:
        await event.message.answer("❌ ID должен быть числом")
        return

    rest = parts[1]
    task_parts = [x.strip() for x in rest.split("|")]
    if len(task_parts) < 3:
        await event.message.answer("❌ Формат: /assign ID Заголовок | Описание | ДД.ММ.ГГГГ ЧЧ:ММ")
        return

    title, desc, due = task_parts[0], task_parts[1], task_parts[2]

    try:
        due_date = datetime.strptime(due, "%d.%m.%Y %H:%M")
        due_date = TIMEZONE.localize(due_date)
    except:
        await event.message.answer("❌ Неверный формат даты. Используйте: ДД.ММ.ГГГГ ЧЧ:ММ")
        return

    if due_date < datetime.now(TIMEZONE):
        await event.message.answer("⚠️ Срок выполнения уже прошёл.")
        return

    task_id = add_task(title, desc, user_id, due_date)

    engineer = get_engineer_by_id(user_id)
    if engineer:
        name = engineer[2]
    else:
        name = f"ID {user_id}"

    await event.message.answer(f"✅ Задача #{task_id} создана для инженера {name} (ID: {user_id})")
    try:
        await bot.send_message(user_id, f"🔔 Новая задача #{task_id}: {title}\nСрок: {due_date.strftime('%d.%m.%Y %H:%M')}")
    except Exception as e:
        logger.warning(f"Не удалось уведомить инженера {user_id}: {e}")

@dp.message_created(Command('my_tasks'))
async def my_tasks(event: MessageCreated):
    tasks = get_user_tasks(event.message.sender.user_id)
    if not tasks:
        await event.message.answer("✅ У вас нет активных задач.")
        return

    text = "📋 Ваши активные задачи:\n\n"
    for t in tasks:
        due = datetime.fromisoformat(t[3])
        if due.tzinfo is None:
            due = TIMEZONE.localize(due)
        text += f"{t[0]}. {t[1]} — {t[2] or ''} (до {due.strftime('%d.%m %H:%M')})\n"
    await event.message.answer(text)

@dp.message_created(Command('done'))
async def done(event: MessageCreated):
    try:
        task_id = int(event.message.body.text.split()[1])
    except:
        await event.message.answer("❌ Использование: /done <номер_задачи>")
        return

    if complete_task(task_id, event.message.sender.user_id):
        await event.message.answer(f"✅ Задача #{task_id} выполнена!")
    else:
        await event.message.answer(f"❌ Задача #{task_id} не найдена или не принадлежит вам.")

@dp.message_created(Command('all_tasks'))
async def all_tasks(event: MessageCreated):
    if event.message.sender.user_id != ADMIN_ID:
        return

    tasks = get_all_tasks_grouped()
    now = datetime.now(TIMEZONE)

    text = "📋 Все задачи:\n\n"
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
        text += f"{task_id}. {title} — {due.strftime('%d.%m %H:%M')} {status_icon}\n"

    await event.message.answer(text)

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
            await bot.send_message(u[0], f"📢 Массовое уведомление:\n\n{msg}")
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.warning(f"Не удалось отправить {u[0]}: {e}")
    await event.message.answer(f"✅ Рассылка завершена. Отправлено {success} из {len(users)} инженерам.")

@dp.message_created(Command('add_engineer'))
async def add_engineer_cmd(event: MessageCreated):
    if event.message.sender.user_id != ADMIN_ID:
        return

    parts = event.message.body.text.split(maxsplit=2)
    if len(parts) < 3:
        await event.message.answer("❌ Использование: /add_engineer <ID> <Имя>")
        return

    try:
        user_id = int(parts[1])
    except:
        await event.message.answer("❌ ID должен быть числом")
        return

    full_name = parts[2]
    register_engineer(user_id, None, full_name)
    await event.message.answer(f"➕ Инженер {full_name} (ID: {user_id}) добавлен.")

@dp.message_created(Command('remove_user'))
async def remove_user(event: MessageCreated):
    if event.message.sender.user_id != ADMIN_ID:
        return

    try:
        user_id = int(event.message.body.text.split()[1])
    except:
        await event.message.answer("❌ Использование: /remove_user <ID>")
        return

    if delete_engineer(user_id):
        await event.message.answer(f"❌ Инженер с ID {user_id} удалён. Все его задачи также удалены.")
    else:
        await event.message.answer(f"❌ Инженер с ID {user_id} не найден.")

@dp.message_created(Command('remind_all_now'))
async def remind_all_now(event: MessageCreated):
    if event.message.sender.user_id != ADMIN_ID:
        return

    tasks = get_all_active_tasks_for_reminder()
    if not tasks:
        await event.message.answer("📭 Нет активных задач.")
        return

    by_user = {}
    for t in tasks:
        task_id, title, due_str, user_id, full_name, username = t
        due = datetime.fromisoformat(due_str)
        if due.tzinfo is None:
            due = TIMEZONE.localize(due)
        if user_id not in by_user:
            by_user[user_id] = {'name': full_name, 'username': username, 'tasks': []}
        by_user[user_id]['tasks'].append((task_id, title, due))

    for user_id, data in by_user.items():
        text = f"⏰ Напоминание о ваших задачах:\n\n"
        for task_id, title, due in data['tasks']:
            delta = due - datetime.now(TIMEZONE)
            if delta.total_seconds() > 0:
                days = delta.days
                hours = delta.seconds // 3600
                text += f"#{task_id} {title} — осталось {days} дн. {hours} ч.\n"
            else:
                text += f"#{task_id} {title} — 🚨 СРОК ПРОШЁЛ!\n"
        try:
            await bot.send_message(user_id, text)
        except Exception as e:
            logger.warning(f"Не удалось отправить напоминание {user_id}: {e}")
        await asyncio.sleep(0.1)

    await event.message.answer("✅ Напоминания отправлены всем инженерам с активными задачами.")

@dp.message_created(Command('stats'))
async def stats(event: MessageCreated):
    if event.message.sender.user_id != ADMIN_ID:
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM tasks")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM tasks WHERE status='active'")
    active = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM tasks WHERE status='expired'")
    expired = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM engineers")
    engineers_count = cur.fetchone()[0]
    conn.close()
    await event.message.answer(
        f"📊 *Статистика:*\n"
        f"👥 Инженеров: {engineers_count}\n"
        f"📋 Всего задач: {total}\n"
        f"🟢 Активных: {active}\n"
        f"🔴 Просрочено: {expired}"
    )

@dp.message_created(Command('task_info'))
async def task_info(event: MessageCreated):
    if event.message.sender.user_id != ADMIN_ID:
        return
    try:
        task_id = int(event.message.body.text.split()[1])
    except:
        await event.message.answer("❌ Использование: /task_info <ID>")
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT t.id, t.title, t.description, t.due_date, t.status,
               e.full_name, e.username
        FROM tasks t
        LEFT JOIN engineers e ON t.assigned_to = e.user_id
        WHERE t.id=?
    """, (task_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        await event.message.answer(f"🔍 Задача #{task_id} не найдена.")
        return
    task_id, title, desc, due_str, status, full_name, username = row
    due = datetime.fromisoformat(due_str)
    if due.tzinfo is None:
        due = TIMEZONE.localize(due)
    await event.message.answer(
        f"ℹ️ *Задача #{task_id}*\n"
        f"📌 Название: {title}\n"
        f"📝 Описание: {desc or '—'}\n"
        f"🔁 Статус: {status}\n"
        f"⏰ Срок: {due.strftime('%d.%m.%Y %H:%M')}\n"
        f"👷 Инженер: {full_name} (@{username or '—'})"
    )

@dp.message_created(Command('check_user'))
async def check_user(event: MessageCreated):
    if event.message.sender.user_id != ADMIN_ID:
        return
    try:
        user_id = int(event.message.body.text.split()[1])
    except:
        await event.message.answer("❌ Использование: /check_user <ID>")
        return
    engineer = get_engineer_by_id(user_id)
    if engineer:
        await event.message.answer(f"🔍 Инженер найден: {engineer[2]} (@{engineer[1] or 'нет username'}) ID: {engineer[0]}")
    else:
        await event.message.answer(f"❌ Инженер с ID {user_id} не найден.")

# ==================== ЕЖЕДНЕВНОЕ НАПОМИНАНИЕ ====================
async def daily_reminder():
    tasks = get_all_active_tasks_for_reminder()
    if not tasks:
        logger.info("Ежедневное напоминание: активных задач нет.")
        return

    by_user = {}
    for t in tasks:
        task_id, title, due_str, user_id, full_name, username = t
        due = datetime.fromisoformat(due_str)
        if due.tzinfo is None:
            due = TIMEZONE.localize(due)
        if user_id not in by_user:
            by_user[user_id] = {'name': full_name, 'username': username, 'tasks': []}
        by_user[user_id]['tasks'].append((task_id, title, due))

    for user_id, data in by_user.items():
        text = f"📅 Ежедневное напоминание о задачах:\n\n"
        for task_id, title, due in data['tasks']:
            delta = due - datetime.now(TIMEZONE)
            if delta.total_seconds() > 0:
                days = delta.days
                hours = delta.seconds // 3600
                text += f"#{task_id} {title} — осталось {days} дн. {hours} ч.\n"
            else:
                text += f"#{task_id} {title} — 🚨 СРОК ПРОШЁЛ!\n"
        try:
            await bot.send_message(user_id, text)
            logger.info(f"Ежедневное напоминание отправлено {user_id}")
        except Exception as e:
            logger.warning(f"Не удалось отправить ежедневное напоминание {user_id}: {e}")
        await asyncio.sleep(0.1)

# ==================== ЗАПУСК ====================
async def main():
    init_db()

    scheduler.add_job(daily_reminder, CronTrigger(hour=9, minute=0, timezone=TIMEZONE))
    scheduler.add_job(check_deadlines, IntervalTrigger(seconds=60))
    scheduler.start()

    logger.info("Бот запущен и ожидает сообщений...")
    await dp.start_polling(bot)

async def check_deadlines():
    tasks = get_all_active_tasks_for_reminder()
    now = datetime.now(TIMEZONE)

    for t in tasks:
        task_id, title, due_str, user_id, full_name, username = t
        due = datetime.fromisoformat(due_str)
        if due.tzinfo is None:
            due = TIMEZONE.localize(due)

        delta = due - now
        if delta.total_seconds() <= 24*3600 and delta.total_seconds() > 23*3600:
            await bot.send_message(user_id, f"⏰ Напоминание: задача #{task_id} «{title}» будет выполнена через 24 часа.")
        elif delta.total_seconds() <= 3600 and delta.total_seconds() > 3550:
            await bot.send_message(user_id, f"⚠️ Напоминание: задача #{task_id} «{title}» будет выполнена через час.")
        elif delta.total_seconds() <= 300 and delta.total_seconds() > 250:
            await bot.send_message(user_id, f"🚨 Срочно! Задача #{task_id} «{title}» должна быть выполнена через 5 минут.")
        elif delta.total_seconds() <= 0:
            # Пометим как expired (можно добавить обновление статуса, но для простоты опустим)
            pass

if __name__ == "__main__":
    asyncio.run(main())
