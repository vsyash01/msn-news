import sqlite3
import logging
import json
import asyncio

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def create_table(db_path):
    logger.debug(f"[TRACE] Создание таблиц в базе: {db_path}")
    loop = asyncio.get_event_loop()
    def sync_create_table():
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS news (
                    news_id TEXT PRIMARY KEY,
                    header TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    news_id TEXT PRIMARY KEY,
                    caption TEXT,
                    message_ids TEXT,
                    file_ids TEXT,
                    category TEXT
                )
            ''')
            conn.commit()
    await loop.run_in_executor(None, sync_create_table)
    logger.debug(f"[TRACE] Таблицы созданы")

async def save_to_db(db_path, news_id, header):
    logger.debug(f"[TRACE] Сохранение новости: news_id={news_id}")
    loop = asyncio.get_event_loop()
    def sync_save_to_db():
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('INSERT OR REPLACE INTO news (news_id, header) VALUES (?, ?)', (news_id, header))
            conn.commit()
    await loop.run_in_executor(None, sync_save_to_db)
    logger.info(f"[TRACE] Новость сохранена: news_id={news_id}")

async def select_for_db(db_path, value, column):
    logger.debug(f"[TRACE] Поиск: {column}={value} в базе: {db_path}")
    original_value = value
    while value.startswith('vk_'):
        value = value[3:]
        logger.warning(f"[TRACE] Обнаружен префикс vk_ в select_for_db: {original_value} -> {value}")
    logger.debug(f"[TRACE] Окончательный value для поиска: {value}")
    loop = asyncio.get_event_loop()
    def sync_select_for_db():
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f'SELECT {column} FROM news WHERE {column} = ?', (value,))
            result = cursor.fetchone()
            return result
    result = await loop.run_in_executor(None, sync_select_for_db)
    logger.debug(f"[TRACE] Результат поиска: {result}")
    return result

async def save_message_data(db_path, news_id, caption, message_ids, file_ids, category):
    logger.debug(f"[TRACE] Сохранение сообщения: news_id={news_id}")
    loop = asyncio.get_event_loop()
    def sync_save_message_data():
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO messages (news_id, caption, message_ids, file_ids, category)
                VALUES (?, ?, ?, ?, ?)
            ''', (news_id, caption, json.dumps(message_ids), json.dumps(file_ids), category))
            conn.commit()
    await loop.run_in_executor(None, sync_save_message_data)
    logger.info(f"[TRACE] Данные сообщения сохранены: news_id={news_id}")

async def get_message_data(db_path, news_id):
    logger.debug(f"[TRACE] Поиск сообщения: news_id={news_id}")
    original_news_id = news_id
    while news_id.startswith('vk_'):
        news_id = news_id[3:]
        logger.warning(f"[TRACE] Обнаружен префикс vk_ в get_message_data: {original_news_id} -> {news_id}")
    logger.debug(f"[TRACE] Окончательный news_id для поиска: {news_id}")
    loop = asyncio.get_event_loop()
    def sync_get_message_data():
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT caption, message_ids, file_ids, category FROM messages WHERE news_id = ?', (news_id,))
            result = cursor.fetchone()
            if result:
                caption, message_ids, file_ids, category = result
                return caption, json.loads(message_ids), json.loads(file_ids), category
            return None
    result = await loop.run_in_executor(None, sync_get_message_data)
    logger.debug(f"[TRACE] Результат поиска: {result}")
    return result