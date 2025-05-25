from aiogram import Bot, Dispatcher
from aiogram.types import InputMediaPhoto, FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError
import aiohttp
import asyncio
import os
import logging
import traceback
from bs4 import BeautifulSoup
import re
from dotenv import load_dotenv
from database import save_message_data, get_message_data, select_for_db
import vk_api
from vk_api.exceptions import ApiError
import requests
from aiohttp import ClientConnectionError, ClientOSError
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import time
from video_generator import generate_shorts

# Настройка логирования
logging.basicConfig(level=logging.DEBUG, force=True)
logger = logging.getLogger(__name__)
os.makedirs('logs', exist_ok=True)
file_handler = logging.FileHandler('logs/debug_callback.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
file_handler.flush = lambda: file_handler.stream.flush()

# Маркер версии файла
logger.info("[TRACE] Загрузка telegram_bot.py, версия с извлечением заголовка из caption v9 от 2025-05-18")

# Загрузка конфиденциальных данных
load_dotenv('keys.env')
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
FORWARD_CHANNEL_ID = os.getenv("FORWARD_CHANNEL_ID")
FASHION_CHANNEL_ID = os.getenv("FASHION_CHANNEL_ID")
FINANCE_CHANNEL_ID = os.getenv("FINANCE_CHANNEL_ID")
VK_DEFAULT_TOKEN = os.getenv("VK_DEFAULT_TOKEN")
VK_FASHION_TOKEN = os.getenv("VK_FASHION_TOKEN")
VK_DEFAULT_GROUP_ID = os.getenv("VK_DEFAULT_GROUP_ID")
VK_FASHION_GROUP_ID = os.getenv("VK_FASHION_GROUP_ID")
CHANNEL_ID1 = os.getenv("CHANNEL_ID")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")

# Проверка переменных
logger.debug(f"[TRACE] Проверка переменных: TELEGRAM_TOKEN={bool(TELEGRAM_TOKEN)}, CHANNEL_ID1={CHANNEL_ID1}, "
             f"FORWARD_CHANNEL_ID={FORWARD_CHANNEL_ID}, FASHION_CHANNEL_ID={FASHION_CHANNEL_ID}, "
             f"VK_DEFAULT_GROUP_ID={VK_DEFAULT_GROUP_ID}, VK_FASHION_GROUP_ID={VK_FASHION_GROUP_ID}")

# Инициализация бота и VK API
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
try:
    vk_default_session = vk_api.VkApi(token=VK_DEFAULT_TOKEN)
    vk_fashion_session = vk_api.VkApi(token=VK_FASHION_TOKEN)
    vk_default = vk_default_session.get_api()
    vk_fashion = vk_fashion_session.get_api()
    logger.debug("[TRACE] VK API инициализирован успешно")
except Exception as e:
    logger.error(f"[TRACE] Ошибка инициализации VK API: {str(e)}")
    vk_default = None
    vk_fashion = None

def clean_caption(caption):
    """Очистка подписи от HTML-тегов, сохраняя переносы строк."""
    logger.debug(f"[TRACE] Очистка подписи, длина: {len(caption)}")
    caption = re.sub(r'<[^>]+>', '', caption)
    caption = re.sub(r'[ \t]+', ' ', caption).strip()
    return caption

def format_vk_caption(caption):
    """Форматирование подписи для VK: выделение заголовка и сохранение структуры."""
    logger.debug(f"[TRACE] Форматирование подписи для VK, длина: {len(caption)}")
    cleaned = re.sub(r'<[^>]+>', '', caption)
    lines = cleaned.split('\n')
    formatted_lines = []
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
        if i == 0:
            formatted_lines.append(f"📊 {line}")
        else:
            formatted_lines.append(line)
    result = '\n\n'.join(formatted_lines)
    logger.debug(f"[TRACE] Форматированная подпись VK, длина: {len(result)}")
    return result

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_exception_type((requests.RequestException, ClientOSError)))
async def upload_photo_to_vk(photo_url, group_id, category):
    """Загружает фотографию в VK."""
    vk = vk_fashion if category == "fashion" else vk_default
    logger.debug(f"[TRACE] upload_photo_to_vk: group_id={group_id}, category={category}, photo_url={photo_url}")
    if not vk:
        logger.error("[TRACE] VK API не инициализирован")
        raise ValueError("VK API не инициализирован")
    try:
        upload_server = vk.photos.getMessagesUploadServer(group_id=abs(group_id))
        upload_url = upload_server['upload_url']
        logger.debug(f"[TRACE] Получен upload_url: {upload_url}")
        photo_response = requests.get(photo_url, timeout=10)
        photo_response.raise_for_status()
        photo_file = photo_response.content
        upload_response = requests.post(upload_url, files={'photo': ('photo.jpg', photo_file)}, timeout=10)
        upload_response.raise_for_status()
        upload_data = upload_response.json()
        logger.debug(f"[TRACE] Ответ upload: {upload_data}")
        saved_photo = vk.photos.saveMessagesPhoto(
            photo=upload_data['photo'],
            server=upload_data['server'],
            hash=upload_data['hash']
        )
        photo_id = f"photo{saved_photo[0]['owner_id']}_{saved_photo[0]['id']}"
        logger.info(f"[TRACE] Фото загружено в VK: {photo_id}")
        return photo_id
    except (ApiError, requests.RequestException, ClientOSError) as e:
        logger.error(f"[TRACE] Ошибка загрузки фото в VK: {str(e)}")
        raise

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_exception_type((ApiError, requests.RequestException)))
async def post_to_vk(message_text, attachments, group_id, category):
    """Публикует пост в VK."""
    vk = vk_fashion if category == "fashion" else vk_default
    logger.debug(f"[TRACE] post_to_vk: group_id={group_id}, category={category}, attachments={attachments}, text_len={len(message_text)}")
    if not vk:
        logger.error("[TRACE] VK API не инициализирован")
        raise ValueError("VK API не инициализирован")
    try:
        publish_time1 = int(time.time()) + 600
        response = vk.wall.post(
            owner_id=group_id,
            message=message_text,
            attachments=','.join(attachments) if attachments else None,
            from_group=1,
            close_comments=1,
            publish_date=publish_time1
        )
        logger.info(f"[TRACE] Пост создан в VK: post_id={response['post_id']}")
        return True, response['post_id']
    except ApiError as e:
        logger.error(f"[TRACE] Ошибка VK API: {str(e)}")
        return False, str(e)
    except Exception as e:
        logger.error(f"[TRACE] Неизвестная ошибка VK: {str(e)}")
        return False, str(e)

async def translate_with_deepseek(text, api_key, max_length=980):
    logger.debug(f"[TRACE] translate_with_deepseek: длина текста={len(text)}")
    async with aiohttp.ClientSession() as session:
        payload = {
            "model": "deepseek-chat",
            "messages": [
                {
                    "role": "user",
                    "content": (
                        f"Перепиши текст в кратком стиле для Telegram. Один вариант на русском языке, без Markdown, HTML, эмодзи, рекламы, ссылок. "
                        f"Формат: заголовок, пустая строка, текст с абзацами. Макс. длина: {max_length} символов: {text}"
                    )
                }
            ],
            "max_tokens": 750
        }
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        async with session.post("https://api.deepseek.com/v1/chat/completions", json=payload, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                translated_text = data["choices"][0]["message"]["content"]
                if len(translated_text) > max_length:
                    logger.info(f"[TRACE] Текст превышает лимит ({len(translated_text)} > {max_length}), повторная обработка")
                    payload["messages"][0]["content"] = (
                        f"Сократи текст до {max_length-100} символов, сохранив информацию, не указывай итоговое количество символов либо иную постороннюю информацию. Формат: заголовок, пустая строка, текст: {translated_text}"
                    )
                    async with session.post("https://api.deepseek.com/v1/chat/completions", json=payload, headers=headers) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            translated_text = data["choices"][0]["message"]["content"]
                logger.debug(f"[TRACE] Перевод успешен, длина: {len(translated_text)}")
                return translated_text
            logger.warning(f"[TRACE] Ошибка DeepSeek: {resp.status}")
            return text

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_exception_type((TelegramNetworkError, ClientConnectionError, ClientOSError)))
async def send_to_telegram(channel_id, link, header, text, api_key, db_path, category):
    logger.debug(f"[TRACE] send_to_telegram: channel_id={channel_id}, category={category}")
    if not link:
        logger.error(f"[TRACE] Некорректная ссылка: {link}")
        return None, None
    
    news_id = link[43:58]
    logger.debug(f"[TRACE] Сформирован news_id={news_id} для ссылки: {link}")
    
    # Логируем содержимое папки img/msn
    try:
        img_files = os.listdir('img/msn')
        logger.debug(f"[TRACE] Файлы в img/msn: {img_files}")
    except Exception as e:
        logger.error(f"[TRACE] Ошибка доступа к img/msn: {str(e)}")
    
    translated_text = await translate_with_deepseek(f"{header}\n\n{text}", api_key)
    logger.debug(f"[TRACE] Очистка текста, длина: {len(translated_text)}")
    translated_text = re.sub(r'[\*_\[\]]', '', translated_text)
    soup = BeautifulSoup(translated_text, 'html.parser')
    clean_text = soup.get_text()
    
    lines = clean_text.split('\n')
    if lines:
        caption = f"<b>{lines[0].strip()}</b>\n\n"
        caption += '\n\n'.join(line.strip() for line in lines[1:] if line.strip())
    else:
        caption = clean_text
    
    if len(caption) > 1021:
        logger.debug(f"[TRACE] Обрезка до 1000 символов, длина: {len(caption)}")
        caption = caption[:1018]
        last_tag = caption.rfind('>')
        if last_tag != -1 and caption.count('<') > caption.count('>'):
            caption = caption[:last_tag + 1]
        caption += "..."
    elif len(caption) > 4096:
        logger.debug(f"[TRACE] Обрезка до 4096 символов, длина: {len(caption)}")
        caption = caption[:4093]
        last_tag = caption.rfind('>')
        if last_tag != -1 and caption.count('<') > caption.count('>'):
            caption = caption[:last_tag + 1]
        caption += "..."
    
    logger.info(f"[TRACE] Длина подписи: {len(caption)} символов")
    logger.debug(f"[TRACE] Подпись (первые 700): {caption[:700]}...")
    
    button_text = "Переслать в Fashion" if category == "fashion" else "Переслать"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=button_text, callback_data=f"forward_{news_id}"),
            InlineKeyboardButton(text="Переслать и Опубликовать в VK", callback_data=f"forward_vk_{news_id}"),
            InlineKeyboardButton(text="Создать Shorts", callback_data=f"create_shorts_{news_id}")
        ]
    ])
    logger.debug(f"[TRACE] Создана клавиатура: forward_{news_id}, forward_vk_{news_id}, create_shorts_{news_id}")
    
    media = []
    j = 0
    while os.path.isfile(f'img/msn/{news_id}_{j}.png') and j < 10:
        file_path = f'img/msn/{news_id}_{j}.png'
        logger.debug(f"[TRACE] Проверка файла: {file_path}, exists={os.path.exists(file_path)}, readable={os.access(file_path, os.R_OK)}")
        if os.access(file_path, os.R_OK):
            logger.debug(f"[TRACE] Найден файл: {file_path}")
            media.append(InputMediaPhoto(media=FSInputFile(file_path)))
        else:
            logger.warning(f"[TRACE] Файл недоступен: {file_path}")
        j += 1
    
    message_ids = []
    file_ids = []
    try:
        if len(media) == 1:
            logger.debug(f"[TRACE] Отправка одного изображения: news_id={news_id}")
            try:
                message = await bot.send_photo(
                    chat_id=channel_id,
                    photo=media[0].media,
                    caption=caption,
                    parse_mode="HTML",
                    reply_markup=keyboard
                )
            except TelegramBadRequest as e:
                logger.warning(f"[TRACE] Ошибка HTML: {str(e)}. Отправка без HTML")
                message = await bot.send_photo(
                    chat_id=channel_id,
                    photo=media[0].media,
                    caption=clean_caption(caption),
                    parse_mode=None,
                    reply_markup=keyboard
                )
            message_ids.append(message.message_id)
            file_ids.append(message.photo[-1].file_id)
            logger.info(f"[TRACE] Отправлено изображение: news_id={news_id}, message_id={message.message_id}")
        elif len(media) > 1:
            logger.debug(f"[TRACE] Отправка медиагруппы: news_id={news_id}")
            for i, m in enumerate(media):
                try:
                    message = await bot.send_photo(
                        chat_id=channel_id,
                        photo=m.media,
                        caption=caption if i == 0 else None,
                        parse_mode="HTML" if i == 0 else None,
                        reply_markup=keyboard if i == 0 else None
                    )
                except TelegramBadRequest as e:
                    logger.warning(f"[TRACE] Ошибка HTML: {str(e)}. Отправка без HTML")
                    message = await bot.send_photo(
                        chat_id=channel_id,
                        photo=m.media,
                        caption=clean_caption(caption) if i == 0 else None,
                        parse_mode=None if i == 0 else None,
                        reply_markup=keyboard if i == 0 else None
                    )
                message_ids.append(message.message_id)
                file_ids.append(message.photo[-1].file_id)
            logger.info(f"[TRACE] Отправлена медиагруппа: news_id={news_id}, message_ids={message_ids}")
        else:
            logger.debug(f"[TRACE] Отправка текста: news_id={news_id}")
            try:
                message = await bot.send_message(
                    chat_id=channel_id,
                    text=caption,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                    reply_markup=keyboard
                )
            except TelegramBadRequest as e:
                logger.warning(f"[TRACE] Ошибка HTML: {str(e)}. Отправка без HTML")
                message = await bot.send_message(
                    chat_id=channel_id,
                    text=clean_caption(caption),
                    parse_mode=None,
                    disable_web_page_preview=True,
                    reply_markup=keyboard
                )
            message_ids.append(message.message_id)
            logger.info(f"[TRACE] Отправлено текстовое сообщение: news_id={news_id}, message_id={message.message_id}")
        
        logger.debug(f"[TRACE] Сохранение данных: news_id={news_id}")
        try:
            await save_message_data(db_path, news_id, caption, message_ids, file_ids, category)
            logger.info(f"[TRACE] Данные сохранены: news_id={news_id}")
        except Exception as e:
            logger.error(f"[TRACE] Ошибка сохранения: {str(e)}")
            raise
        
    except (TelegramNetworkError, ClientConnectionError, ClientOSError) as e:
        logger.error(f"[TRACE] Сетевая ошибка: news_id={news_id}, ошибка: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"[TRACE] Ошибка отправки: news_id={news_id}, ошибка: {str(e)}")
        return None, None
    
    for j in range(10):
        path = f'img/msn/{news_id}_{j}.png'
        if os.path.isfile(path):
            try:
                os.remove(path)
                logger.debug(f"[TRACE] Удалён файл: {path}")
            except Exception as e:
                logger.warning(f"[TRACE] Ошибка удаления файла: {path}, ошибка: {str(e)}")
    
    logger.debug(f"[TRACE] Завершение send_to_telegram: news_id={news_id}")
    return message_ids[0], news_id

@dp.callback_query(lambda c: c.data.startswith('forward_') and not c.data.startswith('forward_vk_'))
async def process_forward_callback(callback_query: CallbackQuery):
    logger.debug(f"[TRACE] Начало process_forward_callback")
    callback_data = callback_query.data
    logger.debug(f"[TRACE] Получен callback_data: {callback_data}")
    
    try:
        logger.debug(f"[TRACE] Проверка формата: {callback_data}")
        news_id = callback_data.replace('forward_', '')
        logger.debug(f"[TRACE] Извлечён news_id: {news_id}")
        if len(news_id) < 5:
            logger.error(f"[TRACE] Короткий news_id: {news_id}")
            await callback_query.answer("Ошибка: некорректный ID", show_alert=True)
            return
    except Exception as e:
        logger.error(f"[TRACE] Ошибка разбора: {callback_data}, ошибка: {str(e)}")
        logger.error(f"[TRACE] Стек: {traceback.format_exc()}")
        await callback_query.answer("Ошибка: неверный формат", show_alert=True)
        return
    
    logger.debug(f"[TRACE] Проверка переменных")
    if not FORWARD_CHANNEL_ID or not FASHION_CHANNEL_ID or not FINANCE_CHANNEL_ID:
        logger.error(f"[TRACE] Отсутствуют FORWARD_CHANNEL_ID или FASHION_CHANNEL_ID")
        await callback_query.answer("Ошибка: канал не настроен", show_alert=True)
        return
    
    try:
        logger.debug(f"[TRACE] Поиск данных: news_id={news_id}")
        data = await get_message_data("msn_news.db", news_id)
        if not data:
            logger.error(f"[TRACE] Данные не найдены: news_id={news_id}")
            await callback_query.answer(f"Ошибка: данные отсутствуют", show_alert=True)
            return
        
        logger.debug(f"[TRACE] Данные: caption_len={len(data[0])}, message_ids={data[1]}, file_ids={data[2]}, category={data[3]}")
        caption, message_ids, file_ids, category = data
        target_channel = FASHION_CHANNEL_ID if category == "fashion" else FORWARD_CHANNEL_ID
        
        logger.debug(f"[TRACE] Пересылка: target_channel={target_channel}")
        if file_ids:
            media = [InputMediaPhoto(media=file_id) for file_id in file_ids]
            media[0].caption = caption
            media[0].parse_mode = "HTML"
            try:
                await bot.send_media_group(chat_id=target_channel, media=media, disable_notification=True)
            except TelegramBadRequest as e:
                logger.warning(f"[TRACE] Ошибка HTML: {str(e)}. Отправка без HTML")
                media[0].caption = clean_caption(caption)
                media[0].parse_mode = None
                await bot.send_media_group(chat_id=target_channel, media=media, disable_notification=True)
            logger.info(f"[TRACE] Переслана медиагруппа: news_id={news_id}, file_ids={file_ids}")
        else:
            try:
                await bot.copy_message(
                    chat_id=target_channel,
                    from_chat_id=callback_query.message.chat.id,
                    message_id=message_ids[0],
                    disable_notification=True
                )
            except TelegramBadRequest as e:
                logger.warning(f"[TRACE] Ошибка HTML: {str(e)}. Отправка без HTML")
                await bot.copy_message(
                    chat_id=target_channel,
                    from_chat_id=callback_query.message.chat.id,
                    message_id=message_ids[0],
                    disable_notification=True,
                    parse_mode=None
                )
            logger.info(f"[TRACE] Переслано сообщение: news_id={news_id}, message_id={message_ids[0]}")
        await callback_query.answer("Сообщение переслано!")
    except Exception as e:
        logger.error(f"[TRACE] Ошибка пересылки: news_id={news_id}, ошибка: {str(e)}")
        logger.error(f"[TRACE] Стек: {traceback.format_exc()}")
        await callback_query.answer("Ошибка при пересылке", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith('forward_vk_'))
async def process_forward_vk_callback(callback_query: CallbackQuery):
    logger.debug(f"[TRACE] Начало process_forward_vk_callback")
    callback_data = callback_query.data
    logger.debug(f"[TRACE] Получен callback_data: {callback_data}")
    logger.info(f"[TRACE] Начало обработки callback_data: {callback_data}")
    
    # Отправка callback_data в CHANNEL_ID1
    logger.debug(f"[TRACE] Попытка отправки в CHANNEL_ID1={CHANNEL_ID1}")
    try:
        await bot.send_message(
            chat_id=CHANNEL_ID1,
            text=f"[TRACE] Получен callback_data: {callback_data}",
            parse_mode=None
        )
        logger.debug(f"[TRACE] Отправлено в CHANNEL_ID1: {callback_data}")
    except Exception as e:
        logger.error(f"[TRACE] Ошибка отправки в CHANNEL_ID1: {str(e)}")
        logger.error(f"[TRACE] Стек: {traceback.format_exc()}")
    
    try:
        logger.debug(f"[TRACE] Проверка формата: {callback_data}")
        news_id = callback_data.replace('forward_vk_', '')
        logger.debug(f"[TRACE] Извлечён news_id: {news_id}")
        if len(news_id) < 5:
            logger.error(f"[TRACE] Короткий news_id: {news_id}")
            await callback_query.answer("Ошибка: некорректный ID", show_alert=True)
            return
    except Exception as e:
        logger.error(f"[TRACE] Ошибка разбора: {callback_data}, ошибка: {str(e)}")
        logger.error(f"[TRACE] Стек: {traceback.format_exc()}")
        await callback_query.answer("Ошибка: неверный формат", show_alert=True)
        return
    
    logger.debug(f"[TRACE] Проверка переменных")
    if not (FORWARD_CHANNEL_ID and FINANCE_CHANNEL_ID and FASHION_CHANNEL_ID and VK_DEFAULT_TOKEN and VK_FASHION_TOKEN and VK_DEFAULT_GROUP_ID and VK_FASHION_GROUP_ID):
        logger.error(f"[TRACE] Отсутствуют переменные в keys.env")
        await callback_query.answer("Ошибка: конфигурация не настроена", show_alert=True)
        return
    
    try:
        logger.debug(f"[TRACE] Поиск данных: news_id={news_id}")
        data = await get_message_data("msn_news.db", news_id)
        if not data:
            logger.error(f"[TRACE] Данные не найдены: news_id={news_id}")
            await callback_query.answer(f"Ошибка: данные отсутствуют", show_alert=True)
            return
        
        logger.debug(f"[TRACE] Данные: caption_len={len(data[0])}, message_ids={data[1]}, file_ids={data[2]}, category={data[3]}")
        caption, message_ids, file_ids, category = data
        target_channel = FASHION_CHANNEL_ID if category == "fashion" else FINANCE_CHANNEL_ID
        try:
            target_vk_group = int(VK_FASHION_GROUP_ID) if category == "fashion" else int(VK_DEFAULT_GROUP_ID)
        except ValueError as e:
            logger.error(f"[TRACE] Неверный формат VK_GROUP_ID: {VK_FASHION_GROUP_ID if category == 'fashion' else VK_DEFAULT_GROUP_ID}")
            await callback_query.answer("Ошибка: неверная конфигурация VK", show_alert=True)
            return
        
        # Пересылка в Telegram
        logger.debug(f"[TRACE] Пересылка в Telegram: target_channel={target_channel}")
        if file_ids:
            media = [InputMediaPhoto(media=file_id) for file_id in file_ids]
            media[0].caption = caption
            media[0].parse_mode = "HTML"
            try:
                await bot.send_media_group(chat_id=target_channel, media=media, disable_notification=True)
            except TelegramBadRequest as e:
                logger.warning(f"[TRACE] Ошибка HTML: {str(e)}. Отправка без HTML")
                media[0].caption = clean_caption(caption)
                media[0].parse_mode = None
                await bot.send_media_group(chat_id=target_channel, media=media, disable_notification=True)
            logger.info(f"[TRACE] Переслана медиагруппа: news_id={news_id}, file_ids={file_ids}")
        else:
            try:
                await bot.copy_message(
                    chat_id=target_channel,
                    from_chat_id=callback_query.message.chat.id,
                    message_id=message_ids[0],
                    disable_notification=True
                )
            except TelegramBadRequest as e:
                logger.warning(f"[TRACE] Ошибка HTML: {str(e)}. Отправка без HTML")
                await bot.copy_message(
                    chat_id=target_channel,
                    from_chat_id=callback_query.message.chat.id,
                    message_id=message_ids[0],
                    disable_notification=True,
                    parse_mode=None
                )
            logger.info(f"[TRACE] Переслано сообщение: news_id={news_id}, message_id={message_ids[0]}")
        
        # Публикация в VK
        logger.debug(f"[TRACE] Начало публикации в VK: target_vk_group={target_vk_group}")
        attachments = []
        if target_vk_group == int(VK_FASHION_GROUP_ID):
            caption = f'{caption}\n\nhttps://t.me/women_fashionstyle'
        elif target_vk_group == int(VK_DEFAULT_GROUP_ID):
            caption = f'{caption}\n\nhttps://t.me/financemonitoring'
        else:
            caption = caption
        message_text = format_vk_caption(f'{caption}')
        
        if file_ids:
            logger.debug(f"[TRACE] Обработка изображений: file_ids={file_ids}")
            for file_id in file_ids:
                try:
                    logger.debug(f"[TRACE] Получение file_info: file_id={file_id}")
                    file_info = await bot.get_file(file_id)
                    photo_url = f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file_info.file_path}"
                    photo_id = await upload_photo_to_vk(photo_url, target_vk_group, category)
                    if photo_id:
                        attachments.append(photo_id)
                except Exception as e:
                    logger.error(f"[TRACE] Ошибка обработки file_id={file_id}: {str(e)}")
                    continue
        
        logger.debug(f"[TRACE] Вызов post_to_vk: attachments={attachments}")
        success, result = await post_to_vk(message_text, attachments, target_vk_group, category)
        if success:
            logger.info(f"[TRACE] Успешная публикация в VK: news_id={news_id}, post_id={result}")
            await callback_query.answer("Сообщение переслано и опубликовано в VK!")
        else:
            logger.error(f"[TRACE] Ошибка публикации в VK: news_id={news_id}, причина: {result}")
            await callback_query.answer("Переслано, но ошибка в VK!", show_alert=True)
            
    except Exception as e:
        logger.error(f"[TRACE] Ошибка обработки: news_id={news_id}, ошибка: {str(e)}")
        logger.error(f"[TRACE] Стек: {traceback.format_exc()}")
        await callback_query.answer("Ошибка при обработке", show_alert=True)
    finally:
        logger.debug(f"[TRACE] Завершение process_forward_vk_callback")

@dp.callback_query(lambda c: c.data.startswith('create_shorts_'))
async def process_create_shorts_callback(callback_query: CallbackQuery):
    logger.debug(f"[TRACE] Начало process_create_shorts_callback")
    callback_data = callback_query.data
    logger.debug(f"[TRACE] Получен callback_data: {callback_data}")
    
    try:
        original_news_id = callback_data.replace('create_shorts_', '')
        news_id = original_news_id
        while news_id.startswith('vk_'):
            news_id = news_id[3:]
            logger.warning(f"[TRACE] Обнаружен префикс vk_ в create_shorts: {original_news_id} -> {news_id}")
        logger.debug(f"[TRACE] Извлечён news_id: {news_id}")
        if len(news_id) < 5:
            logger.error(f"[TRACE] Короткий news_id: {news_id}")
            await callback_query.answer("Ошибка: некорректный ID", show_alert=True)
            return
    except Exception as e:
        logger.error(f"[TRACE] Ошибка разбора: {callback_data}, ошибка: {str(e)}")
        await callback_query.answer("Ошибка: неверный формат", show_alert=True)
        return
    
    try:
        # Получение данных из базы
        message_data = await get_message_data("msn_news.db", news_id)
        if not message_data:
            logger.error(f"[TRACE] Данные не найдены: news_id={news_id}")
            await callback_query.answer("Ошибка: данные отсутствуют", show_alert=True)
            return
        
        caption, message_ids, file_ids, category = message_data
        logger.debug(f"[TRACE] Данные: caption_len={len(caption)}, message_ids={message_ids}, file_ids={file_ids}, category={category}")
        
        # Получение заголовка из базы
        header_data = await select_for_db("msn_news.db", news_id, "header")
        if header_data:
            header = header_data[0]
            logger.debug(f"[TRACE] Заголовок из базы: {header}")
        else:
            logger.warning(f"[TRACE] Заголовок не найден в базе, извлечение из caption: news_id={news_id}")
            soup = BeautifulSoup(caption, 'html.parser')
            bold_tags = soup.find_all('b')
            header = bold_tags[0].get_text().strip() if bold_tags else caption.split('\n')[0].strip()
            logger.debug(f"[TRACE] Извлечённый заголовок: {header}")
        
        # Получение текста из caption
        soup = BeautifulSoup(caption, 'html.parser')
        text = soup.get_text()
        
        # Загрузка изображений по file_ids
        image_paths = []
        if file_ids:
            for idx, file_id in enumerate(file_ids):
                try:
                    file = await bot.get_file(file_id)
                    file_path = file.file_path
                    local_path = f"tmp/{news_id}_{idx}.png"
                    os.makedirs("tmp", exist_ok=True)
                    await bot.download_file(file_path, local_path)
                    image_paths.append(local_path)
                    logger.debug(f"[TRACE] Изображение загружено: {local_path}")
                except Exception as e:
                    logger.warning(f"[TRACE] Ошибка загрузки изображения {file_id}: {str(e)}")
        if len(text) > 600:
            text = await translate_with_deepseek(text, DEEPSEEK_API_KEY, max_length=450)
        # Генерация видео
        logger.debug(f"[TRACE] Запуск генерации Shorts: news_id={news_id}")
        video_path = await generate_shorts(news_id, header, text, image_paths, category)
        if not video_path:
            logger.error(f"[TRACE] Не удалось создать видео: news_id={news_id}")
            await callback_query.answer("Ошибка при создания видео", show_alert=True)
            return
        
        logger.info(f"[TRACE] Видео создано: {video_path}")
        
        # Отправка видео в тот же канал
        logger.debug(f"[TRACE] Отправка видео в канал: chat_id={callback_query.message.chat.id}, video_path={video_path}")
        try:
            await bot.send_video(
                chat_id=callback_query.message.chat.id,
                video=FSInputFile(video_path),
                caption=f"Shorts: {header}",
                parse_mode="HTML",
                disable_notification=True
            )
            logger.info(f"[TRACE] Видео отправлено: news_id={news_id}, video_path={video_path}")
        except TelegramBadRequest as e:
            logger.warning(f"[TRACE] Ошибка HTML при отправке видео: {str(e)}. Отправка без HTML")
            await bot.send_video(
                chat_id=callback_query.message.chat.id,
                video=FSInputFile(video_path),
                caption=f"Shorts: {header}",
                parse_mode=None,
                disable_notification=True
            )
            logger.info(f"[TRACE] Видео отправлено без HTML: news_id={news_id}, video_path={video_path}")
        except Exception as e:
            logger.error(f"[TRACE] Ошибка отправки видео: news_id={news_id}, ошибка: {str(e)}")
            await callback_query.answer("Ошибка при отправке видео", show_alert=True)
            return
        
        await callback_query.answer(f"Видео создано и отправлено: {video_path}")
        
        # Удаление видеофайла
        try:
            os.remove(video_path)
            logger.debug(f"[TRACE] Удалён видеофайл: {video_path}")
        except Exception as e:
            logger.warning(f"[TRACE] Ошибка удаления видеофайла: {video_path}, ошибка: {str(e)}")
        
        # Очистка временных файлов
        for path in image_paths:
            try:
                os.remove(path)
                logger.debug(f"[TRACE] Удалён файл: {path}")
            except Exception as e:
                logger.warning(f"[TRACE] Ошибка удаления файла: {path}, ошибка: {str(e)}")
        
    except Exception as e:
        logger.error(f"[TRACE] Ошибка обработки Shorts: news_id={news_id}, ошибка: {str(e)}")
        logger.error(f"[TRACE] Стек: {traceback.format_exc()}")
        await callback_query.answer("Ошибка при создании видео", show_alert=True)

@dp.callback_query()
async def debug_callback(callback_query: CallbackQuery):
    logger.debug(f"[TRACE] debug_callback вызван: callback_data={callback_query.data}")

async def start_dispatcher():
    logger.info(f"[TRACE] Диспетчер запущен")
    await dp.start_polling(bot, polling_timeout=15)