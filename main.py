import asyncio
import os
from dotenv import load_dotenv
from msn_parser import parse_msn
from telegram_bot import send_to_telegram, start_dispatcher
from database import create_table, save_to_db, select_for_db
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Загрузка конфиденциальных данных
load_dotenv('keys.env')
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")

# Проверка переменных окружения
if not all([TELEGRAM_TOKEN, CHANNEL_ID, DEEPSEEK_API_KEY]):
    missing = [key for key, value in {"TELEGRAM_TOKEN": TELEGRAM_TOKEN, "CHANNEL_ID": CHANNEL_ID, "DEEPSEEK_API_KEY": DEEPSEEK_API_KEY}.items() if not value]
    raise ValueError(f"Не найдены переменные окружения: {', '.join(missing)}. Проверьте файл keys.env")

# Источники MSN с категориями
MSN_SOURCES = {
#    "Investing.com": {"url": "https://www.msn.com/en-us/channel/source/Investing.com/sr-vid-09jfs0v25ptvf09rctrgr4yq8xv8me8ecwggjywpbjxqexp44s2a?item=flightsprg-tipsubsc-v1a?loadi", "category": "default"},
    #"Benzinga": {"url": "https://www.msn.com/en-us/channel/source/Benzinga/sr-vid-bev0jc7bneie4wxhbsia4yhgci2wcs69hn4kv2py2hqriaf7em3s?cvid=21d3675a444b47b2874d46851b8c8b49&ei=12", "category": "default"},
    #"AMBCrypto": {"url": "https://www.msn.com/en-us/channel/source/AMBCrypto/sr-cid-e98186bd6b200c7e?cvid=9283fd9486d04370aa174f530f4ec927&ei=8", "category": "default"},
    "Cryptopolitan": {"url": "https://www.msn.com/en-us/channel/source/Cryptopolitan/sr-cid-5d9aa60cd5b751a0?cvid=d79d0b87437e4c42bcbac4f357787bb1&ei=4", "category": "default"},
    "CoinDesk": {"url": "https://www.msn.com/en-us/channel/source/CoinDesk/sr-vid-24nuhyyhqjwd8gwmwc58wwedksacv5dfsifbxr9hy57viwe4v5xa?ocid=msedgntp&cvid=8277ce8140c142d5bdfba053421428e9&ei=1", "category": "default"},
    "CoinTelegraph": {"url": "https://www.msn.com/en-us/channel/source/Coin%20Telegraph/sr-vid-2hru70snc0jyk9hdjii9ggmievarhp55v4ewdgf8rrajvvnbpfxa?ocid=msedgntp&cvid=dcfab9d953ff498f986b6a978ecc61ac&ei=8", "category": "default"},
    #"Investopedia": {"url": "https://www.msn.com/en-us/channel/source/Investopedia/sr-vid-amr3i060khhst72fvq2gyqw8fme80y38vamea03hg5dpncac76rs?cvid=364e18cac36d4344a86511903e207ac8&ei=14", "category": "default"},
    "Bloomberg": {"url": "https://www.msn.com/en-us/channel/source/Bloomberg/sr-vid-08gw7ky4u229xjsjvnf4n6n7v67gxm0pjmv9fr4y2x9jjmwcri4s?item=flightsprg-tipsubsc-v1a%3Floadi&cvid=57ba83f2c655480ca891d077efa8f2ed&ei=11", "category": "default"},
    #"InvestingChannel": {"url": "https://www.msn.com/en-us/channel/source/InvestingChannel/sr-vid-32rnxu4cxf28fhtwkbiukmmkvv886fm9a6wbvkgfy5hh33t8sx0s?item=flightsprg-tipsubsc-v1a%3Floadi&cvid=57ba83f2c655480ca891d077efa8f2ed&ei=11", "category": "default"},
    #"Markets Insider": {"url": "https://www.msn.com/en-us/channel/source/Markets%20Insider/sr-vid-jmweaa5gchk850f3chwmu4bptj84k890wf58uigkj0sn8avp79ts?item=flightsprg-tipsubsc-v1a%3Floadi&cvid=57ba83f2c655480ca891d077efa8f2ed&ei=11", "category": "default"},
    "InStyle": {"url": "https://www.msn.com/en-us/channel/source/InStyle/sr-vid-v69869a93qsbvidbhbnpdrp6bn9cax3yibkp5dbc5wdit2vkbema?ocid=msedgntp&cvid=5c5dca5935174db2939fe0255ba7049f&ei=14", "category": "fashion"},
    "ELLE US": {"url": "https://www.msn.com/en-us/channel/source/ELLE%20US/sr-vid-0gfi0p87cpg5dkrkd9ah7k4h7jebaeufu8c0pdt4cbewim5r4s0s?ocid=msedgntp&cvid=bc3d734afb9b4033a2ed8b2c0e4d0641&ei=6", "category": "fashion"},
    "Redbook": {"url": "https://www.msn.com/en-us/channel/source/Redbook/sr-vid-9x9tj4dghqp3kpdq6nvcxuxvejyn9i4j6e99wvwswj9hgikvx35s?ocid=msedgntp&cvid=7cf996d611a8433f8c64568db1b523a5&ei=4", "category": "fashion"},
    "Woman's Day": {"url": "https://www.msn.com/en-us/channel/source/Womans%20Day/sr-vid-aj5ja2k0nq3frvkmauarpcu2h6avpr7b48400j5inrnktshqf2ya?ocid=msedgntp&cvid=97da98d6f05040d785e08da4c2b5cb15&ei=4", "category": "fashion"},
    #"Delish": {"url": "https://www.msn.com/en-us/channel/source/Delish/sr-vid-sh7xrfvrfe97yvphtxf7akxab5hdkk8smik2p2j3872qbdkeur3s?ocid=msedgntp&cvid=97da98d6f05040d785e08da4c2b5cb15&ei=4", "category": "fashion"},
    "Fashion Times": {"url": "https://www.msn.com/en-us/channel/source/Fashion%20Times/sr-vid-smfkuh4ainj0bscfqkhcrt39i0ehmrxwsk23kf3bucftnmf8bt2a?ocid=msedgntp", "category": "fashion"}
}

async def parse_and_send():
    # Инициализация базы данных
    await create_table("msn_news.db")
    
    for name, source in MSN_SOURCES.items():
        logger.info(f"Парсинг {name}...")
        list_link, list_header, list_text = await parse_msn(name, source["url"])
        
        for link, header, text in zip(list_link, list_header, list_text):
            news_id = link[43:58]
            logger.debug(f"DEBUG: Обработана ссылка: {link}, news_id для таблицы news: {news_id}")
            if await select_for_db("msn_news.db", news_id, "news_id") is None:
                logger.info(f"Сохранение новости {news_id} в базу данных")
                await save_to_db("msn_news.db", news_id, header)
                await send_to_telegram(CHANNEL_ID, link, header, text, DEEPSEEK_API_KEY, "msn_news.db", source["category"])
                await asyncio.sleep(2)  # Задержка для избежания лимитов Telegram
            else:
                logger.info(f"Новость {news_id} уже обработана")
                
        logger.info(f"Завершён парсинг {name}")

async def main():
    # Запуск парсинга в фоновом режиме
    asyncio.create_task(parse_and_send())
    
    # Запуск диспетчера для обработки callback-запросов
    logger.info("Запуск бота для обработки инлайн-кнопок...")
    await start_dispatcher()

async def handle_shutdown():
    logger.info("Остановка бота...")
    tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    logger.info("Бот остановлен")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        loop.run_until_complete(handle_shutdown())
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()