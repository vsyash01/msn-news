from playwright.async_api import async_playwright
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import os
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def download_image(url, path):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                with open(path, 'wb') as f:
                    f.write(await resp.read())
                return True
    return False

async def parse_article(context, link, name):
    page = await context.new_page()
    await page.goto(link, wait_until="commit")
    await asyncio.sleep(3)

    try:
        await page.locator('//fluent-button[@name="Continue reading"]').first.click(timeout=20000)
        logger.info(f"Нажал 'Continue reading' для {link}")
        await asyncio.sleep(5)
    except:
        logger.info(f"Нет кнопки 'Continue reading' для {link}")
        await asyncio.sleep(2)  # Меньшая задержка для коротких статей

    # Извлечение заголовка
    header_elem = await page.query_selector('.viewsHeader')
    header = await header_elem.inner_text() if header_elem else "Без заголовка"

    # Извлечение изображений
    news_id = link[43:58]
    image_paths = []
    imgs_area = await page.query_selector('.article-page')
    if imgs_area:
        imgs = await imgs_area.query_selector_all('.article-image-container img')
        for j, img in enumerate(imgs[:10]):  # Ограничение до 10 изображений
            img_url = await img.get_attribute('src')
            if img_url:
                path = f'img/msn/{news_id}_{j}.png'
                if await download_image(img_url, path):
                    image_paths.append(path)
                    logger.info(f"Скачал изображение {j} для {news_id}")

    # Извлечение текста
    try:
        text = await page.locator('cp-article').first.evaluate("node => node.shadowRoot.innerHTML")
        if not text:
            text = "Текст не найден"
    except:
        text = "Текст не найден"

    soup = BeautifulSoup(text, 'html.parser')
    if name not in ['Benzinga', 'Investopedia', 'CoinTelegraph']:
        for tag in soup.find_all(['a', 'strong']):
            tag.extract()
    text = ''.join(p.get_text() for p in soup.find_all('p') if p.get_text())

    await page.close()
    return link, header, text, image_paths

async def parse_msn(name, url):
    async with async_playwright() as playwright:
        browser = await playwright.firefox.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto(url, wait_until="commit")
        await asyncio.sleep(5)

        list_link = []
        list_header = []
        list_text = []
        list_images = []

        news = await page.query_selector_all('.text')
        links = []
        for item in news:
            text = await item.inner_html()
            flag_start = text.find('https://www.msn.com/')
            flag_end = text.find('" target="_blank"')
            if flag_start >= 0:
                link = text[flag_start:flag_end]
                links.append(link)
        
        # Фильтрация уникальных ссылок
        links = list(dict.fromkeys(links))

        tasks = [parse_article(context, link, name) for link in links]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, tuple):
                link, header, text, images = result
                list_link.append(link)
                list_header.append(header)
                list_text.append(text)
                list_images.append(images)

        await context.close()
        await browser.close()
        return list_link, list_header, list_text