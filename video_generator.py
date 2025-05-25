import os
import logging
import io  # Добавлено для gRPC
import requests  # Добавлено для IAM-токена
import grpc  # Добавлено для gRPC
from pydub import AudioSegment  # Добавлено для конвертации аудио
from PIL import Image, ImageDraw, ImageFont
import textwrap
import ffmpeg  # Используем ffmpeg-python
import asyncio
from moviepy.editor import ImageClip, AudioFileClip, concatenate_videoclips
from PIL import ImageOps
import yandex.cloud.ai.tts.v3.tts_pb2 as tts_pb2  # Добавлено для Yandex SpeechKit
import yandex.cloud.ai.tts.v3.tts_service_pb2_grpc as tts_service_pb2_grpc  # Добавлено для Yandex SpeechKit

# Настройка логирования
logging.basicConfig(level=logging.DEBUG, force=True)
logger = logging.getLogger(__name__)
os.makedirs('logs', exist_ok=True)
file_handler = logging.FileHandler('logs/debug_callback.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
file_handler.flush = lambda: file_handler.stream.flush()

logger.info("[TRACE] Загрузка video_generator.py, версия с чередованием изображений v5 от 2025-05-20")

# Настройки Yandex SpeechKit (Добавлено)
FOLDER_ID = "b1gohtlu79ud434biqak"
SERVICE_FUNCTION_ID = "d4ee8ts11lsrrghan8l6"

# Функция для получения IAM-токена (Добавлено)
def iam_renew():
    try:
        url = f"https://functions.yandexcloud.net/{SERVICE_FUNCTION_ID}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logger.debug("New IAM token for Yandex SpeechKit created")
        return data["access_token"]
    except (requests.RequestException, KeyError) as e:
        logger.error(f"Error getting IAM token: {e}")
        raise

# Функция для синтеза речи через Yandex SpeechKit (Добавлено)
def synthesize_speech(iam_token, text, output_file="tmp/speech.wav"):
    request = tts_pb2.UtteranceSynthesisRequest(
        text=text,
        output_audio_spec=tts_pb2.AudioFormatOptions(
            container_audio=tts_pb2.ContainerAudio(
                container_audio_type=tts_pb2.ContainerAudio.WAV
            )
        ),
        hints=[
            tts_pb2.Hints(voice="filipp"),
            tts_pb2.Hints(speed=1.1),
        ],
        loudness_normalization_type=tts_pb2.UtteranceSynthesisRequest.LUFS,
        unsafe_mode=True
    )

    cred = grpc.ssl_channel_credentials()
    channel = grpc.secure_channel("tts.api.cloud.yandex.net:443", cred)
    stub = tts_service_pb2_grpc.SynthesizerStub(channel)

    try:
        it = stub.UtteranceSynthesis(
            request,
            metadata=(("authorization", f"Bearer {iam_token}"),)
        )
        audio = io.BytesIO()
        for response in it:
            audio.write(response.audio_chunk.data)
        audio.seek(0)
        audio_segment = AudioSegment.from_wav(audio)
        audio_segment.export(output_file, format="wav")
        return output_file
    except grpc.RpcError as err:
        logger.error(f"gRPC error: {err}")
        raise

def prepare_image(img_path, header, category, news_id, idx):
    """Подготовка изображения: добавление заголовка."""
    logger.debug(f"[TRACE] prepare_image: img_path={img_path}, header={header}, category={category}, news_id={news_id}, idx={idx}")
    
    try:
        width, height = 1080, 1920
        if img_path and os.path.exists(img_path):
            logger.debug(f"[TRACE] Открытие изображения: {img_path}")
            img = Image.open(img_path).convert("RGB")
        else:
            logger.warning(f"[TRACE] Изображение отсутствует, создание фона: {img_path}")
            img = Image.new("RGB", (width, height), color=(0, 0, 0))
            
            # Масштабирование по ширине с сохранением пропорций
        aspect_ratio = img.width / img.height
        new_width = width
        new_height = int(new_width / aspect_ratio)
        img = img.resize((new_width, new_height), Image.LANCZOS)
        
        # Создание нового изображения с черным фоном
        background = Image.new("RGB", (width, height), color=(0, 0, 0))
        
        # Вставка масштабированного изображения по центру
        paste_x = (width - new_width) // 2
        paste_y = (height - new_height) // 2
        background.paste(img, (paste_x, paste_y))
        img = background
        
        draw = ImageDraw.Draw(img)
        try:
            font_path = os.path.join("fonts", "DejaVuSans.ttf")
            logger.debug(f"[TRACE] Попытка загрузки шрифта: {font_path}")
            if not os.path.exists(font_path):
                raise FileNotFoundError(f"Шрифт не найден по пути: {font_path}")
            font = ImageFont.truetype(font_path, size=50)
        except Exception as e:
            logger.error(f"[TRACE] Ошибка загрузки шрифта DejaVuSans: {str(e)}")
            raise Exception(f"Не удалось загрузить шрифт: {str(e)}")
        
        header = header.upper()
        wrapped_text = textwrap.wrap(header, width=25)
        y_position = 50
        for line in wrapped_text:
            text_bbox = draw.textbbox((0, 0), line, font=font)
            text_width = text_bbox[2] - text_bbox[0]
            x_position = (width - text_width) // 2
            draw.text((x_position, y_position), line, font=font, fill=(255, 255, 255))
            y_position += 70
        
        output_path = f"tmp/{news_id}_background_{idx}.png"
        os.makedirs("tmp", exist_ok=True)
        img.save(output_path, "PNG")
        logger.debug(f"[TRACE] Изображение сохранено: {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"[TRACE] Ошибка в prepare_image: {str(e)}")
        return None

async def generate_shorts(news_id, header, text, image_paths, category):
    """Генерация короткого видео с текстом, озвучкой и чередованием изображений с использованием moviepy."""
    logger.debug(f"[TRACE] Генерация Shorts: news_id={news_id}, category={category}, images={image_paths}")
    
    try:
        # Нормализация news_id для имён файлов (замена начального дефиса на подчёркивание)
        safe_news_id = news_id.lstrip('-').replace('-', '_') if news_id.startswith('-') else news_id
        
        # Генерация аудио с Yandex SpeechKit (Заменено gTTS)
        tts_text = '\n'.join(text.split('\n')[1:]) if '\n' in text else text
        iam_token = iam_renew()  # Получаем IAM-токен
        wav_path = f"tmp/{safe_news_id}_speech.wav"
        audio_path = f"tmp/{safe_news_id}_audio.mp3"
        os.makedirs("tmp", exist_ok=True)
        synthesize_speech(iam_token, tts_text, wav_path)  # Синтез речи в WAV
        audio_wav = AudioSegment.from_file(wav_path, format="wav")
        audio_wav.export(audio_path, format="mp3")  # Конвертация в MP3
        logger.debug(f"[TRACE] Аудио сохранено: {audio_path}")
        
        # Подготовка изображений
        prepared_images = []
        if not image_paths:
            logger.warning("[TRACE] Изображения отсутствуют, используется фон")
            prepared_img = prepare_image(None, header, category, safe_news_id, 0)
            if prepared_img:
                prepared_images.append(prepared_img)
        else:
            for idx, img_path in enumerate(image_paths):
                prepared_img = prepare_image(img_path, header, category, safe_news_id, idx)
                if prepared_img:
                    prepared_images.append(prepared_img)
        
        if not prepared_images:
            logger.error("[TRACE] Не удалось подготовить ни одно изображение")
            return None
        
        # Загрузка аудио и создание видео с помощью moviepy
        audio_clip = AudioFileClip(audio_path)
        duration = audio_clip.duration
        
        clips = []
        duration_per_image = duration / len(prepared_images) if len(prepared_images) > 1 else duration
        for img in prepared_images:
            img_clip = ImageClip(img).set_duration(duration_per_image)
            clips.append(img_clip)
        
        video = concatenate_videoclips(clips, method="compose") if len(clips) > 1 else clips[0]
        video = video.set_audio(audio_clip)
        
        # Сохранение видео
        output_path = f"shorts/{safe_news_id}_shorts.mp4"
        os.makedirs("shorts", exist_ok=True)
        video.write_videofile(output_path, codec="libx264", fps=30, audio_codec="aac")
        logger.debug(f"[TRACE] Видео создано: {output_path}")
        
        # Очистка временных файлов
        for path in [audio_path, wav_path] + prepared_images:  # Добавлен wav_path
            try:
                if os.path.exists(path):
                    os.remove(path)
                    logger.debug(f"[TRACE] Удалён файл: {path}")
            except Exception as e:
                logger.warning(f"[TRACE] Ошибка удаления: {path}, ошибка: {e}")
        
        return output_path
    except Exception as e:
        logger.error(f"[TRACE] Ошибка в generate_shorts: {str(e)}")
        return None