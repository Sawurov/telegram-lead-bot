import os
import sys
import json
import logging
import gspread
import warnings
import asyncio
import re
import time
from datetime import datetime
from dotenv import load_dotenv
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters
from telegram import Update
from telegram.error import TelegramError
from typing import Dict, Any, Optional
from flask import Flask
from threading import Thread

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Загружаем переменные окружения
load_dotenv()

import os
import json
import gspread
from google.oauth2.service_account import Credentials

class Config:
    """production config"""
    """Конфигурация для загрузки и проверки данных"""
    def __init__(self):
        self.TELEGRAM_BOT_TOKEN = self._get_env("TELEGRAM_BOT_TOKEN")
        self.SPREADSHEET_ID = self._get_env("SPREADSHEET_ID")

        google_creds_path = "/etc/secrets/service_account.json"  
        with open(google_creds_path, "r") as file:
            self.GOOGLE_CREDS = json.load(file)

        # Исправляем формат приватного ключа
        self.GOOGLE_CREDS["private_key"] = self.GOOGLE_CREDS["private_key"].replace("\\n", "\n")

        self.PORT = int(os.getenv("PORT", 8080))
        self._validate_google_creds()

    def _get_env(self, key: str, default: Optional[str] = None) -> str:
        value = os.getenv(key, default)
        if value is None:
            raise ValueError(f"Отсутствует переменная окружения: {key}")
        return value

    def _validate_google_creds(self):
        """Проверка учетных данных Google"""
        required_keys = [
            "type", "project_id", "private_key_id",
            "private_key", "client_email", "client_id"
        ]
        for key in required_keys:
            if not self.GOOGLE_CREDS.get(key):
                raise ValueError(f"Отсутствует обязательный ключ в Google Credentials: {key}")

        if "-----BEGIN PRIVATE KEY-----" not in self.GOOGLE_CREDS["private_key"]:
            raise ValueError("Неверный формат приватного ключа")

# Функция подключения к Google Sheets
def connect_to_google_sheets():
    config = Config()
    creds = Credentials.from_service_account_info(config.GOOGLE_CREDS)
    client = gspread.authorize(creds)
    return client

class GoogleSheetsManager:
    """Класс для работы с Google Sheets"""
    def __init__(self, config: Config):
        self.config = config
        self._client = None
        self._spreadsheet = None
        self._retry_count = 0
        self._max_retries = 3
        self._backoff_base = 1.5  # База для экспоненциального бэкоффа
        
    @property
    def client(self):
        """Ленивая инициализация Google Sheets клиента с повторными попытками"""
        if self._client is None:
            for attempt in range(1, self._max_retries + 1):
                try:
                    logger.info(f"Попытка аутентификации Google (попытка {attempt})")
                    self._client = gspread.service_account_from_dict(self.config.GOOGLE_CREDS)
                    # Тестируем подключение
                    self._client.openall()
                    logger.info("Аутентификация Google Sheets успешна")
                    return self._client
                except Exception as e:
                    logger.error(f"Аутентификация не удалась: {str(e)}")
                    if attempt == self._max_retries:
                        raise
                    sleep_time = self._backoff_base ** attempt
                    time.sleep(sleep_time)
        return self._client
    
    @property
    def spreadsheet(self):
        """Ленивая загрузка Google Таблицы с повторными попытками"""
        if self._spreadsheet is None:
            while self._retry_count < self._max_retries:
                try:
                    self._spreadsheet = self.client.open_by_key(self.config.SPREADSHEET_ID)
                    self._retry_count = 0  # Сброс счетчика после успешного подключения
                    break
                except Exception as e:
                    self._retry_count += 1
                    logger.error(f"Ошибка доступа к Google Таблице (попытка {self._retry_count}): {e}")
                    if self._retry_count >= self._max_retries:
                        raise
                    time.sleep(2 ** self._retry_count)
        return self._spreadsheet

    async def append_lead(self, worksheet_name: str, data: list) -> bool:
        """Добавление данных в таблицу с улучшенной обработкой ошибок"""
        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
            worksheet.append_row(data)
            logger.info(f"Данные успешно добавлены в {worksheet_name}")
            return True
        except gspread.exceptions.APIError as e:
            logger.error(f"Ошибка API Google: {e.response.text}")
            return False
        except Exception as e:
            logger.error(f"Непредвиденная ошибка append_lead: {str(e)}")
            return False

class LeadBot:
    def __init__(self, config: Config, sheets_manager: GoogleSheetsManager):
        self.config = config
        self.sheets = sheets_manager
        self.user_tabs = {
            "saliq_008": "Технопос",
            "abdukhafizov95": "Самарканд",
            "aqly_office": "Хорезм",
            "aqly_uz": "Хорезм",
            "aqly_hr": "Хорезм",
            "utkirraimov": "Джиззак",
            "bob_7007": "Джиззак",
            "farhod_developer": "Термез",
            "shoxjaxon055": "Ташкент",
            "user3": "Бухара",
            "ravshan_billz": "All"
        }
        self.unconfirmed_leads: Dict[str, Dict[str, Any]] = {}
        self.lead_timers: Dict[str, asyncio.Task] = {}
        self.waiting_confirmation: Dict[int, str] = {}
        self.application = None
        
        # Шаблоны сообщений
        self.messages = {
            'start': "👋 Добро пожаловать!\nОтправьте сообщение с упоминанием пользователя и ссылкой на сделку.",
            'lead_success': "📨 Лид передан для @{username}!\n\n❗️ Пожалуйста, подтвердите получение лида (можно ответить 'ок' или 'принял')",
            'reminder': "❗ Напоминание для @{username}. Подтвердите получение лида.\nПопытка {attempt}/3",
            'confirmation_success': (
                "✅ Спасибо, @{username}! Лид успешно принят!\n\n"
                "🔍 Пожалуйста:\n"
                "1️⃣ Проверьте и заполните все необходимые поля в AmoCRM\n"
                "2️⃣ Установите корректную стадию сделки\n"
                "3️⃣ Следуйте этапам воронки продаж\n\n"
                "💪 Удачи в работе с клиентом!"
            ),
            'lead_expired': "❌ Лид для @{username} удален из системы, так как не был подтвержден.",
            'error': "⚠️ Произошла ошибка при обработке лида. Пожалуйста, попробуйте еще раз позже."
        }

    async def check_google_connection(self):
        """Проверка подключения к Google Sheets"""
        try:
            test_sheet = self.sheets.spreadsheet
            worksheet = test_sheet.worksheet("All")
            worksheet.append_row(["Connection Test", datetime.now().isoformat()])
            logger.info("Тест подключения к Google Sheets успешен")
            return True
        except Exception as e:
            logger.error(f"Тест подключения к Google Sheets не удался: {e}")
            return False

    async def send_reminder(self, chat_id: int, username: str, attempt: int):
        """Отправка напоминания с обработкой ошибок"""
        try:
            message = self.messages['reminder'].format(username=username, attempt=attempt)
            await self.application.bot.send_message(chat_id=chat_id, text=message)
        except TelegramError as e:
            logger.error(f"Ошибка отправки напоминания: {e}")

    async def handle_confirmation(self, chat_id: int, username: str):
        """Обработка подтверждения лида"""
        if chat_id not in self.waiting_confirmation:
            return False

        # Отменяем все таймеры для этого чата
        for lead_key in list(self.lead_timers.keys()):
            if lead_key.startswith(f"{chat_id}:"):
                self.lead_timers[lead_key].cancel()
                del self.lead_timers[lead_key]

        # Очищаем данные
        for lead_key in list(self.unconfirmed_leads.keys()):
            if lead_key.startswith(f"{chat_id}:"):
                del self.unconfirmed_leads[lead_key]

        del self.waiting_confirmation[chat_id]

        await self.application.bot.send_message(
            chat_id=chat_id,
            text=self.messages['confirmation_success'].format(username=username)
        )
        return True

    async def start_lead_timer(self, chat_id: int, message_id: int, username: str):
        """Запуск таймера подтверждения лида"""
        lead_key = f"{chat_id}:{message_id}"
        reminder_intervals = [120, 180, 180, 180]  # Интервалы в секундах
        
        for attempt, delay in enumerate(reminder_intervals, 1):
            await asyncio.sleep(delay)
            if lead_key not in self.unconfirmed_leads:
                return
                
            if attempt <= 3:
                await self.send_reminder(chat_id, username, attempt)
            else:
                del self.unconfirmed_leads[lead_key]
                if chat_id in self.waiting_confirmation:
                    del self.waiting_confirmation[chat_id]
                await self.application.bot.send_message(
                    chat_id=chat_id,
                    text=self.messages['lead_expired'].format(username=username)
                )
                return

    async def handle_message(self, update: Update, context):
        """Обработка входящих сообщений"""
        try:
            message = update.message
            logger.info(f"Получено сообщение: {message.text}")
            
            if not message or not message.text:
                logger.warning("Получено пустое сообщение")
                return

            # Проверка на подтверждение
            if re.search(r'^(ок|принял|спасибо|ok|oke?)$', message.text, re.I):
                if await self.handle_confirmation(message.chat_id, message.from_user.username):
                    return

            # Поиск лида с улучшенным сопоставлением шаблонов
            logger.info("Пытаемся найти совпадение в сообщении")
            match = re.search(r'(?:@(\w+).*?(https?://[^\s]+amocrm\.ru[^\s]*))|(?:(https?://[^\s]+amocrm\.ru[^\s]*).*?@(\w+))', message.text)
            
            if not match:
                logger.warning("Совпадений не найдено")
                return

            # Извлекаем имя пользователя и ссылку
            groups = match.groups()
            if groups[0] is not None:
                username, amo_link = groups[0], groups[1]
            else:
                username, amo_link = groups[3], groups[2]

            if await self.sheets.append_lead(self.user_tabs.get(username, "All"), 
                                          [datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                                           message.text, 
                                           amo_link]):
                sent_message = await message.reply_text(
                    self.messages['lead_success'].format(username=username)
                )
                
                lead_key = f"{message.chat_id}:{sent_message.message_id}"
                self.unconfirmed_leads[lead_key] = {
                    'assigned_to': username,
                    'amo_link': amo_link
                }
                self.waiting_confirmation[message.chat_id] = username
                self.lead_timers[lead_key] = asyncio.create_task(
                    self.start_lead_timer(message.chat_id, sent_message.message_id, username)
                )
            else:
                await message.reply_text(self.messages['error'])

        except Exception as e:
            logger.error(f"Ошибка обработки сообщения: {e}", exc_info=True)
            await message.reply_text(self.messages['error'])

    async def shutdown(self):
        """Обработка завершения работы"""
        for task in self.lead_timers.values():
            task.cancel()
        await asyncio.gather(*self.lead_timers.values(), return_exceptions=True)

    def run(self):
        """Запуск бота с обработкой ошибок и логикой переподключения"""
        # Проверяем подключение к Google Sheets перед запуском
        if not asyncio.run(self.check_google_connection()):
            logger.critical("Не удалось подключиться к Google Sheets. Выход.")
            sys.exit(1)

        # Создаем Flask приложение для поддержания работы
        app = Flask(__name__)
        
        @app.route('/')
        def keep_alive():
            return "Bot is running!"
        
        def run_flask():
            app.run(host='0.0.0.0', port=self.config.PORT)

        # Запускаем Flask в отдельном потоке
        Thread(target=run_flask, daemon=True).start()
        
        while True:
            try:
                self.application = ApplicationBuilder().token(self.config.TELEGRAM_BOT_TOKEN).build()
                self.application.add_handler(CommandHandler("start", lambda u, c: self.start_command(u, c)))
                self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
                self.application.add_error_handler(self.error_handler)
                
                # Регистрируем обработчик завершения
                self.application.post_shutdown = self.shutdown
                
                # Запускаем бота
                self.application.run_polling(close_loop=False)
            except Exception as e:
                logger.error(f"Бот упал с ошибкой: {e}")
                time.sleep(10)  # Ждем перед повторной попыткой

    async def start_command(self, update: Update, context):
        """Обработка команды /start"""
        try:
            # Тестируем подключение к Google Sheets
            logger.info("Тестируем подключение к Google Sheets...")
            test_result = await self.sheets.append_lead(
                "All",  # Используем лист "All" для тестирования
                ["TEST", "Test Message", "Test Link"]
            )
            if test_result:
                logger.info("Успешно подключились к Google Sheets")
            else:
                logger.error("Не удалось подключиться к Google Sheets")
        except Exception as e:
            logger.error(f"Ошибка при тестировании Google Sheets: {e}")
        
        await update.message.reply_text(self.messages['start'])

    async def error_handler(self, update: Update, context):
        """Обработка ошибок"""
        logger.error(f"Ошибка обработки обновления: {context.error}", exc_info=context.error)

if __name__ == "__main__":
    try:
        config = Config()
        sheets_manager = GoogleSheetsManager(config)
        bot = LeadBot(config, sheets_manager)
        bot.run()
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)