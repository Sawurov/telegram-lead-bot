import os
import sys
import json
import logging
import gspread
import re
from datetime import datetime
from dotenv import load_dotenv
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters
from telegram import Update
from telegram.error import TelegramError
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

class Config:
    """Конфигурация для загрузки и проверки данных"""
    def __init__(self):
        self.TELEGRAM_BOT_TOKEN = self._get_env("TELEGRAM_BOT_TOKEN")
        self.SPREADSHEET_ID = self._get_env("SPREADSHEET_ID")
        
        # Загружаем учетные данные Google как JSON-строку и преобразуем в словарь
        google_creds_json = self._get_env("GOOGLE_CREDENTIALS")
        self.GOOGLE_CREDS = json.loads(google_creds_json)

        # Исправляем формат приватного ключа
        self.GOOGLE_CREDS["private_key"] = self.GOOGLE_CREDS["private_key"].replace("\\n", "\n")

        self.PORT = int(os.getenv("PORT", 8080))
        self._validate_google_creds()

    def _get_env(self, key: str, default: str = None) -> str:
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

class GoogleSheetsManager:
    """Класс для работы с Google Sheets"""
    def __init__(self, config: Config):
        self.config = config
        self._client = None
        self._spreadsheet = None
        
    @property
    def client(self):
        """Ленивая инициализация Google Sheets клиента"""
        if self._client is None:
            try:
                logger.info("Аутентификация Google Sheets...")
                self._client = gspread.service_account_from_dict(self.config.GOOGLE_CREDS)
                logger.info("Аутентификация Google Sheets успешна")
            except Exception as e:
                logger.error(f"Ошибка аутентификации Google Sheets: {str(e)}")
                raise
        return self._client
    
    @property
    def spreadsheet(self):
        """Ленивая загрузка Google Таблицы"""
        if self._spreadsheet is None:
            try:
                self._spreadsheet = self.client.open_by_key(self.config.SPREADSHEET_ID)
            except Exception as e:
                logger.error(f"Ошибка доступа к Google Таблице: {e}")
                raise
        return self._spreadsheet

    def append_lead(self, worksheet_name: str, data: list) -> bool:
        """Добавление данных в таблицу"""
        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
            worksheet.append_row(data)
            logger.info(f"Данные добавлены в {worksheet_name}: {data}")
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления данных: {str(e)}")
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
        self.application = None

    async def handle_message(self, update: Update, context):
        """Обработка входящих сообщений"""
        try:
            message = update.message
            
            if not message or not message.text:
                logger.warning("Получено пустое сообщение")
                return

            # Игнорируем сообщения от бота уведомлений
            if message.from_user and message.from_user.username == "billzsalesnotificationsbot":
                logger.info("Игнорируем сообщение от @billzsalesnotificationsbot")
                return

            # Поиск лида с улучшенным сопоставлением шаблонов
            match = re.search(r'(?:@(\w+).*?(https?://[^\s]+amocrm\.ru[^\s]*))|(?:(https?://[^\s]+amocrm\.ru[^\s]*).*?@(\w+))', message.text)
            
            if not match:
                logger.info("Лид не найден в сообщении")
                return

            # Извлекаем имя пользователя и ссылку
            groups = match.groups()
            if groups[0] is not None:
                username, amo_link = groups[0], groups[1]
            else:
                username, amo_link = groups[3], groups[2]

            # Определяем таб для пользователя
            worksheet_name = self.user_tabs.get(username, "All")
            
            # Добавляем данные в Google Sheets
            success = self.sheets.append_lead(
                worksheet_name, 
                [datetime.now().strftime("%Y-%m-%d %H:%M:%S"), message.text, amo_link]
            )
            
            if success:
                logger.info(f"Лид успешно добавлен для @{username} в таб {worksheet_name}")
            else:
                logger.error(f"Не удалось добавить лид для @{username}")

        except Exception as e:
            logger.error(f"Ошибка обработки сообщения: {e}", exc_info=True)

    def run(self):
        """Запуск бота"""
        # Создаем Flask приложение для поддержания работы на Render
        app = Flask(__name__)
        
        @app.route('/')
        def keep_alive():
            return "Bot is running!"
        
        def run_flask():
            app.run(host='0.0.0.0', port=self.config.PORT)

        # Запускаем Flask в отдельном потоке
        Thread(target=run_flask, daemon=True).start()
        
        try:
            self.application = ApplicationBuilder().token(self.config.TELEGRAM_BOT_TOKEN).build()
            self.application.add_handler(CommandHandler("start", self.start_command))
            self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
            self.application.add_error_handler(self.error_handler)
            
            logger.info("Запуск бота...")
            self.application.run_polling()
            
        except Exception as e:
            logger.error(f"Критическая ошибка при запуске бота: {e}")
            raise

    async def start_command(self, update: Update, context):
        """Обработка команды /start"""
        await update.message.reply_text("👋 Бот готов к работе! Отправьте сообщение с упоминанием пользователя и ссылкой на сделку.")

    async def error_handler(self, update: Update, context):
        """Обработка ошибок"""
        logger.error(f"Ошибка: {context.error}", exc_info=context.error)

if __name__ == "__main__":
    try:
        config = Config()
        sheets_manager = GoogleSheetsManager(config)
        bot = LeadBot(config, sheets_manager)
        bot.run()
    except Exception as e:
        logger.error(f"Критическая ошибка запуска: {e}", exc_info=True)
        sys.exit(1)