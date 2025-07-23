import os
import sys
import json
import logging
import gspread
import re
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters
from telegram import Update
from telegram.error import TelegramError, NetworkError, TimedOut
from flask import Flask
from threading import Thread, Timer

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
        self.RENDER_URL = os.getenv("RENDER_EXTERNAL_URL", "")  # URL вашего Render приложения
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

class KeepAliveService:
    """Сервис для поддержания активности Render инстанса"""
    def __init__(self, url: str, interval: int = 840):  # 14 минут
        self.url = url
        self.interval = interval
        self.timer = None
        self.running = False

    def ping_self(self):
        """Отправляем запрос к себе для поддержания активности"""
        try:
            if self.url:
                response = requests.get(self.url, timeout=10)
                logger.info(f"Keep-alive ping: {response.status_code}")
            else:
                logger.info("Keep-alive ping: URL не настроен")
        except Exception as e:
            logger.warning(f"Keep-alive ping failed: {e}")
        
        # Планируем следующий ping
        if self.running:
            self.timer = Timer(self.interval, self.ping_self)
            self.timer.start()

    def start(self):
        """Запуск keep-alive сервиса"""
        if not self.running:
            self.running = True
            logger.info(f"Запуск keep-alive сервиса (интервал: {self.interval}s)")
            self.ping_self()

    def stop(self):
        """Остановка keep-alive сервиса"""
        self.running = False
        if self.timer:
            self.timer.cancel()
        logger.info("Keep-alive сервис остановлен")

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
            "texnopos_company": "Технопос",
            "abdukhafizov95": "Самарканд",
            "aqly_office": "Хорезм",
            "aqly_uz": "Хорезм",
            "aqly_hr": "Хорезм",
            "billz_Namangan": "Наманган",
            "uzstylegroup": "Наманган",
            "utkirraimov": "Джиззак",
            "bob_7007": "Джиззак",
            "farhod_developer": "Термез",
            "burhan_ergashov": "Ташкент",
            "mfarrux": "Бухара",
            "nasimjon_2014": "Бухара",
            "billzfergana": "Фергана",
            "billzfergana": "Фергана",
            "bobur_abdukahharov":"Ош",
            "sysadmin7777":"Кходжанд",
            "ravshan_billz": "All"
        }
        self.application = None
        self.keep_alive = KeepAliveService(config.RENDER_URL)

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
        """Запуск бота с улучшенной обработкой переподключений"""
        # Создаем Flask приложение для поддержания работы на Render
        app = Flask(__name__)
        
        @app.route('/')
        def keep_alive_endpoint():
            return f"Bot is running! Time: {datetime.now().isoformat()}"
        
        @app.route('/health')
        def health_check():
            return {"status": "healthy", "timestamp": datetime.now().isoformat()}
        
        def run_flask():
            app.run(host='0.0.0.0', port=self.config.PORT)

        # Запускаем Flask в отдельном потоке
        Thread(target=run_flask, daemon=True).start()
        
        # Запускаем keep-alive сервис
        self.keep_alive.start()
        
        # Основной цикл с переподключениями
        retry_count = 0
        max_retries = 5
        
        while retry_count < max_retries:
            try:
                logger.info(f"Попытка запуска бота #{retry_count + 1}")
                
                self.application = ApplicationBuilder().token(self.config.TELEGRAM_BOT_TOKEN).build()
                self.application.add_handler(CommandHandler("start", self.start_command))
                self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
                self.application.add_error_handler(self.error_handler)
                
                logger.info("Бот запущен успешно")
                retry_count = 0  # Сбрасываем счетчик при успешном запуске
                
                # Запуск polling с обработкой сетевых ошибок
                self.application.run_polling(
                    poll_interval=1.0,
                    timeout=20,
                    read_timeout=30,
                    write_timeout=30,
                    connect_timeout=30,
                    pool_timeout=30
                )
                
            except (NetworkError, TimedOut) as e:
                retry_count += 1
                wait_time = min(2 ** retry_count, 60)  # Экспоненциальная задержка, максимум 60 сек
                logger.error(f"Сетевая ошибка (попытка {retry_count}/{max_retries}): {e}")
                logger.info(f"Переподключение через {wait_time} секунд...")
                time.sleep(wait_time)
                
            except Exception as e:
                retry_count += 1
                wait_time = min(2 ** retry_count, 60)
                logger.error(f"Критическая ошибка (попытка {retry_count}/{max_retries}): {e}")
                if retry_count < max_retries:
                    logger.info(f"Перезапуск через {wait_time} секунд...")
                    time.sleep(wait_time)
                else:
                    logger.critical("Превышено максимальное количество попыток перезапуска")
                    raise
        
        # Останавливаем keep-alive при завершении
        self.keep_alive.stop()

    async def start_command(self, update: Update, context):
        """Обработка команды /start"""
        await update.message.reply_text("👋 Бот готов к работе! Отправьте сообщение с упоминанием пользователя и ссылкой на сделку.")

    async def error_handler(self, update: Update, context):
        """Обработка ошибок"""
        error = context.error
        
        # Игнорируем некоторые типы ошибок для уменьшения шума в логах
        if isinstance(error, (NetworkError, TimedOut)):
            logger.warning(f"Временная сетевая ошибка: {error}")
            return
            
        logger.error(f"Ошибка обработки обновления: {error}", exc_info=error)

if __name__ == "__main__":
    try:
        config = Config()
        sheets_manager = GoogleSheetsManager(config)
        bot = LeadBot(config, sheets_manager)
        
        logger.info("=== ЗАПУСК БОТА ===")
        logger.info(f"Render URL: {config.RENDER_URL}")
        
        bot.run()
        
    except KeyboardInterrupt:
        logger.info("Получен сигнал завершения работы")
    except Exception as e:
        logger.error(f"Критическая ошибка запуска: {e}", exc_info=True)
        sys.exit(1)