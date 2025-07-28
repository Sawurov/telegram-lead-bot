import os
import sys
import json
import logging
import gspread
import re
import time
import requests
import asyncio
import glob
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters
from telegram import Update
from telegram.error import TelegramError, NetworkError, TimedOut
from flask import Flask
from threading import Thread, Timer

# Настройка логирования с более детальным уровнем
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
        self.ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")  # ID администратора для отчетов
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

    def count_leads_for_date(self, worksheet_name: str, date: str) -> int:
        """Подсчет лидов за определенную дату"""
        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
            records = worksheet.get_all_records()
            
            count = 0
            for record in records:
                # Предполагаем, что первая колонка содержит дату
                first_value = list(record.values())[0] if record else ""
                record_date = str(first_value).split(' ')[0]
                if record_date == date:
                    count += 1
                    
            return count
            
        except Exception as e:
            logger.error(f"Ошибка подсчета лидов для {worksheet_name}: {e}")
            return 0

class LeadStatsManager:
    """Менеджер статистики лидов"""
    def __init__(self, sheets_manager: GoogleSheetsManager, user_tabs: dict):
        self.sheets = sheets_manager
        self.user_tabs = user_tabs
        self.daily_stats = defaultdict(lambda: defaultdict(int))
        
    async def get_daily_stats_command(self, update: Update, context):
        """Команда для получения статистики за день"""
        today = datetime.now().strftime("%Y-%m-%d")
        
        try:
            stats_text = "📊 **Статистика лидов за сегодня:**\n\n"
            
            total_leads = 0
            unique_tabs = set(self.user_tabs.values())
            
            for tab_name in unique_tabs:
                count = self.sheets.count_leads_for_date(tab_name, today)
                if count > 0:
                    stats_text += f"• {tab_name}: {count} лидов\n"
                    total_leads += count
            
            stats_text += f"\n🎯 **Всего за день: {total_leads} лидов**"
            
            # Проверяем наличие пропущенных лидов
            backup_count = self._count_backup_leads()
            if backup_count > 0:
                stats_text += f"\n⚠️ **Не записано: {backup_count} лидов**"
                stats_text += "\nВыполните /restore для восстановления"
            
            await update.message.reply_text(stats_text, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            await update.message.reply_text("❌ Ошибка получения статистики")

    def _count_backup_leads(self) -> int:
        """Подсчет лидов в резервных файлах"""
        total_count = 0
        backup_files = glob.glob("failed_leads_*.json")
        
        for backup_file in backup_files:
            try:
                with open(backup_file, 'r', encoding='utf-8') as f:
                    backup_data = json.load(f)
                    total_count += len(backup_data)
            except Exception:
                continue
                
        return total_count

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
            "okmurtazaev": "Фергана",
            "bobur_abdukahharov":"Ош",
            "sysadmin7777":"Кходжанд",
            "ravshan_billz": "All"
        }
        self.application = None
        self.keep_alive = KeepAliveService(config.RENDER_URL)
        self.stats_manager = LeadStatsManager(sheets_manager, self.user_tabs)

    def _extract_message_text(self, message):
        """Извлекает текст из различных типов сообщений"""
        # Приоритет: caption > text > forwarded content
        if message.caption:
            logger.debug("Извлекаем текст из caption")
            return message.caption
        elif message.text:
            logger.debug("Извлекаем текст из text")
            return message.text
        elif hasattr(message, 'forward_from') and message.forward_from:
            logger.debug("Пытаемся извлечь текст из forwarded сообщения")
            return message.text or message.caption
        return None

    def _get_message_type(self, message):
        """Определяет тип сообщения для логирования"""
        if message.photo:
            return "photo"
        elif message.document:
            return "document"
        elif message.video:
            return "video"
        elif message.audio:
            return "audio"
        elif message.voice:
            return "voice"
        elif message.sticker:
            return "sticker"
        elif message.text:
            return "text"
        elif message.caption:
            return "caption"
        else:
            return "unknown"

    def _extract_lead_info(self, text):
        """Улучшенное извлечение информации о лиде с множественными паттернами"""
        
        # Различные паттерны для поиска лидов
        patterns = [
            # Основной паттерн: @username ... ссылка
            r'@(\w+).*?(https?://[^\s]*amocrm\.ru[^\s]*)',
            # Обратный порядок: ссылка ... @username  
            r'(https?://[^\s]*amocrm\.ru[^\s]*).*?@(\w+)',
            # Более гибкий поиск с разными доменами
            r'@(\w+).*?(https?://.*?amocrm.*?)(?:\s|$)',
            r'(https?://.*?amocrm.*?)(?:\s|$).*?@(\w+)',
            # Поиск в начале строки
            r'^@?(\w+).*?(https?://[^\s]*amocrm[^\s]*)',
            # Поиск без @ в начале
            r'(\w+).*?(https?://[^\s]*amocrm\.ru[^\s]*)',
            # Еще более гибкий поиск
            r'@(\w+)[^\n]*?(https?://[^\s]*amocrm[^\s]*)',
        ]
        
        for i, pattern in enumerate(patterns):
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE | re.DOTALL)
            if match:
                groups = match.groups()
                logger.debug(f"Паттерн {i+1} сработал: {groups}")
                
                if len(groups) == 2:
                    # Определяем порядок: username, link или link, username
                    if 'http' in groups[0]:
                        username, link = groups[1], groups[0]
                    else:
                        username, link = groups[0], groups[1]
                    
                    # Проверяем, что username существует в наших табах или это валидный username
                    if username and link and ('amocrm' in link.lower()):
                        return username, link
        
        # Если ничего не найдено, логируем для анализа
        logger.debug(f"Ни один паттерн не сработал для текста: {text[:200]}...")
        return None, None

    async def _add_lead_with_retry(self, worksheet_name, data, max_retries=3):
        """Добавление лида с повторными попытками"""
        for attempt in range(max_retries):
            try:
                success = self.sheets.append_lead(worksheet_name, data)
                if success:
                    return True
                else:
                    logger.warning(f"Попытка {attempt + 1}/{max_retries} не удалась для {worksheet_name}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)  # Экспоненциальная задержка
            except Exception as e:
                logger.error(f"Ошибка при добавлении лида (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
        
        return False

    async def _save_to_backup(self, worksheet_name, data):
        """Сохранение неудачных записей в локальный буфер"""
        try:
            backup_file = f"failed_leads_{datetime.now().strftime('%Y%m%d')}.json"
            
            backup_entry = {
                "timestamp": datetime.now().isoformat(),
                "worksheet": worksheet_name,
                "data": data
            }
            
            # Читаем существующие данные
            try:
                with open(backup_file, 'r', encoding='utf-8') as f:
                    backup_data = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                backup_data = []
            
            # Добавляем новую запись
            backup_data.append(backup_entry)
            
            # Сохраняем обратно
            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, ensure_ascii=False, indent=2)
                
            logger.info(f"Лид сохранен в резервный файл: {backup_file}")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения в резерв: {e}")

    async def handle_message(self, update: Update, context):
        """Улучшенная обработка входящих сообщений с детальным логированием"""
        try:
            message = update.message
            
            if not message:
                logger.warning("Получено обновление без сообщения")
                return

            # Игнорируем сообщения от бота уведомлений
            if message.from_user and message.from_user.username == "billzsalesnotificationsbot":
                logger.info("Игнорируем сообщение от @billzsalesnotificationsbot")
                return

            # Получаем текст из разных типов сообщений
            message_text = self._extract_message_text(message)
            
            if not message_text:
                # Детальное логирование для отладки
                msg_info = {
                    "message_id": message.message_id,
                    "from_user": message.from_user.username if message.from_user else "Unknown",
                    "chat_id": message.chat_id,
                    "message_type": self._get_message_type(message),
                    "has_text": bool(message.text),
                    "has_caption": bool(message.caption),
                    "has_entities": bool(message.entities),
                    "has_caption_entities": bool(message.caption_entities)
                }
                logger.warning(f"Получено сообщение без текста для обработки: {msg_info}")
                return

            logger.info(f"Обрабатываем сообщение от @{message.from_user.username if message.from_user else 'Unknown'}: {message_text[:100]}...")

            # Улучшенный поиск лида с множественными паттернами
            username, amo_link = self._extract_lead_info(message_text)
            
            if not username or not amo_link:
                logger.info(f"Лид не найден в сообщении. Username: {username}, Link: {bool(amo_link)}")
                logger.debug(f"Полный текст сообщения: {message_text}")
                return

            # Определяем таб для пользователя
            worksheet_name = self.user_tabs.get(username, "All")
            
            # Подготавливаем данные для записи
            lead_data = [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                message_text,
                amo_link,
                username,
                message.from_user.username if message.from_user else "Unknown"
            ]
            
            # Добавляем данные в Google Sheets с повторными попытками
            success = await self._add_lead_with_retry(worksheet_name, lead_data)
            
            if success:
                logger.info(f"✅ Лид успешно добавлен для @{username} в таб '{worksheet_name}'")
            else:
                logger.error(f"❌ Не удалось добавить лид для @{username} после всех попыток")
                # Сохраняем в локальный буфер для повторной отправки
                await self._save_to_backup(worksheet_name, lead_data)

        except Exception as e:
            logger.error(f"Критическая ошибка обработки сообщения: {e}", exc_info=True)

    async def restore_failed_leads(self):
        """Восстановление лидов из резервных файлов"""
        backup_files = glob.glob("failed_leads_*.json")
        
        total_restored = 0
        total_failed = 0
        
        for backup_file in backup_files:
            try:
                with open(backup_file, 'r', encoding='utf-8') as f:
                    failed_leads = json.load(f)
                
                restored_count = 0
                for lead in failed_leads:
                    success = await self._add_lead_with_retry(
                        lead["worksheet"], 
                        lead["data"], 
                        max_retries=1
                    )
                    if success:
                        restored_count += 1
                        total_restored += 1
                    else:
                        total_failed += 1
                
                if restored_count > 0:
                    logger.info(f"Восстановлено {restored_count}/{len(failed_leads)} лидов из {backup_file}")
                    # Переименовываем файл после успешного восстановления
                    os.rename(backup_file, f"processed_{backup_file}")
                    
            except Exception as e:
                logger.error(f"Ошибка восстановления из {backup_file}: {e}")
        
        return total_restored, total_failed

    async def restore_failed_leads_command(self, update: Update, context):
        """Команда для восстановления пропущенных лидов"""
        await update.message.reply_text("🔄 Начинаю восстановление пропущенных лидов...")
        
        try:
            restored, failed = await self.restore_failed_leads()
            
            if restored > 0:
                await update.message.reply_text(
                    f"✅ Восстановление завершено!\n"
                    f"📈 Восстановлено: {restored} лидов\n"
                    f"❌ Не удалось: {failed} лидов"
                )
            else:
                await update.message.reply_text("ℹ️ Нет лидов для восстановления")
                
        except Exception as e:
            logger.error(f"Ошибка восстановления: {e}")
            await update.message.reply_text("❌ Ошибка при восстановлении лидов")

    def setup_handlers(self):
        """Настройка всех обработчиков команд"""
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("stats", self.stats_manager.get_daily_stats_command))
        self.application.add_handler(CommandHandler("restore", self.restore_failed_leads_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        self.application.add_error_handler(self.error_handler)

    async def start_command(self, update: Update, context):
        """Обработка команды /start"""
        help_text = """
👋 **Бот готов к работе!**

**Доступные команды:**
• /start - Показать это сообщение
• /stats - Статистика лидов за сегодня
• /restore - Восстановить пропущенные лиды
• /help - Помощь

**Как работает бот:**
Отправьте сообщение с упоминанием пользователя (@username) и ссылкой на сделку AmoCRM.

**Поддерживаемые форматы:**
• Текстовые сообщения
• Сообщения с фото и подписью
• Пересланные сообщения
• Документы с подписью
        """
        await update.message.reply_text(help_text, parse_mode='Markdown')

    async def help_command(self, update: Update, context):
        """Команда помощи"""
        help_text = """
🔧 **Справка по боту лидов**

**Что делает бот:**
✅ Автоматически находит лиды в сообщениях
✅ Сохраняет их в Google Sheets по табам
✅ Ведет статистику по пользователям
✅ Восстанавливает пропущенные записи

**Формат лида:**
`@username ... https://subdomain.amocrm.ru/...`

**Команды:**
• `/stats` - Показать статистику за день
• `/restore` - Восстановить несохраненные лиды

**При проблемах:**
• Проверьте формат сообщения
• Убедитесь, что ссылка содержит amocrm.ru
• Используйте /restore для восстановления
        """
        await update.message.reply_text(help_text, parse_mode='Markdown')

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
                self.setup_handlers()
                
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
        
        logger.info("=== ЗАПУСК ОБНОВЛЕННОГО БОТА ===")
        logger.info(f"Render URL: {config.RENDER_URL}")
        logger.info("Новые функции:")
        logger.info("- Обработка всех типов сообщений (фото, документы, caption)")  
        logger.info("- Улучшенные регулярные выражения для поиска лидов")
        logger.info("- Резервное сохранение при сбоях")
        logger.info("- Команды /stats и /restore")
        logger.info("- Детальное логирование")
        
        bot.run()
        
    except KeyboardInterrupt:
        logger.info("Получен сигнал завершения работы")
    except Exception as e:
        logger.error(f"Критическая ошибка запуска: {e}", exc_info=True)
        sys.exit(1)