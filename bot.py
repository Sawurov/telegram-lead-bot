import os
import logging
import gspread
import warnings
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters
from telegram import Update
from telegram.error import TelegramError
from typing import Dict, Any
from oauth2client.service_account import ServiceAccountCredentials

warnings.filterwarnings("ignore", message="urllib3")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

if not TELEGRAM_BOT_TOKEN or not SPREADSHEET_ID:
    raise ValueError("Не найдены необходимые переменные окружения")

class LeadBot:
    def __init__(self):
        self.spreadsheet = self.connect_to_google_sheets()
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
        self.waiting_confirmation: Dict[int, str] = {}  # chat_id -> username

    @staticmethod
    def connect_to_google_sheets():
        try:
            credentials = {
                "type": os.getenv("TYPE"),
                "project_id": os.getenv("PROJECT_ID"),
                "private_key_id": os.getenv("PRIVATE_KEY_ID"),
                "private_key": os.getenv("PRIVATE_KEY", "").replace("\\n", "\n"),
                "client_email": os.getenv("CLIENT_EMAIL"),
                "client_id": os.getenv("CLIENT_ID"),
                "auth_uri": os.getenv("AUTH_URI"),
                "token_uri": os.getenv("TOKEN_URI"),
                "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_CERT_URL"),
                "client_x509_cert_url": os.getenv("CLIENT_CERT_URL"),
            }
            client = gspread.service_account_from_dict(credentials)
            return client.open_by_key(SPREADSHEET_ID)
        except Exception as e:
            logger.error(f"Ошибка при подключении к Google Sheets: {e}")
            raise

    def add_lead_to_sheet(self, username: str, message_text: str, user_link: str) -> bool:
        try:
            if username not in self.user_tabs:
                logger.warning(f"Вкладка для пользователя @{username} не найдена")
                return False

            worksheet = self.spreadsheet.worksheet(self.user_tabs[username])
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            worksheet.append_row([current_time, message_text, user_link])
            return True
        except Exception as e:
            logger.error(f"Ошибка при добавлении данных в таблицу: {e}")
            return False

    async def send_reminder(self, chat_id: int, username: str, attempt: int):
        """Отправка напоминания пользователю."""
        try:
            message = (
                f"❗ Напоминание для @{username}. Подтвердите получение лида.\n"
                f"Попытка {attempt}/3"
            )
            await self.application.bot.send_message(chat_id=chat_id, text=message)
        except Exception as e:
            logger.error(f"Ошибка при отправке напоминания: {e}")

    async def handle_confirmation(self, chat_id: int, username: str):
        """Обработка подтверждения получения лида."""
        if chat_id in self.waiting_confirmation:
            # Отменяем все существующие таймеры для этого чата
            for lead_key in list(self.lead_timers.keys()):
                if lead_key.startswith(f"{chat_id}:"):
                    if self.lead_timers[lead_key]:
                        self.lead_timers[lead_key].cancel()
                    del self.lead_timers[lead_key]

            # Очищаем информацию о неподтвержденных лидах
            for lead_key in list(self.unconfirmed_leads.keys()):
                if lead_key.startswith(f"{chat_id}:"):
                    del self.unconfirmed_leads[lead_key]

            # Удаляем из ожидающих подтверждения
            del self.waiting_confirmation[chat_id]

            # Отправляем позитивное сообщение с инструкциями
            success_message = (
                f"✅ Спасибо, @{username}! Лид успешно принят!\n\n"
                "🔍 Пожалуйста:\n"
                "1️⃣ Проверьте и заполните все необходимые поля в AmoCRM\n"
                "2️⃣ Установите корректную стадию сделки\n"
                "3️⃣ Следуйте этапам воронки продаж\n\n"
                "💪 Удачи в работе с клиентом!"
            )
            await self.application.bot.send_message(chat_id=chat_id, text=success_message)
            return True
        return False

    async def start_lead_timer(self, chat_id: int, message_id: int, username: str):
        """Запуск таймера для отслеживания подтверждения лида."""
        lead_key = f"{chat_id}:{message_id}"
        
        # Первое напоминание через 2 минуты
        await asyncio.sleep(120)
        if lead_key in self.unconfirmed_leads:
            await self.send_reminder(chat_id, username, 1)
            
            # Второе напоминание через 3 минуты
            await asyncio.sleep(180)
            if lead_key in self.unconfirmed_leads:
                await self.send_reminder(chat_id, username, 2)
                
                # Третье напоминание через 3 минуты
                await asyncio.sleep(180)
                if lead_key in self.unconfirmed_leads:
                    await self.send_reminder(chat_id, username, 3)
                    
                    # Финальное ожидание 3 минуты перед удалением
                    await asyncio.sleep(180)
                    if lead_key in self.unconfirmed_leads:
                        # Очищаем все данные о лиде
                        del self.unconfirmed_leads[lead_key]
                        if chat_id in self.waiting_confirmation:
                            del self.waiting_confirmation[chat_id]
                        await self.application.bot.send_message(
                            chat_id=chat_id,
                            text=f"❌ Лид для @{username} удален из системы, так как не был подтвержден."
                        )

    async def handle_message(self, update: Update, context):
        try:
            message = update.message
            chat_id = message.chat_id
            message_id = message.message_id
            message_text = message.text.lower() if message.text else ""
            sender_username = message.from_user.username

            # Проверяем подтверждение получения лида
            if message_text in ["принял", "ок", "OK", "Ок", "OK", "спасибо", "ок, спасибо", "оке", "ok", "Ok"  "oke"]:
                if chat_id in self.waiting_confirmation:
                    username = self.waiting_confirmation[chat_id]
                    await self.handle_confirmation(chat_id, username)
                    return

            # Проверяем наличие @username и ссылки AmoCRM
            if "@" in message_text:
                username = message_text.split("@")[1].split()[0].strip()
                amo_link = next((word for word in message_text.split() 
                               if "amocrm.ru/leads/detail/" in word 
                               or "billz.amocrm.ru" in word), None)
                
                if amo_link and username:
                    if self.add_lead_to_sheet(username, message_text, amo_link):
                        sent_message = await message.reply_text(
                            f"📨 Лид передан для @{username}!\n\n"
                            "❗️ Пожалуйста, подтвердите получение лида.(Можно написать в ответь 'ок', 'ok')"
                        )
                        
                        lead_key = f"{chat_id}:{sent_message.message_id}"
                        self.unconfirmed_leads[lead_key] = {
                            "assigned_to": username,
                            "amo_link": amo_link
                        }
                        
                        # Сохраняем информацию об ожидании подтверждения
                        self.waiting_confirmation[chat_id] = username
                        
                        # Запускаем таймер для отслеживания подтверждения
                        self.lead_timers[lead_key] = asyncio.create_task(
                            self.start_lead_timer(chat_id, sent_message.message_id, username)
                        )
                    else:
                        await message.reply_text("❌ Ошибка при сохранении данных.")

        except Exception as e:
            logger.error(f"Ошибка при обработке сообщения: {e}")

    async def start_command(self, update: Update, context):
        await update.message.reply_text(
            "👋 Добро пожаловать!\n"
            "Отправьте сообщение с упоминанием пользователя и ссылкой на сделку."
        )

    def run(self):
        self.application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(MessageHandler(filters.TEXT, self.handle_message))
        logger.info("🚀 Бот запущен...")
        self.application.run_polling()

if __name__ == "__main__":
    bot = LeadBot()
    bot.run()
