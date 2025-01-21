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

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Загрузка .env
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

if not TELEGRAM_BOT_TOKEN or not SPREADSHEET_ID:
    raise ValueError("Не найдены необходимые переменные окружения: TELEGRAM_BOT_TOKEN или SPREADSHEET_ID")

class LeadBot:
    def __init__(self):
        self.spreadsheet = self.connect_to_google_sheets()
        self.user_tabs = {
            "SALIQ_008": "Технопос",
            "Abdukhafizov95": "Самарканд",
            "aqly_office": "Хорезм",
            "aqly_uz": "Хорезм",
            "Aqly_hr": "Хорезм",
            "utkirraimov": "Джиззак",
            "bob_7007": "Джиззак",
            "user1": "Термез",
            "user2": "Ташкент",
            "user3":"Бухара"
        }
        self.unconfirmed_leads: Dict[int, Dict[str, Any]] = {}

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

    async def handle_message(self, update: Update, context):
        try:
            message_text = update.message.text
            sender = update.message.from_user.username
            chat_id = update.message.chat_id

            if message_text.lower() in ["принял", "ок, спасибо", "ок", "спасибо"]:
                for msg_id, lead_info in list(self.unconfirmed_leads.items()):
                    if lead_info["assigned_to"] == sender:
                        del self.unconfirmed_leads[msg_id]
                        await update.message.reply_text(
                            f"✅ @{sender}, спасибо за подтверждение!"
                        )
                        return

            if "@" in message_text and "amocrm.ru/leads/detail/" in message_text:
                username = message_text.split("@")[1].split()[0]
                amo_link = next((word for word in message_text.split() if "amocrm.ru/leads/detail/" in word), "")
                if self.add_lead_to_sheet(username, message_text, amo_link):
                    sent_message = await update.message.reply_text(
                        f"📨 Лид передан для @{username}!\n\n"
                        "❗️ Пожалуйста, подтвердите получение лида."
                    )
                    self.unconfirmed_leads[sent_message.message_id] = {
                        "assigned_to": username,
                        "amo_link": amo_link
                    }
                else:
                    await update.message.reply_text("❌ Ошибка при сохранении данных.")
            else:
                await update.message.reply_text("⚠️ Неверный формат сообщения.")
        except TelegramError as e:
            logger.error(f"Ошибка Telegram: {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}")

    async def start_command(self, update: Update, context):
        await update.message.reply_text(
            "👋 Добро пожаловать!\n"
            "Отправьте сообщение с упоминанием пользователя и ссылкой на сделку."
        )

    def run(self):
        application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
        application.add_handler(CommandHandler("start", self.start_command))
        application.add_handler(MessageHandler(filters.TEXT, self.handle_message))
        logger.info("🚀 Бот запущен...")
        application.run_polling()

if __name__ == "__main__":
    bot = LeadBot()
    bot.run()
