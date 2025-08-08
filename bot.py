import os
import re
import json
import asyncio
import logging
from datetime import datetime
from typing import List, Tuple
import glob
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, MessageEntity
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# Настройки логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LeadBot:
    def __init__(self, token: str, creds_file: str, spreadsheet_name: str, user_tabs: dict):
        self.token = token
        self.creds_file = creds_file
        self.spreadsheet_name = spreadsheet_name
        self.user_tabs = user_tabs

        # Инициализация Google Sheets
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.creds_file, scope)
        client = gspread.authorize(creds)
        self.sheet = client.open(self.spreadsheet_name)

        # Очередь лидов
        self.lead_queue = asyncio.Queue()
        asyncio.create_task(self._lead_worker())

    async def _lead_worker(self):
        while True:
            worksheet_name, lead_data = await self.lead_queue.get()
            try:
                success = await self._add_lead_with_retry(worksheet_name, lead_data)
                if not success:
                    await self._save_to_backup(worksheet_name, lead_data)
            except Exception as e:
                logger.error(f"Ошибка воркера: {e}")
            finally:
                self.lead_queue.task_done()

    async def _add_lead_with_retry(self, worksheet_name: str, lead_data: List[str], retries=3) -> bool:
        for attempt in range(retries):
            try:
                ws = self.sheet.worksheet(worksheet_name)
                ws.append_row(lead_data)
                logger.info(f"Лид добавлен: {lead_data}")
                return True
            except Exception as e:
                logger.warning(f"Ошибка добавления лида ({attempt+1}/{retries}): {e}")
                await asyncio.sleep(2)
        return False

    async def _save_to_backup(self, worksheet_name: str, lead_data: List[str]):
        filename = f"failed_leads_{datetime.now().strftime('%Y%m%d')}.json"
        backup_data = []
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as f:
                backup_data = json.load(f)
        backup_data.append({"worksheet": worksheet_name, "data": lead_data})
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(backup_data, f, ensure_ascii=False, indent=2)
        logger.info(f"Лид сохранён в бэкап: {lead_data}")

    async def restore_backup(self):
        for file in glob.glob("failed_leads_*.json"):
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            for entry in data:
                await self._add_lead_with_retry(entry["worksheet"], entry["data"])
            os.remove(file)
            logger.info(f"Бэкап {file} восстановлен")

    def _extract_all_leads_info(self, text: str, entities=None) -> List[Tuple[str, str]]:
        links = re.findall(r'(https?://[^\s]*amocrm[^\s]*)', text, re.IGNORECASE)

        # Извлечение ссылок из entities
        if entities:
            for ent in entities:
                if ent.type in [MessageEntity.URL, MessageEntity.TEXT_LINK]:
                    url = text[ent.offset: ent.offset + ent.length]
                    if "amocrm" in url.lower() and url not in links:
                        links.append(url)

        usernames = re.findall(r'@(\w+)', text, re.IGNORECASE)

        result = []
        for link in links:
            for user in usernames:
                result.append((user, link))
        return result

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        message = update.message
        text = message.text or message.caption or ""
        if not text:
            return

        leads_info = self._extract_all_leads_info(text, message.entities or message.caption_entities)
        for username, amo_link in leads_info:
            worksheet_name = self.user_tabs.get(username, "All")
            lead_data = [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                text,
                amo_link,
                username,
                message.from_user.username or "Unknown"
            ]
            await self.lead_queue.put((worksheet_name, lead_data))

    async def start(self):
        app = Application.builder().token(self.token).build()
        app.add_handler(MessageHandler(filters.ALL, self.handle_message))
        await app.initialize()
        await app.start()
        await self.restore_backup()
        logger.info("Бот запущен")
        await app.run_polling()

if __name__ == "__main__":
    TOKEN = os.environ.get("TELEGRAM_TOKEN")
    CREDS_FILE = os.environ.get("GOOGLE_CREDS_FILE", "creds.json")
    SPREADSHEET = os.environ.get("SPREADSHEET_NAME")

    USER_TABS = {
        "texnopos_company": "Технопос",
        "abdukhafizov95": "Самарканд",
        "aqly_office": "Хорезм",
        "aqly_uz": "Хорезм",
        "aqly_hr": "Хорезм",
        "billz_namangan": "Наманган",
        "uzstylegroup": "Наманган",
        "utkirraimov": "Джиззак",
        "bob_7007": "Джиззак",
        "farhod_developer": "Термез",
        "burhan_ergashov": "Ташкент",
        "mfarrux": "Бухара",
        "nasimjon_2014": "Бухара",
        "billzfergana": "Фергана",
        "okmurtazaev": "Фергана",
        "bobur_abdukahharov": "Ош",
        "sysadmin7777": "Кходжанд",
        "sibrohimovg": "All",
        "makhmud23": "All",
        "ravshan_billz": "All"
    }

    bot = LeadBot(TOKEN, CREDS_FILE, SPREADSHEET, USER_TABS)
    asyncio.run(bot.start())
