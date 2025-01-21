import os
import logging
import gspread
import warnings
import asyncio
from datetime import datetime, timedelta
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, filters
from telegram import InlineKeyboardButton, InlineKeyboardMarkup


warnings.filterwarnings('ignore', message='urllib3')

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    raise ValueError("Ошибка: TELEGRAM_BOT_TOKEN не найден в переменных окружения!")

# Словарь для хранения соответствия пользователей и их вкладок
USER_TABS = {
    "Islomalinuraliev": "Таб1",
    "user2": "Таб2",
    "user3": "Таб3"
    # Добавьте других пользователей и их табы
}

spreadsheet = None

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

        if not all(credentials.values()):
            raise ValueError("Отсутствуют некоторые переменные Google Sheets в .env файле")

        client = gspread.service_account_from_dict(credentials)
        return client.open_by_key("1HfqKVaqUrCkOnvawQ9SkucjIlYujtLtZqcXfgE03lss")
    except Exception as e:
        logging.error(f"Ошибка при подключении к Google Sheets: {e}")
        raise

def add_lead_to_sheet(username, message_text, user_link):
    try:
        global spreadsheet
        if username not in USER_TABS:
            raise ValueError(f"Вкладка для пользователя @{username} не найдена")

        # Получаем нужный лист по имени таба
        worksheet = spreadsheet.worksheet(USER_TABS[username])
        
        # Подготавливаем данные для записи
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Добавляем данные в таблицу
        worksheet.append_row([
            current_time,    # Дата передачи
            message_text,    # Сообщение
            user_link        # Ссылка на лид
        ])
        
        return True
    except Exception as e:
        logging.error(f"Ошибка при добавлении данных в таблицу: {e}")
        return False

unconfirmed_leads = {}

async def remind_confirmation(context, chat_id, message_id, username, amo_link):
    """Функция для отправки напоминаний"""
    while True:
        try:
            if message_id not in unconfirmed_leads:
                break
            
            await context.bot.send_message(
                chat_id,
                f"@{username}, пожалуйста, подтвердите получение лида!\n"
                f"Для подтверждения ответьте 'принял' или 'ок, спасибо'"
            )
            await asyncio.sleep(300)  # Ждем 5 минут
        except Exception as e:
            logging.error(f"Ошибка при отправке напоминания: {e}")
            break

async def check_confirmation_timeout(context, chat_id, message_id, username, amo_link):
    """Функция для проверки таймаута первичного подтверждения"""
    await asyncio.sleep(180)  # Ждем 3 минуты
    if message_id in unconfirmed_leads:
        await context.bot.send_message(
            chat_id,
            f"⚠️ @{username} не подтвердил получение лида в течение 3 минут!\n"
            "Начинаю отправку регулярных напоминаний."
        )
        # Запускаем функцию напоминаний
        asyncio.create_task(remind_confirmation(context, chat_id, message_id, username, amo_link))

async def handle_message(update, context):
    try:
        message_text = update.message.text
        sender = update.message.from_user.username
        chat_id = update.message.chat_id
        
        # Проверяем, является ли сообщение подтверждением
        if message_text.lower() in ['принял', 'ок, спасибо', 'ок', 'спасибо']:
            # Проверяем, есть ли неподтвержденные лиды для этого пользователя
            for msg_id, lead_info in list(unconfirmed_leads.items()):
                if lead_info['assigned_to'] == sender:
                    del unconfirmed_leads[msg_id]
                    await update.message.reply_text(
                        f"✅ @{sender}, спасибо за подтверждение!\n\n"
                        f"⚠️ Пожалуйста, в течение 5 минут:\n"
                        f"1. Оставьте комментарий в AmoCRM\n"
                        f"2. Проверьте и обновите задачи в AmoCRM\n"
                        f"3. Дайте обратную связь в чат\n\n"
                        f"Ссылка на лид: {lead_info['amo_link']}"
                    )
                    return

        # Проверяем наличие и @ и ссылки на AmoCRM
        if "@" in message_text and "amocrm.ru/leads/detail/" in message_text:
            # Извлекаем имя пользователя
            username = message_text.split("@")[1].split()[0]
            
            # Извлекаем ссылку на AmoCRM
            amo_link = ""
            for word in message_text.split():
                if "amocrm.ru/leads/detail/" in word:
                    amo_link = word
                    break
            
            # Добавляем данные в таблицу
            if add_lead_to_sheet(username, message_text, amo_link):
                # Отправляем сообщение о передаче лида
                sent_message = await update.message.reply_text(
                    f"📨 Лид передан для @{username}!\n\n"
                    f"❗️ @{username}, пожалуйста, подтвердите получение лида в течение 3 минут, ответив:\n"
                    f"'принял' или 'ок, спасибо'\n\n"
                    f"Ссылка на лид: {amo_link}"
                )
                
                # Сохраняем информацию о неподтвержденном лиде
                unconfirmed_leads[sent_message.message_id] = {
                    'assigned_to': username,
                    'amo_link': amo_link,
                    'timestamp': datetime.now()
                }
                
                # Запускаем проверку таймаута
                asyncio.create_task(check_confirmation_timeout(
                    context, 
                    chat_id, 
                    sent_message.message_id, 
                    username, 
                    amo_link
                ))
            else:
                await update.message.reply_text(
                    "❌ Произошла ошибка при сохранении данных."
                )
        else:
            # Пропускаем сообщения, которые не содержат @ и ссылку на AmoCRM
            if not (message_text.lower() in ['принял', 'ок, спасибо', 'ок', 'спасибо']):
                await update.message.reply_text(
                    "⚠️ Пожалуйста, отправьте сообщение в корректном формате:\n"
                    "- Упоминание пользователя (@username)\n"
                    "- Ссылка на сделку в AmoCRM"
                )
    except Exception as e:
        logging.error(f"Ошибка при обработке сообщения: {e}")
        await update.message.reply_text("❌ Произошла ошибка при обработке сообщения.")

async def start_command(update, context):
    await update.message.reply_text(
        "👋 Добро пожаловать!\n\n"
        "Для передачи лида, отправьте сообщение с упоминанием пользователя (@username).\n"
        "Данные будут автоматически добавлены в соответствующую вкладку таблицы."
    )

def main():
    try:
        # Инициализируем подключение к Google Sheets
        global spreadsheet
        spreadsheet = connect_to_google_sheets()

        # Создание приложения
        application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

        # Добавляем обработчики
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(MessageHandler(filters.TEXT, handle_message))

        # Запуск бота
        print("🚀 Бот запущен...")
        application.run_polling()

    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")
        raise

if __name__ == "__main__":
    main()