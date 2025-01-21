import logging
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CommandHandler, CallbackQueryHandler, CallbackContext, ApplicationBuilder

# Загрузка переменных окружения из .env файла
load_dotenv()

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Получение токена бота из переменных окружения
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')

# Настройки подключения к базе данных
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'database1'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', '3421'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

def get_database_connection():
    """Создает и возвращает соединение с базой данных."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к базе данных: {e}")
        return None

def get_statistics():
    """Извлекает статистику из базы данных и возвращает в виде словаря."""
    conn = get_database_connection()
    if not conn:
        return None

    try:
        with conn.cursor() as cursor:
            stats = {}

            # Количество сообщений
            cursor.execute("""
                SELECT COUNT(*) FROM user_stats WHERE initial_message_sent = TRUE;
            """)
            stats['Количество сообщений'] = cursor.fetchone()[0]

            # Начато диалогов
            cursor.execute("""
                SELECT COUNT(*) FROM user_stats WHERE user_replied = TRUE;
            """)
            stats['Начато диалогов'] = cursor.fetchone()[0]

            # Получили контакты для консультации
            cursor.execute("""
                SELECT COUNT(*) FROM user_stats WHERE sensitive_info_sent = TRUE;
            """)
            stats['Получили контакты для консультации'] = cursor.fetchone()[0]

            # Промежуточная конверсия
            if stats['Количество сообщений'] > 0:
                conversion_rate = (stats['Получили контакты для консультации'] / stats['Количество сообщений']) * 100
            else:
                conversion_rate = 0.0
            stats['Промежуточная конверсия'] = conversion_rate

            return stats
    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        return None
    finally:
        conn.close()

async def start_command(update: Update, context: CallbackContext):
    """Обработчик команды /start."""
    chat_id = update.message.chat_id
    context.chat_data['chat_id'] = chat_id  # Сохраняем chat_id для текущей сессии

    keyboard = [
        [InlineKeyboardButton("Получить статистику", callback_data='get_stats')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    welcome_message = (
        "Благодарим вас за использование нашей услуги!\n\n"
        "Данный бот является вашим личным ботом статистики.\n"
        "При нажатии на кнопку ниже вы получите статистику по вашему заказу в реальном времени.\n\n"
        "Приятного использования"
    )
    await update.message.reply_text(welcome_message, reply_markup=reply_markup)

async def delete_previous_message(context: CallbackContext):
    """Удаляет предыдущее сообщение со статистикой, если оно существует."""
    chat_id = context.chat_data.get('chat_id')
    last_message_id = context.chat_data.get('last_stats_message_id')

    if chat_id and last_message_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=last_message_id)
        except Exception as e:
            logger.warning(f"Не удалось удалить предыдущее сообщение: {e}")

async def send_stats(update: Update, context: CallbackContext):
    """Обработчик кнопки 'Получить статистику'."""
    await delete_previous_message(context)  # Удаляем предыдущее сообщение, если оно есть

    stats = get_statistics()
    if stats:
        stats_message = (
            f"📊 *Статистика:*\n\n"
            f"• Количество сообщений: *{stats['Количество сообщений']}*\n"
            f"• Начато диалогов: *{stats['Начато диалогов']}*\n"
            f"• Получили контакты для консультации: *{stats['Получили контакты для консультации']}*\n\n"
            f"Промежуточная конверсия составляет: *{stats['Промежуточная конверсия']:.2f}%*"
        )
        sent_message = await update.callback_query.message.reply_text(stats_message, parse_mode='Markdown')
        # Сохраняем ID нового сообщения для дальнейшего удаления
        context.chat_data['last_stats_message_id'] = sent_message.message_id
    else:
        await update.callback_query.message.reply_text("Не удалось получить статистику. Попробуйте позже.")

def main():
    """Основная функция запуска бота."""
    # Создание приложения бота
    application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    # Регистрация обработчиков команд
    application.add_handler(CommandHandler('start', start_command))
    application.add_handler(CallbackQueryHandler(send_stats, pattern='get_stats'))

    # Запуск бота
    logger.info("Бот запущен и готов к работе.")
    application.run_polling()

if __name__ == '__main__':
    main()
