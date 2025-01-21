import asyncio
import logging
import os
import openai
import asyncpg
from dotenv import load_dotenv
from pyrogram import Client
import pandas as pd
from asyncpg import exceptions
import argparse
from datetime import datetime, timedelta
import re
from collections import defaultdict
import json  # Добавлено для работы с JSON
import random


# Инициализация контекста и словаря сообщений для каждого пользователя
context = defaultdict(list)
user_messages = defaultdict(list)
timers = {}

# Загрузка переменных окружения из .env файла
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

# Настройка логгера
log_filename = os.path.join(os.path.dirname(__file__), 'script_logs.txt')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Загрузка аргументов командной строки
parser = argparse.ArgumentParser(description="Запуск скрипта с уникальным именем индекса")
parser.add_argument('--index_name', type=str, required=True, help='Уникальное имя индекса для скрипта')
args = parser.parse_args()

index_name = args.index_name

# Путь к Excel файлу с юзернеймами
EXCEL_FILE = '/opt/Project501/usernames.xlsx'
COLUMN_NAME = 'Script1'
COLUMN_NAME = 'Script1name'
BATCH_SIZE = 4

# Путь к директории логов
LOGS_DIR = '/opt/Project501/Logs'
os.makedirs(LOGS_DIR, exist_ok=True)

# Асинхронное подключение к базе данных PostgreSQL
async def create_db_connection():
    return await asyncpg.connect(
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

# Создание таблицы статистики и индекса, если их нет
async def create_tables():
    conn = await create_db_connection()
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS user_stats (
        id SERIAL PRIMARY KEY,
        username VARCHAR(255) UNIQUE NOT NULL,
        user_replied BOOLEAN DEFAULT FALSE,
        message_count INT DEFAULT 0,
        sensitive_info_sent BOOLEAN DEFAULT FALSE,
        initial_message_sent BOOLEAN DEFAULT FALSE,
        qualification VARCHAR(50),
        summary TEXT,
        monthly_budget INT,
        consultation_agreed BOOLEAN,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS processing_index (
        id SERIAL PRIMARY KEY,
        index_name VARCHAR(255) UNIQUE NOT NULL,
        current_index INT DEFAULT 0
    );
    """)
    await conn.close()

# Вызов функции создания таблиц
async def initialize_tables():
    await create_tables()

# Функция для получения текущего индекса из базы данных
async def get_current_index(conn, index_name):
    row = await conn.fetchrow("""
        SELECT current_index FROM processing_index WHERE index_name=$1;
    """, index_name)
    if row:
        print(f"Индекс найден для {index_name}: {row['current_index']}")
        return row['current_index']
    else:
        print(f"Индекс не найден для {index_name}, создаем новый.")
        await conn.execute("""
            INSERT INTO processing_index (index_name, current_index) VALUES ($1, 0)
            ON CONFLICT (index_name) DO NOTHING;
        """, index_name)
        return 0

# Функция для обновления индекса в базе данных
async def update_current_index(conn, new_index, index_name):
    await conn.execute("""
        UPDATE processing_index SET current_index=$1 WHERE index_name=$2;
    """, new_index, index_name)
    print(f"Индекс обновлен для {index_name}: {new_index}")

# Асинхронная функция для обновления статистики
async def log_and_update_stats_db(conn, username, user_replied, message_count, sensitive_info_sent,
                                  initial_message_sent, qualification=None, summary=None,
                                  monthly_budget=None, consultation_agreed=None):
    try:
        print(f"Запись в БД: username={username}, user_replied={user_replied}, message_count={message_count}, "
              f"sensitive_info_sent={sensitive_info_sent}, initial_message_sent={initial_message_sent}, "
              f"qualification={qualification}, summary={summary}, monthly_budget={monthly_budget}, "
              f"consultation_agreed={consultation_agreed}")
        await conn.execute("""
            INSERT INTO user_stats (
                username, user_replied, message_count, sensitive_info_sent, 
                initial_message_sent, qualification, summary, monthly_budget, 
                consultation_agreed
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (username) 
            DO UPDATE SET 
                user_replied = EXCLUDED.user_replied,
                message_count = EXCLUDED.message_count,
                sensitive_info_sent = EXCLUDED.sensitive_info_sent,
                initial_message_sent = EXCLUDED.initial_message_sent,
                qualification = EXCLUDED.qualification,
                summary = EXCLUDED.summary,
                monthly_budget = EXCLUDED.monthly_budget,
                consultation_agreed = EXCLUDED.consultation_agreed,
                updated_at = CURRENT_TIMESTAMP;
        """, username, user_replied, message_count, sensitive_info_sent, initial_message_sent,
           qualification, summary, monthly_budget, consultation_agreed)
    except exceptions.PostgresError as e:
        print(f"Ошибка при записи в базу данных: {e}")

# Функция для чтения одного юзернейма из Excel файла
def load_single_username_from_excel(username_column, name_column, start_index):
    df = pd.read_excel(EXCEL_FILE)
    usernames = df[username_column].dropna().tolist()
    names = df[name_column].dropna().tolist()

    if start_index >= len(usernames):
        return None, None, start_index

    return usernames[start_index], names[start_index], start_index + 1



# Список аккаунтов с данными для авторизации
accounts = [{'session_name': '+79863547134'}]

# Определение начального сообщения (будет заменено новым промптом)
initial_messages = [
    (
        "Script1name, Здравствуйте!\n\n"
        "Наша компания производит систему очистки воды!\n\n"
        "Мы помогаем людям заботиться о своем здоровье за счет употребления воды самого высокого качества.\n\n"
        "Подскажите, вам интересно подробнее узнать, как вода влияет на наше здоровье?"
    ),
    (
        "Script1name, Добрый день!\n\n"
        "Мы занимаемся производством систем очистки воды, чтобы вы могли наслаждаться водой высочайшего качества.\n\n"
        "Хотите узнать, как вода влияет на здоровье?"
    ),
    (
        "Здравствуйте, Script1name\n\n"
        "Мы занимаемся созданием систем очистки воды, чтобы вы могли заботиться о здоровье с лучшей водой.\n\n"
        "Интересно узнать больше о роли воды в поддержании здоровья?"
    )
]

# Промпт для первой нейросети
prompt_template_1 = [
    {
        "role": "system",
        "content": (
            "Ты - Анастасия, менеджер компании ПВВК. Наша компания производит систему фильтрации воды"
            "Твоя задача предлагать клиентам посетить вебинар нашей компании. Ты не предлагаешь им помощь. Избегай слов 'как я могу вам помочь'. "
            "Действуй строго по шагам:"
            "#Шаг 0. Задай клиенту этот вопрос, добавив к фразе как вода влияет на наше здоровье гиппер ссылку : 'Предлагаю прочитать наш гайд: [как вода влияет на наше здоровье]:(https://drive.google.com/file/d/1pDR1jvWo3vvh65XFCTKYH4PzfXOy8t5P/view) ' "
            "#Слово гайд в сообщени шага 0 должно встречаться только 1 раз"
            "#Никогда не объединяй в одно сообщение вопросы из шага 0 и шага 1"
            "#Шаг 1. Проанализируй весь диалог с пользователем, если ты уже спрашивала его номер телефона, то не добавляй эту часть вопроса еще раз. Просьба оставить номер телефона, должна быть всего 1 за весь диалог и задай клиенту этот вопрос, разбив его на 3 абзаца: 'Прикрепляю ссылку на сайт https://pvvk.ru/veb, где будет проходить вебинар.\n\nВебинар проходит ежедневно в 12:00 и 19:00 по мск.\n\nТак же прошу вас оставить ваш номер телефона для обратной связи)'"
            "#Шаг 2. Если клиент согласился прийти на вебинар и пишет 'хорошо, я прийду' или 'я буду', то ответь 'Отлично! Будем рады видеть вас на вебинаре'"
            "Если клиент отказывается от диалога и пишет 'нет' или  'не интересно', то ответь ему, разбив на 2 абзаца так: 'Хорошо. Если все-таки будет желание ознакомиться с нашей компанией, оставлю ссылку на наш сайт https://pvvk.ru/\n\nЕсли будут вопросы, пишите, я всегда на связи. До свидания', не используй слово 'понимаю'"
            "Если клиент спрашивает делаем ли мы 'пробное видео', то отправь ему контакты менеджера 'С вами свяжется наш менеджер в ближайшее время.\nЕго аккаунт: [@AI_griban](https://t.me/AI_griban)'"
            "Если клиент у спашивает у тебя какую либо информацию или высказывает возражение ищи ответы в 'ОТВЕТЫ НА ВОПРОСЫ'."
            "Никогда не объединяй шаги в одно сообщение. Действуй сторого по шагам." 
            "Если ты не смогла найти ответ на вопрос клиента, то ответь так: 'Я чуть позже уточню информацию у менеджера. А пока предлагаю вернуться к вопросу'и добавь вопрос из нынешнего шага, если в этом шаге есть вопрос. "            

            "# ОТВЕТЫ НА ВОПРОСЫ:\n"

            "# Если клиент спрашивает, 'ты бот?' или 'ты искуственный интеллект?' \n"
            "  \"  Ответь 'нет', представься еще раз и переходи к нынешнему шагу\""

            "# Если клиент спрашивает, сколько длится вебинар:\n"
            "  \" Вебинар длится час. За это время вы узнаете как вода влияет на наше здоровье, различие между разными способами очистки воды, а также преимущества нашего фильтра.\"\n"

            "# Если клиент спрашивает, Давайте сразу к делу/ а что вы предлагаете:\n"
            "  \" Предлагаю вам посетить вебинар. На нем вы узнаете как вода влияет на наше здоровье, различие между разными способами очистки воды, а также преимущества нашего фильтра\"\n"

            "# Если клиент спрашивает, где можно ознакомиться с нашей компанией или просит ссылку на сайт.\n"
            "  \" С нашей компанией можно ознакомиться на сайте https://pvvk.ru/\"\n"

            "# Если клиент спрашивает, стоимость фильтра:\n"
            "  \"Стоимость аппарата для очистки воды стоит 85.000р. Но если посетите вебинар, будет скидка, а также подарки\"\n"


            "# Если клиент спрашивает, какой принцип работы/очистки у наших фильтров:\n"
            "  \"Эта информация есть у нас на сайте. Прикрепляю прямую ссылку на этот раздел:https://pvvk.ru/printsipraboti \"\n"

 
           "# Если клиент спрашивает о о наличие сертификатов или проверках наших фильтров:\n"
            "  \"Эта информация есть у нас на сайте. Прикрепляю прямую ссылку на этот раздел: https://pvvk.ru/sertifikati \"\n"

            "# Если клиент спрашивает про отзывы или про наличие рекомендаций академиков науки профессоров:\n"
            "  \"Эта информация есть у нас на сайте. Прикрепляю прямую ссылку на этот раздел:https://pvvk.ru/otzivi \"\n"


            "# Если клиент спрашивает нужно ли подключать наш аппарат к водопроводу:\n"
            "  \"Нет, наша система очистки воды стационарна, нужно только подключение к розетке \"\n"

            "# Если клиент спрашивает о преимуществах наших фильтров или задает вопрос в чем отличие нашей системы от остальных:\n"
            "  \"Главное преимущество - более тщательная очистка воды. Но именно за счет чего мы этого добиваемся - будет рассказано на вебинаре \"\n"


            "# Если клиент спрашивает какие услуги вы предоставляете?:\n"
            "  \"Мы предлагаем систему очистки воды, которые в разы лучше популярных средств фильтрации \"\n"


	   "#  Всегда будь вежлива при общении с клиентом. Если ты задаешь вопросы клиенту, то делай это максимально вежливо" 

	   "#  В кадом твоем сообщении должно быть не более 1 вопроса."

        )
    }
]


def validate_messages(messages):
    if not isinstance(messages, list):
        raise ValueError("Expected 'messages' to be a list.")
    for msg in messages:
        if not isinstance(msg, dict):
            raise ValueError("Each message should be a dictionary.")
        if "role" not in msg or "content" not in msg:
            raise ValueError("Each message dictionary should contain 'role' and 'content'.")

# Обновленная функция для получения ответа от нейросети 1
async def get_4o_answer(messages, max_retries=3, temperature=0.7, top_p=0.6):
    retries = 0
    # Объединяем начальный промпт с контекстом сообщений
    full_context = prompt_template_1.copy()
    full_context.extend(messages)

    print(f"Запускаем get_4o_answer с контекстом: {full_context}")  # Лог перед отправкой в API
    while retries < max_retries:
        try:
            print(f"Попытка #{retries + 1} отправки запроса к OpenAI API")  # Лог попытки вызова API
            response = await openai.ChatCompletion.acreate(
                model="gpt-4o-mini",
                messages=full_context,
                temperature=temperature,
                top_p=top_p
            )
            content = response['choices'][0]['message']['content'].strip()
            print(f"Ответ от модели: {content}")  # Логируем ответ
            return content
        except openai.error.RateLimitError as e:
            print(f"Превышен лимит запросов к OpenAI API: {e}")
            retries += 1
        except openai.error.APIConnectionError as e:
            print(f"Ошибка подключения к OpenAI API: {e}")
            retries += 1
        except openai.error.InvalidRequestError as e:
            print(f"Некорректный запрос к OpenAI API: {e}")
            break
        except Exception as e:
            print(f"Неизвестная ошибка при запросе к OpenAI API: {e}")
            retries += 1

    print(f"Не удалось получить ответ после {retries} попыток")
    return "Извините, мне сейчас неудобно слушать ваше сообщение в таком формате. Можете написать текстом?"




# Функция для сохранения диалога в файл
def save_dialog_to_file(username, messages):
    user_log_file = os.path.join(LOGS_DIR, f"{username}.txt")
    with open(user_log_file, 'a', encoding='utf-8') as file:
        for message in messages:
            file.write(f"{message['role'].capitalize()}: {message['content']}\n\n")

# Функция для отправки приветственного сообщения и запуска таймера
async def send_message(client, username, client_name, context, conn):
    try:
        # Случайный выбор приветственного сообщения
        initial_message = random.choice(initial_messages)
        personalized_message = initial_message.replace('Script1name', client_name)

        logging.info(f"Отправка сообщения пользователю {username}: {personalized_message}")

        # Инициализация статистики в context
        if 'stats' not in context:
            context['stats'] = {}

        if username not in context['stats']:
            context['stats'][username] = {
                "user_replied": False,
                "message_count": 0,
                "sensitive_info_sent": False,
                "initial_message_sent": True,
                "reminder_sent": False
            }

        await client.send_message(username, personalized_message)
        logging.info(f"Приветственное сообщение успешно отправлено пользователю {username}")

        # Логирование в базу данных
        await log_and_update_stats_db(
            conn,
            username=username,
            user_replied=False,
            message_count=context['stats'][username].get('message_count', 0) + 1,
            sensitive_info_sent=False,
            initial_message_sent=True
        )

        # Таймер для напоминания
        asyncio.create_task(reminder_timer(client, username, context, conn))
        await asyncio.sleep(1)
    except Exception as e:
        logging.error(f"Ошибка при отправке сообщения пользователю {username}: {e}")


# Функция, запускающая таймер на 2 часа
async def reminder_timer(client, username, context, conn):
    reminder_message = (
        "Хотелось бы задать вам буквально пару вопросов. Это не займет много времени\n"
        "Буду рада вашему ответу!"
    )
    try:
        # Ждем 2 часа (7200 секунд)
        await asyncio.sleep(7200)

        # Проверяем, если пользователь не ответил и напоминание еще не отправлено
        if not context['stats'][username]['user_replied'] and not context['stats'][username]['reminder_sent']:
            await client.send_message(username, reminder_message)
            context['stats'][username]['reminder_sent'] = True
            # Логируем отправку напоминания в базу данных
            await log_and_update_stats_db(
                conn,
                username=username,
                user_replied=False,
                message_count=context['stats'][username].get('message_count', 0),
                sensitive_info_sent=False,
                initial_message_sent=context['stats'][username].get('initial_message_sent', False)
            )
    except asyncio.CancelledError:
        # Если таймер был отменен, просто выходим
        return

context = {}

# Обработчик ответов от пользователей
async def handle_response(client, context, conn):
    @client.on_message()
    async def on_message(client, message):
        print("Получено сообщение от пользователя")
        username = message.chat.username if message.chat.username else "unknown_user"

        # Проверяем, если пользователь уже есть в статистике
        if username in context['stats']:
            context['stats'][username]['user_replied'] = True


        # Если таймер на напоминание существует, отменяем его
        if username in timers:
            timers[username].cancel()
            del timers[username]

        # Логируем ответ пользователя в базу данных
        await log_and_update_stats_db(
            conn,
            username=username,
            user_replied=True,
            message_count=context['stats'][username].get('message_count', 0) + 1,
            sensitive_info_sent=False,
            initial_message_sent=context['stats'][username].get('initial_message_sent', False)
        )

        # Проверка на тип сообщения (голосовое сообщение, стикеры и т.д.)
        if message.voice or message.video_note:
            print(f"Пользователь {username} отправил неподдерживаемый тип сообщения.")
            ai_response = "Извините, мне сейчас неудобно слушать ваше сообщение в таком формате. Можете написать текстом?"
            await client.send_message(username, ai_response)
            return

        if message.sticker:
            print(f"Пользователь {username} отправил стикер.")
            ai_response = "Извините, мне сейчас неудобно обрабатывать стикеры. Можете написать текстом?"
            await client.send_message(username, ai_response)
            return

        # Инициализация контекста для нового пользователя
        if username not in context:
            print(f"Инициализация контекста для {username}")
            context[username] = []

        # Добавляем сообщение пользователя в буфер (user_messages)
        user_messages[username].append({"role": "user", "content": message.text})

        # Сбрасываем таймер на 15 секунд, после которого будет отправлен ответ
        await reset_timer(username, client, conn)

# Функция для сброса и обновления таймера
async def reset_timer(username, client, conn):
    # Проверяем, есть ли активный таймер для пользователя, и отменяем его
    if username in timers:
        print(f"Сбрасываем таймер для пользователя {username}")
        timers[username].cancel()  # Отменяем предыдущий таймер

    # Запускаем новый таймер
    timers[username] = asyncio.create_task(start_timer(username, client, conn))

# Обновление функции start_timer для анализа и переключения на следующий промпт
async def start_timer(username, client, conn):
    print(f"Запускаем таймер для пользователя {username} на 15 секунд")
    await asyncio.sleep(15)  # Ожидаем 15 секунд


# Проверка и инициализация context['stats'] для пользователя
    if 'stats' not in context:
        context['stats'] = {}

# Проверка, есть ли уже запись для пользователя; если да, сохраняем текущие значения
    if username not in context['stats']:
        context['stats'][username] = {
            "user_replied": False,
            "message_count": 0,
            "sensitive_info_sent": False,
            "initial_message_sent": True,
            "reminder_sent": False
        }
    else:
    # Сохраняем текущие значения, если запись для пользователя существует
        current_stats = context['stats'][username]
        context['stats'][username] = {
            "user_replied": current_stats.get("user_replied", False),
            "message_count": current_stats.get("message_count", 0),
            "sensitive_info_sent": current_stats.get("sensitive_info_sent", False),
            "initial_message_sent": current_stats.get("initial_message_sent", True),
            "reminder_sent": current_stats.get("reminder_sent", False)
        }


    if username in user_messages and user_messages[username]:
        print(f"Таймер для пользователя {username} истек. Формируем запрос к нейросети.")

        # Инициализация контекста, если это новый пользователь
        if username not in context:
            context[username] = {
                "messages": [],
                "use_alternate_prompt": False,
                "in_secondary_prompt": False,
                "current_prompt": 1  # Начинаем с первого промпта
            }

        # Добавляем временные сообщения в основной контекст
        context[username]["messages"].extend(user_messages[username])
        user_messages[username].clear()  # Очищаем временный буфер сообщений

        save_dialog_to_file(username, context[username]["messages"])  # Сохраняем сообщения пользователя

        try:
            # Получение ответа от нейросети
            ai_response = await get_4o_answer(context[username]["messages"])


            # Проверка наличия ссылки на менеджера в ответе
            sensitive_info_sent = "https://pvvk.ru/veb" in ai_response


            # Отправляем ответ пользователю и добавляем в контекст
            if ai_response:
                print(f"Отправляем ответ пользователю {username}: {ai_response}")
                await client.send_message(username, ai_response)
                context[username]["messages"].append({"role": "assistant", "content": ai_response})


            # Обновляем sensitive_info_sent в контексте
            context['stats'][username]['sensitive_info_sent'] = sensitive_info_sent

            # Логирование в базу данных после отправки ответа
            await log_and_update_stats_db(
                conn,
                username=username,
                user_replied=True,
                message_count=context['stats'][username].get('message_count', 0) + 1,
                sensitive_info_sent=sensitive_info_sent,
                initial_message_sent=context['stats'][username].get('initial_message_sent', False)
            )


            if any(keyword in ai_response.lower() for keyword in ["прочитать наш гайд"]):
                print(f"Найдено упоминание 'КП'. Отправляем документ.")
                await client.send_message(
                    chat_id=username,
                    text=("Конечно сложно уместить в гайд всю важность употребления качественной воды для нашего организма, "
                          "поэтому предлагаю посетить наш бесплатный вебинар, чтобы узнать больше информации о нашей компании, "
                          "а также об отличиях разных фильтров и принципов работы.\n\n"
                          "Подскажите, вам было бы интересно посетить наш вебинар?")


                )

            
        except Exception as e:
            print(f"Ошибка при получении ответа от модели для пользователя {username}: {e}")
    else:
        print(f"Новое сообщение от пользователя {username} пришло, таймер сброшен.")



# Определяем названия столбцов для юзернеймов и имен клиентов
username_column = 'Script1'
name_column = 'Script1name'

# Основная функция выполнения программы
async def main(index_name):
    clients = []
    context = {}
    users_processed = 0
    max_users_per_day = 4

    conn = await create_db_connection()

    for account in accounts:
        client = Client(account['session_name'])
        await client.start()
        clients.append(client)

        # Регистрация обработчика сообщений
        asyncio.create_task(handle_response(clients[0], context, conn))

    try:
        while True:
            if users_processed < max_users_per_day:
                start_index = await get_current_index(conn, index_name)
                logging.info(f"Текущий индекс в работе: {start_index}")  # Логируем индекс
                
                # Вызов функции и передача названий столбцов
                username, client_name, next_index = load_single_username_from_excel(username_column, name_column, start_index)

                if username and client_name:
                    logging.info(f"Username взят в работу: {username}, Имя клиента: {client_name}")  # Логируем username и имя
                    
                    # Форматируем приветственное сообщение с использованием имени клиента
                    initial_message = random.choice(initial_messages)
                    
                    # Отправляем сообщение
                    await send_message(clients[0], username, client_name, context, conn)
                    
                    # Обновляем индекс
                    await update_current_index(conn, next_index, index_name)
                    
                    users_processed += 1

                    if users_processed < max_users_per_day:
                        await asyncio.sleep(3600)  # Ждем 1200 сек 20 мин перед обработкой следующего пользователя
                else:
                    logging.info("Нет больше пользователей для обработки")
                    break
            else:
                now = datetime.now()
                tomorrow = now + timedelta(days=1)
                next_run_time = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 11, 0, 0)

                wait_time = (next_run_time - now).total_seconds()
                logging.info(f"Все пользователи обработаны. Ждем до {next_run_time}")
                await asyncio.sleep(wait_time)

                users_processed = 0

        # Бесконечный цикл для удержания скрипта активным
        while True:
            await asyncio.sleep(4800)  # Периодически спим, чтобы не занимать ресурсы
    finally:
        await conn.close()
        for client in clients:
            await client.stop()

# Запуск скрипта с указанием уникального имени индекса
if __name__ == "__main__":
    asyncio.run(initialize_tables())  # Инициализируем таблицы перед запуском main
    asyncio.run(main(index_name))