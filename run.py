import subprocess
import os
import time
from concurrent.futures import ThreadPoolExecutor

# Корневая папка
root_folder = os.path.dirname(os.path.abspath(__file__))

# Отдельный скрипт для tgbot.py
tgbot_script = os.path.join(root_folder, "tgbot.py")

# Список путей к Python-скриптам, которые нужно запустить
scripts = [
    (os.path.join(root_folder, "script1", "script1.py"), "script1_index"),
    (os.path.join(root_folder, "script2", "script2.py"), "script2_index"),
    (os.path.join(root_folder, "script3", "script3.py"), "script3_index"),
    (os.path.join(root_folder, "script4", "script4.py"), "script4_index"),
    (os.path.join(root_folder, "script5", "script5.py"), "script5_index")
]

# Функция для запуска одного скрипта с задержкой
def run_script(script, index_name=None, delay=0):
    try:
        print(f"Запуск {script} с задержкой {delay} секунд...")
        time.sleep(delay)
        if index_name:
            result = subprocess.run(["python", script, "--index_name", index_name], check=True, capture_output=True, text=True)
        else:
            result = subprocess.run(["python", script], check=True, capture_output=True, text=True)
        print(f"{script} завершён успешно с результатом: {result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при выполнении {script}: {e}")
        print(f"Вывод ошибки: {e.stderr}")
    except FileNotFoundError:
        print(f"Скрипт {script} не найден.")

# Запуск всех скриптов параллельно с задержкой
if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=len(scripts) + 1) as executor:
        # Запуск tgbot.py без index_name
        executor.submit(run_script, tgbot_script, delay=0)

        # Запуск остальных скриптов с index_name
        for i, (script, index_name) in enumerate(scripts):
            delay = (i + 1) * 30  # Увеличение задержки для каждого последующего скрипта
            executor.submit(run_script, script, index_name, delay)
