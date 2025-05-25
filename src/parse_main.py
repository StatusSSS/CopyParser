import json
import time
import re
import numpy as np
from datetime import datetime
from src.db.database import SessionLocal
from src.db.models import Wallet

from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from rich.console import Console
from rich.table import Table



RESULTS_FILE = 'results.txt'
TABLE_TEXT_FILE = 'table_results.txt'
BASE_URL = 'https://gmgn.ai/sol/address/'


def add_may_good(
        address: str,
        rockets: int,
        trade_counts: int,
        profit_trades: int,
        good_profit: int,
        fast_trades: int,
        median: float,
        label: str
):
    session = SessionLocal()

    try:
        wallet = session.query(Wallet).filter_by(address=address).first()
        if wallet:
            wallet.rockets = rockets
            wallet.trade_counts = trade_counts
            wallet.profit_trades = profit_trades
            wallet.good_profit = good_profit
            wallet.fast_trades = fast_trades
            wallet.median = float(median)
            wallet.lable = label
        else:
            wallet = Wallet(
                address=address,
                rockets=rockets,
                trade_counts=trade_counts,
                profit_trades=profit_trades,
                good_profit=good_profit,
                fast_trades=fast_trades,
                median=float(median),
                lable=label,
            )
            session.add(wallet)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Ошибка при добавлении кошелька {address} в базу данных: {e}")
    finally:
        session.close()

def _duration_to_hours(s: str) -> float | None:
    """
    '--'  → None
    '25m' → 0.416…   (25 / 60)
    '5h'  → 5.0
    '3d'  → 72.0
    '40s' → 0.011…   (по желанию)
    """
    if not s or s == "--":
        return None

    value = float(re.sub(r"[^\d.]", "", s))   # поддерживаем и '1.5h'
    if s.endswith("s"):                       # секунды → часы
        return value / 3600
    if s.endswith("m"):                       # минуты → часы
        return value / 60
    if s.endswith("h"):
        return value
    if s.endswith("d"):
        return value * 24
    return None        # неожиданный суффикс


def median_interval_and_label(buy_duration: list[str]):
    """
    • Принимает исходный список строк ['5h', '1d', ...]
    • Возвращает (median_delta_hours | None, label)
    """

    hours = sorted(
        h for h in (_duration_to_hours(x) for x in buy_duration) if h is not None
    )

    if len(hours) < 2:
        return None, "UNKNOWN"


    deltas = np.diff(hours)
    deltas = deltas[deltas > 0]

    if len(deltas) == 0:
        return None, "UNKNOWN"

    median_h = float(np.median(deltas))

    q75 = np.percentile(deltas, 75)

    if q75 <= 24:
        label = "DAILY"
    elif q75 <= 168:
        label = "NORMAL"
    else:
        label = "RARE"

    return q75, label





def load_results_multiline(filename):
    """
    Считываем 'results.txt', где сохранено несколько JSON-объектов (каждый в фигурных скобках),
    записанных друг за другом (по несколько строк на объект).
    """
    objects = []
    current_lines = []
    inside_object = False

    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            line_stripped = line.strip()
            if line_stripped.startswith('{'):
                inside_object = True
                current_lines = [line_stripped]
            elif line_stripped.endswith('}'):
                current_lines.append(line_stripped)
                inside_object = False
                json_text = "\n".join(current_lines)
                try:
                    obj = json.loads(json_text)
                    objects.append(obj)
                except Exception as e:
                    print(f"Ошибка парсинга:\n{json_text}\n{e}")
                current_lines = []
            else:
                if inside_object:
                    current_lines.append(line_stripped)

    return objects



def count_rockets(driver, max_rows=100):
    """
    Считает:
    - rockets_count (профит > 1000),
    - trades_count (всего трейдов),
    - быстрые трейды (<2 минут).
    Возвращает (rockets_count, trades_count, fast_trades_display).
    """
    rockets_count = 0
    trades_count = 0
    fast_trades_count = 0
    profit_trades_count = 0
    good_profit_trades_count = 0
    buy_duration = []

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.g-table-content"))
        )

        table_content = driver.find_element(By.CSS_SELECTOR, "div.g-table-content")
        table_element = table_content.find_element(By.TAG_NAME, "table")
        tbody_element = table_element.find_element(By.CSS_SELECTOR, "tbody.g-table-tbody")

        rows = tbody_element.find_elements(By.CSS_SELECTOR, "tr.g-table-row")
        trades_count = len(rows)
        print(f"Найдено строк в таблице (трейдов): {trades_count}")

        for row in rows[:max_rows]:
            try:
                dur_el = row.find_element(
                    By.CSS_SELECTOR,
                    "td.g-table-cell.g-table-cell-fix-left."
                    "g-table-cell-fix-left-last p.chakra-text"
                )
                dur_text = dur_el.get_attribute("textContent").strip()
                buy_duration.append(dur_text)

                cells = row.find_elements(By.CSS_SELECTOR, "td.g-table-cell")

                if not cells:
                    continue

                for cell in cells:
                    # 1) Holding Duration
                    duration_elements = cell.find_elements(By.CSS_SELECTOR, "span.css-1baulvz")
                    for duration_element in duration_elements:
                        duration_text = duration_element.get_attribute("textContent").strip()

                        # Переводим в секунды
                        duration_seconds = 0
                        if "s" in duration_text:
                            duration_seconds = int(re.sub(r"[^0-9]", "", duration_text))
                        elif "m" in duration_text:
                            duration_seconds = int(re.sub(r"[^0-9]", "", duration_text)) * 60

                        if 0 < duration_seconds < 120:
                            fast_trades_count += 1

                    # 2) Ракета: profit % > 1000
                    profit_divs = cell.find_elements(By.CSS_SELECTOR, "div.css-1i5dkc8")
                    if not profit_divs:
                        continue

                    for div_ in profit_divs:
                        profit_elements = div_.find_elements(By.CSS_SELECTOR, "p.chakra-text.css-b4wymw")

                        for number in profit_elements:
                            profit_trades_count += 1
                            profit_text = number.get_attribute("textContent").strip()
                            numeric_part = re.sub(r"[^0-9.]", "", profit_text)
                            clear_number = int(float(numeric_part))
                            if clear_number > 40:
                                good_profit_trades_count += 1

                        for profit_element in profit_elements:
                            profit_text = profit_element.get_attribute("textContent").strip()
                            multiplier = 1
                            if "K" in profit_text:
                                multiplier = 1000

                            numeric_part = re.sub(r"[^0-9.]", "", profit_text)
                            if numeric_part:
                                profit_value = float(numeric_part) * multiplier
                                if profit_value > 1000:
                                    print("НАЙДЕНА РАКЕТА!")
                                    rockets_count += 1

            except Exception as e:
                print(f"Ошибка при обработке строки: {e}")
                continue

    except Exception as e:
        print(f"Не удалось разобрать историю сделок: {e}")
        return 0, 0, 0

    if trades_count > 0:
        fast_trade_percentage = (fast_trades_count / trades_count) * 100
        fast_trades_display = f"{round(fast_trade_percentage, 1)}%"
    else:
        fast_trades_display = 0

    median_h, freq_label = median_interval_and_label(buy_duration)


    return rockets_count, trades_count, fast_trades_display, profit_trades_count, good_profit_trades_count, median_h, freq_label


def process_one_wallet(driver, item):
    wallet_address = item.get("Wallet Address", "")
    if not wallet_address:
        return None

    url = f"{BASE_URL}{wallet_address}"
    print(f"Переходим по ссылке: {url}")
    driver.get(url)
    rockets, trades_count, fast_trades_percent, profit_trades, good_trades, median, freq_l = count_rockets(driver, max_rows=100)

    row_data = {
        "Wallet Address": wallet_address,
        "SOL Balance": item.get("SOL Balance", ""),
        "PnL 7": item.get("PnL 7d", ""),
        "Winrate": item.get("Winrate", ""),
        "Rockets": rockets,
        "Trades Count": trades_count,
        "Profit trades": profit_trades,
        "Good profit": good_trades,
        "Fast Trades": fast_trades_percent,
        "Median_H": median,
        "Label": freq_l
    }

    add_may_good(
        address=wallet_address,
        rockets=rockets,
        trade_counts=trades_count,
        profit_trades=profit_trades,
        good_profit=good_trades,
        fast_trades=float(fast_trades_percent.strip().rstrip("%")),
        median=median,
        label=freq_l,
    )



    return row_data


def main():
    # 1) Считываем объекты из файла results.txt
    saved_results = load_results_multiline(RESULTS_FILE)
    if not saved_results:
        print("В файле нет данных или файл отсутствует.")
        return

    # 2) Создаём единый драйвер
    driver = Driver(uc=True, headless=False)
    final_data = []

    try:
        # 3) Для каждого кошелька парсим данные
        for item in saved_results:
            row_data = process_one_wallet(driver, item)
            if row_data is not None:
                final_data.append(row_data)
    finally:
        # Закрываем драйвер только один раз
        driver.quit()

    # 4) Сохраняем результат в clear_results.txt (в JSON-формате)
    with open("clear_results.txt", "w", encoding="utf-8") as out_file:
        # Запишем как массив JSON:
        json.dump(final_data, out_file, ensure_ascii=False, indent=4)



if __name__ == "__main__":
    main()
