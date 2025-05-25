import multiprocessing

import json
from datetime import datetime
from tabulate import tabulate
from termcolor import colored
import time

from src.db.database import SessionLocal
from src.db.models import Wallet

from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

API_URL = 'https://gmgn.ai/defi/quotation/v1/smartmoney/sol/walletNew/'

BROWSER_COUNT = 0



def add_may_normal(
        address: str,
        winrate: float,
        sol_balance: float,
        pnl: float,
):
    session = SessionLocal()

    try:
        wallet = session.query(Wallet).filter_by(address=address).first()
        if wallet:
            wallet.checked_count += 1
            wallet.last_updated = datetime.now()
            wallet.sol_balance = int(sol_balance)
            wallet.pnl = int(pnl)
            wallet.winrate = int(winrate*100)
        else:
            wallet = Wallet(
                address=address,
                winrate=int(winrate*100),
                sol_balance=int(sol_balance),
                pnl=int(pnl),
                checked_count=1
            )
            session.add(wallet)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Ошибка при добавлении кошелька {address} в базу данных: {e}")
    finally:
        session.close()



def setup_driver(headless=False):
    """Настраиваем и возвращаем драйвер SeleniumBase."""
    driver = Driver(uc=True, headless=headless)
    return driver


def fetch_wallet_data(driver, wallet_address, period):
    """Загружает данные по конкретному кошельку через Selenium."""
    global BROWSER_COUNT
    if BROWSER_COUNT < 4:
        BROWSER_COUNT += 1
    url = f'{API_URL}{wallet_address}?period={period}'

    try:
        driver.get(url)

        if BROWSER_COUNT < 4:
            time.sleep(3)
            driver.uc_gui_click_captcha()
        # Подождём, пока появится <pre> на странице.
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "pre"))
        )
        # Извлекаем json из тега <pre>
        page_source = driver.page_source
        start_index = page_source.find("<pre>") + 5
        end_index = page_source.find("</pre>")
        json_data = page_source[start_index:end_index]
        return json.loads(json_data)
    except Exception as e:
        print(f'Error fetching data for wallet {wallet_address}: {e}')
        return None


def process_data(data, wallet_address, period):
    """Обрабатываем JSON и формируем структуру с результатами."""
    if data:
        try:
            sol_balance = data['data'].get('sol_balance') or 0
            pnl_key = 'pnl_30d' if period == '30d' else 'pnl_7d'
            pnl = data['data'].get(pnl_key) or 0
            winrate = data['data'].get('winrate') or 0
            realized_profit_key = 'realized_profit_30d' if period == '30d' else 'realized_profit_7d'
            realized_profit = data['data'].get(realized_profit_key) or 0
            last_active_timestamp = data['data'].get('last_active_timestamp', 0)
            if last_active_timestamp is not None:
                last_active_str = datetime.fromtimestamp(last_active_timestamp).strftime('%Y-%m-%d %H:%M:%S')
            else:
                last_active_str = 'N/A'

            last_pnl = pnl * 100  # в проценты
            last_winrate = winrate * 100

            winrate_str = f'{round(last_winrate, 2)}%'
            if last_winrate > 0:
                winrate_str = colored(winrate_str, 'green')

            result = {
                'Wallet Address': wallet_address,
                'SOL Balance': f'{float(sol_balance):.2f}',
                f'PnL {period}': f'{round(last_pnl, 2)}%',
                'Winrate': winrate,
                f'Realized Profit {period}': f'{realized_profit:.2f}$',
                'Last Active Timestamp': last_active_str,
                # Для фильтрации:
                'PnL_value': last_pnl,
                'SOL_value': float(sol_balance),
            }
            return result
        except KeyError as e:
            print(f'ERROR: Make sure your list is correct.')
    return None


def worker_func(wallet_addresses, period, output_file):
    """
    Функция-воркер: инициализирует драйвер и обрабатывает
    список кошельков, переданный в wallet_addresses.
    """
    driver = setup_driver()
    results = []

    for wallet_address in wallet_addresses:
        data = fetch_wallet_data(driver, wallet_address, period)
        processed = process_data(data, wallet_address, period)
        if processed:
            if processed['Winrate'] > 0.45 and processed['SOL_value'] > 5 and processed['PnL_value'] > 50:
                results.append(processed)
                add_may_normal(
                    wallet_address,
                    processed['Winrate'],
                    processed['SOL_value'],
                    processed['PnL_value'],
                )
                print("Добавил в хороший")
                print(tabulate([processed], headers="keys", tablefmt="grid"))
                with open('results.txt', 'a', encoding='utf-8') as file:
                    result_to_save = {k: v for k, v in processed.items() if k != 'Winrate Value'}
                    file.write(json.dumps(result_to_save, indent=4) + '\n')


    driver.quit()


def main():
    start_time = time.time()
    period = '7d'  # или '30d'
    output_file = 'src/results.txt'

    # Считываем все адреса
    try:
        with open('./list.txt', 'r') as f:
            all_wallet_addresses = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("The file 'list.txt' was not found.")
        return

    # Допустим, хотим разбить на 4 части
    num_processes = 4
    chunk_size = len(all_wallet_addresses) // num_processes + 1
    chunks = [
        all_wallet_addresses[i:i + chunk_size]
        for i in range(0, len(all_wallet_addresses), chunk_size)
    ]

    processes = []
    for i, chunk in enumerate(chunks):
        # Для удобства можно задать отдельные файлы результатов
        # чтобы избежать одновременной записи. Например:
        worker_output_file = "./results.txt"

        p = multiprocessing.Process(
            target=worker_func,
            args=(chunk, period, worker_output_file)
        )
        p.start()
        time.sleep(4)
        processes.append(p)

    # Ждём завершения всех процессов
    for p in processes:
        p.join()

    print("Все процессы завершены.")
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"Время выполнения скрипта: {elapsed:.2f} секунд")

if __name__ == '__main__':
    main()


