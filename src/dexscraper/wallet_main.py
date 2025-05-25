
import multiprocessing
from pathlib import Path
import json
from datetime import datetime
from tabulate import tabulate
from termcolor import colored
import time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from src.db.models import Wallet

from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



DB_URL = "postgresql://solparse_owner:npg_SYBi1yxqgp0f@ep-raspy-cake-a48iue75-pooler.us-east-1.aws.neon.tech/solparse?sslmode=require"
API_URL = "https://gmgn.ai/defi/quotation/v1/smartmoney/sol/walletNew/"
NUM_PROCESSES = 4



BROWSER_COUNT = multiprocessing.Value("i", 0)


def make_session_factory() -> sessionmaker:
    engine = create_engine(
        DB_URL,
        poolclass=NullPool,
        pool_pre_ping=True,
    )
    return sessionmaker(bind=engine)


def add_wallet(
    SessionLocal,
    address: str,
    winrate: float,
    sol_balance: float,
    pnl: float,
):
    """Добавляем/обновляем кошелёк в БД."""
    session = SessionLocal()
    try:
        wallet = session.query(Wallet).filter_by(address=address).first()
        if wallet:
            wallet.checked_count += 1
            wallet.last_updated = datetime.now()
            wallet.sol_balance = int(sol_balance)
            wallet.pnl = int(pnl)
            wallet.winrate = int(winrate * 100)
        else:
            wallet = Wallet(
                address=address,
                winrate=int(winrate * 100),
                sol_balance=int(sol_balance),
                pnl=int(pnl),
                checked_count=1,
            )
            session.add(wallet)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Ошибка при добавлении кошелька {address}: {e}")
    finally:
        session.close()



def setup_driver(headless: bool = False) -> Driver:
    return Driver(uc=True, headless=headless)


def fetch_wallet_data(driver: Driver, wallet_address: str, period: str) -> dict | None:
    url = f"{API_URL}{wallet_address}?period={period}"

    with BROWSER_COUNT.get_lock():
        first_calls = BROWSER_COUNT.value < 4
        BROWSER_COUNT.value += 1

    try:
        driver.get(url)
        if first_calls:  # обойдём капчу в первых 4 запросах
            time.sleep(3)
            driver.uc_gui_click_captcha()

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "pre"))
        )
        page_source = driver.page_source
        json_data = page_source.split("<pre>", 1)[1].split("</pre>", 1)[0]
        return json.loads(json_data)
    except Exception as e:
        try:
            print(f"ERROR TRYING AGAIN: {wallet_address}: {e}")
            driver.get(url)
            if first_calls:
                time.sleep(3)
                driver.uc_gui_click_captcha()

            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "pre"))
            )
            page_source = driver.page_source
            json_data = page_source.split("<pre>", 1)[1].split("</pre>", 1)[0]
            return json.loads(json_data)
        except Exception as e:
            print(f"ERROR WHILE FETCHING DATA: {wallet_address}: {e}")
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


def worker_func(wallet_addresses: list[str], period: str, output_file: Path) -> None:
    SessionLocal = make_session_factory()
    driver = setup_driver()

    for address in wallet_addresses:
        data = fetch_wallet_data(driver, address, period)
        processed = process_data(data, address, period)
        if (
            processed
            and processed["Winrate"] > 0.45
            and processed["SOL_value"] > 5
            and processed["PnL_value"] > 50
        ):
            add_wallet(
                SessionLocal,
                address,
                processed["Winrate"],
                processed["SOL_value"],
                processed["PnL_value"],
            )
            print(tabulate([processed], headers="keys", tablefmt="grid"))

            with open(output_file, "a", encoding="utf-8") as f:
                json.dump(
                    {k: v for k, v in processed.items() if k != "Winrate Value"},
                    f,
                    ensure_ascii=False,
                    indent=4,
                )
                f.write("\n")

    driver.quit()



def wallet_main(list_path: Path, results_path: Path):
    start_time = time.time()
    period = "7d"

    try:
        with open(list_path, "r", encoding="utf-8") as f:
            all_addresses = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("Файл list.txt не найден.")
        return results_path

    chunk_size = len(all_addresses) // NUM_PROCESSES + 1
    chunks = [
        all_addresses[i : i + chunk_size] for i in range(0, len(all_addresses), chunk_size)
    ]

    processes = []
    for chunk in chunks:
        p = multiprocessing.Process(
            target=worker_func,
            args=(chunk, period, Path(results_path)),
            daemon=False,
        )
        p.start()
        time.sleep(4)  # небольшая пауза для обхода капчи
        processes.append(p)

    for p in processes:
        p.join()

    print("Все процессы завершены.")
    print(f"Время работы: {time.time() - start_time:.2f} сек")
    return results_path


if __name__ == "__main__":
    wallet_main()
