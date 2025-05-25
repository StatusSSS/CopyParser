
from __future__ import annotations

from multiprocessing import Process
from pathlib import Path
from typing import List

import time
import os

from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ---------- вспомогательная логика DOM ---------------------------------
def _extract_top_traders(driver: Driver) -> str:
    """
    Нажимаем кнопку ‘Top Traders’ и возвращаем HTML-код блока,
    где содержится таблица с кошельками.
    """
    try:
        top_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Top Traders')]"))
        )
        top_button.click()
        time.sleep(2)

        root = driver.find_element(By.ID, "root")
        custom_cwirlr = root.find_element(By.CSS_SELECTOR, "div.custom-cwirlr")
        main = custom_cwirlr.find_element(By.CSS_SELECTOR, "main.custom-1ip3p22")
        c1 = main.find_element(By.CSS_SELECTOR, "div.custom-697dix")
        c2 = c1.find_element(By.CSS_SELECTOR, "div.custom-1mgfq9c")
        c3 = c2.find_element(By.CSS_SELECTOR, "div.custom-19qkkht")
        c4 = c3.find_element(By.CSS_SELECTOR, "div.custom-1vjv7zm")
        c5 = c4.find_element(By.CSS_SELECTOR, "div.custom-1mxzest")

        return c5.get_attribute("outerHTML")
    except Exception as exc:
        print(f"[extract] Ошибка: {exc}")
        return "None"


# ---------- основная «работа» с одним txt-файлом -----------------------
def _process_token_file(txt_file: Path, dst_root: Path, page_idx: int) -> None:
    """
    Для каждого адреса токена из txt-файла:
      • открываем DexScreener,
      • вытягиваем блок Top Traders,
      • сохраняем в …/wallet_html/{N}_token_wallets.html.
    Ошибочные токены кладём в …/error_tokens/.
    """
    tokens = [
        line.strip().replace("/solana/", "")
        for line in txt_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not tokens:
        print(f"[Page {page_idx}] Пустой список токенов ― пропуск.")
        return

    save_dir = dst_root / f"{page_idx}_page_tokens" / "wallet_html"
    err_dir = dst_root / f"{page_idx}_page_tokens" / "error_tokens"
    save_dir.mkdir(parents=True, exist_ok=True)
    err_dir.mkdir(parents=True, exist_ok=True)

    driver = Driver(uc=True, headless=False)
    total = len(tokens)

    for n, token in enumerate(tokens, 1):
        url = f"https://dexscreener.com/solana/{token}"
        print(f"[Page {page_idx}] [{n}/{total}] → {url}")
        try:
            driver.get(url)
            time.sleep(3)
            driver.uc_gui_click_captcha()

            html = _extract_top_traders(driver)
            (save_dir / f"{n}_token_wallets.html").write_text(html, encoding="utf-8")
        except Exception as e1:                      # первая попытка
            print(f"  ⚠️  ошибка: {e1} — retry")
            try:
                driver.get(url)
                time.sleep(3)
                driver.uc_gui_click_captcha()
                time.sleep(3)

                html = _extract_top_traders(driver)
                (save_dir / f"{n}_token_wallets.html").write_text(html, encoding="utf-8")
            except Exception as e2:                  # вторая попытка
                print(f"  ❌ не смог: {e2}")
                (err_dir / f"{n}_token.txt").write_text(token, encoding="utf-8")

    driver.quit()
    print(f"[Page {page_idx}] ✅ завершено.")


# ---------- публичная функция, вызываемая из pipeline ------------------
def fetch_wallet_html(token_txt_files: List[Path], dst_dir: Path) -> List[Path]:
    """
    Принимает список txt-файлов с адресами токенов и корневую папку `dst_dir`
    (обычно data/runs/<run_id>/interim).  Для каждого txt-файла
    создаёт подпапку `<N>_page_tokens/wallet_html` и запускает
    парсинг в отдельном процессе.  Возвращает список созданных
    каталогов wallet_html.
    """
    wallet_html_dirs: List[Path] = []
    procs: List[Process] = []

    for idx, txt in enumerate(token_txt_files, 1):
        wallet_html_dir = dst_dir / f"{idx}_page_tokens" / "wallet_html"
        wallet_html_dirs.append(wallet_html_dir)
        p = Process(target=_process_token_file, args=(txt, dst_dir, idx))
        p.start()
        procs.append(p)
        time.sleep(5)                 # небольшая «ступенька», чтобы CF капча реже срабатывала

    for p in procs:
        p.join()

    print("[fetch_wallet_html] все процессы завершены.")
    return wallet_html_dirs
