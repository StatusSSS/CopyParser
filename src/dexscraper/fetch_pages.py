
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from src.dexscraper.utils import create_run_dir, cleanup_old_runs
from pathlib import Path

from seleniumbase import Driver
from typing import List
import time

def fetch_pages(hours: int, dst_dir: Path) -> List[Path]:
    """
    Загружает «горячую» страницу DexScreener, выдёргивает саму
    таблицу (div.ds-dex-table.ds-dex-table-top) и сохраняет
    HTML в dst_dir/page‑1.html. Возвращает список сохранённых файлов.
    """
    saved: List[Path] = []
    driver = Driver(uc=True, headless=False)

    try:
        url = (
            "https://dexscreener.com/solana"
            f"?rankBy=trendingScoreH6&order=desc&minMarketCap=50000&maxAge={hours}"
        )
        driver.get(url)
        time.sleep(10)
        driver.uc_gui_click_captcha()
        time.sleep(10)
        driver.refresh()

        root = WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.ID, "root"))
        )
        custom_cwirlr = root.find_element(By.CSS_SELECTOR, "div.custom-cwirlr")
        main_element = custom_cwirlr.find_element(By.CSS_SELECTOR, "main.custom-1ip3p22")
        custom_a3 = main_element.find_element(By.CSS_SELECTOR, "div.custom-a3qv9n")

        target_table = WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.ds-dex-table.ds-dex-table-top"))
        )

        table_html = target_table.get_attribute("outerHTML")
        # --------------------------------------

        file_path = dst_dir / "page-1.html"
        file_path.write_text(table_html, encoding="utf-8")
        saved.append(file_path)
        print(f"[fetch_pages] сохранено → {file_path}")
    except Exception as e:
        print(f"Ошибка на странице: {e}")

    finally:
        driver.quit()

    return saved


def main(hours: int = 24, keep_interim: bool = False) -> None:
    run_id, run_path = create_run_dir()
    print(f"Started run {run_id}")

    raw_dir, interim_dir, processed_dir = (
        run_path / "raw",
        run_path / "interim",
        run_path / "processed",
    )

    pages = fetch_pages(hours, raw_dir)

    cleanup_old_runs(keep=3)
    print(f"Run {run_id} finished. Result:")


if __name__ == "__main__":
    main()



