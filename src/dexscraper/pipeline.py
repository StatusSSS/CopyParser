from __future__ import annotations

import argparse
import multiprocessing
import shutil


from src.dexscraper.utils import create_run_dir, cleanup_old_runs
from src.dexscraper.fetch_pages import fetch_pages
from src.dexscraper.parse_pages import parse_token_addresses
from src.dexscraper.fetch_wallet_html import fetch_wallet_html
from src.dexscraper.extract_wallets import extract_wallets
from src.dexscraper.merge_wallets import merge_wallets
from src.dexscraper.remove_duplicates import deduplicate_wallets   # ← добавили
from src.dexscraper.wallet_main import wallet_main
from src.dexscraper.wallet_parse_main import wallet_parse_main

def run_pipeline(hours: int, keep_interim: bool) -> None:
    run_id, run_path = create_run_dir()
    print(f"=== RUN {run_id} started ===")

    raw_dir       = run_path / "raw"
    interim_dir   = run_path / "interim"
    processed_dir = run_path / "processed"


    html_pages     = fetch_pages(hours, raw_dir)
    token_txts     = parse_token_addresses(html_pages, interim_dir)
    wallet_dirs    = fetch_wallet_html(token_txts, interim_dir)
    clear_txts     = extract_wallets(wallet_dirs, interim_dir)
    merged_file    = merge_wallets(clear_txts, processed_dir / "merged_wallets.txt")


    final_file = deduplicate_wallets(merged_file, processed_dir / "list.txt")

    list_wallets = wallet_main(final_file, processed_dir / "results.txt")

    wallet_parse_main(list_wallets, run_path)


    if not keep_interim:
        shutil.rmtree(raw_dir, ignore_errors=True)
        shutil.rmtree(interim_dir, ignore_errors=True)


    #cleanup_old_runs(keep=3)

    print(f"=== RUN {run_id} finished — final file: {final_file} ===")


def _cli() -> None:
    parser = argparse.ArgumentParser(description="DexScreener scraping pipeline")
    parser.add_argument("--hours", type=int, default=12, help="максимальный возраст токенов (часы)")
    parser.add_argument("--keep-interim", action="store_true", help="не удалять raw & interim после выполнения")
    args = parser.parse_args()
    run_pipeline(hours=args.hours, keep_interim=args.keep_interim)

if __name__ == "__main__":
    multiprocessing.freeze_support()
    _cli()