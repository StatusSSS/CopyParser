from src.dexscraper.wallet_main import wallet_main
from src.dexscraper.wallet_parse_main import wallet_parse_main
from pathlib import Path
from src.dexscraper.utils import create_run_dir

#
# run_id, run_path = create_run_dir()
# print(f"=== RUN {run_id} started ===")
#
# raw_dir       = run_path / "raw"
# interim_dir   = run_path / "interim"
# processed_dir = run_path / "processed"


if __name__ == "__main__":

    import multiprocessing
    multiprocessing.freeze_support()

    list_wallets = wallet_main("list.txt", "results.txt")