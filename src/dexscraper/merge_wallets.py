from __future__ import annotations

from pathlib import Path
from typing import List, Set


def merge_wallets(
    clear_wallet_txts: List[Path],
    output_path: Path,
) -> Path:
    """
    Склеивает все txt-файлы из clear_wallets в единый список,
    убирая дубликаты.  Никаких проверок по БД не делает.
    """
    seen: Set[str] = set()
    merged: List[str] = []

    for txt in clear_wallet_txts:
        for address in txt.read_text(encoding="utf-8").splitlines():
            if address and address not in seen:
                merged.append(address + "\n")
                seen.add(address)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("".join(merged), encoding="utf-8")
    print(f"[merge_wallets] итоговый файл → {output_path}  ({len(merged)} адресов)")
    return output_path


if __name__ == "__main__":
    import argparse, sys
    parser = argparse.ArgumentParser("merge_wallets helper")
    parser.add_argument("base_dir", type=Path, help="каталог interim или любой parent clear_wallets")
    parser.add_argument("-o", "--output", type=Path, default=Path("merged_wallets.txt"))
    args = parser.parse_args()

    clear_txts = list(args.base_dir.rglob("clear_wallets/*.txt"))
    if not clear_txts:
        sys.exit("clear_wallets/*.txt не найдено")

    merge_wallets(clear_txts, args.output)