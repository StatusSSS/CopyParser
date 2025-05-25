from __future__ import annotations

from pathlib import Path
from typing import List
from bs4 import BeautifulSoup


# ---------- парсинг одного HTML-файла ----------------------------------
def _wallets_from_html(html_file: Path) -> List[str]:
    """Извлекает адреса кошельков с ≤ 10 txns."""
    soup = BeautifulSoup(html_file.read_text(encoding="utf-8"), "html.parser")
    wallets: List[str] = []

    rows = soup.find_all("div", class_="custom-1nvxwu0")
    for row in rows:
        wax_blocks = row.find_all("div", class_="custom-1o79wax")
        if len(wax_blocks) > 1:
            sold_span = wax_blocks[1].find("span", class_="chakra-text custom-13ppmr2")
            if not sold_span:
                continue

            sold_text = sold_span.get_text(strip=True)
            if "txns" not in sold_text:
                continue

            txn_part = sold_text.split("/")[1].replace("txns", "").strip()
            txns = float(txn_part.replace("K", "")) * 1_000 if "K" in txn_part else int(txn_part)

            if txns <= 10:
                link = row.find("a", href=True)
                if link and "solscan.io/account" in link["href"]:
                    wallets.append(link["href"].split("/")[-1])

    return wallets


# ---------- публичная функция, вызываемая из pipeline ------------------
def extract_wallets(
    wallet_html_dirs: List[Path],
    dst_dir: Path,
) -> List[Path]:
    """
    Для каждой директории wallet_html извлекает кошельки и
    сохраняет *.txt в `<parent>/clear_wallets/`.
    Возвращает список созданных txt-файлов.
    """
    produced_txts: List[Path] = []

    for wdir in wallet_html_dirs:
        if not wdir.exists():
            print(f"[extract_wallets] {wdir} не найден — пропуск")
            continue

        clear_dir = wdir.parent / "clear_wallets"
        clear_dir.mkdir(exist_ok=True)

        for html_file in wdir.glob("*.html"):
            wallets = _wallets_from_html(html_file)
            if wallets:
                out_file = clear_dir / html_file.with_suffix(".txt").name
                out_file.write_text("\n".join(wallets), encoding="utf-8")
                produced_txts.append(out_file)

    print(f"[extract_wallets] создано {len(produced_txts)} файлов.")
    return produced_txts
