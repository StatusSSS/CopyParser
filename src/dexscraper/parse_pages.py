
from __future__ import annotations

from pathlib import Path
from typing import List
from bs4 import BeautifulSoup


def parse_token_addresses(html_files: List[Path], dst_dir: Path) -> List[Path]:
    """
    Принимает список HTML-файлов (страниц DexScreener),
    извлекает адреса токенов вида `/solana/<addr>` и
    сохраняет их постранично в `dst_dir/page-<N>_tokens.txt`.

    Возвращает список созданных txt-файлов.
    """
    token_txts: List[Path] = []

    for idx, html_file in enumerate(html_files, 1):
        soup = BeautifulSoup(html_file.read_text(encoding="utf-8"), "html.parser")
        addresses = [
            a["href"]
            for a in soup.find_all("a", href=True)
            if a["href"].startswith("/solana/")
        ]

        out_file = dst_dir / f"page-{idx}_tokens.txt"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text("\n".join(addresses), encoding="utf-8")

        token_txts.append(out_file)
        print(f"[parse_pages] {len(addresses)} адресов → {out_file}")

    return token_txts
