from pathlib import Path
from typing import Set, List


def deduplicate_wallets(src_file: Path, dst_file: Path) -> Path:
    """
    Берёт файл `src_file` с адресами, убирает дубликаты,
    сортирует, пишет результат в `dst_file`.
    Возвращает путь к dst_file.
    """
    if not src_file.exists():
        raise FileNotFoundError(src_file)

    # читаем и сразу выбрасываем пустые строки
    unique: Set[str] = {line.strip() for line in src_file.read_text(encoding="utf-8").splitlines() if line.strip()}

    sorted_wallets: List[str] = sorted(unique)
    dst_file.parent.mkdir(parents=True, exist_ok=True)
    dst_file.write_text("\n".join(sorted_wallets) + "\n", encoding="utf-8")

    print(f"[remove_duplicates] {len(sorted_wallets)} уникальных адресов → {dst_file}")
    return dst_file
