
from pathlib import Path
from datetime import datetime
import shutil

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
RAW, INTERIM, PROCESSED, RUNS = "raw", "interim", "processed", "runs"


def create_run_dir() -> tuple[str, Path]:
    """Создаёт каталог одного запуска и возвращает (run_id, Path)."""
    run_id = datetime.now().strftime("%Y-%m-%d_%H-%M")
    run_path = DATA_DIR / RUNS / run_id
    for sub in (RAW, INTERIM, PROCESSED):
        (run_path / sub).mkdir(parents=True, exist_ok=True)
    return run_id, run_path


def cleanup_old_runs(keep: int = 3) -> None:
    """Оставляет только `keep` последних run’ов, остальные удаляет."""
    runs_root = DATA_DIR / RUNS
    if not runs_root.exists():
        return
    runs = sorted(runs_root.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
    for old in runs[keep:]:
        shutil.rmtree(old, ignore_errors=True)
