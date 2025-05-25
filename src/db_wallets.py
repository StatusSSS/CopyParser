from sqlalchemy.orm import Session
from src.db.database import engine
from src.db.models import Wallet  # путь к вашему классу Wallet

def export_wallets_with_rockets(min_rockets: int = 2, outfile: str = "db_wallets.txt"):
    # создаём сессию
    with Session(engine) as session:
        # формируем запрос: все кошельки, у которых rockets >= min_rockets
        wallets = (
            session
            .query(Wallet)
            .filter(Wallet.rockets >= min_rockets)
            .all()
        )

    # записываем в файл
    with open(outfile, "w", encoding="utf-8") as f:
        for w in wallets:
            # здесь записываем, что нужно: например, только адрес
            f.write(f"{w.address}\n")

    print(f"Экспортировано {len(wallets)} кошельков с rockets >= {min_rockets} в файл {outfile}")

if __name__ == "__main__":
    export_wallets_with_rockets()
