from datetime import datetime
from sqlalchemy import (
    Integer, String, Float, TIMESTAMP, func, Computed
)
from sqlalchemy.orm import Mapped, mapped_column
from src.db.database import Base




class Wallet(Base):
    __tablename__ = "wallets"

    id:          Mapped[int]   = mapped_column(primary_key=True, autoincrement=True)
    address:     Mapped[str]   = mapped_column(String, unique=True, nullable=False)

    winrate:     Mapped[float] = mapped_column(Float, nullable=False)
    sol_balance: Mapped[float] = mapped_column(Float, nullable=False)
    pnl:         Mapped[float] = mapped_column(Float, nullable=False)

    rockets:       Mapped[int]  = mapped_column(Integer, nullable=False, default=0)
    trade_counts:  Mapped[int]  = mapped_column(Integer, nullable=False, default=0)
    profit_trades: Mapped[int]  = mapped_column(Integer, nullable=False, default=0)
    good_profit:   Mapped[int]= mapped_column(Integer,  nullable=False, default=0)

    profit_ratio:  Mapped[float] = mapped_column(
        Float,
        Computed("COALESCE(profit_trades::float / NULLIF(trade_counts,0), 0)", persisted=True),
        nullable=True,
    )

    good_ratio:    Mapped[float] = mapped_column(
        Float,
        Computed("COALESCE(good_profit::float  / NULLIF(trade_counts,0), 0)", persisted=True),
        nullable=True,
    )


    fast_trades:   Mapped[float | None] = mapped_column(Float,  nullable=False, default=0)
    median:        Mapped[float] = mapped_column(Float, nullable=False, default=0)
    lable:         Mapped[str] = mapped_column(String, nullable=True)


    checked_count: Mapped[int]  = mapped_column(Integer, default=0)
    last_updated:  Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )
