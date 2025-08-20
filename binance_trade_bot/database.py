import json
import os
import time
from collections import namedtuple
from contextlib import contextmanager, nullcontext
from datetime import datetime, timedelta
from typing import List, Optional, Union

from sqlalchemy import bindparam, create_engine, func, insert, select, update
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from binance_trade_bot.postpone import heavy_call
from binance_trade_bot.ratios import CoinStub, RatiosManager

from .config import Config
from .logger import Logger
from .models import *  # pylint: disable=wildcard-import

LogScout = namedtuple("LogScout", ["pair_id", "ratio_diff", "target_ratio", "coin_price", "optional_coin_price"])


class Database:
    def __init__(self, logger: Logger, config: Config, uri="sqlite:///data/crypto_trading.db"):
        self.logger = logger
        self.config = config
        self.engine = create_engine(uri)
        self.session_factory = scoped_session(sessionmaker(bind=self.engine))
        self.ratios_manager: Optional[RatiosManager] = None
        self.socketio_client = None

    def socketio_connect(self):
        from socketio.exceptions import ConnectionError as SocketIOConnectionError
        if self.socketio_client is None:
            from socketio import Client
            self.socketio_client = Client()
        if self.socketio_client.connected and self.socketio_client.namespaces:
            return True
        try:
            if not self.socketio_client.connected:
                self.socketio_client.connect("http://api:5123", namespaces=["/backend"])
            while not self.socketio_client.connected or not self.socketio_client.namespaces:
                time.sleep(0.1)
            return True
        except SocketIOConnectionError:
            return False

    @contextmanager
    def db_session(self):
        """
        Creates a context with an open SQLAlchemy session.
        """
        session: Session = self.session_factory()
        yield session
        session.commit()
        session.close()

    def manage_session(self, session=None):
        if session is None:
            return self.db_session()
        return nullcontext(session)

    def set_coins(self, symbols: List[str]):
        session: Session

        # Add coins to the database and set them as enabled or not
        with self.db_session() as session:
            # For all the coins in the database, if the symbol no longer appears
            # in the config file, set the coin as disabled
            coins: List[Coin] = session.query(Coin).all()
            for coin in coins:
                if coin.symbol not in symbols:
                    coin.enabled = False

            # For all the symbols in the config file, add them to the database
            # if they don't exist
            for symbol in symbols:
                coin = next((coin for coin in coins if coin.symbol == symbol), None)
                if coin is None:
                    session.add(Coin(symbol))
                else:
                    coin.enabled = True

        CoinStub.reset()

        # For all the combinations of coins in the database, add a pair to the database
        with self.db_session() as session:
            coins: List[Coin] = session.query(Coin).filter(Coin.enabled).order_by(Coin.symbol).all()
            for coin in coins:
                CoinStub.create(coin.symbol)
            for from_coin in coins:
                for to_coin in coins:
                    if from_coin != to_coin:
                        pair = session.query(Pair).filter(Pair.from_coin == from_coin, Pair.to_coin == to_coin).first()
                        if pair is None:
                            session.add(Pair(from_coin, to_coin))

        # Fill lookup table for id discovery
        with self.db_session() as session:
            pairs = session.query(Pair).filter(Pair.enabled.is_(True)).all()
            self.ratios_manager = RatiosManager(pairs)

    def get_coins(self, only_enabled=True) -> List[Coin]:
        session: Session
        with self.db_session() as session:
            if only_enabled:
                coins = session.query(Coin).filter(Coin.enabled).all()
            else:
                coins = session.query(Coin).all()
            session.expunge_all()
            return coins

    def get_coin(self, coin: Union[Coin, str]) -> Coin:
        if isinstance(coin, Coin):
            return coin
        session: Session
        with self.db_session() as session:
            coin = session.query(Coin).get(coin)
            session.expunge(coin)
            return coin

    def set_current_coin(self, coin: Union[Coin, str]):
        coin = self.get_coin(coin)
        session: Session
        with self.db_session() as session:
            if isinstance(coin, Coin):
                coin = session.merge(coin)
            cc = CurrentCoin(coin)
            session.add(cc)
            self.send_update(cc)

    def get_current_coin(self) -> Optional[Coin]:
        session: Session
        with self.db_session() as session:
            current_coin = session.query(CurrentCoin).order_by(CurrentCoin.datetime.desc()).first()
            if current_coin is None:
                return None
            coin = current_coin.coin
            session.expunge(coin)
            return coin

    def get_pair(self, from_coin: Union[Coin, str], to_coin: Union[Coin, str]):
        from_coin = self.get_coin(from_coin)
        to_coin = self.get_coin(to_coin)
        session: Session
        with self.db_session() as session:
            pair: Pair = session.query(Pair).filter(Pair.from_coin == from_coin, Pair.to_coin == to_coin).first()
            session.expunge(pair)
            return pair

    @heavy_call
    def batch_log_scout(self, logs: List[LogScout]):
        session: Session
        with self.db_session() as session:
            dt = datetime.now()
            session.execute(
                insert(ScoutHistory),
                [
                    {
                        "pair_id": ls.pair_id,
                        "ratio_diff": ls.ratio_diff,
                        "target_ratio": ls.target_ratio,
                        "current_coin_price": ls.coin_price,
                        "other_coin_price": ls.optional_coin_price,
                        "datetime": dt,
                    }
                    for ls in logs
                ],
            )
            # need to repair send_update here for log scouts
            # self.send_update(sh)

    def prune_scout_history(self):
        time_diff = datetime.now() - timedelta(hours=self.config.SCOUT_HISTORY_PRUNE_TIME)
        session: Session
        with self.db_session() as session:
            session.query(ScoutHistory).filter(ScoutHistory.datetime < time_diff).delete()

    def prune_value_history(self):
        def _datetime_id_query(dt_format):
            dt_column = func.strftime(dt_format, CoinValue.datetime)

            grouped = select(CoinValue, func.max(CoinValue.datetime), dt_column).group_by(
                CoinValue.coin_id, CoinValue, dt_column
            )

            return select(grouped.c.id.label("id")).select_from(grouped)

        def _update_query(datetime_query, interval):
            return (
                update(CoinValue)
                .where(CoinValue.id.in_(datetime_query))
                .values(interval=interval)
                .execution_options(synchronize_session="fetch")
            )

        # Sets the first entry for each coin for each hour as 'hourly'
        hourly_update_query = _update_query(_datetime_id_query("%H"), Interval.HOURLY)

        # Sets the first entry for each coin for each month as 'weekly'
        # (Sunday is the start of the week)
        weekly_update_query = _update_query(
            _datetime_id_query("%Y-%W"),
            Interval.WEEKLY,
        )

        # Sets the first entry for each coin for each day as 'daily'
        daily_update_query = _update_query(
            _datetime_id_query("%Y-%j"),
            Interval.DAILY,
        )

        session: Session
        with self.db_session() as session:
            session.execute(hourly_update_query)
            session.execute(daily_update_query)
            session.execute(weekly_update_query)

            # Early commit to make sure the delete statements work properly.
            session.commit()

            # The last 24 hours worth of minutely entries will be kept, so
            # count(coins) * 1440 entries
            time_diff = datetime.now() - timedelta(hours=24)
            session.query(CoinValue).filter(
                CoinValue.interval == Interval.MINUTELY, CoinValue.datetime < time_diff
            ).delete()

            # The last 28 days worth of hourly entries will be kept, so count(coins) * 672 entries
            time_diff = datetime.now() - timedelta(days=28)
            session.query(CoinValue).filter(
                CoinValue.interval == Interval.HOURLY, CoinValue.datetime < time_diff
            ).delete()

            # The last years worth of daily entries will be kept, so count(coins) * 365 entries
            time_diff = datetime.now() - timedelta(days=365)
            session.query(CoinValue).filter(
                CoinValue.interval == Interval.DAILY, CoinValue.datetime < time_diff
            ).delete()

            # All weekly entries will be kept forever

    def create_database(self):
        Base.metadata.create_all(self.engine)
        try:
            with self.db_session() as session:
                session.execute("ALTER TABLE scout_history ADD COLUMN ratio_diff float;")
        except:
            pass

    def start_trade_log(self, from_coin: str, to_coin: str, selling: bool):
        return TradeLog(self, from_coin, to_coin, selling)

    def send_update(self, model):
        if not self.socketio_connect():
            return

        self.socketio_client.emit(
            "update",
            {"table": model.__tablename__, "data": model.info()},
            namespace="/backend",
        )

    def migrate_old_state(self):
        """
        For migrating from old dotfile format to SQL db. This method should be removed in
        the future.
        """
        if os.path.isfile(".current_coin"):
            with open(".current_coin") as f:
                coin = f.read().strip()
                self.logger.info(f".current_coin file found, loading current coin {coin}")
                self.set_current_coin(coin)
            os.rename(".current_coin", ".current_coin.old")
            self.logger.info(".current_coin renamed to .current_coin.old - You can now delete this file")

        if os.path.isfile(".current_coin_table"):
            with open(".current_coin_table") as f:
                self.logger.info(".current_coin_table file found, loading into database")
                table: dict = json.load(f)
                session: Session
                with self.db_session() as session:
                    for from_coin, to_coin_dict in table.items():
                        for to_coin, ratio in to_coin_dict.items():
                            if from_coin == to_coin:
                                continue
                            pair = session.merge(self.get_pair(from_coin, to_coin))
                            pair.ratio = ratio
                            session.add(pair)

            os.rename(".current_coin_table", ".current_coin_table.old")
            self.logger.info(".current_coin_table renamed to .current_coin_table.old - " "You can now delete this file")

    @heavy_call
    def commit_ratios(self):
        dirty_cells = self.ratios_manager.get_dirty()

        if len(dirty_cells) == 0:
            return

        pair_t = Pair.__table__
        stmt = pair_t.update().where(pair_t.c.id == bindparam("pair_id")).values(ratio=bindparam("pair_ratio"))
        with self.db_session() as session:
            session.execute(
                stmt,
                [
                    {
                        "pair_id": self.ratios_manager.get_pair_id(from_idx, to_idx),
                        "pair_ratio": self.ratios_manager.get(from_idx, to_idx),
                    }
                    for from_idx, to_idx in dirty_cells
                ],
            )
        self.ratios_manager.commit()

    def batch_update_coin_values(self, cv_batch: List[CoinValue]):
        session: Session
        with self.db_session() as session:
            session.execute(
                insert(CoinValue),
                [
                    {
                        "coin_id": cv.coin.symbol,
                        "balance": cv.balance,
                        "usd_price": cv.usd_price,
                        "btc_price": cv.btc_price,
                        "interval": cv.interval,
                        "datetime": cv.datetime,
                    }
                    for cv in cv_batch
                ],
            )


class TradeLog:
    def __init__(self, db: Database, from_coin: str, to_coin: str, selling: bool):
        self.db = db
        session: Session
        with self.db.db_session() as session:
            # from_coin = session.merge(from_coin)
            # to_coin = session.merge(to_coin)
            self.trade = Trade(from_coin, to_coin, selling)
            session.add(self.trade)
            # Flush so that SQLAlchemy fills in the id column
            session.flush()
            self.db.send_update(self.trade)

    def set_ordered(self, alt_starting_balance, crypto_starting_balance, alt_trade_amount):
        session: Session
        with self.db.db_session() as session:
            trade: Trade = session.merge(self.trade)
            trade.alt_starting_balance = alt_starting_balance
            trade.alt_trade_amount = alt_trade_amount
            trade.crypto_starting_balance = crypto_starting_balance
            trade.state = TradeState.ORDERED
            self.db.send_update(trade)

    def set_complete(self, crypto_trade_amount):
        session: Session
        with self.db.db_session() as session:
            trade: Trade = session.merge(self.trade)
            trade.crypto_trade_amount = crypto_trade_amount
            trade.state = TradeState.COMPLETE
            self.db.send_update(trade)


if __name__ == "__main__":
    database = Database(Logger(), Config())
    database.create_database()
