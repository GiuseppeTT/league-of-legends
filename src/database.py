import psycopg
import structlog
from psycopg.types.json import Jsonb

from src.client import Queue, QueueId
from src.util import convert_epoch_to_datetime, now

logger = structlog.get_logger()


class DatabaseHandler:
    def __init__(self, user: str, password: str, host: str, port: str, database: str):
        self._connection = self._create_connection(user, password, host, port, database)
        self._create_tables()

    def _create_connection(self, user: str, password: str, host: str, port: str, database: str):
        local_logger = logger.bind(user=user, host=host, port=port, database=database)
        try:
            connection = psycopg.connect(
                user=user, password=password, host=host, port=port, dbname=database
            )
            local_logger.info("Created database connection")
            return connection
        except:
            local_logger.exception("Failed to create database connection")
            raise

    def _create_tables(self):
        local_logger = logger
        try:
            cursor = self._connection.cursor()
            sql_create_leagues_table = """
            CREATE TABLE IF NOT EXISTS leagues (
                crawled_at TIMESTAMPTZ NOT NULL,
                puuid TEXT NOT NULL,
                queue TEXT NOT NULL,
                tier TEXT NOT NULL,
                rank TEXT NOT NULL,
                dump JSONB NOT NULL,
                PRIMARY KEY (puuid, crawled_at)
            );
            """
            sql_create_matches_table = """
            CREATE TABLE IF NOT EXISTS matches (
                crawled_at TIMESTAMPTZ NOT NULL,
                ended_at TIMESTAMPTZ NOT NULL,
                match_id TEXT NOT NULL,
                region TEXT NOT NULL,
                version TEXT NOT NULL,
                queue TEXT NOT NULL,
                dump JSONB NOT NULL,
                PRIMARY KEY (match_id)
            );
            """
            cursor.execute(sql_create_leagues_table)
            cursor.execute(sql_create_matches_table)
            self._connection.commit()
            local_logger.info("Created tables")
        except:
            local_logger.exception("Failed to create tables")
            raise

    def write_leagues(self, leagues: list[dict]):
        local_logger = logger
        try:
            cursor = self._connection.cursor()
            sql = """
            INSERT INTO leagues
            (crawled_at, puuid, queue, tier, rank, dump)
            VALUES
            (%s, %s, %s, %s, %s, %s)
            """
            crawled_at = now()
            values = (
                (
                    crawled_at,
                    league["puuid"],
                    league["queueType"],
                    league["tier"],
                    league["rank"],
                    Jsonb(league),
                )
                for league in leagues
            )
            cursor.executemany(sql, values)
            self._connection.commit()
            local_logger.info("Inserted entries into 'leagues' table")
        except psycopg.Error:
            local_logger.exception("Failed to insert entries into 'leagues' table")
            self._connection.rollback()

    def write_match(self, match_: dict):
        local_logger = logger.bind(match_id=match_["metadata"]["matchId"])
        try:
            cursor = self._connection.cursor()
            sql = """
            INSERT INTO matches
            (crawled_at, ended_at, match_id, region, version, queue, dump)
            VALUES
            (%s, %s, %s, %s, %s, %s, %s)
            """
            crawled_at = now()
            value = (
                crawled_at,
                convert_epoch_to_datetime(match_["info"]["gameEndTimestamp"]),
                match_["metadata"]["matchId"],
                match_["info"]["platformId"],
                match_["info"]["gameVersion"],
                Queue.from_id(match_["info"]["queueId"]).value,
                Jsonb(match_),
            )
            cursor.execute(sql, value)
            self._connection.commit()
            local_logger.info("Inserted entry into 'matches' table")
        except psycopg.Error:
            local_logger.exception("Failed to insert entry into 'matches' table")
            self._connection.rollback()
