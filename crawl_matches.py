"""
# Run
uv run crawl_matches.py | tee -a log/crawler.log

# Objectives
- Get a daily snapshot of the current top leagues
- Get as many matches as possible for the players in the top leagues
- If tied, go with the option with higher tier

# Notes
This crawler assumes that the ultimate bottleneck is the API rate limits
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import count

import structlog
from dotenv import load_dotenv

from src.client import Division, LeagueClient, Queue, QueueId, Region, RegionGroup, Tier
from src.util import now

load_dotenv(override=True)
API_KEY = os.environ["API_KEY"]

# TODO: Use CLI arguments to get this
REGION = Region.NA1
REGION_GROUP = RegionGroup.from_region(REGION)
CRAWL_TIERS = [Tier.CHALLENGER, Tier.GRANDMASTER, Tier.MASTER, Tier.DIAMOND]
LEAGUE_CRAWL_INTERVAL = timedelta(days=1)
MATCH_TIME_WINDOW = timedelta(days=7)
MATCH_COUNT_MAXIMUM = 100


structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.dev.ConsoleRenderer(colors=False, sort_keys=False),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.NOTSET),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=False,
)
logger = structlog.get_logger()


@dataclass
class Player:
    puuid: str
    tier: Tier
    division: Division
    last_league_crawl_time: datetime
    last_match_crawl_time: datetime | None = None
    last_match_count: int | None = None

    def __str__(self):
        puuid = self.puuid
        tier = self.tier.value
        division = self.division.value
        last_league_crawl_time = self.last_league_crawl_time.isoformat().replace("+00:00", "Z")
        last_match_crawl_time = (
            self.last_match_crawl_time.isoformat().replace("+00:00", "Z")
            if self.last_match_crawl_time
            else None
        )
        last_match_count = self.last_match_count if self.last_match_count else None
        return f"Player(puuid={puuid}, tier={tier}, division={division}, last_league_crawl_time={last_league_crawl_time}, last_match_crawl_time={last_match_crawl_time}, last_match_count={last_match_count})"

    def __repr__(self):
        return str(self)

    def from_league_entry(league_entry: dict):
        player = Player(
            puuid=league_entry["puuid"],
            tier=Tier[league_entry["tier"]],
            division=Division[league_entry["rank"]],
            last_league_crawl_time=now(),
        )
        return player

    def update_from_league_entry(self, league_entry: dict):
        self.tier = Tier[league_entry["tier"]]
        self.division = Division[league_entry["rank"]]
        self.last_league_crawl_time = now()

    def update_from_match_ids(self, match_ids: set[str]):
        self.last_match_crawl_time = now()
        self.last_match_count = len(match_ids)

    def is_match_never_crawled(self) -> bool:
        return self.last_match_crawl_time is None

    def get_period_since_last_match_crawl(self) -> timedelta:
        if self.last_match_crawl_time is None:
            return timedelta.max
        period_since_last_match_crawl = now() - self.last_match_crawl_time
        return period_since_last_match_crawl

    def estimate_new_match_count(self) -> float:
        if self.last_match_crawl_time is None:
            return float("inf")
        period_since_last_match_crawl = self.get_period_since_last_match_crawl()
        estimated_new_match_count = (
            self.last_match_count * period_since_last_match_crawl / MATCH_TIME_WINDOW
        )
        return estimated_new_match_count


def main():
    logger.info("Starting crawler")
    league_client = LeagueClient(API_KEY)
    last_league_crawl_time = datetime.min.replace(tzinfo=timezone.utc)
    player_by_puuid = {}
    crawl_time_by_match_id = {}
    while True:
        if now() - last_league_crawl_time >= LEAGUE_CRAWL_INTERVAL:
            logger.info(
                "League data is outdated. Crawling leagues",
                last_league_crawl_time=last_league_crawl_time.isoformat(),
            )
            crawl_leagues(league_client, player_by_puuid)
            player_by_puuid = clean_player_by_puuid(player_by_puuid)
            crawl_time_by_match_id = clean_crawl_time_by_match_id(crawl_time_by_match_id)
            last_league_crawl_time = now()
            continue
        player = get_next_player_for_crawl(player_by_puuid)
        new_match_ids = crawl_new_match_ids(league_client, player, crawl_time_by_match_id)
        crawl_matches(league_client, new_match_ids, crawl_time_by_match_id, player)


def crawl_leagues(league_client: LeagueClient, player_by_puuid: dict[str, Player]):
    for tier in CRAWL_TIERS:
        for division in Division:
            if tier.is_apex_tier() and division != Division.I:
                continue
            for page in count(1):
                league = league_client.get_league(
                    REGION, Queue.RANKED_SOLO_5x5, tier, division, page
                )
                logger.info(
                    f"Got {len(league)} players",
                    tier=tier.value,
                    division=division.value,
                    page=page,
                )
                if len(league) == 0:
                    break
                # TODO: Write league entry to DB here
                for entry in league:
                    puuid = entry["puuid"]
                    if puuid not in player_by_puuid:
                        player_by_puuid[puuid] = Player.from_league_entry(entry)
                    else:
                        player_by_puuid[puuid].update_from_league_entry(entry)


def clean_player_by_puuid(player_by_puuid: dict[str, Player]) -> dict[str, Player]:
    player_by_puuid = remove_old_players(player_by_puuid)
    player_by_puuid = sort_players_by_league(player_by_puuid)
    logger.info(f"There are {len(player_by_puuid)} players being tracked")
    return player_by_puuid


def remove_old_players(player_by_puuid: dict[str, Player]) -> dict[str, Player]:
    player_by_puuid = {
        puuid: player
        for puuid, player in player_by_puuid.items()
        if now() - player.last_league_crawl_time <= MATCH_TIME_WINDOW
    }
    return player_by_puuid


def sort_players_by_league(player_by_puuid: dict[str, Player]) -> dict[str, Player]:
    tier_order = list(Tier)
    division_order = list(Division)
    player_by_puuid = sorted(  # ty: ignore[invalid-assignment]
        player_by_puuid.items(),
        key=lambda item: (tier_order.index(item[1].tier), division_order.index(item[1].division)),
    )
    player_by_puuid = dict(player_by_puuid)
    return player_by_puuid


def clean_crawl_time_by_match_id(
    crawl_time_by_match_id: dict[str, datetime],
) -> dict[str, datetime]:
    crawl_time_by_match_id = {
        match_id: crawl_time
        for match_id, crawl_time in crawl_time_by_match_id.items()
        if now() - crawl_time <= MATCH_TIME_WINDOW
    }
    logger.info(f"There are {len(crawl_time_by_match_id)} matches being tracked")
    return crawl_time_by_match_id


def get_next_player_for_crawl(player_by_puuid: dict[str, Player]) -> Player:
    maximum_player = list(player_by_puuid.values())[0]
    maximum_estimated_new_match_count = maximum_player.estimate_new_match_count()
    for player in player_by_puuid.values():
        if player.is_match_never_crawled():
            logger.info(
                f"Player {player.puuid} has never been crawled for matches. Crawling next",
                player=player,
            )
            return player
        if player.get_period_since_last_match_crawl() >= MATCH_TIME_WINDOW / 2:
            logger.info(
                f"Player {player.puuid} has been crawled for too long. Crawling next",
                player=player,
                period_since_last_match_crawl=player.get_period_since_last_match_crawl(),
            )
            return player
        estimated_new_match_count = player.estimate_new_match_count()
        if estimated_new_match_count >= MATCH_COUNT_MAXIMUM / 2:
            logger.info(
                f"Player {player.puuid} probably has too many new matches. Crawling next",
                player=player,
                estimated_new_match_count=estimated_new_match_count,
            )
            return player
        if estimated_new_match_count >= maximum_estimated_new_match_count:
            maximum_estimated_new_match_count = estimated_new_match_count
            maximum_player = player
    logger.info(
        f"Player {maximum_player.puuid} probably has the most new matches. Crawling next",
        player=maximum_player,
        estimated_new_match_count=maximum_estimated_new_match_count,
    )
    return maximum_player


def crawl_new_match_ids(
    league_client: LeagueClient, player: Player, crawl_time_by_match_id: dict[str, datetime]
) -> set[str]:
    local_logger = logger.bind(player=player)
    start_time = now() - MATCH_TIME_WINDOW
    start_time = int(start_time.timestamp())
    match_ids = league_client.get_match_ids_by_puuid(
        REGION_GROUP, player.puuid, start_time=start_time, queue_id=QueueId.RANKED_SOLO_5x5
    )
    match_ids = set(match_ids)
    player.update_from_match_ids(match_ids)
    new_match_ids = match_ids - set(crawl_time_by_match_id.keys())
    local_logger.info(f"Got {len(match_ids)} match ids, of which {len(new_match_ids)} are new")
    return new_match_ids


def crawl_matches(
    league_client: LeagueClient,
    new_match_ids: set[str],
    crawl_time_by_match_id: dict[str, datetime],
    player: Player,
):
    for i, new_match_id in enumerate(new_match_ids):
        match_ = league_client.get_match(REGION_GROUP, new_match_id)
        logger.info(
            "Got match data",
            index=i + 1,
            max_index=len(new_match_ids),
            match_id=new_match_id,
            player=player,
        )
        # TODO: Write match data to DB here
        crawl_time_by_match_id[new_match_id] = now()


if __name__ == "__main__":
    main()
