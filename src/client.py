import time
from enum import Enum
from typing import Self

import requests
import structlog

logger = structlog.get_logger()


class Region(Enum):
    BR1 = "br1"
    EUN1 = "eun1"
    EUW1 = "euw1"
    JP1 = "jp1"
    KR = "kr"
    LA1 = "la1"
    LA2 = "la2"
    ME1 = "me1"
    NA1 = "na1"
    OC1 = "oc1"
    RU = "ru"
    SG2 = "sg2"
    TR1 = "tr1"
    TW2 = "tw2"
    VN2 = "vn2"


class RegionGroup(Enum):
    AMERICAS = "americas"
    ASIA = "asia"
    EUROPE = "europe"
    SEA = "sea"

    @classmethod
    def from_region(cls, region: Region) -> Self:
        AMERICA_REGIONS = [Region.BR1, Region.LA1, Region.LA2, Region.NA1]
        EUROPE_REGIONS = [Region.EUN1, Region.EUW1, Region.ME1, Region.RU, Region.TR1]
        ASIA_REGIONS = [Region.JP1, Region.KR]
        SEA_REGIONS = [Region.OC1, Region.SG2, Region.TW2, Region.VN2]
        if region in AMERICA_REGIONS:
            return cls.AMERICAS
        if region in EUROPE_REGIONS:
            return cls.EUROPE
        if region in ASIA_REGIONS:
            return cls.ASIA
        if region in SEA_REGIONS:
            return cls.SEA
        raise ValueError("Invalid region")


class Queue(Enum):
    RANKED_SOLO_5x5 = "RANKED_SOLO_5x5"
    RANKED_FLEX_SR = "RANKED_FLEX_SR"
    RANKED_FLEX_TT = "RANKED_FLEX_TT"


# Source: https://static.developer.riotgames.com/docs/lol/queues.json
class QueueId(Enum):
    RANKED_SOLO_5x5 = 420
    RANKED_FLEX_SR = 440
    RANKED_FLEX_TT = 450


class MatchType(Enum):
    RANKED = "ranked"
    NORMAL = "normal"
    TOURNEY = "tourney"
    TUTORIAL = "tutorial"


class Tier(Enum):
    CHALLENGER = "CHALLENGER"
    GRANDMASTER = "GRANDMASTER"
    MASTER = "MASTER"
    DIAMOND = "DIAMOND"
    EMERALD = "EMERALD"
    PLATINUM = "PLATINUM"
    GOLD = "GOLD"
    SILVER = "SILVER"
    BRONZE = "BRONZE"
    IRON = "IRON"

    def is_apex_tier(self):
        apex_tiers = [Tier.MASTER, Tier.GRANDMASTER, Tier.CHALLENGER]
        return self in apex_tiers


class Division(Enum):
    I = "I"
    II = "II"
    III = "III"
    IV = "IV"


class LeagueClient:
    MAX_RETRIES = 5
    HEADER_API_KEY = "X-Riot-Token"
    HEADER_COUNT = "X-App-Rate-Limit-Count"
    HEADER_LIMIT = "X-App-Rate-Limit"
    HEADER_RETRY_AFTER = "Retry-After"

    def __init__(self, api_key: str):
        self._api_key = api_key

    def get_league(
        self, region: Region, queue: Queue, tier: Tier, division: Division, page: int = 1
    ):
        url = f"https://{region.value}.api.riotgames.com/lol/league-exp/v4/entries/{queue.value}/{tier.value}/{division.value}"
        params = {"page": page}
        league = self.get(url, params=params)
        return league

    def get_match_ids_by_puuid(
        self,
        region_group: RegionGroup,
        puuid: str,
        start_time: int | None = None,
        end_time: int | None = None,
        queue_id: QueueId | None = None,
        match_type: MatchType | None = None,
        start: int = 0,
        count: int = 100,
    ):
        assert 0 <= start, "Start must be greater than or equal to 0"
        assert 0 <= count <= 100, "Count must be between 0 and 100"
        url = f"https://{region_group.value}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
        params = {
            "startTime": start_time,
            "endTime": end_time,
            "queue": queue_id.value if queue_id else None,
            "type": match_type.value if match_type else None,
            "start": start,
            "count": count,
        }
        match_ids = self.get(url, params=params)
        return match_ids

    def get_match(self, region_group: RegionGroup, match_id: str):
        url = f"https://{region_group.value}.api.riotgames.com/lol/match/v5/matches/{match_id}"
        match_ = self.get(url)
        return match_

    def get(self, url: str, params: dict | None = None):
        for attempt in range(LeagueClient.MAX_RETRIES + 1):
            try:
                local_logger = logger.bind(
                    attempt=attempt + 1, max_attempt=LeagueClient.MAX_RETRIES, url=url
                )
                headers = {LeagueClient.HEADER_API_KEY: self._api_key}
                response = requests.get(url, params=params, headers=headers)
                response.raise_for_status()
                body = response.json()
                return body
            except requests.HTTPError as e:
                local_logger.warning(
                    "HTTP error occurred",
                    error_type=e.__class__.__name__,
                    error_message=str(e),
                    status_code=response.status_code,
                )
                if attempt >= LeagueClient.MAX_RETRIES:
                    local_logger.exception("Max retries exceeded. Raising exception")
                    raise
                retry_after = response.headers.get(LeagueClient.HEADER_RETRY_AFTER, 2 * attempt)
                retry_after = int(retry_after)
                local_logger.warning(f"Retrying after {retry_after} seconds")
                time.sleep(retry_after)
            except Exception as e:
                local_logger.exception(
                    "An unexpected error occurred",
                    error_type=e.__class__.__name__,
                    error_message=str(e),
                )
                if attempt >= LeagueClient.MAX_RETRIES:
                    local_logger.exception("Max retries exceeded. Raising exception")
                    raise
                retry_after = 2**attempt
                local_logger.warning(f"Retrying after {retry_after} seconds")
                time.sleep(retry_after)
        return None
