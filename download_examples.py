import json
import os

from dotenv import load_dotenv

from src.client import Division, LeagueClient, Queue, Region, RegionGroup, Tier

load_dotenv()
API_KEY = os.environ["API_KEY"]
REGION = Region.NA1
REGION_GROUP = RegionGroup.from_region(REGION)
LEAGUE_EXAMPLE_PATH = "output/league_example.json"
MATCH_IDS_EXAMPLE_PATH = "output/match_ids_example.json"
MATCH_EXAMPLE_PATH = "output/match_example.json"


def main():
    league_client = LeagueClient(API_KEY)
    league = league_client.get_league(REGION, Queue.RANKED_SOLO_5x5, Tier.CHALLENGER, Division.I)
    with open(LEAGUE_EXAMPLE_PATH, "w") as file:
        json.dump(league, file, indent=4, ensure_ascii=False)
    puuid = league[0]["puuid"]
    match_ids = league_client.get_match_ids_by_puuid(REGION_GROUP, puuid)
    with open(MATCH_IDS_EXAMPLE_PATH, "w") as file:
        json.dump(match_ids, file, indent=4, ensure_ascii=False)
    match_id = match_ids[0]
    match_ = league_client.get_match(REGION_GROUP, match_id)
    with open(MATCH_EXAMPLE_PATH, "w") as file:
        json.dump(match_, file, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    main()
