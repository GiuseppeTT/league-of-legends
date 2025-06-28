import os

from dotenv import load_dotenv

from src.client import Division, LeagueClient, Queue, QueueId, Region, RegionGroup, Tier
from src.util import flatten_match

load_dotenv()
API_KEY = os.environ["API_KEY"]
REGION = Region["NA1"]
REGION_GROUP = RegionGroup.from_region(REGION)
GUESSED_MATCH_SCHEMA_PATH = "output/guessed_match_schema.sql"


def main():
    league_client = LeagueClient(API_KEY)
    league_page = league_client.get_league(
        REGION, Queue.RANKED_SOLO_5x5, Tier.CHALLENGER, Division.I, page=1
    )
    puuid = league_page[0]["puuid"]
    match_ids = league_client.get_match_ids_by_puuid(
        REGION_GROUP, puuid, queue_id=QueueId.RANKED_SOLO_5x5
    )
    match_id = match_ids[0]
    match_ = league_client.get_match(REGION_GROUP, match_id)
    flattened_match = flatten_match(match_)
    type_by_column = guess_schema(flattened_match)
    schema_text = ""
    schema_text += "CREATE TABLE IF NOT EXISTS matches (\n"
    schema_text += ",\n".join(f"    {key} {value}" for key, value in type_by_column.items())
    schema_text += "\n);"
    with open(GUESSED_MATCH_SCHEMA_PATH, "w") as file:
        file.write(schema_text)


def guess_schema(dictionary):
    type_by_column = {}
    for key, value in dictionary.items():
        type_by_column[key] = guess_type(value)
    return type_by_column


def guess_type(value):
    if isinstance(value, bool):
        return "INTEGER"
    elif isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "REAL"
    elif isinstance(value, str):
        return "TEXT"
    elif value is None:
        return "NULL VALUE"
    else:
        return "???"


if __name__ == "__main__":
    main()
