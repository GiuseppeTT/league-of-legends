DROP TABLE IF EXISTS league_unpivoted_matches;

CREATE TABLE league_unpivoted_matches AS
WITH unpivoted_matches AS (
    SELECT
        metadata_matchId AS match_id,
        info_gameStartTimestamp  AS started_at,
        info_participants0_puuid AS puuid,
        info_participants0_teamPosition AS position,
        info_participants0_championName AS champion,
        info_participants0_win AS win
    FROM
        matches
    UNION ALL
    SELECT
        metadata_matchId AS match_id,
        info_gameStartTimestamp  AS started_at,
        info_participants1_puuid AS puuid,
        info_participants1_teamPosition AS position,
        info_participants1_championName AS champion,
        info_participants1_win AS win
    FROM
        matches
    UNION ALL
    SELECT
        metadata_matchId AS match_id,
        info_gameStartTimestamp  AS started_at,
        info_participants2_puuid AS puuid,
        info_participants2_teamPosition AS position,
        info_participants2_championName AS champion,
        info_participants2_win AS win
    FROM
        matches
    UNION ALL
    SELECT
        metadata_matchId AS match_id,
        info_gameStartTimestamp  AS started_at,
        info_participants3_puuid AS puuid,
        info_participants3_teamPosition AS position,
        info_participants3_championName AS champion,
        info_participants3_win AS win
    FROM
        matches
    UNION ALL
    SELECT
        metadata_matchId AS match_id,
        info_gameStartTimestamp  AS started_at,
        info_participants4_puuid AS puuid,
        info_participants4_teamPosition AS position,
        info_participants4_championName AS champion,
        info_participants4_win AS win
    FROM
        matches
    UNION ALL
    SELECT
        metadata_matchId AS match_id,
        info_gameStartTimestamp  AS started_at,
        info_participants5_puuid AS puuid,
        info_participants5_teamPosition AS position,
        info_participants5_championName AS champion,
        info_participants5_win AS win
    FROM
        matches
    UNION ALL
    SELECT
        metadata_matchId AS match_id,
        info_gameStartTimestamp  AS started_at,
        info_participants6_puuid AS puuid,
        info_participants6_teamPosition AS position,
        info_participants6_championName AS champion,
        info_participants6_win AS win
    FROM
        matches
    UNION ALL
    SELECT
        metadata_matchId AS match_id,
        info_gameStartTimestamp  AS started_at,
        info_participants7_puuid AS puuid,
        info_participants7_teamPosition AS position,
        info_participants7_championName AS champion,
        info_participants7_win AS win
    FROM
        matches
    UNION ALL
    SELECT
        metadata_matchId AS match_id,
        info_gameStartTimestamp  AS started_at,
        info_participants8_puuid AS puuid,
        info_participants8_teamPosition AS position,
        info_participants8_championName AS champion,
        info_participants8_win AS win
    FROM
        matches
    UNION ALL
    SELECT
        metadata_matchId AS match_id,
        info_gameStartTimestamp  AS started_at,
        info_participants9_puuid AS puuid,
        info_participants9_teamPosition AS position,
        info_participants9_championName AS champion,
        info_participants9_win AS win
    FROM
        matches
),
raw_league_unpivoted_matches AS (
    SELECT
        DATE(um.started_at / 1000, 'unixepoch') AS started_at,
        um.match_id,
        um.puuid,
        pl.tier,
        pl.division,
        um.position,
        um.champion,
        um.win,
        ROW_NUMBER() OVER (PARTITION BY um.match_id, um.puuid ORDER BY pl.crawled_at DESC) AS rn
    FROM
        unpivoted_matches AS um
    JOIN
        players AS pl
    ON 
        um.puuid = pl.puuid AND
        DATE(um.started_at  / 1000, 'unixepoch') >= pl.crawled_at
)
SELECT
    started_at,
    match_id,
    puuid,
    tier,
    division,
    position,
    champion,
    win
FROM
    raw_league_unpivoted_matches
WHERE
    rn = 1;