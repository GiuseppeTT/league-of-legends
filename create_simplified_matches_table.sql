DROP TABLE IF EXISTS simplified_matches;

CREATE TABLE
    simplified_matches
AS
SELECT
    matches.match_id AS match_id,
    matches.ended_at AS ended_at,
    matches.region AS region,
    participant.value ->> 'teamId' AS team,
    participant.value ->> 'teamPosition' AS position,
    participant.value ->> 'championName' AS champion,
    (participant.value ->> 'win')::boolean AS win
FROM
    matches,
    jsonb_array_elements(dump -> 'info' -> 'participants') AS participant