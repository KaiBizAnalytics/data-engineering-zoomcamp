{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for NBA game results.

    Casts types, renames for clarity, and exposes only columns needed downstream.
    Grain: one row per game.
*/

SELECT
    game_id,
    CAST(game_date AS DATE)                         AS game_date,
    CAST(season_id AS STRING)                       AS season_id,
    CAST(season_year AS INT64)                      AS season_year,

    CAST(team_id_home AS STRING)                    AS team_id_home,
    UPPER(TRIM(team_abbreviation_home))             AS team_abbr_home,
    INITCAP(TRIM(team_name_home))                   AS team_name_home,
    wl_home,
    CAST(pts_home AS INT64)                         AS pts_home,

    CAST(team_id_away AS STRING)                    AS team_id_away,
    UPPER(TRIM(team_abbreviation_away))             AS team_abbr_away,
    INITCAP(TRIM(team_name_away))                   AS team_name_away,
    wl_away,
    CAST(pts_away AS INT64)                         AS pts_away,

    CAST(home_win AS INT64)                         AS home_win,
    CAST(is_bubble_season AS BOOL)                  AS is_bubble_season,
    CAST(attendance AS INT64)                       AS attendance

FROM {{ source('nba_raw', 'games') }}
WHERE game_date IS NOT NULL
  AND wl_home   IS NOT NULL
  AND team_abbreviation_home IS NOT NULL
