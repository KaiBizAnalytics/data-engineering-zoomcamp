{{
    config(
        materialized='table',
        partition_by={
            "field": "season_year",
            "data_type": "int64",
            "range": {
                "start": 1946,
                "end": 2030,
                "interval": 1
            }
        },
        cluster_by=["team_abbr"]
    )
}}

/*
    Mart: Home Court Advantage

    Grain: one row per (team, season_year).

    Key metrics:
      - home_win_pct / away_win_pct per team per season
      - home_advantage_delta = home_win_pct - away_win_pct
        (positive = team wins more at home than away → real home advantage)
      - avg_attendance: average fans in the building for home games
      - league_home_win_pct: league-wide baseline for the season

    The COVID bubble flag marks 2019-20 (season_year = 2019) — played at
    Disney World with zero fans, letting us test whether crowd noise drives
    home court advantage. Expected: attendance ≈ 0 → delta collapses.
*/

WITH home_stats AS (
    SELECT
        season_year,
        team_abbr_home                                  AS team_abbr,
        team_name_home                                  AS team_name,
        is_bubble_season,
        COUNT(*)                                        AS home_games,
        SUM(home_win)                                   AS home_wins,
        SAFE_DIVIDE(SUM(home_win), COUNT(*))            AS home_win_pct,
        AVG(attendance)                                 AS avg_attendance
    FROM {{ ref('stg_games') }}
    GROUP BY season_year, team_abbr_home, team_name_home, is_bubble_season
),

away_stats AS (
    SELECT
        season_year,
        team_abbr_away                                  AS team_abbr,
        COUNT(*)                                        AS away_games,
        SUM(CASE WHEN wl_away = 'W' THEN 1 ELSE 0 END) AS away_wins,
        SAFE_DIVIDE(
            SUM(CASE WHEN wl_away = 'W' THEN 1 ELSE 0 END),
            COUNT(*)
        )                                               AS away_win_pct
    FROM {{ ref('stg_games') }}
    GROUP BY season_year, team_abbr_away
),

league_avg AS (
    SELECT
        season_year,
        SAFE_DIVIDE(SUM(home_wins), SUM(home_games))    AS league_home_win_pct,
        AVG(avg_attendance)                             AS league_avg_attendance
    FROM home_stats
    GROUP BY season_year
)

SELECT
    h.season_year,
    h.team_abbr,
    h.team_name,
    h.is_bubble_season,

    h.home_games,
    h.home_wins,
    ROUND(h.home_win_pct, 4)                            AS home_win_pct,

    a.away_games,
    a.away_wins,
    ROUND(a.away_win_pct, 4)                            AS away_win_pct,

    -- Core metric: does home > away? near-zero in bubble = fans matter
    ROUND(h.home_win_pct - a.away_win_pct, 4)           AS home_advantage_delta,

    ROUND(l.league_home_win_pct, 4)                     AS league_home_win_pct,

    -- Attendance metrics
    CAST(ROUND(h.avg_attendance) AS INT64)              AS avg_attendance,
    CAST(ROUND(l.league_avg_attendance) AS INT64)       AS league_avg_attendance

FROM home_stats      h
JOIN away_stats      a USING (season_year, team_abbr)
JOIN league_avg      l USING (season_year)
