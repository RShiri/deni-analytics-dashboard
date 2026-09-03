"""
NBA data fetcher for the Israeli NBA Players Analytics dashboard.

Pulls career stats, per-season game logs, and shot charts for every player in PLAYERS via
nba_api (the real NBA Stats API — not a stub), plus a shared All-Star-roster comparison set
and league free-throw leaders. Everything is written to nba_data.pkl, which app.py reads.

Usage:
    python fetch_data.py                        # fetch every player + shared datasets
    python fetch_data.py --player ben_saraf      # just one player (faster iteration)
    python fetch_data.py --seasons 2025-26 2026-27   # override which seasons to pull
"""

import argparse
import pickle
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from nba_api.stats.endpoints import (
    leaguedashplayerstats,
    playercareerstats,
    playerdashboardbyyearoveryear,
    playergamelog,
    shotchartdetail,
)

DATA_FILE = Path(__file__).parent / "nba_data.pkl"
REQUEST_DELAY = 0.7  # seconds between NBA Stats API calls
TIMEOUT = 30
MAX_SEASONS_BACK = 4  # cap how many past seasons we pull per player

# Active Israeli NBA players this dashboard tracks. IDs are NBA.com PERSON_IDs.
PLAYERS = {
    "deni_avdija": {"id": 1630166, "name": "Deni Avdija", "draft_year": 2020},
    "ben_saraf": {"id": 1642879, "name": "Ben Saraf", "draft_year": 2025},
    "danny_wolf": {"id": 1642874, "name": "Danny Wolf", "draft_year": 2025},
    "emanuel_sharp": {"id": 1643567, "name": "Emanuel Sharp", "draft_year": 2026},
}

# 2026 NBA All-Star Game roster (played Feb 15, 2026 at Intuit Dome) — the comparison
# cohort on the "Deep Dive" page. Deni Avdija made the team as a Western reserve, so he's
# excluded from his own comparison cohort at render time (see app.py). Update every February.
ALL_STAR_ROSTER = [
    "Giannis Antetokounmpo", "Jaylen Brown", "Jalen Brunson", "Cade Cunningham", "Tyrese Maxey",
    "Scottie Barnes", "Jalen Duren", "Jalen Johnson", "Donovan Mitchell", "Norman Powell",
    "Pascal Siakam", "Karl-Anthony Towns",
    "Stephen Curry", "Luka Doncic", "Shai Gilgeous-Alexander", "Nikola Jokic", "Victor Wembanyama",
    "Anthony Edwards", "Kevin Durant", "Devin Booker", "Deni Avdija", "Chet Holmgren",
    "LeBron James", "Jamal Murray",
]
ALL_STAR_SEASON = "2025-26"  # the season ALL_STAR_ROSTER above played in


def get_current_season() -> str:
    """NBA season label for 'today'. Regular season runs Oct-June; July-Sept (off-season)
    is treated as the upcoming season, so a summer draft pick lands under their first season."""
    today = datetime.now()
    y, m = today.year, today.month
    if m <= 6:
        start = y - 1
    else:
        start = y
    return season_str(start)


def season_str(start_year: int) -> str:
    return f"{start_year}-{str(start_year + 1)[2:]}"


def seasons_for_player(draft_year: int) -> list:
    current_start = int(get_current_season()[:4])
    start = max(draft_year, current_start - MAX_SEASONS_BACK + 1)
    return [season_str(y) for y in range(start, current_start + 1)]


def _sleep():
    time.sleep(REQUEST_DELAY)


def fetch_career_stats(player_id: int):
    """Per-game career basic stats + per-season advanced stats (USG%, TS%, NET_RATING...)."""
    basic, advanced = pd.DataFrame(), pd.DataFrame()
    try:
        career = playercareerstats.PlayerCareerStats(player_id=player_id, per_mode36="PerGame", timeout=TIMEOUT)
        basic = career.get_data_frames()[0]
    except Exception as e:
        print(f"  ! career basic stats failed: {e}")
    _sleep()
    try:
        dash = playerdashboardbyyearoveryear.PlayerDashboardByYearOverYear(
            player_id=player_id, measure_type_detailed="Advanced", per_mode_detailed="PerGame", timeout=TIMEOUT,
        )
        advanced = dash.get_data_frames()[1]  # [0]=overall career, [1]=by-year rows
        advanced = advanced.rename(columns={"GROUP_VALUE": "SEASON_ID"})
    except Exception as e:
        print(f"  ! career advanced stats failed: {e}")
    _sleep()
    return basic, advanced


def fetch_game_logs(player_id: int, seasons: list) -> dict:
    logs = {}
    for season in seasons:
        try:
            gl = playergamelog.PlayerGameLog(player_id=player_id, season=season, timeout=TIMEOUT)
            df = gl.get_data_frames()[0]
            if not df.empty:
                logs[season] = df
        except Exception as e:
            print(f"  ! game log fetch failed for {season}: {e}")
        _sleep()
    return logs


def fetch_shot_charts(player_id: int, seasons: list) -> dict:
    charts = {}
    for season in seasons:
        try:
            sc = shotchartdetail.ShotChartDetail(
                team_id=0,
                player_id=player_id,
                context_measure_simple="FGA",
                season_nullable=season,
                timeout=TIMEOUT,
            )
            df = sc.get_data_frames()[0]
            if not df.empty:
                charts[season] = df
        except Exception as e:
            print(f"  ! shot chart fetch failed for {season}: {e}")
        _sleep()
    return charts


def fetch_player(meta: dict, seasons_override=None) -> dict:
    print(f"Fetching {meta['name']} (id={meta['id']})...")
    seasons = seasons_override or seasons_for_player(meta["draft_year"])
    basic, advanced = fetch_career_stats(meta["id"])
    game_logs = fetch_game_logs(meta["id"], seasons)
    shot_charts = fetch_shot_charts(meta["id"], seasons)
    return {
        "player_id": meta["id"],
        "player_name": meta["name"],
        "career_basic": basic,
        "career_advanced": advanced,
        "game_logs": game_logs,
        "shot_charts": shot_charts,
        "seasons": seasons,
        "fetched_at": datetime.now().isoformat(),
    }


def fetch_allstar_stats(season: str):
    """Per-game + advanced stats for the ALL_STAR_ROSTER cohort, used as a comparison bar."""
    empty_basic = pd.DataFrame(columns=["PLAYER_NAME", "GP", "PTS", "REB", "AST", "STL", "BLK", "TOV"])
    empty_detailed = pd.DataFrame(columns=["PLAYER_NAME", "PTS", "REB", "AST", "USG_PCT", "TS_PCT"])
    try:
        base = leaguedashplayerstats.LeagueDashPlayerStats(
            season=season, per_mode_detailed="PerGame", measure_type_detailed_defense="Base", timeout=TIMEOUT,
        ).get_data_frames()[0]
    except Exception as e:
        print(f"  ! all-star base stats failed: {e}")
        return empty_basic, empty_detailed
    _sleep()

    roster_base = base[base["PLAYER_NAME"].isin(ALL_STAR_ROSTER)][
        ["PLAYER_NAME", "GP", "PTS", "REB", "AST", "STL", "BLK", "TOV"]
    ].reset_index(drop=True)

    try:
        adv = leaguedashplayerstats.LeagueDashPlayerStats(
            season=season, per_mode_detailed="PerGame", measure_type_detailed_defense="Advanced", timeout=TIMEOUT,
        ).get_data_frames()[0]
        roster_adv = adv[adv["PLAYER_NAME"].isin(ALL_STAR_ROSTER)][["PLAYER_NAME", "USG_PCT", "TS_PCT"]]
        detailed = roster_base.merge(roster_adv, on="PLAYER_NAME", how="left")[
            ["PLAYER_NAME", "PTS", "REB", "AST", "USG_PCT", "TS_PCT"]
        ]
    except Exception as e:
        print(f"  ! all-star advanced stats failed: {e}")
        detailed = empty_detailed
    _sleep()

    return roster_base, detailed


def fetch_league_ft_stats():
    """Season-total FT leaders across the whole league. Falls back to the prior season if
    the current one hasn't tipped off yet (empty response, e.g. deep off-season)."""

    def _query(season):
        try:
            df = leaguedashplayerstats.LeagueDashPlayerStats(
                season=season, per_mode_detailed="Totals", measure_type_detailed_defense="Base", timeout=TIMEOUT,
            ).get_data_frames()[0]
            return df[["PLAYER_NAME", "TEAM_ABBREVIATION", "GP", "FTM", "FTA", "FT_PCT"]]
        except Exception as e:
            print(f"  ! league FT stats failed for {season}: {e}")
            return pd.DataFrame(columns=["PLAYER_NAME", "TEAM_ABBREVIATION", "GP", "FTM", "FTA", "FT_PCT"])

    season = get_current_season()
    df = _query(season)
    _sleep()
    if df.empty:
        prev = season_str(int(season[:4]) - 1)
        df = _query(prev)
        _sleep()
        season = prev
    return df, season


def main():
    parser = argparse.ArgumentParser(description="Fetch NBA data for the Israeli NBA players dashboard.")
    parser.add_argument("--player", choices=list(PLAYERS), help="Only fetch this player (default: all)")
    parser.add_argument("--seasons", nargs="+", help="Override the season list, e.g. --seasons 2025-26 2026-27")
    args = parser.parse_args()

    existing = {}
    if DATA_FILE.exists():
        try:
            with open(DATA_FILE, "rb") as f:
                existing = pickle.load(f)
        except Exception:
            existing = {}
    players_data = existing.get("players", {})

    keys = [args.player] if args.player else list(PLAYERS)
    for key in keys:
        try:
            players_data[key] = fetch_player(PLAYERS[key], args.seasons)
        except Exception as e:
            print(f"! Failed to fetch {PLAYERS[key]['name']}: {e}")

    print("Fetching All-Star comparison roster...")
    allstar_stats, allstar_detailed_stats = fetch_allstar_stats(ALL_STAR_SEASON)

    print("Fetching league free-throw leaders...")
    league_ft_stats, ft_season = fetch_league_ft_stats()

    data = {
        "players": players_data,
        "allstar_stats": allstar_stats,
        "allstar_detailed_stats": allstar_detailed_stats,
        "allstar_season": ALL_STAR_SEASON,
        "league_ft_stats": league_ft_stats,
        "league_ft_season": ft_season,
        "fetched_at": datetime.now().isoformat(),
    }

    with open(DATA_FILE, "wb") as f:
        pickle.dump(data, f)
    print(f"Saved {DATA_FILE}")


if __name__ == "__main__":
    main()
