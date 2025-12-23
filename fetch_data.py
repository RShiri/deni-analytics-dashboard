"""
Offline Data Fetching Script for Deni Avdija Analytics
Refactored for Smart Incremental Loading & Data Integrity

Run this script manually to fetch all NBA data and save it to nba_data.pkl.
This only needs to be run when you want to update the data.

Usage:
    python fetch_data.py
"""

import pandas as pd
from nba_api.stats.endpoints import (
    PlayerCareerStats,
    PlayerDashboardByYearOverYear,
    PlayerGameLog,
    shotchartdetail,
    leaguedashplayerstats,
)
from nba_api.stats.static import players
import pickle
from pathlib import Path
from datetime import datetime, timedelta
import time
from requests.exceptions import RequestException
import sys

# Constants
PLAYER_NAME = "Deni Avdija"
PLAYER_ID = 1630166  # Known ID for Deni Avdija
SEASONS_SHOT_MAPS = ["2022-23", "2023-24", "2024-25", "2025-26"]
ALL_STAR_NAMES = [
    # Eastern Conference Starters
    "Jalen Brunson", "Donovan Mitchell", "Jayson Tatum", "Giannis Antetokounmpo", "Karl-Anthony Towns",
    # Eastern Conference Reserves
    "Jaylen Brown", "Cade Cunningham", "Darius Garland", "Tyler Herro", "Damian Lillard",
    "Evan Mobley", "Pascal Siakam", "Trae Young",
    # Western Conference Starters
    "Shai Gilgeous-Alexander", "Stephen Curry", "LeBron James", "Kevin Durant", "Nikola Jokic",
    # Western Conference Reserves
    "Anthony Edwards", "James Harden", "Jaren Jackson Jr.", "Jalen Williams", "Anthony Davis",
    "Alperen Sengun", "Victor Wembanyama", "Kyrie Irving",
]

OUTPUT_FILE = "nba_data.pkl"


def is_valid_df(df) -> bool:
    """Strict validation: ensure DataFrame is not None and NOT empty."""
    return df is not None and not df.empty


def get_player_id(full_name: str = PLAYER_NAME) -> int:
    """Resolve NBA player ID."""
    try:
        hits = players.find_players_by_full_name(full_name)
        if hits:
            return hits[0]["id"]
    except Exception as e:
        print(f"⚠️  Could not resolve player ID from name, using known ID: {e}")
    return PLAYER_ID


def local_patch_career_stats(career_df: pd.DataFrame, logs: pd.DataFrame) -> pd.DataFrame:
    """Locally update 2025-26 career stats from game logs to avoid re-fetching."""
    if logs.empty or career_df.empty:
        return career_df
    
    # Ensure logs columns are numeric for aggregation
    cols_to_agg = ["MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV", "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA", "PF"]
    # We assume fetch_game_logs already converted them, but safety check:
    for c in cols_to_agg:
        if c in logs.columns:
            logs[c] = pd.to_numeric(logs[c], errors="coerce").fillna(0)

    true_gp = len(logs)
    if true_gp == 0: return career_df

    # Calculate Fresh Averages
    updated_row = {
        "GP": true_gp,
        "GS": true_gp, # Assuming all starts for simplicity or need real GS logic? Logs usually don't have GS easily unless inferred. Keeping explicit for now.
        "MIN": logs["MIN"].mean() if "MIN" in logs.columns else 0,
        "PTS": logs["PTS"].mean(),
        "REB": logs["REB"].mean(),
        "AST": logs["AST"].mean(),
        "STL": logs["STL"].mean() if "STL" in logs.columns else 0,
        "BLK": logs["BLK"].mean() if "BLK" in logs.columns else 0,
        "TOV": logs["TOV"].mean() if "TOV" in logs.columns else 0,
        "PF": logs["PF"].mean() if "PF" in logs.columns else 0,
        "FGM": logs["FGM"].mean() if "FGM" in logs.columns else 0,
        "FGA": logs["FGA"].mean() if "FGA" in logs.columns else 0,
        "FG3M": logs["FG3M"].mean() if "FG3M" in logs.columns else 0,
        "FG3A": logs["FG3A"].mean() if "FG3A" in logs.columns else 0,
        "FTM": logs["FTM"].mean() if "FTM" in logs.columns else 0,
        "FTA": logs["FTA"].mean() if "FTA" in logs.columns else 0,
        # Recalculate PCTs from Totals (Sum)
        "FG_PCT": (logs["FGM"].sum() / logs["FGA"].sum()) if logs["FGA"].sum() > 0 else 0,
        "FG3_PCT": (logs["FG3M"].sum() / logs["FG3A"].sum()) if logs["FG3A"].sum() > 0 else 0,
        "FT_PCT": (logs["FTM"].sum() / logs["FTA"].sum()) if logs["FTA"].sum() > 0 else 0,
    }

    mask = career_df["SEASON_ID"] == "2025-26"
    if mask.any():
        idx = career_df.index[mask][0]
        # Update columns
        for col, val in updated_row.items():
            if col in career_df.columns:
                career_df.at[idx, col] = val
                
    return career_df


def fetch_career_basic(player_id: int) -> pd.DataFrame:
    """Fetch career basic per-season stats."""
    print("📊 Fetching Deni's Career Basic Stats...")
    try:
        df = PlayerCareerStats(player_id=player_id, per_mode36="PerGame").get_data_frames()[0]
        df = df.copy()
        df["SEASON_ID"] = df["SEASON_ID"].astype(str)
        print(f"✅ Career Basic Stats: {len(df)} seasons")
        return df
    except Exception as e:
        print(f"❌ Error fetching career basic stats: {e}")
        return pd.DataFrame()


def fetch_career_advanced(player_id: int) -> pd.DataFrame:
    """Fetch career advanced per-season stats."""
    print("📊 Fetching Deni's Career Advanced Stats...")
    try:
        adv = PlayerDashboardByYearOverYear(
            player_id=player_id,
            per_mode_detailed="PerGame",
            measure_type_detailed="Advanced",
        ).get_data_frames()[1]
        adv = adv.copy()
        adv["SEASON_ID"] = adv["GROUP_VALUE"].astype(str)
        print(f"✅ Career Advanced Stats: {len(adv)} seasons")
        return adv
    except Exception as e:
        print(f"❌ Error fetching career advanced stats: {e}")
        return pd.DataFrame()


def fetch_game_logs(player_id: int, season: str) -> pd.DataFrame:
    """Fetch game logs for a target season."""
    print(f"📅 Fetching Game Logs for {season}...")
    try:
        logs = PlayerGameLog(player_id=player_id, season=season).get_data_frames()[0]
        logs = logs.copy()
        
        if "SEASON_ID" in logs.columns:
            logs["SEASON_ID"] = logs["SEASON_ID"].astype(str)
        
        if "GAME_DATE" in logs.columns:
            logs["GAME_DATE"] = pd.to_datetime(logs["GAME_DATE"])
        
        for col in ("MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV", "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA", "PF"):
            if col in logs.columns:
                logs[col] = pd.to_numeric(logs[col], errors="coerce")
        
        print(f"✅ {season} Game Logs: {len(logs)} games")
        return logs
    except Exception as e:
        print(f"❌ Error fetching {season} game logs: {e}")
        return pd.DataFrame()


def fetch_shot_data(player_id: int, season: str) -> pd.DataFrame:
    """Fetch shot chart data for a player and season."""
    print(f"  🏀 Fetching Shot Chart for {season}...")
    try:
        shot_data = shotchartdetail.ShotChartDetail(
            team_id=0,
            player_id=player_id,
            context_measure_simple="FGA",
            season_nullable=season,
        ).get_data_frames()[0]
        shot_df = shot_data.copy()
        print(f"    ✅ {season}: {len(shot_df)} shots")
        return shot_df
    except Exception as e:
        print(f"    ❌ Error fetching shot data for {season}: {e}")
        return pd.DataFrame()


def fetch_allstar_stats() -> pd.DataFrame:
    """Fetch 2024-25 stats for All-Stars using league API (FAST)."""
    print("⭐ Fetching All-Star Stats (2024-25)...")
    try:
        # Single API call to get ALL players for the season
        league_stats = leaguedashplayerstats.LeagueDashPlayerStats(
            season="2024-25",
            per_mode_detailed="PerGame",
        ).get_data_frames()[0]
        
        # Filter to only All-Stars by matching player names
        allstar_df = league_stats[
            league_stats["PLAYER_NAME"].isin(ALL_STAR_NAMES)
        ].copy()
        
        if allstar_df.empty:
            print("⚠️  No All-Stars found in league data")
            return pd.DataFrame()
            
        # Select and rename columns, round to 1 decimal
        cols_to_select = ["PLAYER_NAME", "GP", "PTS", "REB", "AST"]
        
        # Check for various casings of STL, BLK, TOV
        for stat, targets in [("STL", ["STL", "stl", "STEALS"]), ("BLK", ["BLK", "blk", "BLOCKS"]), ("TOV", ["TOV", "tov", "TURNOVERS"])]:
            for t in targets:
                if t in allstar_df.columns:
                    cols_to_select.append(t)
                    break
                    
        result_df = allstar_df[[col for col in cols_to_select if col in allstar_df.columns]].copy()
        
        # Normalize column names
        rename_map = {c: c.upper() if c not in ["STL", "BLK", "TOV"] else c for c in result_df.columns}
        # Force standard short names if long ones were found
        for c in result_df.columns:
            if c in ["STEALS", "stl"]: rename_map[c] = "STL"
            if c in ["BLOCKS", "blk"]: rename_map[c] = "BLK"
            if c in ["TURNOVERS", "tov"]: rename_map[c] = "TOV"
            
        result_df = result_df.rename(columns=rename_map)
        
        # Numeric cleanup
        for col in ["PTS", "REB", "AST", "STL", "BLK", "TOV"]:
            if col in result_df.columns:
                result_df[col] = pd.to_numeric(result_df[col], errors="coerce").fillna(0).round(1)
                
        result_df["GP"] = result_df["GP"].astype(int)
        
        print(f"✅ All-Star Stats: {len(result_df)} players")
        return result_df
    except Exception as e:
        print(f"❌ Error fetching All-Star stats: {e}")
        return pd.DataFrame()


def fetch_league_ft_stats(season="2025-26") -> pd.DataFrame:
    """Fetch league-wide stats to determine FT leaders."""
    print(f"📊 Fetching League FT Stats for {season}...")
    try:
        # Fetch ALL players
        df = leaguedashplayerstats.LeagueDashPlayerStats(
            season=season,
            per_mode_detailed="Totals"
        ).get_data_frames()[0]
        
        # Keep relevant columns for FT leaderboard
        cols = ["PLAYER_NAME", "TEAM_ABBREVIATION", "GP", "FTM", "FTA", "FT_PCT"]
        df = df[[c for c in cols if c in df.columns]].copy()
        
        # Sort by FTM descending
        df = df.sort_values("FTM", ascending=False)
        
        print(f"✅ League Stats: {len(df)} players fetched.")
        return df
    except Exception as e:
        print(f"❌ Error fetching League FT stats: {e}")
        return pd.DataFrame()


def fetch_allstar_detailed_stats() -> pd.DataFrame:
    """Fetch detailed stats for All-Stars including USG_PCT and TS_PCT."""
    print("⭐ Fetching All-Star Detailed Stats (2024-25)...")
    total_players = len(ALL_STAR_NAMES)
    print(f"   ⚠️  Fetching detailed stats for {total_players} players (this may take a moment)...")
    
    all_stats = []
    
    for idx, player_name in enumerate(ALL_STAR_NAMES, 1):
        try:
            hits = players.find_players_by_full_name(player_name)
            if hits:
                pid = hits[0]["id"]
                # Fetch Advanced Stats
                adv = PlayerDashboardByYearOverYear(
                    player_id=pid,
                    per_mode_detailed="PerGame",
                    measure_type_detailed="Advanced",
                ).get_data_frames()[1]
                
                # Fetch Basic Stats (backup if needed, but usually we use the summary df for basic)
                # Here we just want the advanced metrics for the current season
                
                # Filter for 2024-25
                mask = adv["GROUP_VALUE"].astype(str).str.contains("2024-25")
                season_data = adv[mask]
                
                if not season_data.empty:
                    row = season_data.iloc[0]
                    all_stats.append({
                        "PLAYER_NAME": player_name,
                        "USG_PCT": round(row.get("USG_PCT", 0), 3),
                        "TS_PCT": round(row.get("TS_PCT", 0), 3)
                    })
                    print(".", end="", flush=True)
                else:
                    print("x", end="", flush=True)
            
            # Rate limiting
            time.sleep(0.6)
            
        except Exception:
            print("!", end="", flush=True)
            
    print("\n✅ Detailed fetch complete.")
    return pd.DataFrame(all_stats)


def main():
    # Force UTF-8 for Windows
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

    print("=" * 60)
    print("🚀 Starting NBA Data Fetch (Strict & Smart Refactor)")
    print("=" * 60)
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # 1. Load Existing Data
    existing_data = {}
    if Path(OUTPUT_FILE).exists():
        try:
            print(f"📂 Loading existing data from {OUTPUT_FILE}...")
            with open(OUTPUT_FILE, "rb") as f:
                existing_data = pickle.load(f)
            print(f"✅ Loaded {len(existing_data)} keys.")
        except Exception as e:
            print(f"⚠️  Corrupt data file, starting fresh: {e}")
            existing_data = {}

    player_id = get_player_id()

    # 2. ALWAYS Fetch Current Season Game Logs (CRITICAL for data integrity)
    print("\n📅 Fetching 2025-26 Game Logs (Always fetch)...")
    logs_25_26 = fetch_game_logs(player_id, "2025-26")
    
    # Check actual number of games played
    actual_games_played = len(logs_25_26)
    
    # 3. Smart Career Fetch
    # Check if we can skip career fetch
    need_career_fetch = True
    
    # Use helper is_valid_df to ensure cache is valid
    if "career_basic" in existing_data and is_valid_df(existing_data["career_basic"]):
        cached_df = existing_data["career_basic"]
        # Find 2025-26 row
        current_season_row = cached_df[cached_df["SEASON_ID"] == "2025-26"]
        
        if not current_season_row.empty:
            cached_gp = int(current_season_row.iloc[0]["GP"])
            print(f"   🔍 Integrity Check: Cache says {cached_gp} GP, Logs say {actual_games_played} GP")
            
            if cached_gp >= actual_games_played:
                print("   ♻️  Career stats match game logs. Skipping fetch.")
                career_basic = cached_df
                career_adv = existing_data.get("career_advanced", pd.DataFrame())
                need_career_fetch = False
            else:
                print("   ⚠️  Career stats lagging. Applying local patch from logs...")
                career_basic = local_patch_career_stats(cached_df, logs_25_26)
                career_adv = existing_data.get("career_advanced", pd.DataFrame())
                need_career_fetch = False
        else:
             print("   ⚠️  No 2025-26 entry in cached career stats. Forcing refresh.")
    else:
        print("   ⚠️  Found empty or invalid career_basic in cache. Forcing refresh.")
    
    if need_career_fetch:
        career_basic = fetch_career_basic(player_id)
        career_adv = fetch_career_advanced(player_id)
        
    # 3b. Fetch previous season logs if missing
    if "game_logs_2024_25" in existing_data and is_valid_df(existing_data["game_logs_2024_25"]):
        logs_24_25 = existing_data["game_logs_2024_25"]
    else:
        print("   ⚠️  Cached 2024-25 logs invalid/empty. Refreshing...")
        logs_24_25 = fetch_game_logs(player_id, "2024-25")

    # 3c. Fetch 2023-24 season logs (requested for history)
    if "game_logs_2023_24" in existing_data and is_valid_df(existing_data["game_logs_2023_24"]):
        logs_23_24 = existing_data["game_logs_2023_24"]
    else:
        print("   ⚠️  Fetch 2023-24 logs (New history)...")
        logs_23_24 = fetch_game_logs(player_id, "2023-24")

    # 4. Shot Charts
    print("\n🏀 Fetching Shot Charts...")
    shot_charts = existing_data.get("shot_charts", {})
    if shot_charts is None: shot_charts = {}
    
    # Always fetch current season
    print("   🔄 Fetching 2025-26 Shot Chart (Always fetch)...")
    shot_charts["2025-26"] = fetch_shot_data(player_id, "2025-26")
    
    # Smart fetch past seasons
    for season in ["2022-23", "2023-24", "2024-25"]:
        # Strict check: exists in dict AND is valid
        if season in shot_charts and is_valid_df(shot_charts[season]):
            print(f"   ♻️  Using cached {season} chart")
        else:
            print(f"   ⚠️  Empty/Invalid cached data for {season} chart. Re-fetching...")
            shot_charts[season] = fetch_shot_data(player_id, season)

    # 5. All-Star Data (Static - 2024-25)
    print("\n⭐ Checking All-Star Data...")
    should_fetch_allstar = True
    
    if "allstar_stats" in existing_data and is_valid_df(existing_data["allstar_stats"]):
        print(f"   ♻️  All-Star data found in cache. Using cache (Static).")
        allstar_stats = existing_data["allstar_stats"]
        allstar_detailed = existing_data.get("allstar_detailed_stats", pd.DataFrame())
        should_fetch_allstar = False
    else:
        print("   ⚠️  No All-Star data found (or empty). Fetching...")
        
    if should_fetch_allstar:
        allstar_stats = fetch_allstar_stats()
        allstar_detailed = fetch_allstar_detailed_stats()

    # 6. League FT Stats (2025-26)
    print("\n📊 Checking League FT Stats...")
    league_ft = existing_data.get("league_ft_stats", pd.DataFrame())
    
    # We want to refresh this if we are doing a fresh fetch run, or if it's missing
    # Since we are already running fetch_data, let's refresh it to be safe/current.
    league_ft = fetch_league_ft_stats("2025-26")

    # 6. Save
    data_dict = {
        "career_basic": career_basic,
        "career_advanced": career_adv,
        "game_logs_2025_26": logs_25_26,
        "game_logs_2024_25": logs_24_25,
        "game_logs_2023_24": logs_23_24,
        "shot_charts": shot_charts,
        "allstar_stats": allstar_stats,
        "allstar_detailed_stats": allstar_detailed,
        "league_ft_stats": league_ft,
        "fetched_at": datetime.now().isoformat(),
        "player_id": player_id,
        "player_name": PLAYER_NAME,
    }

    try:
        print(f"\n💾 Saving to {OUTPUT_FILE}...")
        with open(OUTPUT_FILE, "wb") as f:
            pickle.dump(data_dict, f)
        print("✅ Data saved successfully!")
    except Exception as e:
        print(f"❌ Error saving data: {e}")

    print("\n" + "="*60)
    print("✅ FETCH COMPLETE. Run 'python -m streamlit run app.py'")
    print("="*60)

if __name__ == "__main__":
    main()
