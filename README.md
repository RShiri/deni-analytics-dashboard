# Israeli NBA Players: 360° Performance Analytics

A Streamlit dashboard tracking every active Israeli NBA player: **Deni Avdija** (Portland
Trail Blazers, 2026 All-Star), **Ben Saraf** and **Danny Wolf** (Brooklyn Nets), and
**Emanuel Sharp** (Sacramento Kings).

**Live dashboard:** https://nba-dashboard-ramshiri.streamlit.app/

## Features
- **Player switcher** — every page re-renders for whichever of the four players is selected.
- **Smart Data Patching** — the current season's career row is refreshed from game logs if stale.
- **Hexbin & 14-Zone Shot Maps** — density and FG% efficiency visualizations with fixed court geometry.
- **Career Analysis** — per-game and per-36 trends, usage rate, true shooting %.
- **Deep Dive** — percentile ranking and case-study charts against the current NBA All-Star roster,
  plus a live league free-throw leaderboard.
- Visual design ported from the [XLALIGA](https://github.com/RShiri/XLALIGA) "Broadcast Kinetic"
  skin: carbon ground, one signal colour (lime), condensed/uppercase display type.

## Project layout
```
app.py              Streamlit app (all pages, all players)
theme.py            Shared CSS + Plotly theme (Broadcast Kinetic skin)
fetch_data.py        Data fetcher — pulls career stats, game logs, shot charts (nba_api)
nba_data.pkl          Cached data consumed by app.py (generated, committed so the deployed
                      app has data without needing network access at runtime)
.github/workflows/    Daily automated data refresh
```

## Running locally
```bash
pip install -r requirements.txt
python fetch_data.py          # refresh nba_data.pkl from the NBA Stats API
streamlit run app.py
```

To iterate on a single player without waiting on a full fetch:
```bash
python fetch_data.py --player ben_saraf
```

## Automated Data Updates
`.github/workflows/update_data.yml` runs `fetch_data.py` daily (09:00 UTC) via GitHub Actions
and commits `nba_data.pkl` back to the repo if it changed. It can also be triggered manually
from the **Actions** tab (`workflow_dispatch`), optionally scoped to a single player.

### How the scraper works
`fetch_data.py` calls the real NBA Stats API through the [`nba_api`](https://github.com/swar/nba_api)
package — no manual JSON files, no stubs:
- **Career stats**: `playercareerstats` (per-game) + `playerdashboardbyyearoveryear` (advanced,
  for USG%/TS%/NET_RATING).
- **Game logs**: `playergamelog`, per season, for up to the last 4 seasons since each player's draft year.
- **Shot charts**: `shotchartdetail`, same season range.
- **All-Star comparison cohort**: `leaguedashplayerstats` filtered to the current NBA All-Star
  roster (`ALL_STAR_ROSTER` in `fetch_data.py` — update it every February after All-Star rosters
  are announced).
- **League free-throw leaders**: `leaguedashplayerstats`, season totals, with an automatic
  fallback to the prior season if the current one hasn't started yet.

### Troubleshooting
1. Check the workflow run logs under the **Actions** tab for the failing step's output.
2. `nba_api` calls stats.nba.com directly — if NBA.com is rate-limiting or blocking GitHub's
   IP ranges, re-run later or add delays (see `REQUEST_DELAY` in `fetch_data.py`).
3. A newly-drafted player (like Emanuel Sharp before his rookie season tips off) will have
   empty game logs / shot charts until games are actually played — the dashboard handles this
   gracefully rather than erroring.
