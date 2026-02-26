#!/usr/bin/env python3
"""
Bump-chart visualization: analogue ranking vs Gaussian sigma radius.

Shows how analogue dates change rank (or drop off) as sigma_km varies.

Usage:
  python bump_ranking.py --event antarctica_peninsula_2020
  python bump_ranking.py --event antarctica_peninsula_2015 --dataset era5

Output:
  Figs/F02_analogue_search/<dataset>/<event>/sigma_ranking_bump.png
"""
import argparse
import re
from pathlib import Path
from typing import Optional

try:
    import colorsys
    import numpy as np
    import pandas as pd
    import matplotlib.pyplot as plt
    from matplotlib.colors import to_rgba
except ImportError:
    print("Missing packages. Install: pip install numpy pandas matplotlib")
    raise

from data_utils import load_env_setting, get_data_paths, ensure_dir

GLOB_PATTERN = "analogues_*km.csv"


def extract_sigma_km(filepath: Path) -> Optional[int]:
    """Extract sigma value in km from filename like analogues_1000km.csv"""
    m = re.search(r"analogues_(\d+)km", filepath.stem, re.IGNORECASE)
    return int(m.group(1)) if m else None


def load_all_sigma_data(data_dir: Path) -> pd.DataFrame:
    """Load all analogues_*km.csv files from data_dir into a single DataFrame."""
    files = sorted(data_dir.glob(GLOB_PATTERN), key=lambda p: extract_sigma_km(p) or 0)
    if not files:
        raise FileNotFoundError(f"No files matching {GLOB_PATTERN} in {data_dir}")

    records = []
    for fp in files:
        sigma = extract_sigma_km(fp)
        if sigma is None:
            continue
        df = pd.read_csv(fp)
        for _, row in df.iterrows():
            records.append({
                "sigma_km": sigma,
                "date": row["date"],
                "rank": row["rank"],
                "period": row["period"],
            })
    return pd.DataFrame(records)


def date_to_color(date_str: str, year_min: int, year_max: int) -> tuple:
    """Generate distinct color from date: hue by year, saturation/value by day-of-year."""
    y, m, d = int(date_str[:4]), int(date_str[5:7]), int(date_str[8:10])
    yr_range = max(1, year_max - year_min)
    h = (y - year_min) / yr_range * 0.85
    doy = m * 31 + d
    s = 0.6 + 0.35 * ((doy * 7) % 41) / 41
    v = 0.55 + 0.4 * ((doy * 11) % 37) / 37
    return to_rgba(colorsys.hsv_to_rgb(h % 1, s, v))


def plot_bump_chart(df: pd.DataFrame, out_path: Path, event_name: str):
    """Create bump chart with past/present subplots."""
    fig, (ax_past, ax_present) = plt.subplots(1, 2, figsize=(14, 7), sharey=True)
    fig.suptitle(f"Analogue Ranking vs Sigma (km) — {event_name}", fontsize=14)

    sigmas = np.array(sorted(df["sigma_km"].unique()))
    n_sigmas = len(sigmas)

    for period, ax in [("past", ax_past), ("present", ax_present)]:
        sub = df[df["period"] == period]
        if sub.empty:
            ax.set_title(f"{period.capitalize()} (no data)")
            continue

        dates = np.sort(sub["date"].unique())
        years = [int(d[:4]) for d in dates]
        year_min, year_max = min(years), max(years)
        colors = [date_to_color(d, year_min, year_max) for d in dates]

        for j, date in enumerate(dates):
            pts = sub[sub["date"] == date].set_index("sigma_km")["rank"]
            rank_arr = np.full(n_sigmas, np.nan)
            for s, r in pts.items():
                idx = np.searchsorted(sigmas, s)
                if idx < n_sigmas and sigmas[idx] == s:
                    rank_arr[idx] = r

            ax.plot(
                sigmas,
                rank_arr,
                "o-",
                color=colors[j],
                alpha=0.8,
                markersize=7,
                linewidth=1.5,
                label=date if j < 15 else None,
            )

        ax.set_xlabel("Sigma (km)")
        ax.set_ylabel("Analogue Rank (1 = best)")
        ax.set_title(period.capitalize())

        rank_max = int(np.nanmax(sub["rank"])) if not sub.empty else 15
        ax.set_ylim(rank_max + 0.5, 0.5)
        ax.set_xticks(sigmas)
        ax.tick_params(axis='x', rotation=45)
        ax.grid(True, alpha=0.3)
        ax.set_xlim(min(sigmas) - 50, max(sigmas) + 50)

        if len(dates) <= 20:
            ax.legend(loc="upper right", fontsize=8, ncol=1)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {out_path}")


def main():
    parser = argparse.ArgumentParser(description="Bump chart: ranking vs sigma_km")
    parser.add_argument(
        "--event",
        type=str,
        required=True,
        help="Event name (e.g., antarctica_peninsula_2020)"
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default="era5",
        choices=["era5", "mswx", "jra3q"],
        help="Dataset (default: era5)"
    )
    args = parser.parse_args()

    env = load_env_setting()
    paths = get_data_paths(env)

    data_dir = paths["analogue"] / args.dataset / args.event
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    df = load_all_sigma_data(data_dir)
    print(f"Loaded {len(df)} records from {data_dir}")
    print(f"  Sigma values: {sorted(df['sigma_km'].unique())}")
    print(f"  Unique dates: {df['date'].nunique()}")

    fig_dir = ensure_dir(paths["root"] / "Figs" / "F02_analogue_search" / args.dataset / args.event)
    out_path = fig_dir / "sigma_ranking_bump.png"

    plot_bump_chart(df, out_path, args.event)


if __name__ == "__main__":
    main()
