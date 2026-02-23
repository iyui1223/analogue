#!/usr/bin/env python3
"""
T2m box-and-whisker plot by lead time for analogue members.

Uses pre-sliced T2m data (Data/data_slice) for fast loading. Domain mean
over the slice (Antarctic Peninsula). Per lead day: box = 25th–75th percentile,
whiskers = min–max. Blue = past analogues, red = present analogues, black dot = target.

Usage:
  python3 plot_t2m_boxplot.py --data-dir /path/to/data_slice \\
    --analogues /path/to/analogues.csv --events-yaml /path/to/extreme_events.yaml \\
    --event antarctica_peninsula_2020 --outdir /path/to/Figs \\
    [--ntop 5] [--lead-days 15]

Reads:
  - extreme_events.yaml (event start/end, snapshot_date)
  - analogues.csv (analogue dates, period, rank)
  - data_slice/YYYYMM.nc (T2m, monthly files)
"""
import argparse
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import xarray as xr
import yaml
import matplotlib.pyplot as plt

K2C = 273.15
DEFAULT_NTOP = 5
DEFAULT_LEAD_DAYS = 15
PAST_RANGE = (1948, 1987)
PRESENT_RANGE = (1988, 2026)


def load_event_config(yaml_path: str, event_name: str) -> dict:
    """Return event dict with start_date, end_date, snapshot_date."""
    with open(yaml_path) as f:
        data = yaml.safe_load(f)
    for ev in data.get("events", []):
        if ev.get("name") == event_name:
            return ev
    raise KeyError(f"Event {event_name!r} not found in {yaml_path}")


def load_analogues(csv_path: str, n_top: int):
    """Return (past_list, present_list) of analogue dicts with date, period, rank.
    Each list has top n_top analogues by rank."""
    df = pd.read_csv(csv_path)
    past = []
    present = []
    for _, r in df.iterrows():
        date = datetime(int(r["year"]), int(r["month"]), int(r["day"]))
        entry = {"date": date, "period": r["period"], "rank": int(r["rank"])}
        if r["period"] == "past" and r["rank"] <= n_top:
            past.append(entry)
        elif r["period"] == "present" and r["rank"] <= n_top:
            present.append(entry)
    # Sort by rank and take top n_top
    past = sorted(past, key=lambda x: x["rank"])[:n_top]
    present = sorted(present, key=lambda x: x["rank"])[:n_top]
    return past, present


def data_slice_file_path(data_dir: str, d: datetime) -> str:
    """Return path to monthly slice file for given date."""
    return os.path.join(data_dir, f"{d.year}{d.month:02d}.nc")


def get_t2m_domain_mean_series(
    start_date: datetime,
    ndays: int,
    data_dir: str,
) -> np.ndarray:
    """Load T2m from data_slice monthly files; spatial mean over domain. Return 1D array in °C."""
    months_needed = sorted(
        set(
            (
                (start_date + timedelta(days=i)).year,
                (start_date + timedelta(days=i)).month,
            )
            for i in range(ndays)
        )
    )
    monthly_data = {}
    for (year, month) in months_needed:
        path = data_slice_file_path(data_dir, datetime(year, month, 1))
        if not os.path.isfile(path):
            raise FileNotFoundError(path)
        ds = xr.open_dataset(path)
        t2m = ds["t2m"]
        vals = (t2m.values - K2C).astype(np.float64)
        daily_means = np.nanmean(vals, axis=tuple(range(1, vals.ndim)))
        monthly_data[(year, month)] = daily_means
        ds.close()

    out = np.full(ndays, np.nan, dtype=np.float64)
    for i in range(ndays):
        d = start_date + timedelta(days=i)
        key = (d.year, d.month)
        day_idx = d.day - 1
        if key in monthly_data and day_idx < len(monthly_data[key]):
            out[i] = monthly_data[key][day_idx]
    return out


def compute_background_series(data_dir: str, start_date: datetime, lead_days: int):
    """
    Mean T2m over past half and present half for the same calendar window as the event.
    Uses event's calendar window (e.g. Feb 1–15) for each year in past/present ranges.
    """
    past_series = []
    for year in range(PAST_RANGE[0], PAST_RANGE[1] + 1):
        try:
            ref_start = start_date.replace(year=year)
            s = get_t2m_domain_mean_series(ref_start, lead_days, data_dir)
            past_series.append(s)
        except (FileNotFoundError, ValueError):
            pass
    present_series = []
    for year in range(PRESENT_RANGE[0], PRESENT_RANGE[1] + 1):
        try:
            ref_start = start_date.replace(year=year)
            s = get_t2m_domain_mean_series(ref_start, lead_days, data_dir)
            present_series.append(s)
        except (FileNotFoundError, ValueError):
            pass
    past_mean = (
        np.nanmean(past_series, axis=0) if past_series else np.full(lead_days, np.nan)
    )
    present_mean = (
        np.nanmean(present_series, axis=0)
        if present_series
        else np.full(lead_days, np.nan)
    )
    return past_mean, present_mean


def main():
    parser = argparse.ArgumentParser(
        description="T2m box-and-whisker plot by lead time for analogue members"
    )
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Absolute path to data_slice directory (YYYYMM.nc monthly T2m files)",
    )
    parser.add_argument(
        "--analogues",
        required=True,
        help="Path to analogues.csv",
    )
    parser.add_argument(
        "--events-yaml",
        required=True,
        help="Path to extreme_events.yaml",
    )
    parser.add_argument(
        "--event",
        required=True,
        help="Event name (e.g. antarctica_peninsula_2020)",
    )
    parser.add_argument(
        "--outdir",
        required=True,
        help="Output directory for PNG",
    )
    parser.add_argument(
        "--ntop",
        type=int,
        default=DEFAULT_NTOP,
        help=f"Number of top analogues per period (default: {DEFAULT_NTOP}). "
        f"Present panel uses (ntop-1) analogues + target.",
    )
    parser.add_argument(
        "--lead-days",
        type=int,
        default=DEFAULT_LEAD_DAYS,
        help=f"Number of lead days (default: {DEFAULT_LEAD_DAYS})",
    )
    args = parser.parse_args()

    event_cfg = load_event_config(args.events_yaml, args.event)
    target_start = datetime.strptime(event_cfg["start_date"], "%Y-%m-%d")
    target_end = datetime.strptime(event_cfg["end_date"], "%Y-%m-%d")
    lead_days = min(args.lead_days, (target_end - target_start).days + 1)
    if lead_days < 1:
        lead_days = args.lead_days

    past_analogues, present_analogues = load_analogues(args.analogues, args.ntop)

    target_series = get_t2m_domain_mean_series(
        target_start, lead_days, args.data_dir
    )

    past_series = []
    for a in past_analogues:
        snap = a["date"]
        start = snap - timedelta(days=7)
        try:
            s = get_t2m_domain_mean_series(start, lead_days, args.data_dir)
            past_series.append(s)
        except FileNotFoundError as e:
            print("Skip past", snap.date(), e)

    present_series = []
    n_present_analogues = max(0, args.ntop - 1)
    for a in present_analogues[:n_present_analogues]:
        snap = a["date"]
        start = snap - timedelta(days=7)
        try:
            s = get_t2m_domain_mean_series(start, lead_days, args.data_dir)
            present_series.append(s)
        except FileNotFoundError as e:
            print("Skip present", snap.date(), e)
    present_series.append(target_series)

    past_arr = np.array(past_series) if past_series else np.zeros((0, lead_days))
    present_arr = np.array(present_series) if present_series else np.zeros((0, lead_days))

    past_bg, present_bg = compute_background_series(
        args.data_dir, target_start, lead_days
    )

    os.makedirs(args.outdir, exist_ok=True)
    lead = np.arange(lead_days)
    fig, ax = plt.subplots(figsize=(9, 5))

    ax.plot(
        lead,
        past_bg,
        color="blue",
        linewidth=3,
        alpha=0.35,
        label=f"Past mean ({PAST_RANGE[0]}-{PAST_RANGE[1]})",
    )
    ax.plot(
        lead,
        present_bg,
        color="red",
        linewidth=3,
        alpha=0.35,
        label=f"Present mean ({PRESENT_RANGE[0]}-{PRESENT_RANGE[1]})",
    )

    width = 0.35
    past_positions = np.arange(lead_days) - width / 2
    present_positions = np.arange(lead_days) + width / 2
    past_data = (
        [past_arr[:, i] for i in range(lead_days)]
        if past_arr.size
        else [np.array([np.nan])] * lead_days
    )
    present_data = (
        [present_arr[:, i] for i in range(lead_days)]
        if present_arr.size
        else [np.array([np.nan])] * lead_days
    )

    ax.boxplot(
        past_data,
        positions=past_positions,
        widths=width * 0.9,
        patch_artist=True,
        whis=(0, 100),
        whiskerprops=dict(color="blue"),
        capprops=dict(color="blue"),
        medianprops=dict(color="blue", linewidth=1.2),
        boxprops=dict(facecolor="blue", alpha=0.4),
        showfliers=False,
    )
    ax.boxplot(
        present_data,
        positions=present_positions,
        widths=width * 0.9,
        patch_artist=True,
        whis=(0, 100),
        whiskerprops=dict(color="red"),
        capprops=dict(color="red"),
        medianprops=dict(color="red", linewidth=1.2),
        boxprops=dict(facecolor="red", alpha=0.4),
        showfliers=False,
    )
    ax.scatter(
        lead,
        target_series,
        color="black",
        s=32,
        zorder=5,
        label=f"Target ({event_cfg['snapshot_date']})",
        edgecolors="none",
    )

    # X-axis: target event dates (e.g. "1 Feb", "15 Feb", "1 Mar")
    month_abbr = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    date_labels = []
    for i in range(lead_days):
        d = target_start + timedelta(days=i)
        date_labels.append(f"{d.day} {month_abbr[d.month - 1]}")
    ax.set_xticks(lead)
    ax.set_xticklabels(date_labels, rotation=45, ha="right")
    ax.set_ylabel("2 m temperature (°C)")
    ax.set_title(
        f"T2m peninsula mean — top {args.ntop} past; top {n_present_analogues} present + target"
    )
    ax.legend(loc="best", fontsize=8)
    # Vertical grid at target event dates; horizontal at 0 and every 2 degrees (°C)
    ax.xaxis.grid(True, alpha=0.3)
    ax.yaxis.grid(True, alpha=0.3)
    ymin, ymax = ax.get_ylim()
    yticks = np.arange(
        int(np.floor(ymin / 2) * 2),
        int(np.ceil(ymax / 2) * 2) + 1,
        2,
    )
    ax.set_yticks(yticks)
    ax.set_xlim(-0.5, lead_days - 0.5)
    plt.tight_layout()
    out = os.path.join(args.outdir, f"t2m_boxplot_top{args.ntop}.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print("Saved", out)


if __name__ == "__main__":
    main()
