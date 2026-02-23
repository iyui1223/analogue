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
from typing import Optional

import numpy as np
import pandas as pd
import xarray as xr
import yaml
import matplotlib.pyplot as plt

K2C = 273.15
DEFAULT_NTOP = 5
DEFAULT_LEAD_DAYS = 15
LAND_THRESHOLD = 0.5
PAST_RANGE = (1948, 1987)
PRESENT_RANGE = (1988, 2026)
DEFAULT_LSM_PATH = "/lustre/soge1/data/analysis/era5/0.28125x0.28125/invariant/land-sea_mask/nc/era5_invariant_land-sea_mask_20000101.nc"


def load_event_config(yaml_path: str, event_name: str) -> dict:
    """Return event dict with start_date, end_date, snapshot_date, boxplot_region."""
    with open(yaml_path) as f:
        data = yaml.safe_load(f)
    for ev in data.get("events", []):
        if ev.get("name") == event_name:
            return ev
    raise KeyError(f"Event {event_name!r} not found in {yaml_path}")


def get_boxplot_bbox(event_cfg: dict) -> tuple:
    """Return (lat_min, lat_max, lon_min, lon_max) for box plot domain.
    Longitude is in 0–360 (netCDF convention). Uses boxplot_region, else region."""
    box = event_cfg.get("boxplot_region") or event_cfg.get("region", {})
    lat_min = float(box.get("lat_min", -70.0))
    lat_max = float(box.get("lat_max", -60.0))
    lon_min = float(box.get("lon_min", 280.0))   # 0–360 (e.g. 280 = 80°W)
    lon_max = float(box.get("lon_max", 310.0))   # 0–360 (e.g. 310 = 50°W)
    return lat_min, lat_max, lon_min, lon_max


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


def load_land_mask(
    lsm_path: str,
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    t2m_template: xr.DataArray,
) -> np.ndarray:
    """Load ERA5 LSM, subset to bbox, align to T2m grid. Return 2D boolean (True=land)."""
    ds = xr.open_dataset(lsm_path)
    lsm = ds["lsm"].isel(time=0)
    if str(lsm.dtype) in ("int16", "int32"):
        sf = float(getattr(ds["lsm"], "scale_factor", 1.0))
        ao = float(getattr(ds["lsm"], "add_offset", 0.0))
        lsm = lsm.astype(np.float64) * sf + ao
    lon = lsm.coords["longitude"]
    lon_0_360 = float(lon.min()) >= 0
    # bbox lon is 0–360; LSM uses 0–360, so use as-is (or +360 if stored as -180–180)
    lon_lo = lon_min + 360 if (lon_0_360 and lon_min < 0) else lon_min
    lon_hi = lon_max + 360 if (lon_0_360 and lon_max < 0) else lon_max
    # LSM lat typically -90 to 90 (increasing); use slice(lat_min, lat_max)
    lsm_sub = lsm.sel(
        latitude=slice(lat_min, lat_max),
        longitude=slice(lon_lo, lon_hi),
    )
    lsm_aligned = lsm_sub.reindex_like(t2m_template, method="nearest")
    land = (lsm_aligned.values >= LAND_THRESHOLD).squeeze()
    ds.close()
    return land


def get_t2m_domain_mean_series(
    start_date: datetime,
    ndays: int,
    data_dir: str,
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    land_mask: Optional[np.ndarray] = None,
) -> np.ndarray:
    """Load T2m from data_slice monthly files; subset to bbox; mean over land if mask given.
    Return 1D array in °C."""
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
        # Subset to bounding box (Layer 1: lat/lon domain)
        lat_slice = slice(lat_max, lat_min) if lat_max > lat_min else slice(lat_min, lat_max)
        # Longitude: bbox is 0–360; convert if data uses -180–180
        data_lon = t2m.coords["longitude"]
        if float(data_lon.min()) < 0:
            sel_lo = lon_min - 360 if lon_min > 180 else lon_min
            sel_hi = lon_max - 360 if lon_max > 180 else lon_max
        else:
            sel_lo, sel_hi = lon_min, lon_max
        sub = t2m.sel(
            latitude=lat_slice,
            longitude=slice(sel_lo, sel_hi),
        )
        vals = (sub.values - K2C).astype(np.float64)
        # Layer 2: land mask (skip if land_mask is None)
        if land_mask is not None and land_mask.size > 0:
            mask_bc = np.broadcast_to(land_mask, vals.shape)
            vals = np.where(mask_bc, vals, np.nan)
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


def compute_background_series(
    data_dir: str,
    start_date: datetime,
    lead_days: int,
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    land_mask: Optional[np.ndarray] = None,
):
    """
    Mean T2m over past half and present half for the same calendar window as the event.
    Uses event's calendar window (e.g. Feb 1–15) for each year in past/present ranges.
    """
    past_series = []
    for year in range(PAST_RANGE[0], PAST_RANGE[1] + 1):
        try:
            ref_start = start_date.replace(year=year)
            s = get_t2m_domain_mean_series(
                ref_start, lead_days, data_dir,
                lat_min, lat_max, lon_min, lon_max,
                land_mask,
            )
            past_series.append(s)
        except (FileNotFoundError, ValueError):
            pass
    present_series = []
    for year in range(PRESENT_RANGE[0], PRESENT_RANGE[1] + 1):
        try:
            ref_start = start_date.replace(year=year)
            s = get_t2m_domain_mean_series(
                ref_start, lead_days, data_dir,
                lat_min, lat_max, lon_min, lon_max,
                land_mask,
            )
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
    parser.add_argument(
        "--no-land-mask",
        action="store_true",
        help="Skip land-sea mask; use all grid points in bbox (default: use land only)",
    )
    parser.add_argument(
        "--lsm-path",
        default=os.environ.get("ERA5_LSM_PATH", DEFAULT_LSM_PATH),
        help="Path to ERA5 land-sea mask netCDF (default: ERA5_LSM_PATH or built-in)",
    )
    args = parser.parse_args()

    event_cfg = load_event_config(args.events_yaml, args.event)
    target_start = datetime.strptime(event_cfg["start_date"], "%Y-%m-%d")
    target_end = datetime.strptime(event_cfg["end_date"], "%Y-%m-%d")
    lead_days = min(args.lead_days, (target_end - target_start).days + 1)
    if lead_days < 1:
        lead_days = args.lead_days

    lat_min, lat_max, lon_min, lon_max = get_boxplot_bbox(event_cfg)

    land_mask = None
    if not args.no_land_mask:
        path0 = data_slice_file_path(args.data_dir, target_start)
        if os.path.isfile(path0):
            with xr.open_dataset(path0) as ds0:
                t2m0 = ds0["t2m"]
                lat_slice = slice(lat_max, lat_min) if lat_max > lat_min else slice(lat_min, lat_max)
                data_lon = t2m0.coords["longitude"]
                if float(data_lon.min()) < 0:
                    sel_lo = lon_min - 360 if lon_min > 180 else lon_min
                    sel_hi = lon_max - 360 if lon_max > 180 else lon_max
                else:
                    sel_lo, sel_hi = lon_min, lon_max
                t2m_sub = t2m0.sel(
                    latitude=lat_slice,
                    longitude=slice(sel_lo, sel_hi),
                )
                if os.path.isfile(args.lsm_path):
                    land_mask = load_land_mask(
                        args.lsm_path, lat_min, lat_max, lon_min, lon_max, t2m_sub
                    )
                    if np.sum(land_mask) == 0:
                        print("No land points in LSM for this bbox; using all points")
                        land_mask = None
                    else:
                        print("Using land-sea mask: land only")
                else:
                    print("LSM file not found, using all points:", args.lsm_path)
        else:
            print("Cannot load LSM template (data not found), using all points")
    else:
        print("Skipping land mask (--no-land-mask)")

    past_analogues, present_analogues = load_analogues(args.analogues, args.ntop)

    target_series = get_t2m_domain_mean_series(
        target_start, lead_days, args.data_dir,
        lat_min, lat_max, lon_min, lon_max,
        land_mask,
    )

    past_series = []
    for a in past_analogues:
        snap = a["date"]
        start = snap - timedelta(days=7)
        try:
            s = get_t2m_domain_mean_series(
                start, lead_days, args.data_dir,
                lat_min, lat_max, lon_min, lon_max,
                land_mask,
            )
            past_series.append(s)
        except FileNotFoundError as e:
            print("Skip past", snap.date(), e)

    present_series = []
    n_present_analogues = max(0, args.ntop - 1)
    for a in present_analogues[:n_present_analogues]:
        snap = a["date"]
        start = snap - timedelta(days=7)
        try:
            s = get_t2m_domain_mean_series(
                start, lead_days, args.data_dir,
                lat_min, lat_max, lon_min, lon_max,
                land_mask,
            )
            present_series.append(s)
        except FileNotFoundError as e:
            print("Skip present", snap.date(), e)
    present_series.append(target_series)

    past_arr = np.array(past_series) if past_series else np.zeros((0, lead_days))
    present_arr = np.array(present_series) if present_series else np.zeros((0, lead_days))

    past_bg, present_bg = compute_background_series(
        args.data_dir, target_start, lead_days,
        lat_min, lat_max, lon_min, lon_max,
        land_mask,
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
    region_label = "land " if (land_mask is not None and land_mask.size > 0) else ""
    # Display lon as °W for 0–360 (e.g. 280 → 80°W)
    lon_lo_disp = 360 - lon_min if lon_min > 180 else abs(lon_min)
    lon_hi_disp = 360 - lon_max if lon_max > 180 else abs(lon_max)
    ax.set_title(
        f"T2m {region_label}mean ({abs(lat_min):.0f}–{abs(lat_max):.0f}°S, {lon_lo_disp:.0f}–{lon_hi_disp:.0f}°W) — "
        f"top {args.ntop} past; top {n_present_analogues} present + target"
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
