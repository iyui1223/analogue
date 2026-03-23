#!/usr/bin/env python3
"""
Cramér–von Mises (CvM) test for past vs present analogue-conditioned T2m distributions.

Uses snapshot-date daily max T2m (domain mean over land in boxplot_region) for each
analogue member. Compares past vs present distributions with:
  1) CvM test (scipy, asymptotic p-value at α=0.05)
  2) Permutation + CvM (finite-sample, assumption-free p-value)

Sample: analogue members from analogues.csv (snapshot dates only).
Variable: daily maximum T2m, averaged over land within boxplot_region.
Default: all 15 members (configurable via --nmembers).

Usage:
  python3 cvm_test.py --data-dir Data/data_slice --analogues analogues.csv \\
    --events-yaml extreme_events.yaml --event antarctica_peninsula_2020 \\
    --outdir Figs/F03_visualization/event/era5 [--nmembers 15]
"""

import argparse
import os
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import xarray as xr
import yaml
from scipy import stats

# Reuse constants from plot_t2m_boxplot
K2C = 273.15
LAND_THRESHOLD = 0.5
DEFAULT_LSM_PATH = os.environ.get(
    "ERA5_LSM_PATH",
    str(Path(__file__).resolve().parent.parent
        / "Data" / "F01_preprocess" / "era5" / "invariant" / "land_sea_mask.nc"),
)
DEFAULT_NMEMBERS = 15

def load_event_config(yaml_path: str, event_name: str) -> dict:
    """Return event dict with boxplot_region."""
    with open(yaml_path) as f:
        data = yaml.safe_load(f)
    for ev in data.get("events", []):
        if ev.get("name") == event_name:
            return ev
    raise KeyError(f"Event {event_name!r} not found in {yaml_path}")


def get_boxplot_bbox(event_cfg: dict) -> tuple:
    """Return (lat_min, lat_max, lon_min, lon_max). Longitude 0–360."""
    box = event_cfg.get("boxplot_region") or event_cfg.get("region", {})
    lat_min = float(box.get("lat_min", -70.0))
    lat_max = float(box.get("lat_max", -60.0))
    lon_min = float(box.get("lon_min", 280.0))
    lon_max = float(box.get("lon_max", 310.0))
    return lat_min, lat_max, lon_min, lon_max


def load_analogues(csv_path: str, n_members: int) -> Tuple[List, List]:
    """Return (past_list, present_list) of analogue dicts with date, period, rank.
    Up to n_members per group."""
    df = pd.read_csv(csv_path)
    past, present = [], []
    for _, r in df.iterrows():
        date = datetime(int(r["year"]), int(r["month"]), int(r["day"]))
        entry = {"date": date, "period": r["period"], "rank": int(r["rank"])}
        if r["period"] == "past" and r["rank"] <= n_members:
            past.append(entry)
        elif r["period"] == "present" and r["rank"] <= n_members:
            present.append(entry)
    past = sorted(past, key=lambda x: x["rank"])[:n_members]
    present = sorted(present, key=lambda x: x["rank"])[:n_members]
    return past, present


def data_slice_file_path(data_dir: str, d: datetime) -> str:
    return os.path.join(data_dir, f"{d.year}{d.month:02d}.nc")


def open_t2m_dataset(path: str) -> xr.Dataset:
    """Open data-slice dataset, handling both plain NetCDF and ZIP archives.
    Data slices can be YYYYMM.nc (NetCDF) or ZIP containing 2m_temperature*.nc"""
    with open(path, "rb") as f:
        magic = f.read(4)
    if magic == b"PK\x03\x04":
        with zipfile.ZipFile(path, "r") as z:
            t2m_name = None
            for n in z.namelist():
                if "2m_temperature" in n or "t2m" in n.lower():
                    t2m_name = n
                    break
            if t2m_name is None:
                raise ValueError(f"No 2m_temperature/t2m file in zip {path}")
            with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
                tmp.write(z.read(t2m_name))
                tmp_path = tmp.name
        try:
            ds = xr.open_dataset(tmp_path, engine="netcdf4")
            os.unlink(tmp_path)
        except Exception:
            os.unlink(tmp_path)
            raise
        return ds
    return xr.open_dataset(path, engine="netcdf4")


def load_land_mask(
    lsm_path: str,
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    t2m_template: xr.DataArray,
) -> np.ndarray:
    """Return 2D boolean (True=land)."""
    ds = xr.open_dataset(lsm_path, engine="netcdf4")
    lsm = ds["lsm"].isel(time=0)
    if str(lsm.dtype) in ("int16", "int32"):
        sf = float(getattr(ds["lsm"], "scale_factor", 1.0))
        ao = float(getattr(ds["lsm"], "add_offset", 0.0))
        lsm = lsm.astype(np.float64) * sf + ao
    lon = lsm.coords["longitude"]
    lat_coord = lsm.coords["latitude"]
    lon_0_360 = float(lon.min()) >= 0
    lat_desc = float(lat_coord[0]) > float(lat_coord[-1])
    # bbox lon is 0–360; convert to LSM convention for selection
    if lon_0_360:
        lon_lo, lon_hi = lon_min, lon_max
    else:
        # LSM uses -180 to 180; convert bbox 0–360 -> -180–180
        lon_lo = lon_min - 360 if lon_min > 180 else lon_min
        lon_hi = lon_max - 360 if lon_max > 180 else lon_max
    # LSM lat: use slice(lat_max, lat_min) when descending (90 -> -90)
    lat_slice = slice(lat_max, lat_min) if lat_desc else slice(lat_min, lat_max)
    lsm_sub = lsm.sel(
        latitude=lat_slice,
        longitude=slice(lon_lo, lon_hi),
    )
    # reindex_like needs matching lon convention: convert t2m -180-180 -> 0-360
    # so nearest-neighbor works (xarray does not handle lon wraparound)
    tpl = t2m_template
    tpl_lon = tpl.coords["longitude"]
    if float(tpl_lon.min()) < 0 and lon_0_360:
        lon_0360 = tpl_lon.where(tpl_lon >= 0, tpl_lon + 360)
        tpl = tpl.assign_coords(longitude=lon_0360)
    lsm_aligned = lsm_sub.reindex_like(tpl, method="nearest")
    land = (lsm_aligned.values >= LAND_THRESHOLD).squeeze()
    ds.close()
    return land


def get_t2m_snapshot_value(
    snapshot_date: datetime,
    data_dir: str,
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    land_mask: Optional[np.ndarray],
) -> float:
    """Return daily max T2m domain mean at snapshot date (°C). One scalar per analogue."""
    path = data_slice_file_path(data_dir, snapshot_date)
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    ds = open_t2m_dataset(path)
    t2m = ds["t2m"]
    lat_slice = slice(lat_max, lat_min) if lat_max > lat_min else slice(lat_min, lat_max)
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
    if land_mask is not None and land_mask.size > 0:
        mask_bc = np.broadcast_to(land_mask, vals.shape)
        vals = np.where(mask_bc, vals, np.nan)
    # Snapshot date: need the day index for that month
    day_idx = snapshot_date.day - 1
    if vals.ndim == 3:  # (time, lat, lon)
        daily_vals = np.nanmean(vals, axis=(1, 2))
    else:
        daily_vals = np.nanmean(vals, axis=tuple(range(1, vals.ndim)))
    if day_idx >= len(daily_vals):
        ds.close()
        return np.nan
    out = float(np.nanmean(daily_vals[day_idx]))
    ds.close()
    return out


def cramer_von_mises_permutation(
    past: np.ndarray,
    present: np.ndarray,
    n_perm: int = 10000,
    seed: Optional[int] = 42,
) -> tuple[float, float]:
    """Permutation-based CvM test. Returns (T_obs, p_value)."""
    rng = np.random.RandomState(seed)
    pooled = np.concatenate([past, present])
    n, m = len(past), len(present)
    result = stats.cramervonmises_2samp(past, present)
    t_obs = result.statistic
    count = 0
    for _ in range(n_perm):
        perm = rng.permutation(pooled)
        x_perm, y_perm = perm[:n], perm[n:]
        t_perm = stats.cramervonmises_2samp(x_perm, y_perm).statistic
        if t_perm >= t_obs:
            count += 1
    p_val = (count + 1) / (n_perm + 1)
    return float(t_obs), p_val


def main():
    parser = argparse.ArgumentParser(
        description="Cramér–von Mises test: past vs present analogue T2m distributions"
    )
    parser.add_argument("--data-dir", required=True, help="Path to data_slice (daily max T2m)")
    parser.add_argument("--analogues", required=True, help="Path to analogues.csv")
    parser.add_argument("--events-yaml", required=True, help="Path to extreme_events.yaml")
    parser.add_argument("--event", required=True, help="Event name")
    parser.add_argument("--outdir", required=True, help="Output directory")
    parser.add_argument(
        "--nmembers",
        type=int,
        default=DEFAULT_NMEMBERS,
        help=f"Max analogue members per period (default {DEFAULT_NMEMBERS})",
    )
    parser.add_argument("--lsm-path", default=DEFAULT_LSM_PATH, help="ERA5 land-sea mask")
    parser.add_argument("--no-land-mask", action="store_true", help="Use all points, not land only")
    parser.add_argument("--nperm", type=int, default=10000, help="Permutations for test 2")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed for permutations")
    args = parser.parse_args()

    event_cfg = load_event_config(args.events_yaml, args.event)
    lat_min, lat_max, lon_min, lon_max = get_boxplot_bbox(event_cfg)

    past_analogues, present_analogues = load_analogues(args.analogues, args.nmembers)
    if not past_analogues or not present_analogues:
        print("ERROR: Need at least one past and one present analogue")
        return 1

    land_mask = None
    if not args.no_land_mask:
        path0 = data_slice_file_path(args.data_dir, past_analogues[0]["date"])
        if os.path.isfile(path0):
            with open_t2m_dataset(path0) as ds0:
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
                        print("WARN: No land points in LSM; using all points")
                        land_mask = None

    past_vals = []
    for a in past_analogues:
        try:
            v = get_t2m_snapshot_value(
                a["date"], args.data_dir,
                lat_min, lat_max, lon_min, lon_max,
                land_mask,
            )
            if not np.isnan(v):
                past_vals.append(v)
        except FileNotFoundError as e:
            print(f"Skip past {a['date'].date()}: {e}")
    present_vals = []
    for a in present_analogues:
        try:
            v = get_t2m_snapshot_value(
                a["date"], args.data_dir,
                lat_min, lat_max, lon_min, lon_max,
                land_mask,
            )
            if not np.isnan(v):
                present_vals.append(v)
        except FileNotFoundError as e:
            print(f"Skip present {a['date'].date()}: {e}")

    past_arr = np.array(past_vals)
    present_arr = np.array(present_vals)
    n_past, n_present = len(past_arr), len(present_arr)

    if n_past < 2 or n_present < 2:
        print("ERROR: Need at least 2 observations per group for CvM test")
        return 1

    os.makedirs(args.outdir, exist_ok=True)

    # ---- Test 1: CvM (scipy asymptotic) ----
    result_cvm = stats.cramervonmises_2samp(past_arr, present_arr)
    t_cvm = result_cvm.statistic
    p_cvm = result_cvm.pvalue
    reject_cvm = p_cvm < 0.05

    # ---- Test 2: Permutation + CvM ----
    t_perm, p_perm = cramer_von_mises_permutation(
        past_arr, present_arr, n_perm=args.nperm, seed=args.seed
    )
    reject_perm = p_perm < 0.05

    # ---- Report ----
    lines = [
        "",
        "=" * 60,
        "Cramér–von Mises: Past vs Present analogue T2m (snapshot, daily max)",
        "=" * 60,
        f"Event:        {args.event}",
        f"Past n:       {n_past}  (mean {np.mean(past_arr):.2f} °C)",
        f"Present n:   {n_present}  (mean {np.mean(present_arr):.2f} °C)",
        "",
        "Test 1 — CvM (scipy asymptotic):",
        f"  Statistic:  {t_cvm:.6f}",
        f"  p-value:   {p_cvm:.6f}",
        f"  α=0.05:    {'REJECT H0 (distributions differ)' if reject_cvm else 'Do not reject H0'}",
        "",
        "Test 2 — Permutation + CvM:",
        f"  Statistic:  {t_perm:.6f}",
        f"  p-value:   {p_perm:.6f}  (n_perm={args.nperm})",
        f"  α=0.05:    {'REJECT H0 (distributions differ)' if reject_perm else 'Do not reject H0'}",
        "=" * 60,
    ]
    report = "\n".join(lines)
    print(report)

    out_txt = os.path.join(args.outdir, "cvm_test_results.txt")
    with open(out_txt, "w") as f:
        f.write(report)
    print(f"\nSaved: {out_txt}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
