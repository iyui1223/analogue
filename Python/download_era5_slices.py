#!/usr/bin/env python3
"""
ERA5 daily-mean slice downloader for F01 preprocessing.

Downloads pre-sliced ERA5 data from CDS (derived-era5-single-levels-daily-statistics):
  - mean_sea_level_pressure, 2m_temperature, 2m_dewpoint_temperature,
    sea_surface_temperature, 10m_u/v_component_of_wind (daily_mean)
  - total_precipitation (daily_sum)
Output: monthly NetCDF files YYYYMM.nc with all variables.

Downloads full global domain (no area subset). No yearly bundling.

Usage:
  python3 download_era5_slices.py
  START_YEAR=1948 END_YEAR=2022 OUTPUT_DIR=Data/F01_preprocess/era5/slices python3 download_era5_slices.py

Env:
  OUTPUT_DIR, START_YEAR, END_YEAR, MONTHS, FORCE, CDSAPI_RC
"""

import os
import sys
import time
import calendar
import zipfile
from datetime import date
from pathlib import Path

import cdsapi
import xarray as xr


def extract_nc_from_zip(zip_path: str, nc_path: str) -> None:
    """Extract first .nc from zip into nc_path, then remove zip."""
    tmp = nc_path + ".tmp"
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if name.endswith(".nc"):
                with zf.open(name) as src:
                    Path(tmp).write_bytes(src.read())
                break
    Path(zip_path).unlink(missing_ok=True)
    Path(tmp).rename(nc_path)

DATASET = "derived-era5-single-levels-daily-statistics"

# daily_mean variables (psurf=msl, t2m, d2m, sst, u10, v10)
VARS_DAILY_MEAN = [
    "mean_sea_level_pressure",
    "2m_temperature",
    "2m_dewpoint_temperature",
    "sea_surface_temperature",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
]

# total_precipitation uses daily_sum (accumulated)
VARS_DAILY_SUM = ["total_precipitation"]


def get_config():
    """Read config from environment."""
    start = int(os.environ.get("START_YEAR", "1948"))
    end = int(os.environ.get("END_YEAR", str(date.today().year)))
    out = os.environ.get("OUTPUT_DIR", "Data/F01_preprocess/era5/slices")
    force = os.environ.get("FORCE", "0") == "1"
    months_env = os.environ.get("MONTHS", "")
    months = (
        [int(x.strip()) for x in months_env.split(",") if x.strip()]
        if months_env
        else list(range(1, 13))
    )
    retries = int(os.environ.get("MAX_RETRIES", "5"))
    return start, end, out, force, months, retries


def days_in_month(y: int, m: int) -> list[str]:
    """Return zero-padded day strings for the month."""
    _, ndays = calendar.monthrange(y, m)
    return [f"{d:02d}" for d in range(1, ndays + 1)]


def _base_request(y: int, m: int) -> dict:
    """Base request dict (common fields). No area = full global."""
    return {
        "product_type": "reanalysis",
        "year": f"{y}",
        "month": f"{m:02d}",
        "day": days_in_month(y, m),
        "time_zone": "utc+00:00",
        "frequency": "6_hourly",
        "format": "netcdf",
    }


def build_request_mean(y: int, m: int) -> dict:
    """Request payload for daily_mean variables."""
    req = _base_request(y, m)
    req["variable"] = VARS_DAILY_MEAN
    req["daily_statistic"] = "daily_mean"
    return req


def build_request_precip(y: int, m: int) -> dict:
    """Request payload for total_precipitation (daily_sum)."""
    req = _base_request(y, m)
    req["variable"] = VARS_DAILY_SUM
    req["daily_statistic"] = "daily_sum"
    return req


def download_with_retry(client, dataset: str, request: dict, target: str, max_retries: int) -> None:
    """Download with exponential backoff. Handles zip if CDS returns it."""
    backoff = 10
    for attempt in range(1, max_retries + 1):
        try:
            client.retrieve(dataset, request, target)
            # CDS may return zip (saved as target or target.zip)
            p = Path(target)
            zip_candidates = [p, Path(target + ".zip")] if not target.endswith(".zip") else [Path(target)]
            for cand in zip_candidates:
                if cand.exists() and cand.read_bytes()[:2] == b"PK":
                    extract_nc_from_zip(str(cand), target)
                    break
            return
        except Exception as e:
            if attempt == max_retries:
                raise
            print(f"  attempt {attempt}/{max_retries} failed: {e}", file=sys.stderr)
            time.sleep(backoff)
            backoff = min(backoff * 2, 600)


def merge_monthly_files(path_mean: str, path_tp: str, path_out: str) -> None:
    """Merge daily_mean and daily_sum (tp) into a single YYYYMM.nc."""
    ds_mean = xr.open_dataset(path_mean)
    ds_tp = xr.open_dataset(path_tp)
    try:
        ds = xr.merge([ds_mean, ds_tp], compat="override")
        ds.to_netcdf(path_out)
    finally:
        ds_mean.close()
        ds_tp.close()
    Path(path_mean).unlink(missing_ok=True)
    Path(path_tp).unlink(missing_ok=True)


def main():
    start_year, end_year, out_dir, force, months, max_retries = get_config()

    today = date.today()
    end_month_limit = today.month if end_year == today.year else 12

    os.makedirs(out_dir, exist_ok=True)

    # CDS API client (uses CDSAPI_RC from env if set)
    client = cdsapi.Client()

    print("ERA5 daily-mean slice download (F01)")
    print(f"  Output: {out_dir}")
    print(f"  Years: {start_year}..{end_year}")
    print(f"  Months: {months}")
    print(f"  Domain: global (full)")
    print(f"  Variables (daily_mean): {', '.join(VARS_DAILY_MEAN)}")
    print(f"  Variables (daily_sum): {', '.join(VARS_DAILY_SUM)}")
    print("")

    for y in range(start_year, end_year + 1):
        last_m = end_month_limit if y == end_year else 12
        for m in sorted(set(months) & set(range(1, last_m + 1))):
            fname = f"{y}{m:02d}.nc"
            target = os.path.join(out_dir, fname)

            if os.path.exists(target) and os.path.getsize(target) > 0 and not force:
                print(f"[SKIP] {fname}")
                continue

            print(f"[GET ] {fname}")
            tmp_mean = os.path.join(out_dir, f".{y}{m:02d}_mean.nc")
            tmp_tp = os.path.join(out_dir, f".{y}{m:02d}_tp.nc")

            try:
                req_mean = build_request_mean(y, m)
                download_with_retry(client, DATASET, req_mean, tmp_mean, max_retries)

                req_tp = build_request_precip(y, m)
                download_with_retry(client, DATASET, req_tp, tmp_tp, max_retries)

                merge_monthly_files(tmp_mean, tmp_tp, target)
                print(f"[DONE] {fname}")
            except Exception as e:
                for p in (tmp_mean, tmp_tp):
                    Path(p).unlink(missing_ok=True)
                if os.path.exists(target) and os.path.getsize(target) == 0:
                    Path(target).unlink(missing_ok=True)
                print(f"[FAIL] {fname}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
