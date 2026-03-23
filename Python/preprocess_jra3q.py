#!/usr/bin/env python3
"""
JRA-3Q SLP preprocessor for F01.

Reads 6-hourly GRIB2 surface analysis files from
/lustre/soge1/data/analysis/jra-q3/anl_surf125/,
selects the prmsl (mean-sea-level pressure) variable, computes daily
means, crops to the circulation domain, and writes monthly NetCDF files
to Data/F01_preprocess/jra3q/slp_daily_mean/.

Source file naming:  anl_surf125.YYYYMMDDHH  (HH = 00, 06, 12, 18)

Usage:
    python3 preprocess_jra3q.py
    python3 preprocess_jra3q.py --force
    python3 preprocess_jra3q.py --start-year 1948 --end-year 1993

Env (all overridable via CLI):
    JRA3Q_DIR        Source directory with anl_surf125.* files
    EVENTS_CONFIG    Path to extreme_events.yaml (for region definition)
"""

import argparse
import calendar
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import yaml


def load_region(events_yaml: str) -> dict:
    with open(events_yaml) as f:
        cfg = yaml.safe_load(f)
    return cfg["events"][0]["region"]


def run_cdo(cmd: str, verbose: bool = True) -> int:
    if verbose:
        print(f"  [CDO] {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  [CDO ERROR] {result.stderr.strip()}", file=sys.stderr)
    return result.returncode


def grib_files_for_month(src_dir: Path, year: int, month: int) -> list[Path]:
    """Return sorted list of 6-hourly GRIB2 files for a given month."""
    _, ndays = calendar.monthrange(year, month)
    files = []
    for day in range(1, ndays + 1):
        for hour in (0, 6, 12, 18):
            name = f"anl_surf125.{year}{month:02d}{day:02d}{hour:02d}"
            p = src_dir / name
            if p.exists():
                files.append(p)
    return files


def process_month(
    src_dir: Path,
    dst: Path,
    year: int,
    month: int,
    lon_min: float,
    lon_max: float,
    lat_min: float,
    lat_max: float,
    verbose: bool = True,
) -> bool:
    """Process one month of JRA-3Q 6-hourly GRIB2 -> daily-mean SLP NetCDF."""

    grib_files = grib_files_for_month(src_dir, year, month)
    if not grib_files:
        if verbose:
            print(f"  No GRIB files found for {year}-{month:02d}")
        return False

    _, ndays = calendar.monthrange(year, month)
    expected = ndays * 4
    if len(grib_files) < expected:
        if verbose:
            print(f"  Warning: {len(grib_files)}/{expected} files for "
                  f"{year}-{month:02d} (partial month)")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)

        # CDO pipeline:
        #   1. Merge all 6-hourly files for the month
        #   2. Select prmsl variable
        #   3. Compute daily mean from 6-hourly
        #   4. Crop to circulation domain
        input_list = " ".join(str(f) for f in grib_files)
        merged = tmp / "merged.grb"
        selected = tmp / "selected.grb"
        daily = tmp / "daily.nc"
        cropped = tmp / "cropped.nc"

        if run_cdo(f"cdo -s mergetime {input_list} {merged}", verbose) != 0:
            return False

        if run_cdo(f"cdo -s selname,prmsl {merged} {selected}", verbose) != 0:
            return False

        if run_cdo(f"cdo -s -f nc daymean {selected} {daily}", verbose) != 0:
            return False

        cmd_crop = (
            f"cdo -s sellonlatbox,{lon_min},{lon_max},{lat_min},{lat_max} "
            f"{daily} {cropped}"
        )
        if run_cdo(cmd_crop, verbose) != 0:
            return False

        dst.parent.mkdir(parents=True, exist_ok=True)
        if cropped.exists():
            cropped.rename(dst)

    return dst.exists()


def main():
    parser = argparse.ArgumentParser(
        description="F01 JRA-3Q SLP preprocessor"
    )
    parser.add_argument("--source-dir", default=None,
                        help="Directory with anl_surf125.* GRIB2 files "
                             "(default: $JRA3Q_DIR)")
    parser.add_argument("--output-dir", default=None,
                        help="Output directory for monthly SLP files")
    parser.add_argument("--events-yaml", default=None,
                        help="Path to extreme_events.yaml")
    parser.add_argument("--months", default="12,1,2,3,4",
                        help="Comma-separated months to keep (default: DJFMA)")
    parser.add_argument("--start-year", type=int, default=None)
    parser.add_argument("--end-year", type=int, default=None)
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing output files")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    root = Path(__file__).resolve().parent.parent
    source_dir = Path(
        args.source_dir
        or os.environ.get("JRA3Q_DIR", "")
    )
    output_dir = Path(
        args.output_dir
        or os.environ.get("F01_JRA3Q_SLP", "")
        or str(root / "Data" / "F01_preprocess" / "jra3q" / "slp_daily_mean")
    )
    events_yaml = (
        args.events_yaml
        or os.environ.get("EVENTS_CONFIG", "")
        or str(root / "Const" / "extreme_events.yaml")
    )
    months = {int(m.strip()) for m in args.months.split(",")}
    start_year = args.start_year or int(os.environ.get("JRA3Q_START_YEAR", "1948"))
    end_year = args.end_year or int(os.environ.get("END_YEAR", "1993"))
    verbose = not args.quiet

    if not source_dir or not source_dir.is_dir():
        print(f"ERROR: Source directory not found: {source_dir}", file=sys.stderr)
        print("Set JRA3Q_DIR or pass --source-dir.", file=sys.stderr)
        sys.exit(1)

    region = load_region(events_yaml)
    lon_min, lon_max = region["lon_min"], region["lon_max"]
    lat_min, lat_max = region["lat_min"], region["lat_max"]

    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("F01: JRA-3Q SLP preprocessor")
    print("=" * 60)
    print(f"Source:  {source_dir}")
    print(f"Output:  {output_dir}")
    print(f"Region:  lon[{lon_min}, {lon_max}] lat[{lat_min}, {lat_max}]")
    print(f"Months:  {sorted(months)}")
    print(f"Years:   {start_year}–{end_year}")
    print(f"Force:   {args.force}")
    print("=" * 60)
    print()

    ok, skipped, failed, missing = 0, 0, 0, 0

    for year in range(start_year, end_year + 1):
        for month in sorted(months):
            fname = f"{year}{month:02d}.nc"
            dst = output_dir / fname

            if dst.exists() and not args.force:
                if verbose:
                    print(f"[SKIP] {fname}")
                skipped += 1
                continue

            grib_files = grib_files_for_month(source_dir, year, month)
            if not grib_files:
                if verbose:
                    print(f"[MISS] {fname}  (no GRIB files)")
                missing += 1
                continue

            print(f"[PROC] {fname}  ({len(grib_files)} GRIB files)")
            if process_month(source_dir, dst, year, month,
                             lon_min, lon_max, lat_min, lat_max, verbose):
                ok += 1
                print(f"[DONE] {fname}")
            else:
                failed += 1
                print(f"[FAIL] {fname}")

    print()
    print("=" * 60)
    print(f"F01 JRA-3Q SLP preprocessing complete.")
    print(f"  Processed: {ok}  Skipped: {skipped}  "
          f"Missing: {missing}  Failed: {failed}")
    print("=" * 60)

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
