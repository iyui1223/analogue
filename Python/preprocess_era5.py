#!/usr/bin/env python3
"""
ERA5 domain-slice preprocessor for F01.

Reads global monthly NetCDFs from the user-managed heavy/ ERA5 data store,
applies a spatial domain crop and month filter, and writes smaller files
to Data/F01_preprocess/era5/daily_mean/.

Handles both proper NetCDF files and raw CDS ZIP archives (older downloads).

Usage:
    python3 preprocess_era5.py
    python3 preprocess_era5.py --force
    python3 preprocess_era5.py --start-year 2000 --end-year 2020

Env (all overridable via CLI):
    ERA5_HEAVY_DIR   Source directory with global YYYYMM.nc files
    F01_ERA5_DAILY_MEAN  Output directory for domain-sliced files
    EVENTS_CONFIG    Path to extreme_events.yaml (for region definition)
"""

import argparse
import os
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

import yaml


def load_region(events_yaml: str) -> dict:
    """Extract the circulation region from the first event definition."""
    with open(events_yaml) as f:
        cfg = yaml.safe_load(f)
    return cfg["events"][0]["region"]


def is_zip(filepath: Path) -> bool:
    """Check whether a file is a ZIP archive (CDS raw download)."""
    try:
        with open(filepath, "rb") as f:
            return f.read(2) == b"PK"
    except OSError:
        return False


def extract_nc_from_zip(zip_path: Path, dest: Path) -> None:
    """Extract the first .nc file from a ZIP into *dest*."""
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if name.endswith(".nc"):
                with zf.open(name) as src:
                    dest.write_bytes(src.read())
                return
    raise RuntimeError(f"No .nc found inside {zip_path}")


def run_cdo(cmd: str, verbose: bool = True) -> int:
    if verbose:
        print(f"  [CDO] {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  [CDO ERROR] {result.stderr.strip()}", file=sys.stderr)
    return result.returncode


def process_file(
    src: Path,
    dst: Path,
    lon_min: float,
    lon_max: float,
    lat_min: float,
    lat_max: float,
    verbose: bool = True,
) -> bool:
    """Domain-slice a single monthly file.  Returns True on success."""

    with tempfile.TemporaryDirectory() as tmpdir:
        # Resolve source: extract ZIP if needed
        if is_zip(src) or (src.is_symlink() and is_zip(src.resolve())):
            real_src = src.resolve() if src.is_symlink() else src
            nc_tmp = Path(tmpdir) / "extracted.nc"
            if verbose:
                print(f"  Extracting ZIP: {real_src.name}")
            try:
                extract_nc_from_zip(real_src, nc_tmp)
            except Exception as e:
                print(f"  [ERROR] ZIP extraction failed: {e}", file=sys.stderr)
                return False
            input_path = nc_tmp
        else:
            input_path = src

        dst_tmp = Path(tmpdir) / dst.name
        cmd = (
            f"cdo -s sellonlatbox,{lon_min},{lon_max},{lat_min},{lat_max} "
            f"{input_path} {dst_tmp}"
        )
        if run_cdo(cmd, verbose) != 0:
            return False

        dst.parent.mkdir(parents=True, exist_ok=True)
        dst_tmp.rename(dst) if dst_tmp.exists() else None

    return dst.exists()


def main():
    parser = argparse.ArgumentParser(
        description="F01 ERA5 domain-slice preprocessor"
    )
    parser.add_argument("--source-dir", default=None,
                        help="Directory with global YYYYMM.nc files "
                             "(default: $ERA5_HEAVY_DIR)")
    parser.add_argument("--output-dir", default=None,
                        help="Output directory for sliced files "
                             "(default: $F01_ERA5_DAILY_MEAN)")
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
        or os.environ.get("ERA5_HEAVY_DIR", "")
    )
    output_dir = Path(
        args.output_dir
        or os.environ.get("F01_ERA5_DAILY_MEAN", "")
        or str(root / "Data" / "F01_preprocess" / "era5" / "daily_mean")
    )
    events_yaml = (
        args.events_yaml
        or os.environ.get("EVENTS_CONFIG", "")
        or str(root / "Const" / "extreme_events.yaml")
    )
    months = {int(m.strip()) for m in args.months.split(",")}
    start_year = args.start_year or int(os.environ.get("START_YEAR", "1948"))
    end_year = args.end_year or int(os.environ.get("END_YEAR", "2026"))
    verbose = not args.quiet

    if not source_dir or not source_dir.is_dir():
        print(f"ERROR: Source directory not found: {source_dir}", file=sys.stderr)
        print("Set ERA5_HEAVY_DIR or pass --source-dir.", file=sys.stderr)
        sys.exit(1)

    region = load_region(events_yaml)
    lon_min, lon_max = region["lon_min"], region["lon_max"]
    lat_min, lat_max = region["lat_min"], region["lat_max"]

    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("F01: ERA5 domain-slice preprocessor")
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
            src = source_dir / fname
            dst = output_dir / fname

            if not src.exists():
                if verbose:
                    print(f"[MISS] {fname}  (not in source)")
                missing += 1
                continue

            if dst.exists() and not args.force:
                if verbose:
                    print(f"[SKIP] {fname}")
                skipped += 1
                continue

            print(f"[PROC] {fname}")
            if process_file(src, dst, lon_min, lon_max, lat_min, lat_max, verbose):
                ok += 1
                print(f"[DONE] {fname}")
            else:
                failed += 1
                print(f"[FAIL] {fname}")

    print()
    print("=" * 60)
    print(f"F01 ERA5 preprocessing complete.")
    print(f"  Processed: {ok}  Skipped: {skipped}  "
          f"Missing: {missing}  Failed: {failed}")
    print("=" * 60)

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
