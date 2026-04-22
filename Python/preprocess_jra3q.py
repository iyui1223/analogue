#!/usr/bin/env python3
"""
F01 preprocessor for JRA-3Q (anl_surf125).

Pipeline:
1. 6-hourly GRIB2 -> monthly daily-mean psurf NetCDF (domain-sliced)
2. Monthly daily-mean -> yearly psurf files
3. Yearly psurf -> day-of-year climatology
4. Yearly psurf - climatology -> yearly anomaly files

Outputs are written under Data/F01_preprocess/jra3q/ so F02 can run with:
    DATASET=jra3q
"""

import argparse
import calendar
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Tuple

import yaml


def load_region(events_yaml: Path) -> dict:
    with open(events_yaml, "r") as f:
        cfg = yaml.safe_load(f)
    return cfg["events"][0]["region"]


def parse_months(months_str: str) -> List[int]:
    months: List[int] = []
    for raw in months_str.split(","):
        text = raw.strip()
        if not text:
            continue
        month = int(text)
        if month < 1 or month > 12:
            raise ValueError(f"Invalid month '{month}'. Must be 1-12.")
        months.append(month)
    if not months:
        raise ValueError("No valid months were provided.")
    return sorted(set(months))


def run_cdo(args: List[str], verbose: bool = True) -> bool:
    cmd = ["cdo", "-s"] + args
    if verbose:
        print("  [CDO] " + " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        stderr = result.stderr.strip()
        stdout = result.stdout.strip()
        message = stderr if stderr else stdout
        print(f"  [CDO ERROR] {message}", file=sys.stderr)
        return False
    return True


def grib_files_for_month(src_dir: Path, year: int, month: int) -> List[Path]:
    _, ndays = calendar.monthrange(year, month)
    files: List[Path] = []
    for day in range(1, ndays + 1):
        for hour in (0, 6, 12, 18):
            p = src_dir / f"anl_surf125.{year}{month:02d}{day:02d}{hour:02d}"
            if p.exists():
                files.append(p)
    return files


def month_file(daily_dir: Path, year: int, month: int) -> Path:
    return daily_dir / f"{year}{month:02d}.nc"


def yearly_file(yearly_dir: Path, year: int) -> Path:
    return yearly_dir / f"psurf_{year}.nc"


def anomaly_file(anomaly_dir: Path, year: int) -> Path:
    return anomaly_dir / f"anomaly_psurf_{year}.nc"


def process_month(
    src_dir: Path,
    daily_dir: Path,
    year: int,
    month: int,
    lon_min: float,
    lon_max: float,
    lat_min: float,
    lat_max: float,
    force: bool,
    verbose: bool,
) -> Tuple[str, int]:
    dst = month_file(daily_dir, year, month)
    if dst.exists() and not force:
        if verbose:
            print(f"[SKIP] {dst.name}")
        return "skip", 0

    grib_files = grib_files_for_month(src_dir, year, month)
    if not grib_files:
        if verbose:
            print(f"[MISS] {dst.name}  (no GRIB files)")
        return "missing", 0

    _, ndays = calendar.monthrange(year, month)
    expected = ndays * 4
    if len(grib_files) < expected and verbose:
        print(
            f"  Warning: {len(grib_files)}/{expected} files for {year}-{month:02d} "
            "(partial month)"
        )

    print(f"[PROC] {dst.name}  ({len(grib_files)} GRIB files)")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_out = Path(tmpdir) / dst.name

        cmd = [
            "-f",
            "nc",
            "-setname,psurf",
            "-invertlat",
            f"-sellonlatbox,{lon_min},{lon_max},{lat_min},{lat_max}",
            "-daymean",
            "-selname,prmsl",
            "-mergetime",
            *[str(p) for p in grib_files],
            str(tmp_out),
        ]

        if not run_cdo(cmd, verbose):
            print(f"[FAIL] {dst.name}")
            return "failed", len(grib_files)

        daily_dir.mkdir(parents=True, exist_ok=True)
        # /tmp and project storage can be different filesystems (EXDEV).
        # shutil.move safely falls back to copy+remove across devices.
        shutil.move(str(tmp_out), str(dst))

    print(f"[DONE] {dst.name}")
    return "ok", len(grib_files)


def build_yearly_file(
    daily_dir: Path,
    yearly_dir: Path,
    year: int,
    months: List[int],
    force: bool,
    verbose: bool,
) -> str:
    dst = yearly_file(yearly_dir, year)
    if dst.exists() and not force:
        if verbose:
            print(f"[SKIP] yearly {dst.name}")
        return "skip"

    monthly = [month_file(daily_dir, year, m) for m in months]
    monthly = [p for p in monthly if p.exists()]
    if not monthly:
        if verbose:
            print(f"[MISS] yearly psurf_{year}.nc  (no monthly files)")
        return "missing"

    yearly_dir.mkdir(parents=True, exist_ok=True)

    if len(monthly) == 1:
        shutil.copy2(monthly[0], dst)
        if verbose:
            print(f"[DONE] yearly {dst.name} (single month copy)")
        return "ok"

    cmd = ["-f", "nc", "-setname,psurf", "-mergetime", *[str(p) for p in monthly], str(dst)]
    if not run_cdo(cmd, verbose):
        print(f"[FAIL] yearly {dst.name}")
        return "failed"

    if verbose:
        print(f"[DONE] yearly {dst.name}")
    return "ok"


def compute_climatology(
    yearly_dir: Path,
    climatology_dir: Path,
    years: List[int],
    force: bool,
    verbose: bool,
) -> Tuple[str, Path]:
    out = climatology_dir / "climatology_psurf.nc"
    if out.exists() and not force:
        if verbose:
            print(f"[SKIP] climatology {out.name}")
        return "skip", out

    inputs: List[Path] = []
    for year in years:
        path = yearly_file(yearly_dir, year)
        if path.exists():
            inputs.append(path)
    if not inputs:
        return "missing", out

    climatology_dir.mkdir(parents=True, exist_ok=True)
    if len(inputs) == 1:
        cmd = ["-f", "nc", "-setname,psurf", "-ydaymean", str(inputs[0]), str(out)]
        if not run_cdo(cmd, verbose):
            print("[FAIL] climatology")
            return "failed", out
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            merged = Path(tmpdir) / "merged_yearly_psurf.nc"
            cmd_merge = ["-f", "nc", "-setname,psurf", "-mergetime", *[str(p) for p in inputs], str(merged)]
            if not run_cdo(cmd_merge, verbose):
                print("[FAIL] climatology merge")
                return "failed", out

            cmd_clim = ["-f", "nc", "-setname,psurf", "-ydaymean", str(merged), str(out)]
            if not run_cdo(cmd_clim, verbose):
                print("[FAIL] climatology")
                return "failed", out

    if verbose:
        print(f"[DONE] climatology {out.name}")
    return "ok", out


def compute_anomaly(
    yearly_dir: Path,
    anomaly_dir: Path,
    climatology_path: Path,
    year: int,
    force: bool,
    verbose: bool,
) -> str:
    src = yearly_file(yearly_dir, year)
    dst = anomaly_file(anomaly_dir, year)

    if not src.exists():
        return "missing"
    if dst.exists() and not force:
        if verbose:
            print(f"[SKIP] anomaly {dst.name}")
        return "skip"

    anomaly_dir.mkdir(parents=True, exist_ok=True)
    cmd = ["-f", "nc", "-setname,psurf", "-ydaysub", str(src), str(climatology_path), str(dst)]
    if not run_cdo(cmd, verbose):
        print(f"[FAIL] anomaly {dst.name}")
        return "failed"

    if verbose:
        print(f"[DONE] anomaly {dst.name}")
    return "ok"


def main() -> None:
    parser = argparse.ArgumentParser(description="F01 JRA-3Q preprocess -> anomaly_psurf_YYYY.nc")
    parser.add_argument(
        "--source-dir",
        default=None,
        help="Directory with anl_surf125.* GRIB2 files (default: $JRA3Q_DIR)",
    )
    parser.add_argument(
        "--base-dir",
        default=None,
        help="Base output dir (default: $F01_JRA3Q_BASE or Data/F01_preprocess/jra3q)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="DEPRECATED alias for --daily-dir",
    )
    parser.add_argument("--daily-dir", default=None, help="Monthly daily-mean output directory")
    parser.add_argument("--yearly-dir", default=None, help="Yearly psurf output directory")
    parser.add_argument("--climatology-dir", default=None, help="Climatology output directory")
    parser.add_argument("--anomaly-dir", default=None, help="Anomaly output directory")
    parser.add_argument("--events-yaml", default=None, help="Path to extreme_events.yaml")
    parser.add_argument("--months", default="12,1,2,3,4", help="Comma-separated months to process")
    parser.add_argument("--start-year", type=int, default=None)
    parser.add_argument("--end-year", type=int, default=None)
    parser.add_argument("--force", action="store_true", help="Overwrite existing outputs")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    if shutil.which("cdo") is None:
        print("ERROR: cdo not found in PATH.", file=sys.stderr)
        sys.exit(1)

    root = Path(__file__).resolve().parent.parent
    source_dir = Path(
        args.source_dir
        or os.environ.get("JRA3Q_DIR", "")
        or "/soge-home/data/analysis/jra-q3/anl_surf125"
    )

    base_dir = Path(
        args.base_dir
        or os.environ.get("F01_JRA3Q_BASE", "")
        or str(root / "Data" / "F01_preprocess" / "jra3q")
    )
    daily_dir = Path(
        args.daily_dir
        or args.output_dir
        or os.environ.get("F01_JRA3Q_DAILY_MEAN", "")
        or os.environ.get("F01_JRA3Q_SLP", "")
        or str(base_dir / "daily_mean")
    )
    yearly_dir = Path(
        args.yearly_dir
        or os.environ.get("F01_JRA3Q_YEARLY", "")
        or str(base_dir / "yearly")
    )
    climatology_dir = Path(
        args.climatology_dir
        or os.environ.get("F01_JRA3Q_CLIMATOLOGY", "")
        or str(base_dir / "climatology")
    )
    anomaly_dir = Path(
        args.anomaly_dir
        or os.environ.get("F01_JRA3Q_ANOMALY", "")
        or str(base_dir / "anomaly")
    )

    events_yaml = Path(
        args.events_yaml
        or os.environ.get("EVENTS_CONFIG", "")
        or str(root / "Const" / "extreme_events.yaml")
    )

    start_year = args.start_year or int(os.environ.get("JRA3Q_START_YEAR", "1948"))
    end_year = args.end_year or int(os.environ.get("END_YEAR", "2026"))
    if start_year > end_year:
        print(f"ERROR: start-year ({start_year}) > end-year ({end_year})", file=sys.stderr)
        sys.exit(1)

    try:
        months = parse_months(args.months)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    verbose = not args.quiet

    if not source_dir.is_dir():
        print(f"ERROR: Source directory not found: {source_dir}", file=sys.stderr)
        print("Set JRA3Q_DIR or pass --source-dir.", file=sys.stderr)
        sys.exit(1)
    if not events_yaml.exists():
        print(f"ERROR: events YAML not found: {events_yaml}", file=sys.stderr)
        sys.exit(1)

    region = load_region(events_yaml)
    lon_min, lon_max = region["lon_min"], region["lon_max"]
    lat_min, lat_max = region["lat_min"], region["lat_max"]

    print("=" * 72)
    print("F01: JRA-3Q preprocess (monthly -> yearly -> climatology -> anomaly)")
    print("=" * 72)
    print(f"Source GRIB dir : {source_dir}")
    print(f"Daily output    : {daily_dir}")
    print(f"Yearly output   : {yearly_dir}")
    print(f"Climatology dir : {climatology_dir}")
    print(f"Anomaly output  : {anomaly_dir}")
    print(f"Region          : lon[{lon_min}, {lon_max}] lat[{lat_min}, {lat_max}]")
    print(f"Months          : {months}")
    print(f"Years           : {start_year}-{end_year}")
    print(f"Force           : {args.force}")
    print("=" * 72)

    monthly_counts = {"ok": 0, "skip": 0, "missing": 0, "failed": 0}
    for year in range(start_year, end_year + 1):
        for month in months:
            status, _ = process_month(
                src_dir=source_dir,
                daily_dir=daily_dir,
                year=year,
                month=month,
                lon_min=lon_min,
                lon_max=lon_max,
                lat_min=lat_min,
                lat_max=lat_max,
                force=args.force,
                verbose=verbose,
            )
            monthly_counts[status] = monthly_counts.get(status, 0) + 1

    print()
    print(
        "Monthly summary: "
        f"processed={monthly_counts['ok']} "
        f"skipped={monthly_counts['skip']} "
        f"missing={monthly_counts['missing']} "
        f"failed={monthly_counts['failed']}"
    )

    yearly_counts = {"ok": 0, "skip": 0, "missing": 0, "failed": 0}
    yearly_years: List[int] = []
    for year in range(start_year, end_year + 1):
        status = build_yearly_file(
            daily_dir=daily_dir,
            yearly_dir=yearly_dir,
            year=year,
            months=months,
            force=args.force,
            verbose=verbose,
        )
        yearly_counts[status] = yearly_counts.get(status, 0) + 1
        if status in ("ok", "skip") and yearly_file(yearly_dir, year).exists():
            yearly_years.append(year)

    print()
    print(
        "Yearly summary:  "
        f"processed={yearly_counts['ok']} "
        f"skipped={yearly_counts['skip']} "
        f"missing={yearly_counts['missing']} "
        f"failed={yearly_counts['failed']}"
    )

    if not yearly_years:
        print("ERROR: No yearly files available for climatology/anomaly.", file=sys.stderr)
        sys.exit(1)

    climatology_status, climatology_path = compute_climatology(
        yearly_dir=yearly_dir,
        climatology_dir=climatology_dir,
        years=yearly_years,
        force=args.force,
        verbose=verbose,
    )
    if climatology_status in ("failed", "missing") or not climatology_path.exists():
        print("ERROR: Climatology creation failed.", file=sys.stderr)
        sys.exit(1)

    anomaly_counts = {"ok": 0, "skip": 0, "missing": 0, "failed": 0}
    for year in yearly_years:
        status = compute_anomaly(
            yearly_dir=yearly_dir,
            anomaly_dir=anomaly_dir,
            climatology_path=climatology_path,
            year=year,
            force=args.force,
            verbose=verbose,
        )
        anomaly_counts[status] = anomaly_counts.get(status, 0) + 1

    print()
    print(
        "Anomaly summary: "
        f"processed={anomaly_counts['ok']} "
        f"skipped={anomaly_counts['skip']} "
        f"missing={anomaly_counts['missing']} "
        f"failed={anomaly_counts['failed']}"
    )
    print()
    print("=" * 72)
    print("F01 JRA-3Q preprocessing complete.")
    print(f"Output anomaly dir: {anomaly_dir}")
    print("=" * 72)

    total_fail = monthly_counts["failed"] + yearly_counts["failed"] + anomaly_counts["failed"]
    sys.exit(1 if total_fail else 0)


if __name__ == "__main__":
    main()
