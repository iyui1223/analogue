"""
CDO-based pre-slicing for analogue search.

Uses CDO (Climate Data Operators) to slice anomaly data to:
  - Time: ±snapshot_calendar_window days around snapshot_date, for each year in the period
  - Space: lat/lon bounding box from event region

Runs year-by-year CDO calls in parallel, then merges with mergetime.
This dramatically reduces data volume before Python loads it.
"""

import subprocess
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd


def _run_cdo(args: List[str], verbose: bool = False) -> subprocess.CompletedProcess:
    """Run CDO command. Raises on non-zero exit."""
    cmd = ["cdo", "-s"] + args  # -s for silent
    if verbose:
        print(f"  Running: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"CDO failed (exit {result.returncode}): {result.stderr or result.stdout}"
        )
    return result


def _slice_one_year(
    *,
    input_nc: Path,
    output_nc: Path,
    start_date: str,
    end_date: str,
    lon_min: float,
    lon_max: float,
    lat_min: float,
    lat_max: float,
) -> None:
    """
    Slice one year file: seldate + sellonlatbox.
    Designed to be run in a worker process (picklable).
    """
    # CDO: -seldate,start,end -sellonlatbox,lon1,lon2,lat1,lat2 infile outfile
    _run_cdo(
        [
            "-seldate", f"{start_date},{end_date}",
            "-sellonlatbox", f"{lon_min},{lon_max},{lat_min},{lat_max}",
            str(input_nc),
            str(output_nc),
        ],
        verbose=False,
    )


def ensure_cdo_available() -> bool:
    """Check if CDO is installed and in PATH."""
    return shutil.which("cdo") is not None


def compute_calendar_window_dates(
    snapshot_date: pd.Timestamp,
    window_days: int,
    year: int,
) -> Tuple[str, str]:
    """
    Compute start and end dates for the calendar window in a given year.

    For snapshot 2020-02-08 and window 15:
      - Window: 2020-01-24 to 2020-02-23
    For any year Y, returns the same calendar window in that year.

    Parameters
    ----------
    snapshot_date : pd.Timestamp
        Reference date (used for month-day)
    window_days : int
        ± days around snapshot (snapshot_calendar_window)
    year : int
        Year to compute window for

    Returns
    -------
    Tuple[str, str]
        (start_date, end_date) as YYYY-MM-DD strings
    """
    # Handle leap year edge case: if snapshot is Feb 29 and year is not a leap year, use Feb 28
    month, day = snapshot_date.month, snapshot_date.day
    if month == 2 and day == 29:
        import calendar
        if not calendar.isleap(year):
            day = 28
    base = pd.Timestamp(year=year, month=month, day=day)
    start = base - pd.Timedelta(days=window_days)
    end = base + pd.Timedelta(days=window_days)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def create_sliced_anomaly(
    dataset: str,
    var: str,
    event: Dict[str, Any],
    analogue_config: Dict[str, Any],
    paths: Dict[str, Path],
    n_workers: int = 4,
    verbose: bool = True,
) -> Path:
    """
    Create pre-sliced anomaly NetCDF using CDO (parallel over years).

    Slices each year's anomaly file to:
      - Time: ±snapshot_calendar_window days around snapshot_date
      - Space: event region (lat_min/max, lon_min/max)

    Merges all years and writes to:
      Data/F02_analogue_search/sliced/{dataset}/{event_name}/anomaly_{var}_sliced.nc

    Parameters
    ----------
    dataset : str
        Dataset name (era5, mswx, jra3q)
    var : str
        Variable name (e.g. psurf)
    event : dict
        Event config with name, snapshot_date, region
    analogue_config : dict
        analogue_config.yaml with periods, snapshot_calendar_window
    paths : dict
        Data paths from get_data_paths()
    n_workers : int
        Number of parallel CDO processes
    verbose : bool
        Print progress

    Returns
    -------
    Path
        Path to the merged sliced NetCDF file
    """
    if not ensure_cdo_available():
        raise RuntimeError("CDO not found in PATH. Install CDO to use pre-slicing.")

    event_name = event["name"]
    snapshot_date = pd.Timestamp(event["snapshot_date"])
    region = event["region"]
    window_days = analogue_config.get("snapshot_calendar_window", 15)

    past = analogue_config.get("periods", {}).get("past", {})
    present = analogue_config.get("periods", {}).get("present", {})
    start_year = past.get("start_year")
    end_year = present.get("end_year")
    if start_year is None or end_year is None:
        raise ValueError("analogue_config must define periods.past.start_year and periods.present.end_year")

    lat_min = region["lat_min"]
    lat_max = region["lat_max"]
    lon_min = region["lon_min"]
    lon_max = region["lon_max"]

    anom_dir = paths["data"] / "F01_preprocess" / dataset / "anomaly"
    if not anom_dir.exists():
        raise FileNotFoundError(f"Anomaly directory not found: {anom_dir}")

    out_dir = paths["data"] / "F02_analogue_search" / "sliced" / dataset / event_name
    out_dir.mkdir(parents=True, exist_ok=True)
    work_dir = out_dir / "_work"
    work_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / f"anomaly_{var}_sliced.nc"

    # Collect (year, input_path, start_date, end_date)
    tasks = []
    for year in range(start_year, end_year + 1):
        f = anom_dir / f"anomaly_{var}_{year}.nc"
        if not f.exists():
            if verbose:
                print(f"  Skipping year {year}: file not found")
            continue
        start_d, end_d = compute_calendar_window_dates(snapshot_date, window_days, year)
        out_one = work_dir / f"anomaly_{var}_{year}_sliced.nc"
        tasks.append((f, out_one, start_d, end_d))

    if not tasks:
        raise FileNotFoundError(f"No anomaly files for {var} in {start_year}-{end_year}")

    if verbose:
        print(f"CDO pre-slicing: {len(tasks)} years, {n_workers} workers")
        print(f"  Time window: ±{window_days} days around {snapshot_date.strftime('%m-%d')}")
        print(f"  Region: lat[{lat_min},{lat_max}], lon[{lon_min},{lon_max}]")

    def _task(t):
        inp, outp, sd, ed = t
        _slice_one_year(
            input_nc=inp,
            output_nc=outp,
            start_date=sd,
            end_date=ed,
            lon_min=lon_min,
            lon_max=lon_max,
            lat_min=lat_min,
            lat_max=lat_max,
        )

    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        futures = {ex.submit(_task, t): t for t in tasks}
        done = 0
        for f in as_completed(futures):
            f.result()  # raise if failed
            done += 1
            if verbose and done % 10 == 0:
                print(f"  Sliced {done}/{len(tasks)} years")

    # Merge yearly files
    if verbose:
        print("  Merging with mergetime...")
    year_files = sorted(work_dir.glob(f"anomaly_{var}_*_sliced.nc"))
    _run_cdo(["-mergetime"] + [str(p) for p in year_files] + [str(out_file)], verbose=verbose)

    # Cleanup work dir
    for p in year_files:
        p.unlink()
    try:
        work_dir.rmdir()
    except OSError:
        pass

    if verbose:
        print(f"  Sliced file: {out_file}")
    return out_file


def main():
    """CLI for CDO pre-slicing. Run before analogue_search.py."""
    import argparse
    from data_utils import load_env_setting, load_analogue_config, load_events_config, get_data_paths

    parser = argparse.ArgumentParser(description="CDO pre-slice anomaly data for analogue search")
    parser.add_argument("--dataset", required=True, choices=["era5", "mswx", "jra3q"])
    parser.add_argument("--event", required=True, help="Event name from extreme_events.yaml")
    parser.add_argument("--workers", type=int, default=4, help="Parallel CDO processes")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    env = load_env_setting()
    paths = get_data_paths(env)
    analogue_config = load_analogue_config()
    events_config = load_events_config()

    event = None
    for e in events_config.get("events", []):
        if e["name"] == args.event:
            event = e
            break
    if not event:
        raise SystemExit(f"Event '{args.event}' not found in extreme_events.yaml")
    if "snapshot_date" not in event:
        raise SystemExit(f"Event '{args.event}' lacks snapshot_date")

    match_var = analogue_config.get("distance", {}).get("match_variable", "psurf")
    # Note: Shell script checks for existing sliced file before calling this script,
    # so we proceed directly without redundant existence check here.

    create_sliced_anomaly(
        dataset=args.dataset,
        var=match_var,
        event=event,
        analogue_config=analogue_config,
        paths=paths,
        n_workers=args.workers,
        verbose=not args.quiet,
    )


if __name__ == "__main__":
    main()
