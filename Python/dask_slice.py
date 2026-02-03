"""
Dask-based pre-slicing for analogue search.

Alternative to cdo_slice.py - uses dask/xarray for efficient parallel slicing.
Faster than CDO approach because:
  - Single lazy load of all files
  - Dask parallelizes internally (no process spawn overhead)
  - Direct write to output (no intermediate files)

Slices anomaly data to:
  - Time: ±snapshot_calendar_window days around snapshot_date (for each year)
  - Space: lat/lon bounding box from event region
"""

from pathlib import Path
from typing import Dict, Any, Optional

import numpy as np
import pandas as pd
import xarray as xr


def compute_calendar_mask(
    times: pd.DatetimeIndex,
    snapshot_date: pd.Timestamp,
    window_days: int,
) -> np.ndarray:
    """
    Create boolean mask for dates within ±window_days of snapshot's month-day.
    
    For each year in `times`, selects dates within the calendar window
    centered on snapshot_date's month-day.
    
    Parameters
    ----------
    times : pd.DatetimeIndex
        All timestamps in the dataset
    snapshot_date : pd.Timestamp
        Reference date (month-day used for window center)
    window_days : int
        ±days around the snapshot calendar date
        
    Returns
    -------
    np.ndarray
        Boolean mask, True for dates within the window
    """
    # Get day-of-year for snapshot (handle leap year: use a leap year for reference)
    # We'll work with day-of-year, accounting for the ±window wrap-around
    ref_doy = snapshot_date.dayofyear
    
    # Get day-of-year for all times
    doys = times.dayofyear.values
    
    # Handle wrap-around at year boundary
    # Distance in days on a circular calendar (365/366 days)
    # For simplicity, use 365 as base (leap year Feb 29 will be included if close)
    diff = np.abs(doys - ref_doy)
    # Wrap around: if diff > 182, use 365 - diff
    diff = np.minimum(diff, 365 - diff)
    
    mask = diff <= window_days
    return mask


def create_sliced_anomaly_dask(
    dataset: str,
    var: str,
    event: Dict[str, Any],
    analogue_config: Dict[str, Any],
    paths: Dict[str, Path],
    n_workers: int = 8,
    verbose: bool = True,
) -> Path:
    """
    Create pre-sliced anomaly NetCDF using dask/xarray.

    Loads all anomaly files lazily, slices to calendar window and bbox,
    then writes directly to output file.

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
        Number of dask workers/threads
    verbose : bool
        Print progress

    Returns
    -------
    Path
        Path to the sliced NetCDF file
    """
    from dask.diagnostics import ProgressBar
    
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
    out_file = out_dir / f"anomaly_{var}_sliced.nc"

    # Collect anomaly files
    anom_files = []
    for year in range(start_year, end_year + 1):
        f = anom_dir / f"anomaly_{var}_{year}.nc"
        if f.exists():
            anom_files.append(f)
        elif verbose:
            print(f"  Skipping year {year}: file not found")

    if not anom_files:
        raise FileNotFoundError(f"No anomaly files for {var} in {start_year}-{end_year}")

    if verbose:
        print(f"Dask slicing: {len(anom_files)} files")
        print(f"  Time window: ±{window_days} days around {snapshot_date.strftime('%m-%d')}")
        print(f"  Region: lat[{lat_min},{lat_max}], lon[{lon_min},{lon_max}]")
        print(f"  Output: {out_file}")
        print(f"  Loading files (lazy)...", flush=True)

    # Load all files lazily - use synchronous loading to avoid segfaults
    ds = xr.open_mfdataset(
        sorted(anom_files),
        combine='by_coords',
        chunks={'time': 365},  # Chunk by ~1 year
        parallel=False,  # Safer: avoid threading issues during file open
        engine='netcdf4',
    )
    
    # Get the data variable (first one)
    var_names = list(ds.data_vars)
    if not var_names:
        raise ValueError("No data variables in anomaly files")
    data_var = ds[var_names[0]]
    
    # Standardize coordinate names
    coord_renames = {}
    for coord in ds.coords:
        cl = coord.lower()
        if 'lat' in cl and coord != 'lat':
            coord_renames[coord] = 'lat'
        elif 'lon' in cl and coord != 'lon':
            coord_renames[coord] = 'lon'
        elif ('time' in cl or 'valid' in cl) and coord != 'time':
            coord_renames[coord] = 'time'
    if coord_renames:
        data_var = data_var.rename(coord_renames)
    
    if verbose:
        print(f"  Loaded shape: {dict(data_var.sizes)}", flush=True)
    
    # Slice to spatial bbox
    if verbose:
        print("  Slicing to bbox...", flush=True)
    
    # Handle lat direction (could be N->S or S->N)
    if data_var.lat[0] > data_var.lat[-1]:
        data_var = data_var.sel(lat=slice(lat_max, lat_min), lon=slice(lon_min, lon_max))
    else:
        data_var = data_var.sel(lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max))
    
    if verbose:
        print(f"  After bbox: {dict(data_var.sizes)}", flush=True)
    
    # Slice to calendar window
    if verbose:
        print("  Computing calendar window mask...", flush=True)
    
    times = pd.to_datetime(data_var.time.values)
    mask = compute_calendar_mask(times, snapshot_date, window_days)
    
    n_selected = int(mask.sum())
    if verbose:
        print(f"  Selected {n_selected} / {len(times)} timesteps ({n_selected/len(times)*100:.1f}%)", flush=True)
    
    # Apply mask
    data_var = data_var.isel(time=mask)
    
    if verbose:
        print(f"  Final shape: {dict(data_var.sizes)}", flush=True)
    
    # Write to file
    if verbose:
        print("  Writing to NetCDF (this may take a while)...", flush=True)
    
    # Convert to dataset for writing
    out_ds = data_var.to_dataset(name=var_names[0])
    
    # Compute time/lat/lon sizes safely
    n_time = data_var.sizes.get('time', len(data_var.time))
    n_lat = data_var.sizes.get('lat', len(data_var.lat))
    n_lon = data_var.sizes.get('lon', len(data_var.lon))
    
    # Use compression for smaller file
    encoding = {
        var_names[0]: {
            'zlib': True,
            'complevel': 4,
            'chunksizes': (min(365, n_time), n_lat, n_lon),
        }
    }
    
    # Use synchronous scheduler for safer write (avoids HDF5 threading issues)
    with ProgressBar():
        out_ds.to_netcdf(out_file, encoding=encoding, compute=True)
    
    if verbose:
        # Report file size
        size_mb = out_file.stat().st_size / (1024 * 1024)
        print(f"  Done: {out_file.name} ({size_mb:.1f} MB)")
    
    return out_file


def create_sliced_anomaly_sequential(
    dataset: str,
    var: str,
    event: Dict[str, Any],
    analogue_config: Dict[str, Any],
    paths: Dict[str, Path],
    verbose: bool = True,
) -> Path:
    """
    Create pre-sliced anomaly NetCDF by processing files sequentially.
    
    Safer fallback that avoids threading issues - processes year by year.
    """
    event_name = event["name"]
    snapshot_date = pd.Timestamp(event["snapshot_date"])
    region = event["region"]
    window_days = analogue_config.get("snapshot_calendar_window", 15)

    past = analogue_config.get("periods", {}).get("past", {})
    present = analogue_config.get("periods", {}).get("present", {})
    start_year = past.get("start_year")
    end_year = present.get("end_year")

    lat_min, lat_max = region["lat_min"], region["lat_max"]
    lon_min, lon_max = region["lon_min"], region["lon_max"]

    anom_dir = paths["data"] / "F01_preprocess" / dataset / "anomaly"
    out_dir = paths["data"] / "F02_analogue_search" / "sliced" / dataset / event_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"anomaly_{var}_sliced.nc"

    if verbose:
        print(f"Sequential slicing: years {start_year}-{end_year}")
        print(f"  Time window: ±{window_days} days around {snapshot_date.strftime('%m-%d')}")
        print(f"  Region: lat[{lat_min},{lat_max}], lon[{lon_min},{lon_max}]")
        print(f"  Output: {out_file}", flush=True)

    sliced_arrays = []
    
    for year in range(start_year, end_year + 1):
        f = anom_dir / f"anomaly_{var}_{year}.nc"
        if not f.exists():
            if verbose:
                print(f"  Year {year}: file not found, skipping", flush=True)
            continue
        
        if verbose:
            print(f"  Year {year}: processing...", end=" ", flush=True)
        
        # Load single file
        ds = xr.open_dataset(f)
        var_name = list(ds.data_vars)[0]
        data = ds[var_name]
        
        # Standardize coords
        for coord in list(data.coords):
            cl = coord.lower()
            if 'lat' in cl and coord != 'lat':
                data = data.rename({coord: 'lat'})
            elif 'lon' in cl and coord != 'lon':
                data = data.rename({coord: 'lon'})
            elif ('time' in cl or 'valid' in cl) and coord != 'time':
                data = data.rename({coord: 'time'})
        
        # Slice to bbox
        if data.lat[0] > data.lat[-1]:
            data = data.sel(lat=slice(lat_max, lat_min), lon=slice(lon_min, lon_max))
        else:
            data = data.sel(lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max))
        
        # Slice to calendar window
        times = pd.to_datetime(data.time.values)
        mask = compute_calendar_mask(times, snapshot_date, window_days)
        data = data.isel(time=mask)
        
        # Load into memory
        data = data.load()
        
        n_days = len(data.time)
        if verbose:
            print(f"{n_days} days", flush=True)
        
        if n_days > 0:
            sliced_arrays.append(data)
        
        ds.close()
    
    if not sliced_arrays:
        raise ValueError("No data found after slicing")
    
    if verbose:
        print(f"  Concatenating {len(sliced_arrays)} years...", flush=True)
    
    # Concatenate all years
    combined = xr.concat(sliced_arrays, dim='time')
    combined = combined.sortby('time')
    
    if verbose:
        print(f"  Final shape: {dict(combined.sizes)}", flush=True)
        print(f"  Writing to {out_file.name}...", flush=True)
    
    # Write with compression
    out_ds = combined.to_dataset(name=var)
    out_ds.to_netcdf(out_file, encoding={var: {'zlib': True, 'complevel': 4}})
    
    if verbose:
        size_mb = out_file.stat().st_size / (1024 * 1024)
        print(f"  Done: {out_file.name} ({size_mb:.1f} MB)", flush=True)
    
    return out_file


def main():
    """CLI for dask-based pre-slicing."""
    import argparse
    import sys
    from data_utils import load_env_setting, load_analogue_config, load_events_config, get_data_paths

    parser = argparse.ArgumentParser(description="Pre-slice anomaly data for analogue search")
    parser.add_argument("--dataset", required=True, choices=["era5", "mswx", "jra3q"])
    parser.add_argument("--event", required=True, help="Event name from extreme_events.yaml")
    parser.add_argument("--workers", type=int, default=4, help="Number of dask threads (default: 4)")
    parser.add_argument("--sequential", action="store_true", help="Use sequential processing (safer, avoids threading issues)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    print(f"dask_slice.py starting...", flush=True)
    
    try:
        env = load_env_setting()
        paths = get_data_paths(env)
        analogue_config = load_analogue_config()
        events_config = load_events_config()
    except Exception as e:
        print(f"Error loading config: {e}", file=sys.stderr)
        sys.exit(1)

    event = None
    for e in events_config.get("events", []):
        if e["name"] == args.event:
            event = e
            break
    if not event:
        print(f"Event '{args.event}' not found in extreme_events.yaml", file=sys.stderr)
        sys.exit(1)
    if "snapshot_date" not in event:
        print(f"Event '{args.event}' lacks snapshot_date", file=sys.stderr)
        sys.exit(1)

    match_var = analogue_config.get("distance", {}).get("match_variable", "psurf")
    out_file = paths["data"] / "F02_analogue_search" / "sliced" / args.dataset / args.event / f"anomaly_{match_var}_sliced.nc"
    
    if out_file.exists() and not args.force:
        print(f"Sliced file exists, skipping: {out_file.name}")
        print(f"Use --force to overwrite.")
        return

    try:
        if args.sequential:
            # Safer sequential processing
            create_sliced_anomaly_sequential(
                dataset=args.dataset,
                var=match_var,
                event=event,
                analogue_config=analogue_config,
                paths=paths,
                verbose=not args.quiet,
            )
        else:
            # Dask-based processing
            create_sliced_anomaly_dask(
                dataset=args.dataset,
                var=match_var,
                event=event,
                analogue_config=analogue_config,
                paths=paths,
                n_workers=args.workers,
                verbose=not args.quiet,
            )
    except Exception as e:
        print(f"Error during slicing: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
