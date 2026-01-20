"""
Analogue Search Module for Analogue Weather Analysis Pipeline.

This module performs:
1. Load anomaly data from F01 preprocessing for specified dataset
2. Slice to event bounding box on-the-fly
3. Use snapshot_date as reference pattern
4. Compute latitude-weighted Euclidean distance between reference and all days
5. Split into past/present periods
6. Select top N analogues per period with minimum time separation
7. Save results to CSV files

Usage:
    python analogue_search.py --dataset era5 --event antarctica_peninsula
    python analogue_search.py --dataset era5 --all
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime

import numpy as np
import pandas as pd
import xarray as xr

from data_utils import (
    load_env_setting,
    load_analogue_config,
    load_events_config,
    get_data_paths,
    ensure_dir,
)


def compute_latitude_weights(lat: xr.DataArray) -> xr.DataArray:
    """
    Compute latitude weights proportional to cos(lat).
    
    Parameters
    ----------
    lat : xr.DataArray
        Latitude coordinate array
        
    Returns
    -------
    xr.DataArray
        Latitude weights normalized to sum to 1
    """
    weights = np.cos(np.deg2rad(lat))
    # Normalize so weights sum to 1
    weights = weights / weights.sum()
    return weights


def load_anomaly_data(
    dataset: str,
    var: str,
    paths: Dict[str, Path],
    region: Dict[str, float],
    verbose: bool = True
) -> xr.DataArray:
    """
    Load anomaly files from F01 preprocessing and slice to event bounding box.
    
    Parameters
    ----------
    dataset : str
        Dataset name: 'era5', 'mswx', or 'jra3q'
    var : str
        Variable name (e.g., 'psurf', 't2m', 'pres')
    paths : dict
        Data paths from get_data_paths()
    region : dict
        Event region with lat_min, lat_max, lon_min, lon_max
    verbose : bool
        Print progress messages
        
    Returns
    -------
    xr.DataArray
        Anomaly data sliced to event bounding box
    """
    anom_dir = paths['data'] / 'F01_preprocess' / dataset / 'anomaly'
    
    if not anom_dir.exists():
        raise FileNotFoundError(f"Anomaly directory not found: {anom_dir}")
    
    # Find all anomaly files for this variable
    pattern = f"anomaly_{var}_*.nc"
    anom_files = sorted(anom_dir.glob(pattern))
    
    if not anom_files:
        raise FileNotFoundError(f"No anomaly files found: {anom_dir}/{pattern}")
    
    if verbose:
        print(f"Found {len(anom_files)} anomaly files for {var}")
        print(f"  First: {anom_files[0].name}")
        print(f"  Last:  {anom_files[-1].name}")
    
    # Open all files as a single dataset using xarray
    # Use combine='by_coords' to concatenate along time
    ds = xr.open_mfdataset(
        anom_files,
        combine='by_coords',
        chunks={'time': 365}  # Chunk by year for memory efficiency
    )
    
    # Get the data variable (first one if multiple)
    var_names = list(ds.data_vars)
    if len(var_names) == 0:
        raise ValueError("No data variables found in anomaly files")
    
    data_var = ds[var_names[0]]
    if verbose:
        print(f"Using variable: {var_names[0]}")
    
    # Detect coordinate names
    lat_name = None
    lon_name = None
    time_name = None
    
    for coord in ds.coords:
        coord_lower = coord.lower()
        if 'lat' in coord_lower:
            lat_name = coord
        elif 'lon' in coord_lower:
            lon_name = coord
        elif 'time' in coord_lower or 'valid' in coord_lower:
            time_name = coord
    
    if lat_name is None or lon_name is None:
        raise ValueError(f"Could not detect lat/lon coordinates. Found: {list(ds.coords)}")
    
    if verbose:
        print(f"Coordinates: time={time_name}, lat={lat_name}, lon={lon_name}")
    
    # Rename coordinates to standard names
    rename_dict = {}
    if lat_name != 'lat':
        rename_dict[lat_name] = 'lat'
    if lon_name != 'lon':
        rename_dict[lon_name] = 'lon'
    if time_name and time_name != 'time':
        rename_dict[time_name] = 'time'
    
    if rename_dict:
        data_var = data_var.rename(rename_dict)
    
    # Slice to event bounding box
    lat_min = region['lat_min']
    lat_max = region['lat_max']
    lon_min = region['lon_min']
    lon_max = region['lon_max']
    
    if verbose:
        print(f"Slicing to region: lat[{lat_min}, {lat_max}], lon[{lon_min}, {lon_max}]")
    
    # Handle latitude ordering (some datasets have lat from 90 to -90)
    if data_var.lat[0] > data_var.lat[-1]:
        # Latitude is descending
        data_var = data_var.sel(
            lat=slice(lat_max, lat_min),
            lon=slice(lon_min, lon_max)
        )
    else:
        # Latitude is ascending
        data_var = data_var.sel(
            lat=slice(lat_min, lat_max),
            lon=slice(lon_min, lon_max)
        )
    
    if verbose:
        print(f"Data shape after slicing: {dict(data_var.sizes)}")
    
    return data_var


def compute_euclidean_distances(
    data: xr.DataArray,
    reference: xr.DataArray,
    lat_weights: xr.DataArray
) -> xr.DataArray:
    """
    Compute latitude-weighted Euclidean distance between reference and all time steps.
    
    Distance formula:
        d(t) = sqrt( sum_ij( w_j * (data(t,i,j) - ref(i,j))^2 ) )
    
    where w_j = cos(lat_j) / sum(cos(lat))
    
    Parameters
    ----------
    data : xr.DataArray
        Data array with dimensions (time, lat, lon)
    reference : xr.DataArray
        Reference pattern with dimensions (lat, lon)
    lat_weights : xr.DataArray
        Normalized latitude weights with dimension (lat,)
        
    Returns
    -------
    xr.DataArray
        Distance for each time step, dimension (time,)
    """
    # Compute difference (broadcasts reference over time dimension)
    diff = data - reference
    
    # Square the differences
    diff_sq = diff ** 2
    
    # Apply latitude weights (broadcasts over lon dimension)
    weighted_diff_sq = diff_sq * lat_weights
    
    # Sum over spatial dimensions
    sum_weighted_sq = weighted_diff_sq.sum(dim=['lat', 'lon'])
    
    # Square root to get Euclidean distance
    distances = np.sqrt(sum_weighted_sq)
    
    return distances


def select_time_separated_analogues(
    df: pd.DataFrame,
    n_analogues: int,
    time_col: str = 'date',
    distance_col: str = 'distance',
    min_separation: pd.Timedelta = pd.Timedelta('5D')
) -> pd.DataFrame:
    """
    Select top N analogues with minimum time separation.
    
    This ensures analogues are not clustered in time (e.g., consecutive days
    from the same weather pattern).
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with date and distance columns
    n_analogues : int
        Number of analogues to select
    time_col : str
        Name of the datetime column
    distance_col : str
        Name of the distance column
    min_separation : pd.Timedelta
        Minimum time separation between selected analogues
        
    Returns
    -------
    pd.DataFrame
        Selected analogues with rank column
    """
    # Sort by distance (ascending - smaller is better)
    df_sorted = df.sort_values(distance_col).reset_index(drop=True)
    
    chosen_indices = []
    chosen_dates = []
    
    for idx, row in df_sorted.iterrows():
        candidate_date = row[time_col]
        
        # Check if this date is far enough from all chosen dates
        is_separated = True
        for chosen_date in chosen_dates:
            if abs(candidate_date - chosen_date) < min_separation:
                is_separated = False
                break
        
        if is_separated:
            chosen_indices.append(idx)
            chosen_dates.append(candidate_date)
            
            if len(chosen_indices) >= n_analogues:
                break
    
    # Create result DataFrame
    result = df_sorted.loc[chosen_indices].copy()
    result['rank'] = range(1, len(result) + 1)
    
    return result


def find_analogues(
    event: Dict[str, Any],
    dataset: str,
    paths: Dict[str, Path],
    analogue_config: Dict[str, Any],
    verbose: bool = True
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Find analogue dates for a single event using snapshot_date as reference.
    
    Parameters
    ----------
    event : dict
        Event configuration from extreme_events.yaml
    dataset : str
        Dataset name: 'era5', 'mswx', or 'jra3q'
    paths : dict
        Data paths from get_data_paths()
    analogue_config : dict
        Analogue search configuration
    verbose : bool
        Print progress messages
        
    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
        - all_distances: DataFrame with all dates and distances
        - past_analogues: DataFrame with top N past analogues
        - present_analogues: DataFrame with top N present analogues
    """
    event_name = event['name']
    region = event['region']
    
    # Get snapshot date (required field)
    if 'snapshot_date' not in event:
        raise ValueError(f"Event '{event_name}' missing required 'snapshot_date' field")
    
    snapshot_date = pd.Timestamp(event['snapshot_date'])
    
    # Get configuration
    n_analogues = analogue_config.get('n_analogues', 15)
    match_var = analogue_config.get('distance', {}).get('match_variable', 'psurf')
    
    past_period = analogue_config.get('periods', {}).get('past', {})
    present_period = analogue_config.get('periods', {}).get('present', {})
    
    past_start = past_period.get('start_year', 1940)
    past_end = past_period.get('end_year', 1941)
    present_start = present_period.get('start_year', 1942)
    present_end = present_period.get('end_year', 1943)
    
    # Time separation for analogue selection
    smoothing_days = analogue_config.get('smoothing', {}).get('window_days', 5)
    min_separation = pd.Timedelta(days=smoothing_days)
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"Analogue Search: {event_name}")
        print(f"{'='*60}")
        print(f"Dataset: {dataset}")
        print(f"Snapshot date: {snapshot_date.strftime('%Y-%m-%d')}")
        print(f"Match variable: {match_var}")
        print(f"Region: lat[{region['lat_min']}, {region['lat_max']}], lon[{region['lon_min']}, {region['lon_max']}]")
        print(f"Past period: {past_start}-{past_end}")
        print(f"Present period: {present_start}-{present_end}")
        print(f"N analogues: {n_analogues}")
        print(f"Min time separation: {min_separation}")
    
    # Load anomaly data sliced to event region
    if verbose:
        print(f"\nLoading anomaly data...")
    
    data_var = load_anomaly_data(
        dataset=dataset,
        var=match_var,
        paths=paths,
        region=region,
        verbose=verbose
    )
    
    # Get reference pattern from snapshot date
    if verbose:
        print(f"\nExtracting reference pattern for {snapshot_date.strftime('%Y-%m-%d')}...")
    
    reference = data_var.sel(time=snapshot_date, method='nearest')
    actual_snapshot = pd.Timestamp(reference.time.values)
    
    if verbose:
        print(f"Actual snapshot date used: {actual_snapshot.strftime('%Y-%m-%d')}")
        print(f"Reference pattern shape: {reference.shape}")
    
    # Compute latitude weights
    lat_weights = compute_latitude_weights(data_var.lat)
    
    # Compute distances to all time steps
    if verbose:
        print(f"\nComputing distances to all {len(data_var.time)} time steps...")
    
    distances = compute_euclidean_distances(data_var, reference, lat_weights)
    
    # Trigger computation (if using dask)
    distances = distances.compute()
    
    if verbose:
        print(f"Distance computation complete.")
    
    # Convert to DataFrame
    all_distances = pd.DataFrame({
        'date': pd.to_datetime(distances.time.values),
        'distance': distances.values
    })
    all_distances['year'] = all_distances['date'].dt.year
    all_distances['month'] = all_distances['date'].dt.month
    all_distances['day'] = all_distances['date'].dt.day
    
    # Define masks for past and present periods
    past_mask = (all_distances['year'] >= past_start) & (all_distances['year'] <= past_end)
    present_mask = (all_distances['year'] >= present_start) & (all_distances['year'] <= present_end)
    
    # Exclude snapshot date from candidates (and nearby dates)
    snapshot_exclusion = (
        (all_distances['date'] >= actual_snapshot - min_separation) & 
        (all_distances['date'] <= actual_snapshot + min_separation)
    )
    past_mask = past_mask & ~snapshot_exclusion
    present_mask = present_mask & ~snapshot_exclusion
    
    if verbose:
        print(f"\nPast candidates: {past_mask.sum()}")
        print(f"Present candidates (excl. snapshot): {present_mask.sum()}")
    
    # Select top analogues from each period
    past_candidates = all_distances[past_mask].copy()
    past_df = select_time_separated_analogues(
        past_candidates,
        n_analogues=n_analogues,
        time_col='date',
        distance_col='distance',
        min_separation=min_separation
    )
    past_df['period'] = 'past'
    
    present_candidates = all_distances[present_mask].copy()
    present_df = select_time_separated_analogues(
        present_candidates,
        n_analogues=n_analogues,
        time_col='date',
        distance_col='distance',
        min_separation=min_separation
    )
    present_df['period'] = 'present'
    
    if verbose:
        print(f"\n--- Top {len(past_df)} Past Analogues ---")
        if len(past_df) > 0:
            print(past_df[['rank', 'date', 'distance']].to_string(index=False))
        else:
            print("  No past analogues found")
        
        print(f"\n--- Top {len(present_df)} Present Analogues ---")
        if len(present_df) > 0:
            print(present_df[['rank', 'date', 'distance']].to_string(index=False))
        else:
            print("  No present analogues found")
    
    return all_distances, past_df, present_df


def process_event(
    event: Dict[str, Any],
    dataset: str,
    paths: Dict[str, Path],
    analogue_config: Dict[str, Any],
    skip_existing: bool = True,
    verbose: bool = True
) -> bool:
    """
    Process analogue search for a single event and save results.
    
    Parameters
    ----------
    event : dict
        Event configuration
    dataset : str
        Dataset name
    paths : dict
        Data paths
    analogue_config : dict
        Analogue configuration
    skip_existing : bool
        Skip if output exists
    verbose : bool
        Print progress
        
    Returns
    -------
    bool
        True if successful
    """
    event_name = event['name']
    
    # Output directory includes dataset name
    output_dir = ensure_dir(paths['analogue'] / dataset / event_name)
    
    # Output files
    distances_file = output_dir / 'all_distances.csv'
    past_file = output_dir / 'past_analogues.csv'
    present_file = output_dir / 'present_analogues.csv'
    combined_file = output_dir / 'analogues.csv'
    
    # Check if can skip
    if skip_existing and combined_file.exists():
        if verbose:
            print(f"[{event_name}] Output exists, skipping: {combined_file}")
        return True
    
    try:
        # Find analogues
        all_distances, past_df, present_df = find_analogues(
            event=event,
            dataset=dataset,
            paths=paths,
            analogue_config=analogue_config,
            verbose=verbose
        )
        
        # Save results
        all_distances.to_csv(distances_file, index=False)
        past_df.to_csv(past_file, index=False)
        present_df.to_csv(present_file, index=False)
        
        # Combined analogues file
        combined = pd.concat([past_df, present_df], ignore_index=True)
        combined.to_csv(combined_file, index=False)
        
        if verbose:
            print(f"\n[{event_name}] Results saved to:")
            print(f"  - All distances: {distances_file}")
            print(f"  - Past analogues: {past_file}")
            print(f"  - Present analogues: {present_file}")
            print(f"  - Combined: {combined_file}")
        
        return True
        
    except Exception as e:
        print(f"[{event_name}] ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return False


def process_all_events(
    dataset: str,
    skip_existing: bool = True,
    verbose: bool = True
) -> bool:
    """
    Process analogue search for all events.
    
    Parameters
    ----------
    dataset : str
        Dataset name: 'era5', 'mswx', or 'jra3q'
    skip_existing : bool
        Skip if output exists
    verbose : bool
        Print progress
        
    Returns
    -------
    bool
        True if all events processed successfully
    """
    # Load configurations
    env = load_env_setting()
    paths = get_data_paths(env)
    analogue_config = load_analogue_config()
    events_config = load_events_config()
    
    events = events_config.get('events', [])
    
    if not events:
        print("No events defined in extreme_events.yaml")
        return False
    
    # Filter events that have snapshot_date defined
    valid_events = [e for e in events if 'snapshot_date' in e]
    
    if not valid_events:
        print("No events with 'snapshot_date' defined in extreme_events.yaml")
        return False
    
    print(f"\nDataset: {dataset}")
    print(f"Processing {len(valid_events)} event(s) for analogue search...")
    
    all_success = True
    for event in valid_events:
        success = process_event(
            event=event,
            dataset=dataset,
            paths=paths,
            analogue_config=analogue_config,
            skip_existing=skip_existing,
            verbose=verbose
        )
        if not success:
            all_success = False
    
    return all_success


def main():
    """Main entry point for analogue search."""
    parser = argparse.ArgumentParser(
        description='Find weather analogues for extreme events'
    )
    parser.add_argument(
        '--dataset',
        type=str,
        required=True,
        choices=['era5', 'mswx', 'jra3q'],
        help='Dataset to use for analogue search'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Process all events with snapshot_date defined'
    )
    parser.add_argument(
        '--event',
        type=str,
        default=None,
        help='Process specific event by name'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force recomputation even if output exists'
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Suppress verbose output'
    )
    
    args = parser.parse_args()
    
    skip_existing = not args.force
    verbose = not args.quiet
    
    print("=" * 60)
    print("Analogue Search Pipeline")
    print("=" * 60)
    print(f"Dataset: {args.dataset}")
    
    if args.all or args.event:
        if args.event:
            # Process single event
            env = load_env_setting()
            paths = get_data_paths(env)
            analogue_config = load_analogue_config()
            events_config = load_events_config()
            
            # Find the event
            event = None
            for e in events_config.get('events', []):
                if e['name'] == args.event:
                    event = e
                    break
            
            if event is None:
                print(f"Event not found: {args.event}")
                sys.exit(1)
            
            if 'snapshot_date' not in event:
                print(f"Event '{args.event}' is missing required 'snapshot_date' field")
                sys.exit(1)
            
            success = process_event(
                event=event,
                dataset=args.dataset,
                paths=paths,
                analogue_config=analogue_config,
                skip_existing=skip_existing,
                verbose=verbose
            )
        else:
            # Process all events
            success = process_all_events(
                dataset=args.dataset,
                skip_existing=skip_existing,
                verbose=verbose
            )
        
        sys.exit(0 if success else 1)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
