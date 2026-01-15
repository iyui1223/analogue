"""
Analogue Search Module for Analogue Weather Analysis Pipeline.

This module performs:
1. Load smoothed bbox data for an event
2. Compute latitude-weighted Euclidean distance between event pattern and all days
3. Split into past/present periods
4. Select top N analogues per period
5. Save analogue dates, distances, and indices

Uses xarray for vectorized computation - all distances computed in one pass.
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
    load_preprocess_config,
    load_analogue_config,
    load_events_config,
    get_data_paths,
    ensure_dir,
    file_exists_and_valid,
    get_event_bbox_file,
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
        Latitude weights
    """
    return np.cos(np.deg2rad(lat))


def compute_euclidean_distances(
    data: xr.DataArray,
    reference: xr.DataArray,
    lat_weights: xr.DataArray
) -> xr.DataArray:
    """
    Compute latitude-weighted Euclidean distance between reference and all time steps.
    
    Distance formula:
        d(t) = sqrt( sum_ij( w_j * (data(t,i,j) - ref(i,j))^2 ) )
    
    where w_j = cos(lat_j)
    
    Parameters
    ----------
    data : xr.DataArray
        Data array with dimensions (time, lat, lon)
    reference : xr.DataArray
        Reference pattern with dimensions (lat, lon)
    lat_weights : xr.DataArray
        Latitude weights with dimension (lat,)
        
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


def find_analogues(
    event: Dict[str, Any],
    paths: Dict[str, Path],
    analogue_config: Dict[str, Any],
    verbose: bool = True
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Find analogue dates for a single event.
    
    Parameters
    ----------
    event : dict
        Event configuration from extreme_events.yaml
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
    start_date = event['start_date']
    end_date = event['end_date']
    
    # Get configuration
    n_analogues = analogue_config.get('n_analogues', 15)
    match_var = analogue_config.get('distance', {}).get('match_variable', 'pres')
    past_period = analogue_config.get('periods', {}).get('past', {})
    present_period = analogue_config.get('periods', {}).get('present', {})
    
    past_start = past_period.get('start_year', 1979)
    past_end = past_period.get('end_year', 2000)
    present_start = present_period.get('start_year', 2001)
    present_end = present_period.get('end_year', 2022)
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"Analogue Search: {event_name}")
        print(f"{'='*60}")
        print(f"Event dates: {start_date} to {end_date}")
        print(f"Match variable: {match_var}")
        print(f"Past period: {past_start}-{past_end}")
        print(f"Present period: {present_start}-{present_end}")
        print(f"N analogues: {n_analogues}")
    
    # Load smoothed bbox data for matching variable
    bbox_file = get_event_bbox_file(paths, event_name, match_var, is_anomaly=True, smoothed=True)
    
    if not bbox_file.exists():
        raise FileNotFoundError(f"Bbox file not found: {bbox_file}")
    
    if verbose:
        print(f"\nLoading data: {bbox_file}")
    
    # Open dataset with dask chunking for memory efficiency
    ds = xr.open_dataset(bbox_file, chunks={'time': 365})
    
    # Get the variable (try common names)
    var_names = [match_var, 'pres', 'pressure', 'sp', 'msl']
    data_var = None
    for vn in var_names:
        if vn in ds.data_vars:
            data_var = ds[vn]
            break
    
    if data_var is None:
        # Just take the first data variable
        data_var = ds[list(ds.data_vars)[0]]
        if verbose:
            print(f"Using variable: {data_var.name}")
    
    # Get coordinate names (handle different naming conventions)
    lat_name = 'lat' if 'lat' in ds.coords else 'latitude'
    lon_name = 'lon' if 'lon' in ds.coords else 'longitude'
    time_name = 'time'
    
    # Rename coordinates if needed
    if lat_name != 'lat':
        data_var = data_var.rename({lat_name: 'lat'})
    if lon_name != 'lon':
        data_var = data_var.rename({lon_name: 'lon'})
    
    # Compute reference pattern (event mean)
    if verbose:
        print(f"Computing event reference pattern...")
    
    event_start = pd.Timestamp(start_date)
    event_end = pd.Timestamp(end_date)
    
    # Select event time window and compute mean
    event_mask = (data_var.time >= event_start) & (data_var.time <= event_end)
    reference = data_var.where(event_mask, drop=True).mean(dim='time') # !!! better make a snapshot than mean -- should be defined within the .yaml file along with the start/end dates.

    
    if verbose:
        print(f"Reference pattern shape: {reference.shape}")
    
    # Compute latitude weights
    lat_weights = compute_latitude_weights(data_var.lat)
    
    # Compute distances to all time steps
    if verbose:
        print(f"Computing distances to all {len(data_var.time)} time steps...")
    
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
    
    # Exclude event dates from present period
    event_dates_mask = (all_distances['date'] >= event_start) & (all_distances['date'] <= event_end)
    present_mask = present_mask & ~event_dates_mask
    
    if verbose:
        print(f"\nPast candidates: {past_mask.sum()}")
        print(f"Present candidates (excl. event): {present_mask.sum()}")

    def select_time_filtered_events(df, n_analogues,
                                start_col="start_time", end_col="end_time",
                                distance_col="distance",
                                buffer=pd.Timedelta("0D")):
        """
        Select up to n_analogues events with [start, end] intervals.
        We keep an event only if its interval does not overlap
        with any already chosen interval expanded by `buffer` on each side.
        """
    
        df_sorted = df.sort_values(distance_col).reset_index(drop=False)
    
        chosen_idx = []
        chosen_intervals = []  # list of (start, end) tuples
    
        for _, row in df_sorted.iterrows():
            s = row[start_col] - buffer
            e = row[end_col] + buffer
    
            # Check if this interval overlaps any chosen interval
            overlaps = False
            for cs, ce in chosen_intervals:
                # No overlap if e < cs or s > ce
                if not (e < cs or s > ce):
                    overlaps = True
                    break
    
            if not overlaps:
                chosen_idx.append(row["index"])
                chosen_intervals.append((s, e))
    
                if len(chosen_idx) >= n_analogues:
                    break
    
        result = df.loc[chosen_idx].copy()
        result = result.sort_values(distance_col)
        result["rank"] = np.arange(1, len(result) + 1)
    
        return result

    # Past
    past_candidates = all_distances[past_mask]
    past_df = select_time_filtered_analogues(
        past_candidates,
        n_analogues=n_analogues,
        time_col="date_time",          # or whatever your column is called
        distance_col="distance",
        min_separation=pd.Timedelta("5D")  # or whatever your decorrelation time is
    )
    past_df["period"] = "past"
    
    # Present
    present_candidates = all_distances[present_mask]
    present_df = select_time_filtered_analogues(
        present_candidates,
        n_analogues=n_analogues,
        time_col="date_time",
        distance_col="distance",
        min_separation=pd.Timedelta("5D")
    )
    present_df["period"] = "present"
    if verbose:
        print(f"\n--- Top {n_analogues} Past Analogues ---")
        print(past_df[['rank', 'date', 'distance']].to_string(index=False))
        print(f"\n--- Top {n_analogues} Present Analogues ---")
        print(present_df[['rank', 'date', 'distance']].to_string(index=False))
    
    # Close dataset
    ds.close()
    
    return all_distances, past_df, present_df


def process_event(
    event: Dict[str, Any],
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
    
    # Output directory
    output_dir = ensure_dir(paths['analogue'] / event_name)
    
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


def process_all_events(skip_existing: bool = True, verbose: bool = True) -> bool:
    """
    Process analogue search for all events.
    
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
    
    print(f"\nProcessing {len(events)} event(s) for analogue search...")
    
    all_success = True
    for event in events:
        success = process_event(
            event=event,
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
        '--all',
        action='store_true',
        help='Process all events defined in extreme_events.yaml'
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
            
            success = process_event(
                event=event,
                paths=paths,
                analogue_config=analogue_config,
                skip_existing=skip_existing,
                verbose=verbose
            )
        else:
            # Process all events
            success = process_all_events(skip_existing=skip_existing, verbose=verbose)
        
        sys.exit(0 if success else 1)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
