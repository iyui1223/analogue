"""
Preprocessing module for Analogue Weather Analysis Pipeline.

This script handles:
1. Event bounding box extraction from processed yearly/anomaly files
2. Time-window smoothing (running mean) for multi-day events
3. Coordination with CDO for heavy lifting

CDO handles: daily->yearly merging, climatology, anomaly calculation
This script handles: event-specific bbox extraction and smoothing
"""

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any

from data_utils import (
    load_env_setting,
    load_preprocess_config,
    load_analogue_config,
    load_events_config,
    get_data_paths,
    ensure_dir,
    file_exists_and_valid,
    get_yearly_file_path,
    get_anomaly_file_path,
    get_event_dir,
    get_event_bbox_file,
)


def run_cdo(cmd: str, verbose: bool = True) -> int:
    """
    Run a CDO command.
    
    Parameters
    ----------
    cmd : str
        CDO command string
    verbose : bool
        Print command before execution
        
    Returns
    -------
    int
        Return code (0 = success)
    """
    if verbose:
        print(f"[CDO] {cmd}")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"[CDO ERROR] {result.stderr}", file=sys.stderr)
    elif verbose and result.stdout:
        print(result.stdout)
    
    return result.returncode


def extract_event_bbox(
    event: Dict[str, Any],
    paths: Dict[str, Path],
    preprocess_config: Dict[str, Any],
    analogue_config: Dict[str, Any],
    skip_existing: bool = True,
    verbose: bool = True
) -> bool:
    """
    Extract bounding box data for a single event.
    
    For each variable:
    1. Concatenate yearly files (anomaly or raw) within event year range
    2. Extract spatial bounding box using CDO sellonlatbox
    3. Apply time-window smoothing using CDO runmean
    
    Parameters
    ----------
    event : dict
        Event configuration from extreme_events.yaml
    paths : dict
        Data paths from get_data_paths()
    preprocess_config : dict
        Preprocessing configuration
    analogue_config : dict
        Analogue search configuration
    skip_existing : bool
        Skip if output files already exist
    verbose : bool
        Print progress messages
        
    Returns
    -------
    bool
        True if successful
    """
    event_name = event['name']
    region = event['region']
    
    # Create event output directory
    event_dir = ensure_dir(get_event_dir(paths, event_name))
    
    # Get bounding box parameters
    lon_min = region['lon_min']
    lon_max = region['lon_max']
    lat_min = region['lat_min']
    lat_max = region['lat_max']
    
    # Get smoothing window
    smooth_days = analogue_config.get('smoothing', {}).get('window_days', 1)
    
    # Get year range (full climatology period for analogue search)
    clim_config = preprocess_config.get('climatology', {})
    start_year = clim_config.get('start_year', 1979)
    end_year = clim_config.get('end_year', 2022)
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"Processing event: {event_name}")
        print(f"Region: lat[{lat_min}, {lat_max}], lon[{lon_min}, {lon_max}]")
        print(f"Years: {start_year}-{end_year}")
        print(f"Smoothing: {smooth_days}-day running mean")
        print(f"{'='*60}")
    
    # Get variable lists
    anomaly_vars = [v['name'] for v in preprocess_config['variables'].get('anomaly_vars', [])]
    raw_vars = [v['name'] for v in preprocess_config['variables'].get('raw_vars', [])]
    
    success = True
    
    # Process anomaly variables
    for var in anomaly_vars:
        if verbose:
            print(f"\n[{var}] Processing anomaly variable...")
        
        output_bbox = get_event_bbox_file(paths, event_name, var, is_anomaly=True, smoothed=False)
        output_smooth = get_event_bbox_file(paths, event_name, var, is_anomaly=True, smoothed=True)
        
        # Check if can skip
        if skip_existing and file_exists_and_valid(output_smooth):
            if verbose:
                print(f"[{var}] Skipping - output exists: {output_smooth}")
            continue
        
        # Build list of yearly anomaly files
        yearly_files = []
        for year in range(start_year, end_year + 1):
            yf = get_anomaly_file_path(paths, var, year)
            if yf.exists():
                yearly_files.append(str(yf))
            else:
                print(f"[{var}] Warning: Missing anomaly file for {year}: {yf}")
        
        if not yearly_files:
            print(f"[{var}] Error: No anomaly files found!")
            success = False
            continue
        
        # Step 1: Concatenate and extract bbox
        input_files = ' '.join(yearly_files)
        cmd_bbox = f"cdo -sellonlatbox,{lon_min},{lon_max},{lat_min},{lat_max} -cat '{input_files}' {output_bbox}"
        
        if run_cdo(cmd_bbox, verbose) != 0:
            success = False
            continue
        
        # Step 2: Apply smoothing
        if smooth_days > 1:
            cmd_smooth = f"cdo runmean,{smooth_days} {output_bbox} {output_smooth}"
            if run_cdo(cmd_smooth, verbose) != 0:
                success = False
                continue
        else:
            # No smoothing needed, just copy/link
            cmd_copy = f"cp {output_bbox} {output_smooth}"
            subprocess.run(cmd_copy, shell=True)
        
        if verbose:
            print(f"[{var}] Done: {output_smooth}")
    
    # Process raw variables (no anomaly, just yearly merged files)
    for var in raw_vars:
        if verbose:
            print(f"\n[{var}] Processing raw variable...")
        
        output_bbox = get_event_bbox_file(paths, event_name, var, is_anomaly=False, smoothed=False)
        output_smooth = get_event_bbox_file(paths, event_name, var, is_anomaly=False, smoothed=True)
        
        # Check if can skip
        if skip_existing and file_exists_and_valid(output_smooth):
            if verbose:
                print(f"[{var}] Skipping - output exists: {output_smooth}")
            continue
        
        # Build list of yearly files (raw, not anomaly)
        yearly_files = []
        for year in range(start_year, end_year + 1):
            yf = get_yearly_file_path(paths, var, year)
            if yf.exists():
                yearly_files.append(str(yf))
            else:
                print(f"[{var}] Warning: Missing yearly file for {year}: {yf}")
        
        if not yearly_files:
            print(f"[{var}] Error: No yearly files found!")
            success = False
            continue
        
        # Step 1: Concatenate and extract bbox
        input_files = ' '.join(yearly_files)
        cmd_bbox = f"cdo -sellonlatbox,{lon_min},{lon_max},{lat_min},{lat_max} -cat '{input_files}' {output_bbox}"
        
        if run_cdo(cmd_bbox, verbose) != 0:
            success = False
            continue
        
        # Step 2: Apply smoothing
        if smooth_days > 1:
            cmd_smooth = f"cdo runmean,{smooth_days} {output_bbox} {output_smooth}"
            if run_cdo(cmd_smooth, verbose) != 0:
                success = False
                continue
        else:
            cmd_copy = f"cp {output_bbox} {output_smooth}"
            subprocess.run(cmd_copy, shell=True)
        
        if verbose:
            print(f"[{var}] Done: {output_smooth}")
    
    return success


def process_all_events(skip_existing: bool = True, verbose: bool = True) -> bool:
    """
    Process bbox extraction for all events defined in extreme_events.yaml.
    
    Returns
    -------
    bool
        True if all events processed successfully
    """
    # Load configurations
    env = load_env_setting()
    paths = get_data_paths(env)
    preprocess_config = load_preprocess_config()
    analogue_config = load_analogue_config()
    events_config = load_events_config()
    
    events = events_config.get('events', [])
    
    if not events:
        print("No events defined in extreme_events.yaml")
        return False
    
    print(f"\nProcessing {len(events)} event(s)...")
    
    all_success = True
    for event in events:
        success = extract_event_bbox(
            event=event,
            paths=paths,
            preprocess_config=preprocess_config,
            analogue_config=analogue_config,
            skip_existing=skip_existing,
            verbose=verbose
        )
        if not success:
            all_success = False
    
    return all_success


def main():
    """Main entry point for preprocessing script."""
    parser = argparse.ArgumentParser(
        description='Preprocess data for analogue weather analysis'
    )
    parser.add_argument(
        '--extract-bbox',
        action='store_true',
        help='Extract bounding box data for all events'
    )
    parser.add_argument(
        '--event',
        type=str,
        default=None,
        help='Process specific event by name (default: all events)'
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
    
    if args.extract_bbox:
        if args.event:
            # Process single event
            env = load_env_setting()
            paths = get_data_paths(env)
            preprocess_config = load_preprocess_config()
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
            
            success = extract_event_bbox(
                event=event,
                paths=paths,
                preprocess_config=preprocess_config,
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
