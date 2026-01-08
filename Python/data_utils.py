"""
Data utilities for Analogue Weather Analysis Pipeline.

Provides:
- Configuration loading from YAML files
- Path management and file existence checking
- Common I/O operations for netCDF data
"""

import os
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Any


def load_yaml(filepath: str) -> Dict[str, Any]:
    """Load a YAML configuration file."""
    with open(filepath, 'r') as f:
        return yaml.safe_load(f)


def get_root_dir() -> Path:
    """Get the root directory of the analogue project."""
    # Assumes this file is in ROOT/Python/
    return Path(__file__).parent.parent


def load_env_setting() -> Dict[str, str]:
    """
    Parse env_setting.sh and return as dictionary.
    Extracts export VAR=value statements.
    """
    env_file = get_root_dir() / "Const" / "env_setting.sh"
    env_vars = {}
    
    with open(env_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith('export ') and '=' in line:
                # Remove 'export ' prefix
                var_def = line[7:]
                # Split on first '='
                if '=' in var_def:
                    key, value = var_def.split('=', 1)
                    # Remove quotes and resolve ${VAR} references
                    value = value.strip('"\'')
                    # Simple variable substitution
                    for existing_key, existing_val in env_vars.items():
                        value = value.replace(f'${{{existing_key}}}', existing_val)
                        value = value.replace(f'${existing_key}', existing_val)
                    env_vars[key] = value
    
    return env_vars


def load_preprocess_config() -> Dict[str, Any]:
    """Load preprocessing configuration."""
    config_file = get_root_dir() / "Const" / "preprocess_config.yaml"
    return load_yaml(str(config_file))


def load_analogue_config() -> Dict[str, Any]:
    """Load analogue search configuration."""
    config_file = get_root_dir() / "Const" / "analogue_config.yaml"
    return load_yaml(str(config_file))


def load_events_config() -> Dict[str, Any]:
    """Load extreme events configuration."""
    config_file = get_root_dir() / "Const" / "extreme_events.yaml"
    return load_yaml(str(config_file))


def get_data_paths(env: Optional[Dict[str, str]] = None) -> Dict[str, Path]:
    """
    Get standardized data paths for the pipeline.
    
    Returns dict with keys:
        - root: Project root
        - data: Data directory
        - climatology: Climatology output directory
        - anomaly: Anomaly output directory
        - yearly: Yearly merged files directory
        - events: Per-event processed data directory
        - analogue: Analogue search output directory
        - mswx: Raw MSWX data directory
    """
    if env is None:
        env = load_env_setting()
    
    root = Path(env.get('ROOT_DIR', get_root_dir()))
    data = root / "Data"
    
    return {
        'root': root,
        'data': data,
        'climatology': data / "F01_preprocess" / "climatology",
        'anomaly': data / "F01_preprocess" / "anomaly",
        'yearly': data / "F01_preprocess" / "yearly",
        'events': data / "F01_preprocess" / "events",
        'analogue': data / "F02_analogue_search",
        'mswx': Path(env.get('MSWX_DIR', '/path/to/mswx')),
    }


def ensure_dir(path: Path) -> Path:
    """Create directory if it doesn't exist."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def file_exists_and_valid(filepath: Path, min_size_bytes: int = 1000) -> bool:
    """
    Check if file exists and has reasonable size.
    Used to determine if processing step can be skipped.
    """
    if not filepath.exists():
        return False
    return filepath.stat().st_size >= min_size_bytes


def get_yearly_file_path(paths: Dict[str, Path], var: str, year: int) -> Path:
    """Get path for yearly merged file."""
    return paths['yearly'] / f"{var}_{year}.nc"


def get_climatology_file_path(paths: Dict[str, Path], var: str) -> Path:
    """Get path for climatology file."""
    return paths['climatology'] / f"climatology_{var}.nc"


def get_anomaly_file_path(paths: Dict[str, Path], var: str, year: int) -> Path:
    """Get path for yearly anomaly file."""
    return paths['anomaly'] / f"anomaly_{var}_{year}.nc"


def get_event_dir(paths: Dict[str, Path], event_name: str) -> Path:
    """Get directory for event-specific processed data."""
    return paths['events'] / event_name


def get_event_bbox_file(paths: Dict[str, Path], event_name: str, var: str, 
                        is_anomaly: bool = True, smoothed: bool = False) -> Path:
    """
    Get path for event bounding box extracted file.
    
    Naming convention:
        - {var}_anomaly_bbox.nc (anomaly, not smoothed)
        - {var}_anomaly_bbox_smooth.nc (anomaly, smoothed)
        - {var}_raw_bbox.nc (raw, not smoothed)
        - {var}_raw_bbox_smooth.nc (raw, smoothed)
    """
    event_dir = get_event_dir(paths, event_name)
    
    if is_anomaly:
        suffix = "anomaly_bbox_smooth.nc" if smoothed else "anomaly_bbox.nc"
    else:
        suffix = "raw_bbox_smooth.nc" if smoothed else "raw_bbox.nc"
    
    return event_dir / f"{var}_{suffix}"


def list_mswx_daily_files(mswx_dir: Path, var_pattern: str, year: int) -> List[Path]:
    """
    List all daily MSWX files for a given variable and year.
    
    Parameters
    ----------
    mswx_dir : Path
        MSWX data directory
    var_pattern : str
        Pattern like "Pres_{date}.nc" where {date} will be matched as YYYYMMDD
    year : int
        Year to list files for
        
    Returns
    -------
    List[Path]
        Sorted list of file paths
    """
    # Extract prefix from pattern (e.g., "Pres_" from "Pres_{date}.nc")
    prefix = var_pattern.split('{date}')[0]
    suffix = var_pattern.split('{date}')[1] if '{date}' in var_pattern else '.nc'
    
    # Find matching files for the year
    pattern = f"{prefix}{year}*{suffix}"
    files = sorted(mswx_dir.glob(pattern))
    
    return files
