# Analogue Weather Analysis Pipeline

Following ClimaMeter methodology (Faranda et al.) for contextualizing extreme weather.

## Quick Start

```bash
# 1. Configure paths in Const/env_setting.sh (MSWX_DIR, PYTHON_ENV_CMD)
# 2. Define events in Const/extreme_events.yaml
# 3. Run pipeline:
sbatch Sh/F01_preprocess_slurm.sh   # Preprocessing
sbatch Sh/F02_analogue_search_slurm.sh  # Analogue search (after F01)
```

## Pipeline

| Stage | Tool | Description |
|-------|------|-------------|
| **F01** | CDO | Daily→yearly merge, climatology, anomalies, bbox extraction |
| **F02** | xarray | Lat-weighted Euclidean distance, select top N analogues |

## Configuration (`Const/`)

| File | Key Parameters |
|------|----------------|
| `env_setting.sh` | `MSWX_DIR`, `PYTHON_ENV_CMD`, `START_YEAR`, `END_YEAR` |
| `analogue_config.yaml` | `n_analogues`, `past_period`, `present_period`, `smoothing.window_days` |
| `extreme_events.yaml` | Event name, dates, lat/lon bounding box, urban areas |
| `preprocess_config.yaml` | Variable names, MSWX file patterns |

## Methodology

**Variables**: surface pressure, 2m temperature, precipitation, 10m wind speed (MSWX 0.1°)

**Preprocessing**:
- Compute 1979–2022 daily climatology (366 days)
- Anomalies: subtract climatology from pressure & temperature
- Precipitation/wind: no anomaly calculation
- Apply N-day running mean for multi-day events

**Analogue Search**:
- Latitude-weighted Euclidean distance on pressure anomaly
- Split: past (1979–2000) vs present (2001–2022)
- Select top N analogues per period (default: 15)
- Exclude event dates from present period

## Output Structure

```
Data/
├── F01_preprocess/
│   ├── yearly/          # {var}_{year}.nc
│   ├── climatology/     # climatology_{var}.nc
│   ├── anomaly/         # anomaly_{var}_{year}.nc
│   └── events/{name}/   # {var}_anomaly_bbox_smooth.nc
└── F02_analogue_search/{name}/
    ├── all_distances.csv
    ├── past_analogues.csv
    ├── present_analogues.csv
    └── analogues.csv
```

## Not Yet Implemented

- F03: Composite differences (past vs present)
- F04: Visualization (GrADS + HTML viewer)
- Natural variability analysis (ENSO, AMO, PDO)
