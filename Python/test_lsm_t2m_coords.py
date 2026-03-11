#!/usr/bin/env python3
"""
Standalone diagnostic: LSM and T2m coordinate conventions and visualization.

Examines whether the ERA5 land-sea mask uses -180–180 or 0–360 longitude,
and visualizes both LSM and a sample t2m over the boxplot_region domain.
Run once, inspect outputs, then delete. Does not affect other code.

Usage:
  python3 test_lsm_t2m_coords.py [--t2m YYYYMM.nc] [--outdir .]

Output:
  - Prints lon/lat ranges of LSM and t2m
  - Saves LSM and t2m plots to --outdir
"""
import argparse
import os

import numpy as np
import xarray as xr
import matplotlib.pyplot as plt

LSM_PATH = "/lustre/soge1/data/analysis/era5/0.28125x0.28125/invariant/land-sea_mask/nc/era5_invariant_land-sea_mask_20000101.nc"
DATA_SLICE_DIR = "Data/data_slice/Tsurf_max_Antarctic-Peninsula"
# boxplot_region: antarctica_peninsula (0–360 lon)
LAT_MIN, LAT_MAX = -70.0, -60.0
LON_MIN_0360, LON_MAX_0360 = 280.0, 310.0  # 80W–50W
LAND_THRESHOLD = 0.5


def _lon_range(coord, name):
    vals = np.asarray(coord)
    lo, hi = float(vals.min()), float(vals.max())
    print(f"  {name}: lon min={lo:.4f} max={hi:.4f}  → {'0-360' if lo >= 0 else '-180-180'}")


def _lat_range(coord, name):
    vals = np.asarray(coord)
    lo, hi = float(vals.min()), float(vals.max())
    print(f"  {name}: lat min={lo:.4f} max={hi:.4f}")


def main():
    ap = argparse.ArgumentParser(description="LSM and T2m coordinate diagnostic")
    ap.add_argument("--t2m", default="200202", help="YYYYMM for t2m sample (default 200202)")
    ap.add_argument("--outdir", default=".", help="Output directory for plots")
    args = ap.parse_args()

    t2m_path = os.path.join(DATA_SLICE_DIR, f"{args.t2m}.nc")
    if not os.path.isfile(t2m_path):
        t2m_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", t2m_path
        )
    if not os.path.isfile(t2m_path):
        t2m_path = os.path.join(
            os.environ.get("ROOT_DIR", "/lustre/soge1/projects/andante/cenv1201/proj/analogue"),
            DATA_SLICE_DIR,
            f"{args.t2m}.nc",
        )
    os.makedirs(args.outdir, exist_ok=True)

    print("=" * 60)
    print("LSM and T2m coordinate diagnostic")
    print("=" * 60)

    # --- LSM ---
    if not os.path.isfile(LSM_PATH):
        print(f"LSM not found: {LSM_PATH}")
        return 1
    ds_lsm = xr.open_dataset(LSM_PATH)
    lsm = ds_lsm["lsm"].isel(time=0)
    if str(lsm.dtype) in ("int16", "int32"):
        sf = float(getattr(ds_lsm["lsm"], "scale_factor", 1.0))
        ao = float(getattr(ds_lsm["lsm"], "add_offset", 0.0))
        lsm = lsm.astype(np.float64) * sf + ao

    print("\n1. LSM coordinates:")
    _lon_range(lsm.coords["longitude"], "LSM")
    _lat_range(lsm.coords["latitude"], "LSM")

    # --- T2m ---
    if not os.path.isfile(t2m_path):
        print(f"T2m not found: {t2m_path}")
        ds_lsm.close()
        return 1
    ds_t2m = xr.open_dataset(t2m_path)
    t2m = ds_t2m["t2m"]

    print("\n2. T2m (data_slice) coordinates:")
    _lon_range(t2m.coords["longitude"], "T2m")
    _lat_range(t2m.coords["latitude"], "T2m")

    # --- LSM subset logic (same as load_land_mask) ---
    lon = lsm.coords["longitude"]
    lat_coord = lsm.coords["latitude"]
    lon_0_360 = float(lon.min()) >= 0
    lat_desc = float(lat_coord[0]) > float(lat_coord[-1])
    if lon_0_360:
        lon_lo, lon_hi = LON_MIN_0360, LON_MAX_0360
    else:
        lon_lo = LON_MIN_0360 - 360 if LON_MIN_0360 > 180 else LON_MIN_0360
        lon_hi = LON_MAX_0360 - 360 if LON_MAX_0360 > 180 else LON_MAX_0360
    # latitude: use slice(lat_max, lat_min) when descending (90 -> -90)
    lat_slice = slice(LAT_MAX, LAT_MIN) if lat_desc else slice(LAT_MIN, LAT_MAX)

    lsm_sub = lsm.sel(
        latitude=lat_slice,
        longitude=slice(lon_lo, lon_hi),
    )
    n_lsm = lsm_sub.sizes.get("latitude", 0) * lsm_sub.sizes.get("longitude", 0)
    land_vals = lsm_sub.values
    n_land = np.sum(land_vals >= LAND_THRESHOLD)
    n_total = land_vals.size

    print(f"\n3. LSM subset to bbox (lat {LAT_MIN}–{LAT_MAX}, lon {lon_lo:.1f}–{lon_hi:.1f} in LSM conv, lat_desc={lat_desc}):")
    print(f"   Grid points: {n_total}")
    print(f"   Land (>= {LAND_THRESHOLD}): {n_land} ({100*n_land/max(1,n_total):.1f}%)")
    if n_land == 0:
        print("   *** No land points! Check coordinate mismatch or LSM values. ***")

    # --- T2m subset logic (same as get_t2m_domain_mean_series) ---
    data_lon = t2m.coords["longitude"]
    if float(data_lon.min()) < 0:
        sel_lo = LON_MIN_0360 - 360 if LON_MIN_0360 > 180 else LON_MIN_0360
        sel_hi = LON_MAX_0360 - 360 if LON_MAX_0360 > 180 else LON_MAX_0360
    else:
        sel_lo, sel_hi = LON_MIN_0360, LON_MAX_0360
    lat_slice = slice(LAT_MAX, LAT_MIN) if LAT_MAX > LAT_MIN else slice(LAT_MIN, LAT_MAX)
    t2m_sub = t2m.sel(
        latitude=lat_slice,
        longitude=slice(sel_lo, sel_hi),
    )
    n_t2m = t2m_sub.sizes.get("latitude", 0) * t2m_sub.sizes.get("longitude", 0)
    print(f"\n4. T2m subset to bbox (lon {sel_lo:.1f}–{sel_hi:.1f} in data conv):")
    print(f"   Grid points: {n_t2m}")

    # --- Plots ---
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    # LSM
    ax = axes[0]
    x = np.asarray(lsm_sub.coords["longitude"])
    y = np.asarray(lsm_sub.coords["latitude"])
    v = np.squeeze(lsm_sub.values)
    im = ax.pcolormesh(x, y, v, cmap="YlGn", vmin=0, vmax=1)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(f"LSM (bbox) lon={lon_lo:.0f}–{lon_hi:.0f}, land={n_land}/{n_total}")
    plt.colorbar(im, ax=ax, label="LSM fraction")
    ax.grid(True, alpha=0.3)

    # T2m (first time step, convert to °C)
    ax = axes[1]
    x = np.asarray(t2m_sub.coords["longitude"])
    y = np.asarray(t2m_sub.coords["latitude"])
    time_dim = "time" if "time" in t2m_sub.dims else "valid_time"
    v = np.squeeze(t2m_sub.isel({time_dim: 0}).values) - 273.15
    im = ax.pcolormesh(x, y, v, cmap="RdYlBu_r")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(f"T2m sample (bbox) lon={sel_lo:.0f}–{sel_hi:.0f}, °C")
    plt.colorbar(im, ax=ax, label="T2m (°C)")
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    out_png = os.path.join(args.outdir, "test_lsm_t2m_coords.png")
    plt.savefig(out_png, dpi=120)
    plt.close()
    print(f"\n5. Plot saved: {out_png}")

    ds_lsm.close()
    ds_t2m.close()

    print("=" * 60)
    print("Summary: LSM uses 0-360 lon. LSM lat is DESCENDING (90->-90);")
    print("        use slice(lat_max, lat_min) for bbox. data_slice uses -180-180 lon.")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
