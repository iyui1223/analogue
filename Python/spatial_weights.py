"""
Spatial Weights for Analogue Search.

Provides normalized spatial weights (lat, lon) for distance computation.
Weights combine cos(lat) area weighting with optional Gaussian emphasis
on a user-defined center, using great-circle (haversine) distance in km.

Users configure weights via the event's gaussian_center in extreme_events.yaml:
    gaussian_center:
      lat: -68.0           # Center latitude
      lon_deg_east: 296.0   # Center longitude (0-360)
      sigma_km: 1000.0      # Gaussian width in km
"""

from typing import Optional
import numpy as np
import xarray as xr

EARTH_RADIUS_KM = 6371.0


def great_circle_distance_km(
    lat1_deg: float,
    lon1_deg: float,
    lat2: np.ndarray,
    lon2: np.ndarray
) -> np.ndarray:
    """
    Compute great-circle distance in km from a single point to a grid.

    Uses haversine formula. Grid arrays can be any shape; output matches lat2/lon2.

    Parameters
    ----------
    lat1_deg, lon1_deg : float
        Center point in degrees (lat, lon in 0-360)
    lat2, lon2 : np.ndarray
        Grid coordinates in degrees

    Returns
    -------
    np.ndarray
        Distance in km, same shape as lat2
    """
    lat1 = np.deg2rad(lat1_deg)
    lon1 = np.deg2rad(lon1_deg)
    lat2_rad = np.deg2rad(np.asarray(lat2, dtype=float))
    lon2_rad = np.deg2rad(np.asarray(lon2, dtype=float))

    dlat = lat2_rad - lat1
    dlon = lon2_rad - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2_rad) * np.sin(dlon / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return EARTH_RADIUS_KM * c


def compute_spatial_weights(
    lat: xr.DataArray,
    lon: xr.DataArray,
    gaussian_center_spec: Optional[dict] = None,
    sigma_km: float = 1000.0
) -> xr.DataArray:
    """
    Return spatial weights as xr.DataArray (lat, lon) normalized to sum == 1.

    If gaussian_center_spec is None:
        Returns lat-only cos(lat) weights expanded to 2D.

    If gaussian_center_spec is provided:
        Combines cos(lat) with Gaussian weight based on great-circle distance
        from the specified center. Uses sigma_km for the Gaussian width.

    Parameters
    ----------
    lat, lon : xr.DataArray
        Latitude and longitude coordinates
    gaussian_center_spec : dict, optional
        Keys: 'lat' and 'lon_deg_east' (or 'lon_degwest').
        Optional: 'sigma_km' (overridden by sigma_km argument if both given)
    sigma_km : float
        Gaussian width in km (default 1000). Used for exp(-d^2 / (2 * sigma^2)).

    Returns
    -------
    xr.DataArray
        Weights with dims ('lat', 'lon'), sum == 1
    """
    lat1d = np.asarray(lat.values, dtype=float)
    lon1d = np.asarray(lon.values, dtype=float)

    if gaussian_center_spec is None:
        # Lat-only cos(lat) weights expanded to 2D
        w_lat = np.cos(np.deg2rad(lat1d))
        w_lat = w_lat / w_lat.sum()
        w2d = np.broadcast_to(w_lat[:, np.newaxis], (len(lat1d), len(lon1d)))
        w2d = w2d / w2d.sum()
        return xr.DataArray(
            w2d,
            dims=('lat', 'lon'),
            coords={'lat': ('lat', lat1d), 'lon': ('lon', lon1d)}
        )

    # Parse center from spec
    center_lat = float(gaussian_center_spec['lat'])
    if 'lon_deg_east' in gaussian_center_spec:
        center_lon = float(gaussian_center_spec['lon_deg_east'])
    elif 'lon_degwest' in gaussian_center_spec:
        center_lon = (360.0 + float(gaussian_center_spec['lon_degwest'])) % 360.0
    else:
        raise ValueError("gaussian_center must include 'lon_deg_east' or 'lon_degwest'")

    # Build 2D grid
    lon2d, lat2d = np.meshgrid(lon1d, lat1d)

    # Great-circle distance in km
    dist_km = great_circle_distance_km(center_lat, center_lon, lat2d, lon2d)

    # Gaussian weight: exp(-d^2 / (2 * sigma^2))
    gaussian = np.exp(-(dist_km ** 2) / (2.0 * sigma_km ** 2))

    # Cos(lat) for area weighting
    cos_lat = np.cos(np.deg2rad(lat2d))

    # Combine and normalize
    combined = cos_lat * gaussian
    combined = combined / combined.sum()

    return xr.DataArray(
        combined,
        dims=('lat', 'lon'),
        coords={'lat': ('lat', lat1d), 'lon': ('lon', lon1d)}
    )
