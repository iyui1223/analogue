# (C) Copyright 2017- ECMWF.
# Vertical cross section for Antarctic Peninsula event 2020-02-08 19 UTC.
# Plots: T (shaded −40…18 °C, cold/warm spacing), cloud, EPT, wind, precipitation (bar chart).
# Outputs: PNG (quick check), PDF, EPS (publication).
# Requires Metview >= 5.16.0 and ~/.cdsapirc for CDS API.

import os
import metview as mv
import numpy as np

# ---- Event-specific configuration (2020 Feb 08) ----
date_str = "2020-02-08"
time_str = "19:00"  # UTC
area = [-55, -135, -85, -35]  # N, W, S, E
line = [-63.8, -66.5, -68.1, -56.8]  # cross section: [lat1, lon1, lat2, lon2]

# ---- Common configuration ----
use_cds = True
top_level = 10000  # m
bottom_level = 0
level_count = 101
w_scale_factor = 100
cloud_hatch_threshold = 0.2
use_vertical_velocity = True
# Feb events: smaller ref. velocity & precip scale than March 2015 cross section
wind_arrow_unit_velocity = 30
wind_legend_lead_spaces = "          "

pressure_levels = [
    "100", "150", "200", "250", "300", "400", "500",
    "600", "700", "850", "925", "1000"
]

# Output paths: event-specific GRIB in Work, figures in Figs/cross_section/
ROOT_DIR = os.environ.get("ROOT_DIR", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
FIGS_DIR = os.environ.get("FIGS_DIR", os.path.join(ROOT_DIR, "Figs"))
WORK_DIR = os.environ.get("WORK_DIR", os.path.join(ROOT_DIR, "Work"))
EVENT_ID = "antarctica_peninsula_2020"

work_subdir = os.path.join(WORK_DIR, f"cross_section_{EVENT_ID}")
out_dir = os.path.join(FIGS_DIR, "cross_section", EVENT_ID)
os.makedirs(work_subdir, exist_ok=True)
os.makedirs(out_dir, exist_ok=True)

filename_pl = os.path.join(work_subdir, "era5_pl_cross_section.grib")
filename_sfc = os.path.join(work_subdir, "era5_sfc_geopotential_precip.grib")
output_base = "cross_section_wind3d_height_era5"

# ---- Retrieve or load data ----
if use_cds:
    import cdsapi

    c = cdsapi.Client()
    variables_pl = [
        "temperature", "u_component_of_wind", "v_component_of_wind",
        "geopotential", "specific_humidity", "fraction_of_cloud_cover",
    ]
    if use_vertical_velocity:
        variables_pl.append("vertical_velocity")

    try:
        c.retrieve(
            "reanalysis-era5-pressure-levels",
            {
                "product_type": "reanalysis",
                "format": "grib",
                "variable": variables_pl,
                "pressure_level": pressure_levels,
                "year": date_str[:4],
                "month": date_str[5:7],
                "day": date_str[8:10],
                "time": time_str,
                "area": area,
            },
            filename_pl,
        )
    except Exception as e:
        if use_vertical_velocity and "vertical_velocity" in str(e).lower():
            use_vertical_velocity = False
            variables_pl = [v for v in variables_pl if v != "vertical_velocity"]
            c.retrieve(
                "reanalysis-era5-pressure-levels",
                {
                    "product_type": "reanalysis",
                    "format": "grib",
                    "variable": variables_pl,
                    "pressure_level": pressure_levels,
                    "year": date_str[:4],
                    "month": date_str[5:7],
                    "day": date_str[8:10],
                    "time": time_str,
                    "area": area,
                },
                filename_pl,
            )
        else:
            raise

    c.retrieve(
        "reanalysis-era5-single-levels",
        {
            "product_type": "reanalysis",
            "format": "grib",
            "variable": ["geopotential", "total_precipitation"],
            "year": date_str[:4],
            "month": date_str[5:7],
            "day": date_str[8:10],
            "time": time_str,
            "area": area,
        },
        filename_sfc,
    )

    fs_pl = mv.read(filename_pl)
    fs_sfc = mv.read(filename_sfc)
    zs = mv.read(data=fs_sfc, param="z")
else:
    if not mv.exist(filename_pl):
        raise FileNotFoundError(f"Need {filename_pl}. Run with use_cds=True first.")
    if not mv.exist(filename_sfc):
        raise FileNotFoundError(f"Need {filename_sfc}. Run with use_cds=True first.")
    fs_pl = mv.read(filename_pl)
    fs_sfc = mv.read(filename_sfc)
    zs = mv.read(data=fs_sfc, param="z")

# ---- Extract and process data ----
t = mv.read(data=fs_pl, param="t")
q = mv.read(data=fs_pl, param="q")
u = mv.read(data=fs_pl, param="u")
v = mv.read(data=fs_pl, param="v")
z = mv.read(data=fs_pl, param="z")

has_w = False
if use_vertical_velocity:
    omega = mv.read(data=fs_pl, param="w")
    if omega is not None and len(omega) > 0:
        w = mv.w_from_omega(omega, t)
        has_w = True

t_celsius = t - 273.15
ept = mv.eqpott_p(temperature=t, humidity=q)
h = mv.geometric_height_from_geopotential(z)
hs = mv.geometric_height_from_geopotential(zs)

# ---- Cross sections ----
xs_t = mv.mcross_sect(
    data=mv.merge(t_celsius, h),
    line=line,
    level_selection_type="count",
    level_count=level_count,
    vertical_coordinates="user",
    vertical_coordinate_param=3008,
    vertical_coordinate_extrapolate="on",
    vertical_coordinate_extrapolate_mode="constant",
    vertical_coordinate_extrapolate_fixed_sign="on",
    top_level=top_level,
    bottom_level=bottom_level,
)

xs_ept = mv.mcross_sect(
    data=mv.merge(ept, h),
    line=line,
    level_selection_type="count",
    level_count=level_count,
    vertical_coordinates="user",
    vertical_coordinate_param=3008,
    vertical_coordinate_extrapolate="on",
    vertical_coordinate_extrapolate_mode="constant",
    top_level=top_level,
    bottom_level=bottom_level,
)

if has_w:
    xs_wind = mv.mcross_sect(
        data=mv.merge(u, v, w, h),
        line=line,
        wind_parallel="on",
        w_wind_scaling_factor_mode="user",
        w_wind_scaling_factor=w_scale_factor,
        level_selection_type="count",
        level_count=level_count,
        vertical_coordinates="user",
        vertical_coordinate_param=3008,
        vertical_coordinate_extrapolate="on",
        vertical_coordinate_extrapolate_mode="constant",
        top_level=top_level,
        bottom_level=bottom_level,
    )
else:
    xs_wind = mv.mcross_sect(
        data=mv.merge(u, v, h),
        line=line,
        wind_parallel="on",
        level_selection_type="count",
        level_count=level_count,
        vertical_coordinates="user",
        vertical_coordinate_param=3008,
        vertical_coordinate_extrapolate="on",
        vertical_coordinate_extrapolate_mode="constant",
        top_level=top_level,
        bottom_level=bottom_level,
    )

orog_curve = mv.xs_build_orog(xs_t, hs, bottom_level, "charcoal")

has_cloud = False
cc = mv.read(data=fs_pl, param="cc")
if cc is not None and len(cc) > 0:
    xs_cloud = mv.mcross_sect(
        data=mv.merge(cc, h),
        line=line,
        level_selection_type="count",
        level_count=level_count,
        vertical_coordinates="user",
        vertical_coordinate_param=3008,
        vertical_coordinate_extrapolate="on",
        vertical_coordinate_extrapolate_mode="constant",
        vertical_coordinate_extrapolate_fixed_sign="on",
        top_level=top_level,
        bottom_level=bottom_level,
    )
    has_cloud = True


def _interp_rgb_stops(rgb_stops, n):
    """Piecewise linear RGB (0–1) along stops; n output colours. Pure numpy (no matplotlib)."""
    arr = np.asarray(rgb_stops, dtype=float)
    if n < 1:
        raise ValueError("n must be >= 1")
    if arr.ndim != 2 or arr.shape[1] != 3:
        raise ValueError("rgb_stops must be shape (k, 3)")
    if arr.shape[0] == 1:
        return np.repeat(arr, n, axis=0)
    if n == 1:
        return arr[[0]]
    u = np.linspace(0.0, 1.0, n)
    m = arr.shape[0] - 1
    x = u * m
    i = np.floor(x).astype(int)
    i = np.clip(i, 0, m - 1)
    f = (x - i)[:, np.newaxis]
    return (1.0 - f) * arr[i] + f * arr[i + 1]


def _temperature_level_list(t_min, t_max, cold_interval, warm_interval):
    """
    Merged contour levels: coarse steps for cold (e.g. 4 °C), finer for warm (e.g. 2 °C).
    Cold covers [t_min, 0]; warm covers [0, t_max]; 0 appears once.
    """
    cold_edges = np.arange(t_min, 0.0 + cold_interval, cold_interval, dtype=float)
    if cold_edges[-1] > 0:
        cold_edges = cold_edges[cold_edges <= 0]
    if cold_edges[-1] < 0:
        cold_edges = np.append(cold_edges, 0.0)
    warm_edges = np.arange(0.0, t_max + warm_interval, warm_interval, dtype=float)
    warm_edges = warm_edges[warm_edges <= t_max + 1e-9]
    if warm_edges[-1] < t_max - 1e-9:
        warm_edges = np.append(warm_edges, float(t_max))
    return np.unique(np.concatenate([cold_edges, warm_edges]))


def _temperature_colours_for_metview(
    t_min=-40.0,
    t_max=18.0,
    cold_interval=4.0,
    warm_interval=2.0,
):
    """rgb(r,g,b) strings for each shaded band between consecutive levels."""
    levels = _temperature_level_list(t_min, t_max, cold_interval, warm_interval)
    cold_n = 0
    warm_n = 0
    for i in range(len(levels) - 1):
        lo, hi = float(levels[i]), float(levels[i + 1])
        if hi <= 0.0:
            cold_n += 1
        elif lo >= 0.0:
            warm_n += 1
        else:
            cold_n += 1

    cold_nodes = [
        (0.32, 0.32, 0.32),
        (0.40, 0.39, 0.42),
        (0.48, 0.38, 0.52),
        (0.52, 0.12, 0.72),
        (0.38, 0.05, 0.88),
        (0.12, 0.32, 1.00),
        (0.42, 0.72, 1.00),
        (0.62, 0.90, 1.00),
    ]
    cold_rgba = _interp_rgb_stops(cold_nodes, max(cold_n, 1))

    warm_nodes = [
        (1.0, 1.0, 0.75),
        (1.0, 0.93, 0.45),
        (1.0, 0.78, 0.20),
        (1.0, 0.52, 0.06),
        (0.96, 0.32, 0.05),
        (0.84, 0.15, 0.08),
        (0.65, 0.02, 0.12),
        (0.48, 0.00, 0.13),  # tail: 12–18 °C bins
    ]
    warm_rgba = _interp_rgb_stops(warm_nodes, max(warm_n, 1))

    def _mv_rgb(rgb):
        r, g, b = float(rgb[0]), float(rgb[1]), float(rgb[2])
        return f"rgb({r:.5f},{g:.5f},{b:.5f})"

    colours = [_mv_rgb(c) for c in cold_rgba] + [_mv_rgb(c) for c in warm_rgba]
    return levels, colours


# ---- Plotting styles ----
t_min_plot = -40.0
t_max_plot = 18.0
t_cold_interval = 4.0
t_warm_interval = 2.0
_t_levels, _t_shade_colours = _temperature_colours_for_metview(
    t_min_plot, t_max_plot, t_cold_interval, t_warm_interval
)

t_cont = mv.mcont(
    legend="on",
    contour="off",
    contour_level_selection_type="level_list",
    contour_level_list=_t_levels.tolist(),
    contour_max_level=t_max_plot,
    contour_min_level=t_min_plot,
    contour_label="off",
    contour_shade="on",
    contour_shade_colour_method="list",
    contour_shade_method="area_fill",
    contour_shade_colour_list=_t_shade_colours,
    contour_shade_colour_list_policy="dynamic",
    contour_shade_min_level=t_min_plot,
    contour_shade_max_level=t_max_plot,
)

cloud_cont = mv.mcont(
    legend="off",
    contour_legend_text="",
    contour="off",
    contour_level_selection_type="level_list",
    contour_level_list=[cloud_hatch_threshold, 1.0],
    contour_shade_min_level=cloud_hatch_threshold,
    contour_shade_max_level=1.0,
    contour_shade="on",
    contour_shade_method="hatch",
    contour_shade_hatch_thickness=1,
    contour_shade_hatch_density=10,
    contour_shade_hatch_index=1,
    contour_shade_colour_method="list",
    contour_shade_colour_list=["white"],
)

ept_cont = mv.mcont(
    legend="off",
    contour_legend_text="",
    contour_line_style="dash",
    contour_line_colour="brown",
    contour_highlight_colour="brown",
    contour_highlight_thickness=2,
    contour_level_selection_type="interval",
    contour_interval=2,
    contour_label="on",
    contour_label_height=0.5,
)

wind_style = mv.mwind(
    wind_thinning_factor=4,
    wind_arrow_colour="navy",
    wind_arrow_unit_velocity=wind_arrow_unit_velocity,
    legend="on",
    wind_legend_text=f"{wind_legend_lead_spaces}{wind_arrow_unit_velocity:g} m/s",
    wind_arrow_legend_text="",
)

vertical_axis = mv.maxis(
    axis_orientation="vertical",
    axis_title_text="Height ASL (m)",
    axis_tick_label_height=0.5,
    axis_title_height=0.5,
    axis_title_position=100,
)

horizontal_axis = mv.maxis(
    axis_orientation="horizontal",
    axis_tick_label_height=0.5,
    axis_title_height=0.5,
)

xs_view = mv.mxsectview(
    line=line,
    top_level=top_level,
    bottom_level=bottom_level,
    vertical_axis=vertical_axis,
    horizontal_axis=horizontal_axis,
    subpage_x_position=10,
    subpage_y_position=8,
    subpage_x_length=78,
    subpage_y_length=82,
)

legend = mv.mlegend(legend_text_font_size=0.5)

vdate = mv.valid_date(t[0])
wind_label = f"3D Wind (w×{w_scale_factor})" if has_w else "2D Wind"
title = mv.mtext(
    text_lines=[
        f"ERA5 T (°C), EPT and {wind_label}",
        vdate.strftime("%Y-%m-%d %H UTC"),
    ],
    text_font_size=0.5,
)

# ---- Precipitation ----
precip_ymax = 1.2
has_precip = False
precip_vals = []
dist_vals = []
try:
    tp = mv.read(data=fs_sfc, param="tp")
    if tp is not None and len(tp) > 0:
        tp_field = tp[0]
        lat1, lon1, lat2, lon2 = line
        n_pts = 64
        lats = np.linspace(lat1, lat2, n_pts)
        lons = np.linspace(lon1, lon2, n_pts)
        precip_m = mv.nearest_gridpoint(tp_field, lats, lons)
        if precip_m is not None:
            precip_m = np.atleast_1d(precip_m)
            precip_mm = np.clip(np.maximum(0, precip_m) * 1000, 0, precip_ymax)
            precip_vals = (-precip_mm).tolist()
            dist_vals = np.linspace(0, 1, n_pts).tolist()
            has_precip = True
except Exception:
    pass

# ---- Layout ----
page_xs = mv.plot_page(top=0, bottom=72, left=0, right=100, view=xs_view)

h_axis_precip = mv.maxis(
    axis_orientation="horizontal",
    axis_tick_label_height=0.5,
    axis_title_height=0.5,
)
v_axis_precip = mv.maxis(
    axis_orientation="vertical",
    axis_title_text="Precip (mm)",
    axis_tick_label_height=0.5,
    axis_title_height=0.5,
    axis_type="position_list",
    axis_tick_position_list=[0, -0.2, -0.4, -0.6, -0.8, -1.0, -1.2],
    axis_tick_label_type="label_list",
    axis_tick_label_list=["0", "0.2", "0.4", "0.6", "0.8", "1.0", "1.2"],
)
curve_view = mv.cartesianview(
    x_automatic="on",
    y_automatic="off",
    y_min=-precip_ymax,
    y_max=0,
    horizontal_axis=h_axis_precip,
    vertical_axis=v_axis_precip,
    subpage_x_position=10,
    subpage_y_position=10,
    subpage_x_length=80,
    subpage_y_length=78,
)
page_precip = mv.plot_page(top=72, bottom=100, left=0, right=100, view=curve_view)

dw = mv.plot_superpage(
    pages=[page_precip, page_xs],
    layout_size="custom",
    custom_width=15,
    custom_height=12,
)

# ---- Build plot definition ----
if has_precip:
    precip_vis = mv.input_visualiser(
        input_plot_type="xy_points",
        input_x_values=dist_vals,
        input_y_values=precip_vals,
    )
    precip_graph = mv.mgraph(
        graph_type="bar",
        graph_bar_colour="steelblue",
        graph_bar_width=0.012,
        graph_bar_justification="centre",
    )
    plot_def = [
        dw[0],
        precip_vis,
        precip_graph,
        mv.mtext(text_lines="", text_font_size=0.5),
        dw[1],
        xs_view,
        xs_t,
        t_cont,
        *([xs_cloud, cloud_cont] if has_cloud else []),
        xs_ept,
        ept_cont,
        xs_wind,
        wind_style,
        orog_curve,
        legend,
        title,
    ]
else:
    plot_def = [
        xs_view,
        xs_t,
        t_cont,
        *([xs_cloud, cloud_cont] if has_cloud else []),
        xs_ept,
        ept_cont,
        xs_wind,
        wind_style,
        orog_curve,
        legend,
        title,
    ]

# ---- Output: PNG (quick check), PDF, EPS (publication) ----
output_path_base = os.path.join(out_dir, output_base)
for fmt, out_driver in [
    ("png", mv.png_output(output_name=output_path_base)),
    ("pdf", mv.pdf_output(output_name=output_path_base)),
    ("eps", mv.eps_output(output_name=output_path_base)),
]:
    mv.setoutput(out_driver)
    mv.plot(*plot_def)
    print(f"Wrote {output_path_base}.{fmt}")
