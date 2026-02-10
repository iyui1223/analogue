* =============================================================================
* plot_spaghetti.gs  (reference / template)
* =============================================================================
* NOTE: The actual spaghetti plotting scripts are generated dynamically by
*       Sh/F03_spaghetti.sh into Work/F03_spaghetti/spaghetti_*.gs
*       because each plot requires opening many individual monthly ERA5 files
*       (one per member × day offset).
*
*       This file documents the GrADS conventions and colour scheme used.
*       It is NOT called directly by the pipeline.
* =============================================================================
*
* Two-panel Antarctic polar-stereographic spaghetti map concept:
*   Panel: Z500 contours (all members overlaid on one map)
*   Panel: T850 contours (all members overlaid on one map)
*
* Colour scheme (custom RGB indices 20-47):
*   Original event (indices 20-27):
*     Day 0: black (0,0,0)  → Day 7: light gray (220,220,220)
*   Past analogues (indices 30-37):
*     Day 0: dark blue (0,0,180) → Day 7: pale blue (180,210,245)
*   Present analogues (indices 40-47):
*     Day 0: dark red (180,0,0) → Day 7: pale red (245,210,180)
*
* Line thickness:
*   Original: 8 (day 0) tapering to 3 (day 7)
*   Analogues: 5 (day 0) tapering to 2 (day 7)
*
* Data conventions:
*   - ERA5 monthly files: era5_daily_{varname}_{YYYY}_{MM}.nc
*   - Geopotential variable: z (units: m**2 s**-2), plotted as z/9.80665 (gpm)
*   - Temperature variable:  t (units: K)
*   - Level dimension: millibars (set lev 500 for Z500, set lev 850 for T850)
*   - Time: set time DDmonYYYY (e.g. 08feb2020)
*
* Suggested contour levels (keep to 1-2 widely-spaced values):
*   Z500: 5000 5400  (gpm)
*   T850: 255  275   (K)
*
* Projection:
*   Antarctic events: set mproj sps
*   Mid-latitude:     default latlon
* =============================================================================

* --- Example snippet for a single overlay ---
* (This is what the dynamically generated script repeats per member/day)

* 'sdfopen /path/to/era5_daily_geopotential_2020_02.nc'
* 'set dfile 1'
* 'set time 08feb2020'
* 'set lev 500'
* 'set gxout contour'
* 'set clevs 5000 5400'
* 'set ccolor 20'
* 'set cthick 8'
* 'set cstyle 1'
* 'set clab off'
* 'd z/9.80665'
* 'close 1'
