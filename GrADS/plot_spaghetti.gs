* =============================================================================
* plot_spaghetti.gs  (reference / template)
* =============================================================================
* NOTE: The actual spaghetti plotting scripts are generated dynamically by
*       Sh/spaghetti.sh into Work/F03_spaghetti/spaghetti_Z500_*.gs
*       because each plot requires opening many individual monthly ERA5 files
*       (one per member).
*
*       This file documents the GrADS conventions and colour scheme used.
*       It is NOT called directly by the pipeline.
* =============================================================================
*
* Single-panel Z500 spaghetti map:
*   - Full Antarctic domain (lat -90 to -50, lon 0-360) when projection is sps
*   - Original event + top 5 past + top 5 present analogues
*   - Snapshot date only (day 0)
*
* Colour scheme (custom RGB indices 20, 30, 40):
*   Original event:  black (0,0,0)
*   Past analogues:  dark blue (0,0,180)
*   Present analogues: dark red (180,0,0)
*
* Line thickness: Original 6, analogues 4
*
* Data conventions:
*   - ERA5 monthly files: era5_daily_{varname}_{YYYY}_{MM}.nc
*   - Geopotential variable: z (units: m**2 s**-2), plotted as z/9.80665 (gpm)
*   - Level: set lev 500
*   - Time: set time DDmonYYYY (e.g. 08feb2020)
*
* Contour levels: Z500 5000 5400 (gpm)
*
* Projection: Antarctic events use set mproj sps
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
