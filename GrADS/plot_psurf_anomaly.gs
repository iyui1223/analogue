*
* plot_psurf_anomaly.gs
* Plot surface pressure (contour) and anomaly (shaded BWR) for an event
*
* Usage:
*   grads -blc "run plot_psurf_anomaly.gs YEARLY_FILE ANOM_FILE DATE LON1 LON2 LAT1 LAT2 OUTPUT_FILE TITLE"
*
* Arguments:
*   YEARLY_FILE  - Path to yearly pressure file (e.g., psurf_2020.nc)
*   ANOM_FILE    - Path to anomaly file (e.g., anomaly_psurf_2020.nc)
*   DATE         - Date string for title (e.g., 2020-02-08)
*   LON1, LON2   - Longitude range (0-360)
*   LAT1, LAT2   - Latitude range
*   OUTPUT_FILE  - Output PNG file path
*   TITLE        - Plot title
*

function main(args)

* Parse arguments
yearly_file = subwrd(args, 1)
anom_file   = subwrd(args, 2)
date_str    = subwrd(args, 3)
lon1        = subwrd(args, 4)
lon2        = subwrd(args, 5)
lat1        = subwrd(args, 6)
lat2        = subwrd(args, 7)
output_file = subwrd(args, 8)
title       = subwrd(args, 9)

* Read remaining title words
i = 10
while (subwrd(args, i) != '')
  title = title % ' ' % subwrd(args, i)
  i = i + 1
endwhile

say 'Plotting: ' % title
say 'Yearly: ' % yearly_file
say 'Anomaly: ' % anom_file
say 'Date: ' % date_str
say 'Region: lon[' % lon1 % ',' % lon2 % '] lat[' % lat1 % ',' % lat2 % ']'
say 'Output: ' % output_file

* Load custom colors
'run colors.gs'

* Open the anomaly file first (for shading)
'sdfopen ' % anom_file
if (rc != 0)
  say 'Error opening anomaly file: ' % anom_file
  return
endif

* Open the yearly file (for contours)
'sdfopen ' % yearly_file
if (rc != 0)
  say 'Error opening yearly file: ' % yearly_file
  return
endif

* Set up graphics
'set display color white'
'c'

* Set region
'set lon ' % lon1 % ' ' % lon2
'set lat ' % lat1 % ' ' % lat2

* Set time based on date (need to find the right timestep)
* Parse date: YYYY-MM-DD
year = substr(date_str, 1, 4)
month = substr(date_str, 6, 2)
day = substr(date_str, 9, 2)

* Convert to GrADS time format
mon_names = 'jan feb mar apr may jun jul aug sep oct nov dec'
mon_name = subwrd(mon_names, math_int(month))
grads_time = day % mon_name % year

say 'GrADS time: ' % grads_time
'set time ' % grads_time

* Set map projection (polar stereographic for Antarctica)
if (lat2 < -50)
  'set mproj sps'
  'set mpvals ' % lon1 % ' ' % lon2 % ' ' % lat1 % ' ' % lat2
endif

* =============================================================================
* Plot 1: Anomaly shading with BWR colormap
* =============================================================================
'set gxout shaded'

* Set BWR color levels for anomaly (in Pa, symmetric around 0)
* Typical pressure anomalies: -3000 to +3000 Pa
'set clevs -3000 -2500 -2000 -1500 -1000 -500 0 500 1000 1500 2000 2500 3000'
'set ccols 40 42 44 46 48 50 50 52 54 56 58 60'

* Plot anomaly from file 1 (sp is the variable name)
'd sp.1'

* Draw colorbar
'run cbarn.gs 0.8 0 5.5 0.8'

* =============================================================================
* Plot 2: Absolute pressure contours
* =============================================================================
'set gxout contour'
'set cint 500'
'set cthick 3'
'set ccolor 1'
'set clab on'

* Plot absolute pressure from file 2
* Convert from Pa to hPa for contour labels
'd sp.2/100'

* =============================================================================
* Add title and labels
* =============================================================================
'set string 1 c 6'
'set strsiz 0.18'
'draw string 5.5 8.0 ' % title

'set strsiz 0.14'
'draw string 5.5 7.7 Date: ' % date_str

'set strsiz 0.12'
'draw string 5.5 0.3 Shading: Pressure Anomaly (Pa) | Contours: Surface Pressure (hPa)'

* =============================================================================
* Save output (using printim instead of gxprint for compatibility)
* =============================================================================
'printim ' % output_file % ' white x1200 y900'

say 'Saved: ' % output_file

* Cleanup
'close 2'
'close 1'

return
