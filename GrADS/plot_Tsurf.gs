*
* plot_synoptic.gs
* Comprehensive synoptic visualization for extreme weather events
*
* Creates multi-layer plot with:
*   - Temperature (shaded background)
*   - Mean sea level pressure (gray contours)
*   - 0 deg C isotherm (ice-blue line)
*   - Wind vectors at 850 hPa
*   - Terrain/elevation contours
*
* Usage:
*   grads -blc "run plot_synoptic.gs T2M_FILE MSLP_FILE UWIND_FILE VWIND_FILE TOPO_FILE DATE LON1 LON2 LAT1 LAT2 OUTPUT_FILE PERIOD TITLE"
*

function main(args)

t2m_file   = subwrd(args, 1)
mslp_file  = subwrd(args, 2)
uwind_file = subwrd(args, 3)
vwind_file = subwrd(args, 4)
topo_file  = subwrd(args, 5)
date_str   = subwrd(args, 6)
lon1       = subwrd(args, 7)
lon2       = subwrd(args, 8)
lat1       = subwrd(args, 9)
lat2       = subwrd(args, 10)
output_file = subwrd(args, 11)
period     = subwrd(args, 12)
title      = subwrd(args, 13)

i = 14
while (subwrd(args, i) != '')
  title = title % ' ' % subwrd(args, i)
  i = i + 1
endwhile

say '============================================================'
say 'Synoptic Plot Generation'
say '============================================================'
say 'T2M:    ' % t2m_file
say 'MSLP:   ' % mslp_file
say 'U-wind: ' % uwind_file
say 'V-wind: ' % vwind_file
say 'Topo:   ' % topo_file
say 'Date:   ' % date_str
say 'Region: lon[' % lon1 % ',' % lon2 % '] lat[' % lat1 % ',' % lat2 % ']'
say 'Output: ' % output_file
say 'Period: ' % period
say 'Title:  ' % title
say '============================================================'

'run colors.gs'

file_num = 0

'sdfopen ' % t2m_file
if (rc != 0)
  say 'ERROR: Cannot open temperature file'
  return
endif
file_num = file_num + 1
t2m_fnum = file_num
say 'Opened T2M as file ' % t2m_fnum

'sdfopen ' % mslp_file
if (rc != 0)
  say 'ERROR: Cannot open MSLP file'
  return
endif
file_num = file_num + 1
mslp_fnum = file_num
say 'Opened MSLP as file ' % mslp_fnum

has_wind = 0
if (uwind_file != 'NONE' & vwind_file != 'NONE')
  'sdfopen ' % uwind_file
  if (rc = 0)
    file_num = file_num + 1
    uwind_fnum = file_num
    say 'Opened U-wind as file ' % uwind_fnum
    'sdfopen ' % vwind_file
    if (rc = 0)
      file_num = file_num + 1
      vwind_fnum = file_num
      say 'Opened V-wind as file ' % vwind_fnum
      has_wind = 1
    endif
  endif
endif

has_topo = 0
if (topo_file != 'NONE')
  'sdfopen ' % topo_file
  if (rc = 0)
    file_num = file_num + 1
    topo_fnum = file_num
    has_topo = 1
    say 'Opened Topo as file ' % topo_fnum
  endif
endif

'set display color white'
'c'

'set parea 0.8 10.2 1.8 7.8'

year = substr(date_str, 1, 4)
month = substr(date_str, 6, 2)
day = substr(date_str, 9, 2)

mon_names = 'jan feb mar apr may jun jul aug sep oct nov dec'
mon_name = subwrd(mon_names, math_int(month))
grads_time = day % mon_name % year

say 'GrADS time: ' % grads_time

is_antarctic = 0
if (lat2 < -50)
  is_antarctic = 1
  say 'Antarctic region detected - using polar projection'
endif

* =============================================================================
* Set up display coordinates and projection
* =============================================================================
'set lat ' % lat1 % ' ' % lat2
'set lon ' % lon1 % ' ' % lon2

if (is_antarctic = 1)
  'set mproj sps'
endif

* =============================================================================
* Layer 1: Temperature shading (background)
* Uses UK Met Office / earth.nullschool.net style colormap (indices 71-125)
* =============================================================================
say 'Layer 1: Temperature shading...'
'set dfile ' % t2m_fnum
'set t 1'
'q file ' % t2m_fnum
'set time ' % grads_time

'set gxout shaded'
* Use custom temperature colormap (indices 71-125)
* Range: -80C to +55C in 5-degree steps (28 levels, 29 colors)
'set clevs -80 -75 -70 -65 -60 -55 -50 -45 -40 -35 -30 -25 -20 -15 -10 -5 0 5 10 15 20 25 30 35 40 45 50 55'
'set ccols 71 73 75 77 79 81 83 85 87 89 91 93 95 97 99 101 103 105 107 109 111 113 115 117 119 121 123 125 125'
'd t2m-273.15'

* =============================================================================
* Layer 2: Mean Sea Level Pressure contours (gray lines)
* =============================================================================
say 'Layer 2: MSLP contours...'
'set dfile ' % mslp_fnum
'set t 1'
'q file ' % mslp_fnum
'set time ' % grads_time

'set gxout contour'
'set cint 4'
'set cthick 4'
'set ccolor 15'
'set clab on'
'set clskip 2'
'd msl/100'

* =============================================================================
* Layer 3: 0 deg C Isotherm (ice-blue line)
* =============================================================================
say 'Layer 3: 0 deg C isotherm...'
'set dfile ' % t2m_fnum
'set time ' % grads_time

'set gxout contour'
'set clevs 0'
'set cthick 12'
'set ccolor 130'
'set clab off'
'set cstyle 1'
'd t2m-273.15'

* =============================================================================
* Layer 4: Wind vectors at 850 hPa (if available)
* =============================================================================
if (has_wind = 1)
  say 'Layer 4: Wind vectors at 850 hPa...'
  'set dfile ' % uwind_fnum
  'set time ' % grads_time
  'set lev 850'
  'set gxout vector'
  'set ccolor 1'
  'set arrscl 0.4 15'
  'set arrlab off'
  'd skip(u,20);v.' % vwind_fnum
endif

* =============================================================================
* Layer 5: Terrain/Elevation contours (if available)
* Note: Topography often fails with polar projection due to coordinate issues
* Coastlines from 'draw map' provide sufficient boundary information
* =============================================================================
* Skipping topography contours for now - coastlines provide boundaries
* if (has_topo = 1)
*   say 'Layer 5: Elevation contours...'
*   'set dfile ' % topo_fnum
*   'set t 1'
*   'set gxout contour'
*   'set clevs 9800 19600 29400'
*   'set cthick 2'
*   'set ccolor 92'
*   'set cstyle 2'
*   'set clab off'
*   'd z'
* endif
say 'Layer 5: Skipping elevation contours (using coastlines instead)'

* =============================================================================
* Layer 6: Coastlines and map boundaries
* =============================================================================
say 'Layer 6: Map boundaries...'
'set map 1 1 4'
'draw map'

* =============================================================================
* Colorbar - external PNG image
* =============================================================================
# 'draw image /lustre/soge1/projects/andante/cenv1201/proj/analogue/Const/temperature_colorbar.png 1.0 0.3 10.0 1.4'

* =============================================================================
* Titles and labels
* =============================================================================
say 'Adding titles...'

if (period = 'original')
  title_prefix = 'ORIGINAL EVENT'
endif
if (period = 'past')
  title_prefix = 'PAST ANALOGUE'
endif
if (period = 'present')
  title_prefix = 'PRESENT ANALOGUE'
endif

'set string 1 c 6'
'set strsiz 0.20'
'draw string 5.5 8.3 ' % title_prefix % ': ' % title

'set strsiz 0.16'
'draw string 5.5 7.95 ' % date_str

'set strsiz 0.10'
'set string 1 c 4'
'draw string 5.5 1.55 Shading: 2m Temp (C) | Gray contours: MSLP (hPa, 4hPa) | Ice-blue line: 0C isotherm'

* =============================================================================
* Save output
* =============================================================================
say 'Saving to: ' % output_file
'printim ' % output_file % ' white x1400 y1000'

say 'Saved: ' % output_file

allclose 

say '============================================================'
say 'Plot complete'
say '============================================================'

'quit'

return
