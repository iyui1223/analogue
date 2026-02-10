*
* plot_Tsurfdiff.gs
* Temperature difference (analogue minus original) with dual MSLP contours
*
* Creates a layered plot with:
*   - Shaded fill : T2m difference (analogue - original) in deg C, BWR colormap
*   - Black dashed contours : original event MSLP (hPa, 4 hPa interval)
*   - Blue/Red solid contours : analogue MSLP (blue=past, red=present)
*   - Coastlines and map boundaries
*
* Data access uses GrADS .ctl template files so that a single 'open' covers
* all years; the correct NetCDF file is resolved automatically via 'set time'.
*
* Usage:
*   grads -blcx "run plot_Tsurfdiff.gs CTL_DIR ORIG_DATE ANAL_DATE PERIOD LON1 LON2 LAT1 LAT2 OUTPUT TITLE..."
*
* Arguments:
*   CTL_DIR     - Path to directory containing .ctl files
*   ORIG_DATE   - Original event date at this offset (YYYY-MM-DD)
*   ANAL_DATE   - Analogue date at this offset (YYYY-MM-DD)
*   PERIOD      - "past" or "present"
*   LON1, LON2  - Longitude range (0-360)
*   LAT1, LAT2  - Latitude range
*   OUTPUT      - Output PNG file path
*   TITLE...    - Remaining words form the plot title
*

function main(args)

ctl_dir    = subwrd(args, 1)
orig_date  = subwrd(args, 2)
anal_date  = subwrd(args, 3)
period     = subwrd(args, 4)
lon1       = subwrd(args, 5)
lon2       = subwrd(args, 6)
lat1       = subwrd(args, 7)
lat2       = subwrd(args, 8)
output_file = subwrd(args, 9)
title      = subwrd(args, 10)

i = 11
while (subwrd(args, i) != '')
  title = title % ' ' % subwrd(args, i)
  i = i + 1
endwhile

say '============================================================'
say 'Tsurfdiff Plot Generation'
say '============================================================'
say 'CTL dir:  ' % ctl_dir
say 'Original: ' % orig_date
say 'Analogue: ' % anal_date
say 'Period:   ' % period
say 'Region:   lon[' % lon1 % ',' % lon2 % '] lat[' % lat1 % ',' % lat2 % ']'
say 'Output:   ' % output_file
say 'Title:    ' % title
say '============================================================'

* Load custom color definitions (BWR indices 40-60, etc.)
'run colors.gs'

* =============================================================================
* Open data via .ctl templates
* =============================================================================
t2m_ctl = ctl_dir % '/era5_daily_2m_temperature.ctl'
msl_ctl = ctl_dir % '/era5_daily_mean_sea_level_pressure.ctl'

'open ' % t2m_ctl
if (rc != 0)
  say 'ERROR: Cannot open T2M ctl: ' % t2m_ctl
  return
endif
say 'Opened T2M as file 1'

'open ' % msl_ctl
if (rc != 0)
  say 'ERROR: Cannot open MSLP ctl: ' % msl_ctl
  return
endif
say 'Opened MSLP as file 2'

* =============================================================================
* Set up display
* =============================================================================
'set display color white'
'c'
'set parea 0.8 10.2 1.8 7.8'

* =============================================================================
* Parse dates → GrADS time strings (DDmonYYYY)
* =============================================================================
orig_year  = substr(orig_date, 1, 4)
orig_month = substr(orig_date, 6, 2)
orig_day   = substr(orig_date, 9, 2)

anal_year  = substr(anal_date, 1, 4)
anal_month = substr(anal_date, 6, 2)
anal_day   = substr(anal_date, 9, 2)

mon_names = 'jan feb mar apr may jun jul aug sep oct nov dec'
orig_mon  = subwrd(mon_names, math_int(orig_month))
anal_mon  = subwrd(mon_names, math_int(anal_month))

orig_time = orig_day % orig_mon % orig_year
anal_time = anal_day % anal_mon % anal_year

say 'Original GrADS time: ' % orig_time
say 'Analogue GrADS time: ' % anal_time

* =============================================================================
* Detect Antarctic and set projection
* =============================================================================
is_antarctic = 0
if (lat2 < -50)
  is_antarctic = 1
  say 'Antarctic region detected - using polar projection'
endif

'set lat ' % lat1 % ' ' % lat2
'set lon ' % lon1 % ' ' % lon2

if (is_antarctic = 1)
  'set mproj sps'
endif

* =============================================================================
* Capture original fields using 'define'
* =============================================================================
say 'Defining original fields at ' % orig_time % ' ...'

'set dfile 1'
'set time ' % orig_time
'define origt2m = t2m.1 - 273.15'

'set dfile 2'
'set time ' % orig_time
'define origmsl = msl.2 / 100'

say 'Original fields defined.'

* =============================================================================
* Switch to analogue date for plotting
* =============================================================================
'set dfile 1'
'set time ' % anal_time
'set lat ' % lat1 % ' ' % lat2
'set lon ' % lon1 % ' ' % lon2

* =============================================================================
* Layer 1: Shaded T2m difference (analogue - original) in deg C
* Uses BWR diverging colormap (indices 40-60 from colors.gs)
* =============================================================================
say 'Layer 1: Temperature difference shading...'

'set gxout shaded'
'set clevs -12 -8 -5 -3 -1 1 3 5 8 12'
'set ccols 40 42 44 46 48 50 52 54 56 58 60'
'd t2m.1 - 273.15 - origt2m'

* =============================================================================
* Layer 2: Original MSLP contours (black, dashed)
* =============================================================================
say 'Layer 2: Original MSLP contours (black dashed)...'

'set gxout contour'
'set cint 4'
'set cthick 4'
'set ccolor 1'
'set cstyle 3'
'set clab on'
'set clskip 2'
'd origmsl'

* =============================================================================
* Layer 3: Analogue MSLP contours (blue=past or red=present, solid)
* =============================================================================
say 'Layer 3: Analogue MSLP contours (' % period % ')...'

'set dfile 2'
'set time ' % anal_time
'set lat ' % lat1 % ' ' % lat2
'set lon ' % lon1 % ' ' % lon2

'set gxout contour'
'set cint 4'
'set cthick 4'
'set cstyle 1'
'set clab on'
'set clskip 2'

if (period = 'past')
  'set ccolor 4'
endif
if (period = 'present')
  'set ccolor 2'
endif

'd msl.2 / 100'

* =============================================================================
* Layer 4: Coastlines and map boundaries
* =============================================================================
say 'Layer 4: Map boundaries...'
'set map 1 1 4'
'draw map'

* =============================================================================
* Colorbar
* =============================================================================
'run cbarn.gs 0.8 0 5.5 0.8'

* =============================================================================
* Titles and labels
* =============================================================================
say 'Adding titles...'

if (period = 'past')
  title_prefix = 'PAST ANALOGUE'
  contour_lbl  = 'Blue solid: past analogue MSLP'
endif
if (period = 'present')
  title_prefix = 'PRESENT ANALOGUE'
  contour_lbl  = 'Red solid: present analogue MSLP'
endif

'set string 1 c 6'
'set strsiz 0.18'
'draw string 5.5 8.3 ' % title_prefix % ' diff: ' % title

'set strsiz 0.13'
'draw string 5.5 7.95 Original: ' % orig_date % '    Analogue: ' % anal_date

'set strsiz 0.10'
'set string 1 c 4'
'draw string 5.5 1.55 Shading: T2m difference (C) | Black dashed: original MSLP (4hPa) | ' % contour_lbl

* =============================================================================
* Save output
* =============================================================================
say 'Saving to: ' % output_file
'printim ' % output_file % ' white x1400 y1000'

say 'Saved: ' % output_file

allclose

say '============================================================'
say 'Tsurfdiff plot complete'
say '============================================================'

'quit'

return
