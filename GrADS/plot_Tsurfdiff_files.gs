*
* plot_Tsurfdiff_files.gs
* Temperature difference (analogue minus target) using explicit NetCDF paths
*
* Same plot as plot_Tsurfdiff.gs but opens 4 files directly instead of ctl templates.
* Used when ERA5 main archive is missing (e.g. post-2022) and data_slice is used.
*
* Usage:
*   grads -blcx "run plot_Tsurfdiff_files.gs ORIG_T2M ORIG_MSL ANAL_T2M ANAL_MSL ORIG_DATE ANAL_DATE PERIOD LON1 LON2 LAT1 LAT2 OUTPUT TITLE..."
*
*   Use NONE for ORIG_MSL and/or ANAL_MSL when data_slice has t2m only (skips MSLP layers).
*

function main(args)

orig_t2m   = subwrd(args, 1)
orig_msl   = subwrd(args, 2)
anal_t2m   = subwrd(args, 3)
anal_msl   = subwrd(args, 4)
orig_date  = subwrd(args, 5)
anal_date  = subwrd(args, 6)
period     = subwrd(args, 7)
lon1       = subwrd(args, 8)
lon2       = subwrd(args, 9)
lat1       = subwrd(args, 10)
lat2       = subwrd(args, 11)
output_file = subwrd(args, 12)
title      = subwrd(args, 13)

i = 14
while (subwrd(args, i) != '')
  title = title % ' ' % subwrd(args, i)
  i = i + 1
endwhile

say '============================================================'
say 'Tsurfdiff (files mode) Plot Generation'
say '============================================================'
say 'Orig T2m: ' % orig_t2m
say 'Orig MSL: ' % orig_msl
say 'Anal T2m: ' % anal_t2m
say 'Anal MSL: ' % anal_msl
say 'Target:   ' % orig_date
say 'Analogue: ' % anal_date
say 'Output:   ' % output_file
say '============================================================'

'run colors.gs'

* Open files: 1=orig_t2m, 2=orig_msl, 3=anal_t2m, 4=anal_msl
file_num = 0

'sdfopen ' % orig_t2m
if (rc != 0)
  say 'ERROR: Cannot open orig T2m: ' % orig_t2m
  return
endif
file_num = file_num + 1
orig_t2m_fnum = file_num

has_orig_msl = 0
if (orig_msl != 'NONE')
  'sdfopen ' % orig_msl
  if (rc = 0)
    file_num = file_num + 1
    orig_msl_fnum = file_num
    has_orig_msl = 1
  endif
endif

'sdfopen ' % anal_t2m
if (rc != 0)
  say 'ERROR: Cannot open anal T2m: ' % anal_t2m
  return
endif
file_num = file_num + 1
anal_t2m_fnum = file_num

has_anal_msl = 0
if (anal_msl != 'NONE')
  'sdfopen ' % anal_msl
  if (rc = 0)
    file_num = file_num + 1
    anal_msl_fnum = file_num
    has_anal_msl = 1
  endif
endif

* Parse dates
mon_names = 'jan feb mar apr may jun jul aug sep oct nov dec'
orig_month = substr(orig_date, 6, 2)
orig_day   = substr(orig_date, 9, 2)
orig_year  = substr(orig_date, 1, 4)
orig_mon   = subwrd(mon_names, math_int(orig_month))
orig_time  = orig_day % orig_mon % orig_year

anal_month = substr(anal_date, 6, 2)
anal_day   = substr(anal_date, 9, 2)
anal_year  = substr(anal_date, 1, 4)
anal_mon   = subwrd(mon_names, math_int(anal_month))
anal_time  = anal_day % anal_mon % anal_year

'set display color white'
'c'
'set parea 0.8 10.2 1.8 7.8'

is_antarctic = 0
if (lat2 < -50)
  is_antarctic = 1
endif

'set lat ' % lat1 % ' ' % lat2
'set lon ' % lon1 % ' ' % lon2
if (is_antarctic = 1)
  'set mproj sps'
endif

* Define target fields
'set dfile ' % orig_t2m_fnum
'set time ' % orig_time
'define origt2m = t2m.' % orig_t2m_fnum % ' - 273.15'

if (has_orig_msl = 1)
  'set dfile ' % orig_msl_fnum
  'set time ' % orig_time
  'define origmsl = msl.' % orig_msl_fnum % ' / 100'
endif

* Plot: T2m difference
'set dfile ' % anal_t2m_fnum
'set time ' % anal_time
'set lat ' % lat1 % ' ' % lat2
'set lon ' % lon1 % ' ' % lon2

'set gxout shaded'
'set clevs -12 -8 -5 -3 -1 1 3 5 8 12'
'set ccols 40 42 44 46 48 50 52 54 56 58 60'
'd t2m.' % anal_t2m_fnum % ' - 273.15 - origt2m'

* Layer 2: Target MSLP (if available)
if (has_orig_msl = 1)
  'set gxout contour'
  'set cint 4'
  'set cthick 4'
  'set ccolor 1'
  'set cstyle 3'
  'set clab on'
  'set clskip 2'
  'd origmsl'
endif

* Layer 3: Analogue MSLP (if available)
if (has_anal_msl = 1)
  'set dfile ' % anal_msl_fnum
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
  'd msl.' % anal_msl_fnum % ' / 100'
endif

'set map 1 1 4'
'draw map'
'run cbarn.gs 0.8 0 5.5 0.8'

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
'draw string 5.5 7.95 Target: ' % orig_date % '    Analogue: ' % anal_date
'set strsiz 0.10'
'set string 1 c 4'
'draw string 5.5 1.55 Shading: T2m difference (C) | Black dashed: target MSLP (4hPa) | ' % contour_lbl

say 'Saving to: ' % output_file
'printim ' % output_file % ' white x1400 y1000'
say 'Saved: ' % output_file
allclose
'quit'
return
