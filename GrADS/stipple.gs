*
* stipple.gs
* Draw stippling pattern for temperature anomalies
*
* This script overlays stippling dots on the current plot to indicate
* significant temperature anomalies (positive or negative).
*
* Usage:
*   run stipple.gs ANOM_FILE THRESHOLD_NEG THRESHOLD_POS SKIP
*
* Arguments:
*   ANOM_FILE      - Path to anomaly file (NetCDF)
*   THRESHOLD_NEG  - Threshold for negative anomalies (e.g., -2 for < -2K)
*   THRESHOLD_POS  - Threshold for positive anomalies (e.g., 2 for > 2K)
*   SKIP           - Grid skip factor (e.g., 5 means plot every 5th point)
*
* Colors used:
*   96 - Blue dots for cold anomalies (< threshold_neg)
*   97 - Red dots for warm anomalies (> threshold_pos)
*
* Note: This script should be called after the main plot is drawn
*       and before the final output is saved.
*

function stipple(args)

anom_file = subwrd(args, 1)
thresh_neg = subwrd(args, 2)
thresh_pos = subwrd(args, 3)
skip = subwrd(args, 4)

* Default values
if (thresh_neg = ''); thresh_neg = -2; endif
if (thresh_pos = ''); thresh_pos = 2; endif
if (skip = ''); skip = 5; endif

say 'Stippling anomalies...'
say '  File: ' % anom_file
say '  Negative threshold: ' % thresh_neg
say '  Positive threshold: ' % thresh_pos
say '  Skip factor: ' % skip

* Open anomaly file
'sdfopen ' % anom_file
if (rc != 0)
  say 'ERROR: Cannot open anomaly file: ' % anom_file
  return
endif

* Get file number (should be last opened)
'q file'
rec = sublin(result, 2)
anom_fnum = subwrd(rec, 2)

* Get current dimension info
'q dims'
xline = sublin(result, 2)
yline = sublin(result, 3)

* Extract x range
xtype = subwrd(xline, 3)
if (xtype = 'varying')
  x1 = subwrd(xline, 11)
  x2 = subwrd(xline, 13)
else
  x1 = subwrd(xline, 9)
  x2 = x1
endif

* Extract y range  
ytype = subwrd(yline, 3)
if (ytype = 'varying')
  y1 = subwrd(yline, 11)
  y2 = subwrd(yline, 13)
else
  y1 = subwrd(yline, 9)
  y2 = y1
endif

say '  X range: ' % x1 % ' to ' % x2
say '  Y range: ' % y1 % ' to ' % y2

* Loop through grid points with skip factor
* Note: This is computationally expensive but necessary for stippling

'set gxout print'
'set prnopts %g 1 1'

xi = x1
while (xi <= x2)
  yi = y1
  while (yi <= y2)
    
    * Get value at this grid point
    'set x ' % xi
    'set y ' % yi
    'd t2m.' % anom_fnum
    
    rec = sublin(result, 2)
    val = subwrd(rec, 1)
    
    * Check if it's a valid number
    if (val != 'undefined')
      
      * Convert grid coords to world coords for plotting
      'q xy2w ' % xi % ' ' % yi
      wx = subwrd(result, 3)
      wy = subwrd(result, 6)
      
      * Draw stipple dot if anomaly exceeds threshold
      if (val < thresh_neg)
        'set line 96'
        'draw mark 3 ' % wx % ' ' % wy % ' 0.03'
      endif
      
      if (val > thresh_pos)
        'set line 97'  
        'draw mark 3 ' % wx % ' ' % wy % ' 0.03'
      endif
      
    endif
    
    yi = yi + skip
  endwhile
  xi = xi + skip
endwhile

* Reset gxout and close anomaly file
'set gxout shaded'
'close ' % anom_fnum

say 'Stippling complete'

return

*
* Alternative approach using maskout and scatter plot
* This is faster but less flexible
*
function stipple_fast(args)

anom_file = subwrd(args, 1)
thresh_neg = subwrd(args, 2)
thresh_pos = subwrd(args, 3)

if (thresh_neg = ''); thresh_neg = -2; endif
if (thresh_pos = ''); thresh_pos = 2; endif

'sdfopen ' % anom_file
if (rc != 0)
  say 'ERROR: Cannot open anomaly file'
  return
endif

'q file'
rec = sublin(result, 2)
fnum = subwrd(rec, 2)

* Draw cold anomaly stippling (blue)
'set gxout scatter'
'set cmark 3'
'set digsiz 0.02'
'set ccolor 96'
'd maskout(lon,t2m.' % fnum % '-' % thresh_neg % ');maskout(lat,t2m.' % fnum % '-' % thresh_neg % ')'

* Draw warm anomaly stippling (red)
'set ccolor 97'
'd maskout(lon,' % thresh_pos % '-t2m.' % fnum % ');maskout(lat,' % thresh_pos % '-t2m.' % fnum % ')'

'set gxout shaded'
'close ' % fnum

return
