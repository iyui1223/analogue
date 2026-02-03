*
* Custom Color Definitions for GrADS - Analogue Visualization
* Includes BWR (blue-white-red) colormap for anomalies
*

function colors(args)

* =============================================================================
* BWR (Blue-White-Red) diverging colormap for anomalies (indices 40-60)
* Symmetric around white at center
* =============================================================================
'set rgb 40   0   0 139'
'set rgb 41   0   0 178'
'set rgb 42   0   0 217'
'set rgb 43   0   0 255'
'set rgb 44  51  51 255'
'set rgb 45 102 102 255'
'set rgb 46 153 153 255'
'set rgb 47 178 178 255'
'set rgb 48 204 204 255'
'set rgb 49 230 230 255'
'set rgb 50 255 255 255'
'set rgb 51 255 230 230'
'set rgb 52 255 204 204'
'set rgb 53 255 178 178'
'set rgb 54 255 153 153'
'set rgb 55 255 102 102'
'set rgb 56 255  51  51'
'set rgb 57 255   0   0'
'set rgb 58 217   0   0'
'set rgb 59 178   0   0'
'set rgb 60 139   0   0'

* =============================================================================
* Pressure colormap for absolute values (indices 70-85)
* Blues for low pressure, greens/yellows for high pressure
* =============================================================================
'set rgb 70  75   0 130'
'set rgb 71  60  60 180'
'set rgb 72  80  80 200'
'set rgb 73 100 100 220'
'set rgb 74 120 140 230'
'set rgb 75 140 180 235'
'set rgb 76 160 200 240'
'set rgb 77 180 220 245'
'set rgb 78 200 235 250'
'set rgb 79 220 245 255'
'set rgb 80 240 255 240'
'set rgb 81 255 255 200'
'set rgb 82 255 240 150'
'set rgb 83 255 220 100'
'set rgb 84 255 190  50'
'set rgb 85 255 160   0'

return
