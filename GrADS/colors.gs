*
* Custom Color Definitions for GrADS - Analogue Visualization
* 
* Temperature colormap: indices 71-125 (earth.nullschool.net / UK Met Office style)
* Other colormaps use indices 40-60 and 130+ to avoid overlap
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
* Temperature colormap (indices 71-125)
* UK Met Office / earth.nullschool.net style - colorblind friendly
* Cold (purple/blue) to Hot (red)
* =============================================================================
'set rgb 71  75   0 130'
'set rgb 72  60   0 130'
'set rgb 73  44   0 129'
'set rgb 74  29   0 129'
'set rgb 75  14   0 128'
'set rgb 76   2   0 126'
'set rgb 77  28   0 100'
'set rgb 78  55   0  73'
'set rgb 79  81   0  47'
'set rgb 80 107   0  21'
'set rgb 81 128   0   5'
'set rgb 82 128   0  31'
'set rgb 83 128   0  57'
'set rgb 84 128   0  83'
'set rgb 85 128   0 109'
'set rgb 86 128  11 134'
'set rgb 87 130  53 156'
'set rgb 88 131  95 178'
'set rgb 89 133 137 199'
'set rgb 90 134 179 221'
'set rgb 91 138 207 235'
'set rgb 92 146 209 234'
'set rgb 93 153 211 233'
'set rgb 94 161 213 232'
'set rgb 95 169 215 231'
'set rgb 96 157 196 232'
'set rgb 97 122 152 237'
'set rgb 98  86 108 242'
'set rgb 99  51  64 248'
'set rgb 100  16  20 253'
'set rgb 101   0  28 227'
'set rgb 102   0  80 175'
'set rgb 103   0 132 123'
'set rgb 104   0 184  71'
'set rgb 105   0 236  19'
'set rgb 106  33 255   0'
'set rgb 107  85 255   0'
'set rgb 108 137 255   0'
'set rgb 109 189 255   0'
'set rgb 110 241 255   0'
'set rgb 111 255 242   0'
'set rgb 112 255 223   0'
'set rgb 113 255 205   0'
'set rgb 114 255 187   0'
'set rgb 115 255 168   0'
'set rgb 116 255 138   0'
'set rgb 117 255 104   0'
'set rgb 118 255  70   0'
'set rgb 119 255  37   0'
'set rgb 120 255   3   0'
'set rgb 121 234   0   0'
'set rgb 122 210   0   0'
'set rgb 123 186   0   0'
'set rgb 124 163   0   0'
'set rgb 125 139   0   0'

* =============================================================================
* Special colors for contours and overlays (indices 130-145)
* =============================================================================

* Ice-blue for 0 deg C isotherm
'set rgb 130 173 216 230'

* Terrain/Elevation grayscale
'set rgb 131 220 220 220'
'set rgb 132 180 180 180'
'set rgb 133 140 140 140'
'set rgb 134 100 100 100'
'set rgb 135  60  60  60'
'set rgb 136  30  30  30'

* Stippling colors for anomalies
'set rgb 137   0   0 200'
'set rgb 138 200   0   0'
'set rgb 139 128 128 128'

* Pressure colormap (indices 140-155) - if needed separately
'set rgb 140  75   0 130'
'set rgb 141  60  60 180'
'set rgb 142  80  80 200'
'set rgb 143 100 100 220'
'set rgb 144 120 140 230'
'set rgb 145 140 180 235'
'set rgb 146 160 200 240'
'set rgb 147 180 220 245'
'set rgb 148 200 235 250'
'set rgb 149 220 245 255'
'set rgb 150 240 255 240'
'set rgb 151 255 255 200'
'set rgb 152 255 240 150'
'set rgb 153 255 220 100'
'set rgb 154 255 190  50'
'set rgb 155 255 160   0'

return
