#!/bin/sh

python adjust_split.py ETF VTI VTV VOE VBR VEA VWO VXUS VT IEMG TIP VTIP BND MUB LQD BNDX VWOB SHV
python adjust_split.py MF AQRIX AQMIX QGMIX SRPFX ABRZX
python backward_dividend_adjust.py ETF VTI VTV VOE VBR VEA VWO VXUS VT IEMG TIP VTIP BND MUB LQD BNDX VWOB SHV
python backward_dividend_adjust.py MF AQRIX AQMIX QGMIX SRPFX ABRZX
python forward_dividend_adjust.py ETF VTI VTV VOE VBR VEA VWO VXUS VT IEMG TIP VTIP BND MUB LQD BNDX VWOB SHV
python forward_dividend_adjust.py MF AQRIX AQMIX QGMIX SRPFX ABRZX
