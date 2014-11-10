#!/bin/sh

python adjust_split.py etf VTI VTV VOE VBR VEA VWO VXUS VT IEMG TIP VTIP BND MUB LQD BNDX VWOB SHV
python adjust_split.py fund AQRIX AQMIX QGMIX SRPFX ABRZX
python backward_dividend_adjust.py etf VTI VTV VOE VBR VEA VWO VXUS VT IEMG TIP VTIP BND MUB LQD BNDX VWOB SHV
python backward_dividend_adjust.py fund AQRIX AQMIX QGMIX SRPFX ABRZX
python forward_dividend_adjust.py etf VTI VTV VOE VBR VEA VWO VXUS VT IEMG TIP VTIP BND MUB LQD BNDX VWOB SHV
python forward_dividend_adjust.py fund AQRIX AQMIX QGMIX SRPFX ABRZX
python process_db.py etf VTI VTV VOE VBR VEA VWO VXUS VT IEMG TIP VTIP BND MUB LQD BNDX VWOB SHV
python process_db.py fund AQRIX AQMIX QGMIX SRPFX ABRZX
