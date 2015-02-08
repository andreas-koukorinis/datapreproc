#!/usr/bin/bash
python adjust_split.py data/ fund AQRIX AQMIX ABRZX QGMIX SRPFX VBLTX VTSMX
python backward_dividend_adjust.py data/ fund AQRIX AQMIX ABRZX QGMIX SRPFX VBLTX VTSMX
python forward_dividend_adjust.py data/ fund AQRIX AQMIX ABRZX QGMIX SRPFX VBLTX VTSMX
python process_db.py data/ fund AQRIX AQMIX ABRZX QGMIX SRPFX VBLTX VTSMX
