#!/bin/sh
cd /home/cvdev/stratdev/DataCleaning/
python daily_update.py us-stocks VTI VTV VOE VBR VEA VWO VXUS VT IEMG TIP VTIP BND MUB LQD BNDX VWOB SHV >> log_`date +%Y%m%d -d "yesterday"`
python daily_update.py funds AQRIX AQMIX QGMIX SRPFX ABRZX >> log_`date +%Y%m%d -d "yesterday"`
