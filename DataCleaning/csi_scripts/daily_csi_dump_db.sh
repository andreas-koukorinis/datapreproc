#!/bin/sh
cd /home/cvdev/stratdev/DataCleaning/csi_scripts/
python daily_update.py us-stocks VTI VTV VOE VBR VEA VWO VXUS VT IEMG TIP VTIP BND MUB LQD BNDX VWOB SHV >> daily_db_log_`date +%Y%m%d -d "yesterday"`
python daily_update.py funds AQRIX AQMIX QGMIX SRPFX ABRZX >> daily_db_log_`date +%Y%m%d -d "yesterday"`
