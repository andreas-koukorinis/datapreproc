#!/bin/sh
cd /home/cvdev/stratdev/DataCleaning/csi_scripts/
echo "DB DUMP CSI DAT FOR ETF's AND FUNDS STATUS:\n" >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
python daily_update.py us-stocks VTI VTV VOE VBR VEA VWO VXUS VT IEMG TIP VTIP BND MUB LQD BNDX VWOB SHV >> apps/logs/log_`date +%Y%m%d -d "yesterday"`
python daily_update.py funds AQRIX AQMIX QGMIX SRPFX ABRZX >> apps/logs/log_`date +%Y%m%d -d "yesterday"`
