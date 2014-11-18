#!/bin/sh
cd /home/cvdev/stratdev/DataCleaning/csi_scripts/
echo "DB DUMP CSI DAT FOR ETF's AND FUNDS STATUS:\n" >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
python daily_update.py us-stocks 1 VTI VTV VOE VBR VEA VWO VXUS VT IEMG TIP VTIP BND MUB LQD BNDX VWOB SHV >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
python daily_update.py funds 1 AQRIX AQMIX QGMIX SRPFX ABRZX >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
#python daily_update.py futures 1 ES ZN EBL CU1 EX JY1 JT AD1 BP1 CD1 SXE >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
