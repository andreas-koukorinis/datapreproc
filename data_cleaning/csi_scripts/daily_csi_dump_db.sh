#!/bin/sh
cd /home/cvdev/stratdev/data_cleaning/csi_scripts/
echo "DB DUMP CSI DAT FOR ETF's AND FUNDS STATUS:\n" >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
COUNTER=92
#>log
#while [  $COUNTER -gt 0 ]; do
#    echo The counter is $COUNTER
#    python daily_update.py futures $COUNTER TU FV TY US NK NIY ES EMD NQ YM AD BP CD CU1 JY MP NE2 SF GC SI HG PL PA LH ZW ZC ZS ZM ZL EBS EBM EBL SXE FDX SMI SXF CGB FFI FLG AEX KC CT CC SB JTI JGB JNI SIN SSG HCE HSI ALS YAP MFX KOS JPYUSD CADUSD GBPUSD EURUSD AUDUSD NZDUSD CHFUSD SEKUSD NOKUSD TRYUSD MXNUSD ZARUSD ILSUSD SGDUSD HKDUSD TWDUSD >> log 
    #echo "python daily_update.py futures $COUNTER TU"
#    let COUNTER=COUNTER-1 
#done
python daily_update.py futures 1 TU FV TY US NK NIY ES EMD NQ YM AD BP CD CU1 JY MP NE2 SF GC SI HG PL PA LH ZW ZC ZS ZM ZL EBS EBM EBL SXE FDX SMI SXF CGB FFI FLG AEX KC CT CC SB JTI JGB JNI SIN SSG HCE HSI ALS YAP MFX KOS JPYUSD CADUSD GBPUSD EURUSD AUDUSD NZDUSD CHFUSD SEKUSD NOKUSD TRYUSD MXNUSD ZARUSD ILSUSD SGDUSD HKDUSD TWDUSD >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`

python daily_update.py us-stocks 1 VTI VTV VOE VBR VEA VWO VXUS VT IEMG TIP VTIP BND MUB LQD BNDX VWOB SHV >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
python daily_update.py funds 1 AQRIX AQMIX QGMIX SRPFX ABRZX VBLTX VTSMX>> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
#python daily_update.py futures 1 EBL EBM FLG ZF TY AD1 BP1 CD1 CU1 MP FDX SXF NK EX JT ES SXE JY1 CADUSD GBPUSD EURUSD JPYUSD >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
