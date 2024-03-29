#!/bin/sh
source /apps/pythonenv/py2.7/bin/activate

#cd /home/cvdev/stratdev/data_cleaning/csi_scripts/
cd /home/cvdev/datapreproc/data_cleaning/csi_scripts/
echo "DB DUMP CSI DAT FOR ETF's AND FUNDS STATUS:\n" >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
#COUNTER=125
#>log
#while [  $COUNTER -gt 0 ]; do
#    echo The counter is $COUNTER
#    python daily_update.py us-stocks $COUNTER VTI VTV VBR VEA VWO VXUS VT IEMG BND MUB LQD VWOB SHV VNQ BNDX TIP VTIP VIG>> log
#    python daily_update.py funds $COUNTER AQRIX AQMIX QGMIX SRPFX ABRZX VBLTX VTSMX >> log
##    python daily_update.py futures $COUNTER TU FV TY US NK NIY ES EMD NQ YM AD BP CD CU1 JY MP NE2 SF GC SI HG PL PA LH ZW ZC ZS ZM ZL EBS EBM EBL SXE FDX SMI SXF CGB FFI FLG AEX KC CT CC SB JTI JGB JNI SIN SSG HCE HSI ALS YAP MFX KOS VX JPYUSD CADUSD GBPUSD EURUSD AUDUSD NZDUSD CHFUSD SEKUSD NOKUSD TRYUSD MXNUSD ZARUSD ILSUSD SGDUSD HKDUSD TWDUSD >> log
#    python daily_update.py indices $COUNTER VIX >> log
    #echo "python daily_update.py futures $COUNTER TU"
#    let COUNTER=COUNTER-1 
#done
# VOE VWO TIP VTIP BNDX
python daily_update.py futures 1 TU FV TY US NK NIY ES SP EMD NQ YM AD BP CD CU1 JY MP NE2 SF GC SI HG PL PA LH ZW ZC ZS ZM ZL EBS EBM EBL SXE FDX SMI SXF CGB FFI FLG AEX KC CT CC SB JTI JGB JNI SIN SSG HCE HSI ALS YAP MFX KOS VX JPYUSD CADUSD GBPUSD EURUSD AUDUSD NZDUSD CHFUSD SEKUSD NOKUSD TRYUSD MXNUSD ZARUSD ILSUSD SGDUSD HKDUSD TWDUSD INRUSD BRLUSD >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
python daily_update.py us-stocks 1 BND BNDX IEMG LQD MUB SHV TIP VBR VEA VIG VNQ VOE VT VTI VTIP VTV VWO VWOB VXUS >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
python daily_update.py funds 1 AQRIX AQMIX QGMIX SRPFX ABRZX VBLTX VTSMX>> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
python daily_update.py f-indices 1 XMSW ^AXJO ^BSES ^BVSP ^FTSE ^GDAX ^GU15 ^MXX ^N500 ^N150 ^NZ50 ^SSMI ^XMSC >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
python daily_update.py indices 1 VIX TCMP SPX NYA COMP >> /apps/logs/log_`date +%Y%m%d -d "yesterday"`
