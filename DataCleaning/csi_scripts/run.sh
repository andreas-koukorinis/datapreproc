#!/usr/bin/bash
#for i in {36..1}; do echo $i;python daily_update.py futures $i EBL EBM FLG ZF TY AD1 BP1 CD1 CU1 MP FDX SXF NK EX JT ES SXE; done
for i in {36..1}; do echo $i;python daily_update.py funds $i VBLTX VTSMX;done
#for i in {36..1}; do echo $i;python daily_update.py futures $i ZN; done
#python daily_update1.py futures 31 EBL EBM FLG ZF TY AD1 BP1 CD1 CU1 MP FDX SXF NK EX JT ES SXE
