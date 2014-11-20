#!/bin/sh

python Simulator.py ~/stratdev/test/RP1/RP1_rb1_tr10_std100000.cfg
python Simulator.py ~/stratdev/test/RP1/RP1_rb1_tr10_std21_std252.cfg  
python Simulator.py ~/stratdev/test/RP1/RP1_rb1_tr10_std252_std100000.cfg  
python Simulator.py ~/stratdev/test/RP1/RP1_rb1_tr10_std63_std100000.cfg  
python Simulator.py ~/stratdev/test/RP1/RP1_rb252_tr10_std21_std252.cfg
python Simulator.py ~/stratdev/test/RP1/RP1_rb1_tr10_std21.cfg
python Simulator.py ~/stratdev/test/RP1/RP1_rb1_tr10_std21_std63.cfg   
python Simulator.py ~/stratdev/test/RP1/RP1_rb1_tr10_std504.cfg
python Simulator.py ~/stratdev/test/RP1/RP1_rb1_tr10_std63_std252.cfg     
python Simulator.py ~/stratdev/test/RP1/RP1_rb5_tr10_std21_std252.cfg
python Simulator.py ~/stratdev/test/RP1/RP1_rb1_tr10_std21_std100000.cfg  
python Simulator.py ~/stratdev/test/RP1/RP1_rb1_tr10_std252.cfg
python Simulator.py ~/stratdev/test/RP1/RP1_rb1_tr10_std63.cfg
python Simulator.py ~/stratdev/test/RP1/RP1_rb21_tr10_std21_std252.cfg    
python Simulator.py ~/stratdev/test/RP1/RP1_rb63_tr10_std21_std252.cfg
python Simulator.py ~/stratdev/test/6040_rb252.cfg

python Simulator.py ~/stratdev/test/AQRIX_rb100000.cfg

python Tools/ComputePerformance.py ~/stratdev/logs/RP1_rb1_tr10_std100000/returns.txt ~/stratdev/logs/RP1_rb1_tr10_std21_std252/returns.txt ~/stratdev/logs/RP1_rb1_tr10_std252_std100000/returns.txt ~/stratdev/logs/RP1_rb1_tr10_std63_std100000/returns.txt ~/stratdev/logs/RP1_rb252_tr10_std21_std252/returns.txt ~/stratdev/logs/RP1_rb1_tr10_std21/returns.txt ~/stratdev/logs/RP1_rb1_tr10_std21_std63/returns.txt ~/stratdev/logs/RP1_rb1_tr10_std504/returns.txt ~/stratdev/logs/RP1_rb1_tr10_std63_std252/returns.txt ~/stratdev/logs/RP1_rb5_tr10_std21_std252/returns.txt ~/stratdev/logs/RP1_rb1_tr10_std21_std100000/returns.txt ~/stratdev/logs/RP1_rb1_tr10_std252/returns.txt ~/stratdev/logs/RP1_rb1_tr10_std63/returns.txt ~/stratdev/logs/RP1_rb21_tr10_std21_std252/returns.txt ~/stratdev/logs/RP1_rb63_tr10_std21_std252/returns.txt ~/stratdev/logs/AQRIX_rb100000/returns.txt > ~/stratdev/logs/stats_RP_AQRIX.txt

python Tools/compute_correlation.py ~/stratdev/logs/RP1_rb1_tr10_std21/returns.txt ~/stratdev/logs/AQRIX_rb100000/returns.txt >> ~/stratdev/logs/stats_RP_AQRIX.txt
python Tools/compute_correlation.py ~/stratdev/logs/RP1_rb1_tr10_std100000/returns.txt ~/stratdev/logs/AQRIX_rb100000/returns.txt >> ~/stratdev/logs/stats_RP_AQRIX.txt
python Tools/compute_correlation.py ~/stratdev/logs/RP1_rb1_tr10_std21_std252/returns.txt ~/stratdev/logs/AQRIX_rb100000/returns.txt >> ~/stratdev/logs/stats_RP_AQRIX.txt
python Tools/compute_correlation.py ~/stratdev/logs/RP1_rb21_tr10_std21_std252/returns.txt ~/stratdev/logs/AQRIX_rb100000/returns.txt >> ~/stratdev/logs/stats_RP_AQRIX.txt
python Tools/compute_correlation.py ~/stratdev/logs/RP1_rb63_tr10_std21_std252/returns.txt ~/stratdev/logs/AQRIX_rb100000/returns.txt >> ~/stratdev/logs/stats_RP_AQRIX.txt
python Tools/compute_correlation.py ~/stratdev/logs/6040_rb252/returns.txt ~/stratdev/logs/AQRIX_rb100000/returns.txt >> ~/stratdev/logs/stats_RP_AQRIX.txt

python Tools/plot_series.py 0 ~/stratdev/logs/RP1_rb1_tr10_std21/returns.txt ~/stratdev/logs/RP1_rb1_tr10_std21_std252/returns.txt ~/stratdev/logs/RP1_rb1_tr10_std100000/returns.txt ~/stratdev/logs/RP1_rb21_tr10_std21_std252/returns.txt ~/stratdev/logs/6040_rb252/returns.txt ~/stratdev/logs/AQRIX_rb100000/returns.txt

python Tools/plot_series.py 0 ~/stratdev/logs/RP1_rb1_tr10_std21/returns.txt ~/stratdev/logs/AQRIX_rb100000/returns.txt ~/stratdev/logs/6040_rb252/returns.txt

python Tools/plot_series.py 1 ~/stratdev/logs/RP1_rb1_tr10_std21/leverage.txt ~/stratdev/logs/RP1_rb1_tr10_std21_std252/leverage.txt ~/stratdev/logs/RP1_rb1_tr10_std100000/leverage.txt

python Tools/plot_series.py 1 ~/stratdev/logs/RP1_rb1_tr10_std21/weights.txt ~/stratdev/logs/RP1_rb1_tr10_std21_std252/weights.txt ~/stratdev/logs/RP1_rb1_tr10_std100000/weights.txt ~/stratdev/logs/6040_rb252/weights.txt
