#!/bin/sh

python Simulator.py test/RP1/RP1_rb1_tr10_std100000.cfg
python Simulator.py test/RP1/RP1_rb1_tr10_std21_std252.cfg  
python Simulator.py test/RP1/RP1_rb1_tr10_std252_std100000.cfg  
python Simulator.py test/RP1/RP1_rb1_tr10_std63_std100000.cfg  
python Simulator.py test/RP1/RP1_rb252_tr10_std21_std252.cfg
python Simulator.py test/RP1/RP1_rb1_tr10_std21.cfg
python Simulator.py test/RP1/RP1_rb1_tr10_std21_std63.cfg   
python Simulator.py test/RP1/RP1_rb1_tr10_std504.cfg
python Simulator.py test/RP1/RP1_rb1_tr10_std63_std252.cfg     
python Simulator.py test/RP1/RP1_rb5_tr10_std21_std252.cfg
python Simulator.py test/RP1/RP1_rb1_tr10_std21_std100000.cfg  
python Simulator.py test/RP1/RP1_rb1_tr10_std252.cfg
python Simulator.py test/RP1/RP1_rb1_tr10_std63.cfg
python Simulator.py test/RP1/RP1_rb21_tr10_std21_std252.cfg    
python Simulator.py test/RP1/RP1_rb63_tr10_std21_std252.cfg
python Simulator.py test/6040_rb252.cfg

python Simulator.py test/AQRIX_rb100000.cfg

python Tools/ComputePerformance.py logs/RP1_rb1_tr10_std100000/returns.txt logs/RP1_rb1_tr10_std21_std252/returns.txt logs/RP1_rb1_tr10_std252_std100000/returns.txt logs/RP1_rb1_tr10_std63_std100000/returns.txt logs/RP1_rb252_tr10_std21_std252/returns.txt logs/RP1_rb1_tr10_std21/returns.txt logs/RP1_rb1_tr10_std21_std63/returns.txt logs/RP1_rb1_tr10_std504/returns.txt logs/RP1_rb1_tr10_std63_std252/returns.txt logs/RP1_rb5_tr10_std21_std252/returns.txt logs/RP1_rb1_tr10_std21_std100000/returns.txt logs/RP1_rb1_tr10_std252/returns.txt logs/RP1_rb1_tr10_std63/returns.txt logs/RP1_rb21_tr10_std21_std252/returns.txt logs/RP1_rb63_tr10_std21_std252/returns.txt logs/AQRIX_rb100000/returns.txt > logs/stats_RP_AQRIX.txt

python Tools/compute_correlation.py logs/RP1_rb1_tr10_std21/returns.txt logs/AQRIX_rb100000/returns.txt >> logs/stats_RP_AQRIX.txt
python Tools/compute_correlation.py logs/RP1_rb1_tr10_std100000/returns.txt logs/AQRIX_rb100000/returns.txt >> logs/stats_RP_AQRIX.txt
python Tools/compute_correlation.py logs/RP1_rb1_tr10_std21_std252/returns.txt logs/AQRIX_rb100000/returns.txt >> logs/stats_RP_AQRIX.txt
python Tools/compute_correlation.py logs/RP1_rb21_tr10_std21_std252/returns.txt logs/AQRIX_rb100000/returns.txt >> logs/stats_RP_AQRIX.txt
python Tools/compute_correlation.py logs/RP1_rb63_tr10_std21_std252/returns.txt logs/AQRIX_rb100000/returns.txt >> logs/stats_RP_AQRIX.txt
python Tools/compute_correlation.py logs/6040_rb252/returns.txt logs/AQRIX_rb100000/returns.txt >> logs/stats_RP_AQRIX.txt

python Tools/plot_series.py 0 logs/RP1_rb1_tr10_std21/returns.txt logs/RP1_rb1_tr10_std21_std252/returns.txt logs/RP1_rb1_tr10_std100000/returns.txt logs/RP1_rb21_tr10_std21_std252/returns.txt logs/6040_rb252/returns.txt logs/AQRIX_rb100000/returns.txt

python Tools/plot_series.py 0 logs/RP1_rb1_tr10_std21/returns.txt logs/AQRIX_rb100000/returns.txt logs/6040_rb252/returns.txt

python Tools/plot_series.py 1 logs/RP1_rb1_tr10_std21/leverage.txt logs/RP1_rb1_tr10_std21_std252/leverage.txt logs/RP1_rb1_tr10_std100000/leverage.txt

python Tools/plot_series.py 1 logs/RP1_rb1_tr10_std21/weights.txt logs/RP1_rb1_tr10_std21_std252/weights.txt logs/RP1_rb1_tr10_std100000/weights.txt logs/6040_rb252/weights.txt
