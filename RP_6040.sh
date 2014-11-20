#!/bin/sh

python Simulator.py test/RP/RP_rb1_tr10_std100000.cfg
python Simulator.py test/RP/RP_rb1_tr10_std21_std252.cfg  
python Simulator.py test/RP/RP_rb1_tr10_std252_std100000.cfg  
python Simulator.py test/RP/RP_rb1_tr10_std63_std100000.cfg  
python Simulator.py test/RP/RP_rb252_tr10_std21_std252.cfg
python Simulator.py test/RP/RP_rb1_tr10_std21.cfg
python Simulator.py test/RP/RP_rb1_tr10_std21_std63.cfg   
python Simulator.py test/RP/RP_rb1_tr10_std504.cfg
python Simulator.py test/RP/RP_rb1_tr10_std63_std252.cfg     
python Simulator.py test/RP/RP_rb5_tr10_std21_std252.cfg
python Simulator.py test/RP/RP_rb1_tr10_std21_std100000.cfg  
python Simulator.py test/RP/RP_rb1_tr10_std252.cfg
python Simulator.py test/RP/RP_rb1_tr10_std63.cfg
python Simulator.py test/RP/RP_rb21_tr10_std21_std252.cfg    
python Simulator.py test/RP/RP_rb63_tr10_std21_std252.cfg

python Simulator.py test/6040/6040_rb1.cfg
python Simulator.py test/6040/6040_rb21.cfg
python Simulator.py test/6040/6040_rb252.cfg
python Simulator.py test/6040/6040_rb5.cfg
python Simulator.py test/6040/6040_rb63.cfg

python Tools/ComputePerformance.py logs/RP_rb1_tr10_std100000/returns.txt logs/RP_rb1_tr10_std21_std252/returns.txt logs/RP_rb1_tr10_std252_std100000/returns.txt logs/RP_rb1_tr10_std63_std100000/returns.txt logs/RP_rb252_tr10_std21_std252/returns.txt logs/RP_rb1_tr10_std21/returns.txt logs/RP_rb1_tr10_std21_std63/returns.txt logs/RP_rb1_tr10_std504/returns.txt logs/RP_rb1_tr10_std63_std252/returns.txt logs/RP_rb5_tr10_std21_std252/returns.txt logs/RP_rb1_tr10_std21_std100000/returns.txt logs/RP_rb1_tr10_std252/returns.txt logs/RP_rb1_tr10_std63/returns.txt logs/RP_rb21_tr10_std21_std252/returns.txt logs/RP_rb63_tr10_std21_std252/returns.txt logs/6040_rb1/returns.txt logs/6040_rb21/returns.txt logs/6040_rb252/returns.txt logs/6040_rb5/returns.txt logs/6040_rb63/returns.txt > logs/stats_RP_6040.txt

python Tools/plot_series.py 0 logs/RP_rb1_tr10_std21/returns.txt logs/RP_rb1_tr10_std21_std252/returns.txt logs/RP_rb1_tr10_std100000/returns.txt logs/RP_rb21_tr10_std21_std252/returns.txt logs/6040_rb252/returns.txt logs/6040_rb63/returns.txt

python Tools/plot_series.py 1 logs/RP_rb1_tr10_std21/leverage.txt logs/RP_rb1_tr10_std21_std252/leverage.txt logs/RP_rb1_tr10_std100000/leverage.txt logs/RP_rb21_tr10_std21_std252/leverage.txt

python Tools/plot_series.py 1 logs/RP_rb1_tr10_std21/weights.txt logs/RP_rb1_tr10_std21_std252/weights.txt logs/RP_rb1_tr10_std100000/weights.txt logs/RP_rb21_tr10_std21_std252/weights.txt

