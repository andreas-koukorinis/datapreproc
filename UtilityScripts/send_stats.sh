#!/usr/bin/bash
cd /home/cvdev/stratdev/
python UtilityScripts/send_stats.py test/mails/IVWAS_all_rb1_tr10_std21.cfg
python UtilityScripts/send_stats.py test/mails/TRERC_all_rb1_tr10_std63.2_corr252.30_maxiter100.cfg
python UtilityScripts/send_stats.py test/mails/TRMSHC_all_rb5_tr10_std63.5_corr252.30.cfg
