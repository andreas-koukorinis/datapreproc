#!/usr/bin/bash
cd /home/cvdev/stratdev/
python utility_scripts/send_stats.py ~/modeling/strategies/A1_TRVP_all_rb1_model1_rmsim_profile1.cfg
python utility_scripts/send_stats.py ~/modeling/strategies/A1_TRMSHC_all_rb5_model1_rmsim_profile1.cfg
python utility_scripts/send_stats.py ~/modeling/strategies/A1_TRERC_all_rb1_model1_rmsim_profile1.cfg
