#!/usr/bin/bash
cd /home/cvdev/stratdev/
python utility_scripts/send_stats.py ~/modeling/mails/strategies/A1_TRVP_all_rb1_model1_rmp_profile1.cfg
python utility_scripts/send_stats.py ~/modeling/mails/strategies/A1_TRMSHC_all_rb5_model1_rmp_profile1.cfg
python utility_scripts/send_stats.py ~/modeling/mails/strategies/A1_TRERC_all_rb1_model1_rmp_profile1.cfg
python utility_scripts/send_stats.py ~/modeling/mails/strategies/A1_MVO_all_rb1_model1_rmp_profile1.cfg
python utility_scripts/send_stats.py ~/modeling/mails/strategies/A_volcomb2_0.5_0.5_MVO_all_rb1_model1_TRMSHC_all_rb5_model1_rmp_profile1.cfg
python utility_scripts/send_stats.py ~/modeling/mails/strategies/A_volcomb4_0.25_0.25_0.25_0.25_TRVP_TRMSHC_TRERC_MVO_TRVP_all_rb1_model1_TRMSHC_all_rb5_model1_MVO_all_rb1_model1_MVO_all_rb1_model1_rmp_profile1.cfg
