cd /home/cvdev/datapreproc
fab -R workers update_code
fab -R workers update_modeling
fab -R workers update_stratdev
fab -R workers restart_worker