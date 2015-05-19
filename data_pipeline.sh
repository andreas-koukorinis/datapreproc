#!/bin/sh
source /apps/pythonenv/py2.7/bin/activate
python /home/cvdev/datapreproc/data_pipeline.py --date $(date +%Y-%m-%d --date="yesterday")
