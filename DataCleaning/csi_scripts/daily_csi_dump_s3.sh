#!/bin/sh

cd /home/cvdev/data/csi/
python fetch_ftp.py canada.`date +%Y%m%d -d "yesterday"`.gz f-indices.`date +%Y%m%d -d "yesterday"`.gz funds.`date +%Y%m%d -d "yesterday"`.gz futures.`date +%Y%m%d -d "yesterday"`.gz indices.`date +%Y%m%d -d "yesterday"`.gz uk-stocks.`date +%Y%m%d -d "yesterday"`.gz us-stocks.`date +%Y%m%d -d "yesterday"`.gz > log_`date +%Y%m%d -d "yesterday"`

