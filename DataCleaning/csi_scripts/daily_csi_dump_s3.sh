#!/bin/sh

cd /home/cvdev/stratdev/DataCleaning/csi_scripts/
python fetch_ftp.py canada.`date +%Y%m%d -d "yesterday"`.gz f-indices.`date +%Y%m%d -d "yesterday"`.gz funds.`date +%Y%m%d -d "yesterday"`.gz futures.`date +%Y%m%d -d "yesterday"`.gz indices.`date +%Y%m%d -d "yesterday"`.gz uk-stocks.`date +%Y%m%d -d "yesterday"`.gz us-stocks.`date +%Y%m%d -d "yesterday"`.gz > log_`date +%Y%m%d -d "yesterday"`

cp canada.`date +%Y%m%d -d "yesterday"`.gz /home/cvdev/data/csi/
cp f-indices.`date +%Y%m%d -d "yesterday"`.gz /home/cvdev/data/csi/
cp funds.`date +%Y%m%d -d "yesterday"`.gz /home/cvdev/data/csi/
cp futures.`date +%Y%m%d -d "yesterday"`.gz /home/cvdev/data/csi/
cp indices.`date +%Y%m%d -d "yesterday"`.gz /home/cvdev/data/csi/
cp uk-stocks.`date +%Y%m%d -d "yesterday"`.gz /home/cvdev/data/csi/
cp us-stocks.`date +%Y%m%d -d "yesterday"`.gz /home/cvdev/data/csi/
