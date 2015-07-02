#!/usr/bin/bash

uname_upass=`cat /spare/local/credentials/write_credentials.txt`;
uname_upass_arr=(${uname_upass//:/ })
uname=${uname_upass_arr[0]}
upass=${uname_upass_arr[1]}
echo $uname
echo $upass

days_to_keep=7
echo $days_to_keep
database_dump_path='/spare/local/database_dump/'
echo $database_dump_path
database_to_backup=( daily_qplum fixed_income live_trading risk_parity simula webapp workbench dtcc_sdr fed_data )
echo ${database_to_backup[2]}
today_date=`date +%Y%m%d`
echo $today_date
older_date=`date -d "$days_to_keep day ago" +%Y%m%d`
echo $older_date
for database in ${database_to_backup[@]}
do
  echo $database
  today_dump_file=$database_dump_path$database'_'$today_date'.sql'
  echo $today_dump_file
  older_dump_file=$database_dump_path$database'_'$older_date'.sql'
  echo $older_dump_file

  # Dump this database
  `mysqldump -h fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com -u $uname -p$upass $database > $today_dump_file` 

  # Remove old dumps
  `rm -rf $older_dump_file`

  # Notify
  echo $database' backed up successfully'
done

