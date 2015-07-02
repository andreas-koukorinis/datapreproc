#!/usr/bin/bash

uname_upass=`cat /spare/local/credentials/write_credentials.txt`;
uname_upass_arr=(${uname_upass//:/ })
uname=${uname_upass_arr[0]}
upass=${uname_upass_arr[1]}

days_to_keep=7
database_dump_path='/spare/local/database_dumps/'
database_to_backup=( daily_qplum live_trading risk_parity simula webapp workbench )
today_date=`date +%Y%m%d`
older_date=`date -d "$days_to_keep day ago" +%Y%m%d`
for database in ${database_to_backup[@]}
do
  # Notify
  echo 'backing up '$database

  today_dump_file=$database_dump_path$database'_'$today_date'.sql'
  older_dump_file=$database_dump_path$database'_'$older_date'.sql'

  # Dump this database
  `mysqldump -h fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com -u $uname -p$upass $database > $today_dump_file` 

  # Remove old dumps
  `rm -rf $older_dump_file`

  # Notify
  echo $database' backed up successfully'
done

