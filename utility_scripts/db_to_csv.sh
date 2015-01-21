#!/bin/bash

host=<insert actual host>
db=daily_qplum
user=cvmysql
pass=<insert actual password>
 
for table in $(mysql -h$host -u$user -p$pass $db -Be "SHOW tables" | sed 1d); do
  echo "exporting $table.."
  mysql -h$host -u$user -p$pass $db -e "SELECT * FROM $table" | sed 's/\t/","/g;s/^/"/;s/$/"/;' > $table.csv
done
