DATE=`date +%Y-%m-%d`
mysqldump --host=fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com -u cvmysql -pfixedcvincome daily_qplum > /apps/data/backup_db/daily_qplum_$DATE.sql