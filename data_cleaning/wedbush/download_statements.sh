
USER='cvfif'
WEDBUSH_SFTP='ftp.wedbushfutures.com'
if [ "$#" -ne 1 ]; then
    DATE=$(date +%Y%m%d --date="yesterday")
    echo $DATE
else
    DATE=$1
    echo $DATE
fi
OUT_DIR="/apps/wedbush/"
cd $OUT_DIR
mkdir -p $DATE
cd $DATE

/usr/local/bin/sshpass -e sftp -oBatchMode=no -b - $USER@$WEDBUSH_SFTP  << !
  cd CVfif/$DATE/ 
  get *.*
  bye
!

count=$(ls -l|wc -l)
if [ $count -eq 1 ]
then
    cd $OUT_DIR
    rm -r $DATE
    echo "Probably something wrong" | mail -s "Wedbush: No statement to download today $DATE" "sanchit.gupta@tworoads.co.in,debidatta.dwibedi@tworoads.co.in"
fi
