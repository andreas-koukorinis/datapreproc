
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
mkdir $DATE
cd $DATE

#this grabs the list of files available on the server

#this is used to remove everything except xml files

#check to see if each file in the server's list exists 
SERVER=WEDBUSH_SFTP
sftp --password=$SSHPASS $USER@$WEDBUSH_SFTP << EOF
cd CVfif/$DATE/ 
get *.*
bye
exit
EOF

count=$(ls -l|wc -l)
if [ $count -eq 1 ]
then
    cd $OUT_DIR
    rm -r $DATE
    echo "Nothing to download today"
#mail -s "Wedbush: Nothing to download today" "sanchit.gupta@tworoads.co.in,debidatta.dwibedi@tworoads.co.in" < ""
fi
