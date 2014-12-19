#!/usr/bin/bash

dir1_="/apps/data/csi/history_part1/$1"
dir2_="/apps/data/csi/history_part2/$1"
if [ -d "$dir1_" ]; then
    dir=$dir1_
else
    dir=$dir2_  
fi

cd $dir
ls
#exit 0
rm $1__0000.csv
rm $1_0000.csv

>/home/cvdev/combination/stratdev/DataCleaning/Data/$1-oi1.csv
>/home/cvdev/combination/stratdev/DataCleaning/Data/$1-oi2.csv

for dd_ in `cat *csv | sort -g | awk -F, '{ print $1 }' | uniq `;
do
    echo `grep $dd_ *csv | sort -k7,7 -rg -t, | head -n1` >> /home/cvdev/combination/stratdev/DataCleaning/Data/$1-oi1.csv
done

exit 0
for dd_ in `cat *csv | sort -g | awk -F, '{ print $1 }' | uniq `;
do
    firstfile_=`grep $dd_ *csv | sort -k7,7 -rg -t,|awk -F: '{ print $1 }' | head -n1`
    firstnum_=`echo $firstfile_ | sed 's/[^0-9]//g'`
    year_=${firstnum_:0:2}
    month_=${firstnum_:2:4}
    year_=${year_#0} # strip leading 0
    month_=${month_#0}
    if [ $year_ -lt 50 ]; then
        year_=$((year_+2000))
    else
        year_=$((year_+1900))
    fi
    firstnum_=$((year_*100+month_))
    for file_ in `grep $dd_ *csv | sort -k7,7 -rg -t,|awk -F: '{ print $1 }' | tail -n +2`;
    do
        num_=`echo $file_ | sed 's/[^0-9]//g'`
        year_=${num_:0:2}
        month_=${num_:2:4}
        year_=${year_#0}
        month_=${month_#0}
        if [ $year_ -lt 50 ]; then
            year_=$((year_+2000))
        else
            year_=$((year_+1900))
        fi
        num_=$((year_*100+month_))
        if [ $num_ -gt $firstnum_ ]; then
            echo $file_,`grep $dd_ $file_` >> /home/cvdev/combination/stratdev/DataCleaning/Data/$1-oi2.csv
            break
        fi
    done
done
