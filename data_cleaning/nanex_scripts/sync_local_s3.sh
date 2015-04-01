for name in 201502*.nxc ; do 
    localsize=`ls -altr $name | awk '{print $5}'`; 
    ists3=`s3cmd ls s3://cvquantdata/nanex/rawdata/$name | wc -l`; 
    if [ $ists3 -eq 0 ] ; then 
	echo "#$name not present on s3"; 
	s3cmd put $name s3://cvquantdata/nanex/rawdata/ ; 
    else
	s3size=`s3cmd ls s3://cvquantdata/nanex/rawdata/$name |awk '{print $3}'`; 
	echo "sizes localsize = $localsize ; s3size = $s3size"; 
	if [ $localsize == $s3size ] ; then 
	    echo "#deleting $name"; 
	    rm -f $name; 
	else
	    if [ $localsize -gt $s3size ] ; then 
		echo "Localsize is greater than s3size. we should sync it to s3!"
		s3cmd put $name s3://cvquantdata/nanex/rawdata/ ; 
	    else
		echo "Localsize is less than s3size."
	    fi ;
	fi ; 
    fi ; 
done
