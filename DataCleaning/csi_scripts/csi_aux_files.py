#!/usr/bin/env python

import ftplib
import sys
import os
import subprocess

#Open ftp connection
ftp = ftplib.FTP('ftp.csidata.com', 'circulumvite','sv13tjmd')
ftp_files = ftp.nlst()
req_files = filter(lambda x:x.startswith(("Econ", "CFTC","Briese")), ftp_files)
req_files = filter(lambda x:x.endswith("txt"), req_files)
#print req_files
for filename in req_files:
    is_in_s3 = subprocess.Popen(['s3cmd', 'ls', 's3://cvquantdata/csi/rawdata/'+filename], stdout=subprocess.PIPE, stderr=subprocess.STDOUT).communicate()[0]
    if len(is_in_s3) <= 0:
        try:
            print 'Downloading...%s'%filename
            gFile = open(filename, "wb")
            ftp.retrbinary('RETR '+filename, gFile.write)
            gFile.close()
            try:
                subprocess.call(['s3cmd', 'put',filename,'s3://cvquantdata/csi/rawdata/'])
            except:
                print 'ERROR: Could not upload %s to s3 bucket'%filename
            try:
                subprocess.call(['mv', filename,'/apps/data/csi/'])
            except:
                print 'ERROR: Could not move %s to /apps/data/csi/'%filename
        except:
            print 'Could not download %s'%filename
ftp.quit()

