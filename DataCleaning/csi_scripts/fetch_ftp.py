#!/usr/bin/env python

import ftplib
import sys
import os
from subprocess import call

#Open ftp connection
ftp = ftplib.FTP('ftp.csidata.com', 'circulumvite','sv13tjmd')

if len(sys.argv) <= 1:
    #List the files in the current directory
    ftp_files = ftp.nlst()
    #print 'FTP FILES:',ftp_files

    our_files = [f for f in os.listdir('.') if os.path.isfile(f)]

    #print ftp_files,our_files
    #Get the files not present in out directory

    for filename in ftp_files:
        if filename not in our_files:
            try:
                print 'Downloading...%s'%filename
                gFile = open(filename, "wb")
                ftp.retrbinary('RETR '+filename, gFile.write)
                gFile.close()
            except:
                print 'Could not download %s'%filename

else:
    ftp_files = ftp.nlst()
    files = sys.argv[1:]
    for filename in files:
        if filename in ftp_files:
            try:
                print 'Downloading...%s'%filename
                gFile = open(filename, "wb")
                ftp.retrbinary('RETR '+filename, gFile.write)
                gFile.close()
                try:
                    call(['s3cmd', 'put',filename,'s3://cvquantdata/csi/rawdata/'])
                except:
                    print 'ERROR: Could not upload %s to s3 bucket'%filename
            except:
                print 'ERROR: Could not download %s'%filename 
        else:
            print 'ERROR: File %s not present on FTP'%filename
ftp.quit()

