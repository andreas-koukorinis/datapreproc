import ConfigParser
from datetime import date
import sys
import os
import luigi
from luigi.contrib.ftp import RemoteTarget
from luigi.s3 import S3Target, S3Client

#data_path = '/apps/data/csi/'
data_path = '/home/debi/data/'
csi_ftp_server = 'ftp.csidata.com'
csi_ftp_port = 21
s3_cfg = '~/.s3cfg'

global csi_ftp_username, csi_ftp_password, aws_access_key, aws_secret_key

def load_credentials():
    """
    Read credential and configuration files to get required credentials
    """
    global csi_ftp_username, csi_ftp_password, aws_access_key, aws_secret_key
    
    try:
        with open('/spare/local/credentials/csi_ftp.txt') as f:
            csi_ftp_username, csi_ftp_password = f.readlines()[0].strip().split(':')
    except IOError:
        sys.exit('No CSI FTP credentials file found')

    s3_config = ConfigParser.ConfigParser()
    s3_config.readfp(open(os.path.expanduser(s3_cfg), 'r'))
    aws_access_key = s3_config.get('default', 'access_key')
    aws_secret_key = s3_config.get('default', 'secret_key')
    
class CheckFTP_canada(luigi.ExternalTask):
    """
    Task to check CSI FTP for today's data
    """
    date = luigi.DateParameter()
    def output(self):
        return RemoteTarget(self.date.strftime('canada.%Y%m%d.gz'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

class FetchCSI_canada(luigi.Task):
    """
    Task that downloads canada data from CSI FTP in given folder
    """
    date = luigi.DateParameter()    
    def requires(self):
        return CheckFTP_canada(self.date)

    def output(self):
        return luigi.LocalTarget(data_path+self.date.strftime('canada.%Y%m%d.gz'))

    def run(self):
        f = self.input()
        f.get(self.output().path)

class CheckFTP_findices(luigi.ExternalTask):
    """
    Task to check CSI FTP for today's data
    """
    date = luigi.DateParameter()
    def output(self):
        return RemoteTarget(self.date.strftime('f-indices.%Y%m%d.gz'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

class FetchCSI_findices(luigi.Task):
    """
    Task that downloads f-indices data from CSI FTP in given folder
    """
    date = luigi.DateParameter()    
    def requires(self):
        return CheckFTP_findices(self.date)

    def output(self):
        return luigi.LocalTarget(data_path+self.date.strftime('f-indices.%Y%m%d.gz'))

    def run(self):
        f = self.input()
        f.get(self.output().path)

class CheckFTP_funds(luigi.ExternalTask):
    """
    Task to check CSI FTP for today's data
    """
    date = luigi.DateParameter()
    def output(self):
        return RemoteTarget(self.date.strftime('funds.%Y%m%d.gz'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

class FetchCSI_funds(luigi.Task):
    """
    Task that downloads funds data from CSI FTP in given folder
    """
    date = luigi.DateParameter()    
    def requires(self):
        return CheckFTP_funds(self.date)

    def output(self):
        return luigi.LocalTarget(data_path+self.date.strftime('funds.%Y%m%d.gz'))

    def run(self):
        f = self.input()
        f.get(self.output().path)

class CheckFTP_futures(luigi.ExternalTask):
    """
    Task to check CSI FTP for today's data
    """
    date = luigi.DateParameter()
    def output(self):
        return RemoteTarget(self.date.strftime('futures.%Y%m%d.gz'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

class FetchCSI_futures(luigi.Task):
    """
    Task that downloads futures data from CSI FTP in given folder
    """
    date = luigi.DateParameter()    
    def requires(self):
        return CheckFTP_futures(self.date)

    def output(self):
        return luigi.LocalTarget(data_path+self.date.strftime('futures.%Y%m%d.gz'))

    def run(self):
        f = self.input()
        f.get(self.output().path)

class CheckFTP_indices(luigi.ExternalTask):
    """
    Task to check CSI FTP for today's data
    """
    date = luigi.DateParameter()
    def output(self):
        return RemoteTarget(self.date.strftime('indices.%Y%m%d.gz'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

class FetchCSI_indices(luigi.Task):
    """
    Task that downloads indices data from CSI FTP in given folder
    """
    date = luigi.DateParameter()    
    def requires(self):
        return CheckFTP_indices(self.date)

    def output(self):
        return luigi.LocalTarget(data_path+self.date.strftime('indices.%Y%m%d.gz'))

    def run(self):
        f = self.input()
        f.get(self.output().path)

class CheckFTP_ukstocks(luigi.ExternalTask):
    """
    Task to check CSI FTP for today's data
    """
    date = luigi.DateParameter()
    def output(self):
        return RemoteTarget(self.date.strftime('uk-stocks.%Y%m%d.gz'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

class FetchCSI_ukstocks(luigi.Task):
    """
    Task that downloads uk-stocks data from CSI FTP in given folder
    """
    date = luigi.DateParameter()    
    def requires(self):
        return CheckFTP_ukstocks(self.date)

    def output(self):
        return luigi.LocalTarget(data_path+self.date.strftime('uk-stocks.%Y%m%d.gz'))

    def run(self):
        f = self.input()
        f.get(self.output().path)

class CheckFTP_usstocks(luigi.ExternalTask):
    """
    Task to check CSI FTP for today's data
    """
    date = luigi.DateParameter()
    def output(self):
        return RemoteTarget(self.date.strftime('us-stocks.%Y%m%d.gz'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

class FetchCSI_usstocks(luigi.Task):
    """
    Task that downloads us-stocks data from CSI FTP in given folder
    """
    date = luigi.DateParameter()    
    def requires(self):
        return CheckFTP_usstocks(self.date)

    def output(self):
        return luigi.LocalTarget(data_path+self.date.strftime('us-stocks.%Y%m%d.gz'))

    def run(self):
        f = self.input()
        f.get(self.output().path)

class CheckFTP_briese(luigi.ExternalTask):
    """
    Task to check CSI FTP for today's data
    """
    date = luigi.DateParameter()
    def output(self):
        return RemoteTarget(self.date.strftime('Briese%Y%m%d.txt'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

class FetchCSI_briese(luigi.Task):
    """
    Task that downloads us-stocks data from CSI FTP in given folder
    """
    date = luigi.DateParameter()    
    def requires(self):
        return CheckFTP_briese(self.date)

    def output(self):
        return luigi.LocalTarget(data_path+self.date.strftime('Briese%Y%m%d.txt'))

    def run(self):
        f = self.input()
        f.get(self.output().path)

class CheckFTP_cftc(luigi.ExternalTask):
    """
    Task to check CSI FTP for today's data
    """
    date = luigi.DateParameter()
    def output(self):
        return RemoteTarget(self.date.strftime('CFTC%Y%m%d.txt'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

class FetchCSI_cftc(luigi.Task):
    """
    Task that downloads CFTC data from CSI FTP in given folder
    """
    date = luigi.DateParameter()    
    def requires(self):
        return CheckFTP_cftc(self.date)

    def output(self):
        return luigi.LocalTarget(data_path+self.date.strftime('CFTC%Y%m%d.txt'))

    def run(self):
        f = self.input()
        f.get(self.output().path)

class CheckFTP_econ(luigi.ExternalTask):
    """
    Task to check CSI FTP for today's data
    """
    date = luigi.DateParameter()
    def output(self):
        return RemoteTarget(self.date.strftime('Econ%Y%m%d.txt'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

class FetchCSI_econ(luigi.Task):
    """
    Task that downloads Econ data from CSI FTP in given folder
    """
    date = luigi.DateParameter()    
    def requires(self):
        return CheckFTP_econ(self.date)

    def output(self):
        return luigi.LocalTarget(data_path+self.date.strftime('Econ%Y%m%d.txt'))

    def run(self):
        f = self.input()
        f.get(self.output().path)

class FetchCSI_all(luigi.Task):
    """
    Task to fetch all CSI data
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_briese(self.date), FetchCSI_canada(self.date), \
               FetchCSI_cftc(self.date), FetchCSI_econ(self.date), \
               FetchCSI_findices(self.date), FetchCSI_indices(self.date), \
               FetchCSI_funds(self.date), FetchCSI_futures(self.date), \
               FetchCSI_ukstocks(self.date), FetchCSI_usstocks(self.date)

class PutInS3_canada(luigi.Task):
    """
    Task to put canada data in S3
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_canada(self.date)

    def output(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        return S3Target(self.date.strftime('s3://cvquantdata/csi/rawdata/canada.%Y%m%d.gz'), client=s3_client)
    
    def run(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        s3_client.put(self.input().path, self.output().path)


class PutInS3_all(luigi.Task):
    """
    Task to put all CSI data in S3
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return PutInS3_briese(self.date), PutInS3_canada(self.date), \
               PutInS3_cftc(self.date), PutInS3_econ(self.date), \
               PutInS3_findices(self.date), PutInS3_indices(self.date), \
               PutInS3_funds(self.date), PutInS3_futures(self.date), \
               PutInS3_ukstocks(self.date), PutInS3_usstocks(self.date)

class AllReports(luigi.Task):
    """
    Task to trigger all base tasks
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        yield FetchCSI_all(self.date), PutInS3_all(self.date)

if __name__ == '__main__':
    load_credentials()
    luigi.run(main_task_cls=AllReports)