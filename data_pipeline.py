import ConfigParser
from datetime import date
import json
import os
import sys
import traceback
import urllib2
import luigi
from luigi.contrib.ftp import RemoteTarget
from luigi.s3 import S3Target, S3Client
from data_cleaning.csi_scripts.daily_update import push_file_to_db

data_path = '/apps/data/csi/'
log_path = '/home/deedee/logs/'
#data_path = '/home/debi/data/'
#log_path = '/home/debi/logs/'
csi_ftp_server = 'ftp.csidata.com'
csi_ftp_port = 21
s3_cfg = '/home/cvdev/.s3cfg'

global csi_ftp_username, csi_ftp_password, aws_access_key, aws_secret_key

class QPlumTask(luigi.Task):
    """
    Custom task that also sends error messages to Slack.
    Other custom functionality can be added in this class.
    All tasks in our pipeline will inherit this.
    """
    def on_failure(self, exception):
        """
        Override for custom error handling.

        This method gets called if an exception is raised in :py:meth:`run`.
        Return value of this method is json encoded and sent to the scheduler as the `expl` argument. Its string representation will be used as the body of the error email sent out if any.

        Default behavior is to return a string representation of the stack trace.
        """
        traceback_string = traceback.format_exc()
        payload = {"channel": "#datapipeline-errors", "username": "Luigi", "text": traceback_string}
        req = urllib2.Request(' https://hooks.slack.com/services/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')
        response = urllib2.urlopen(req, json.dumps(payload))
        return "Runtime error:\n%s" % traceback_string

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

class FetchCSI_canada(QPlumTask):
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

class FetchCSI_findices(QPlumTask):
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

class FetchCSI_funds(QPlumTask):
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

class FetchCSI_futures(QPlumTask):
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

class FetchCSI_indices(QPlumTask):
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

class FetchCSI_ukstocks(QPlumTask):
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

class FetchCSI_usstocks(QPlumTask):
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

class FetchCSI_briese(QPlumTask):
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

class FetchCSI_cftc(QPlumTask):
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

class FetchCSI_econ(QPlumTask):
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

class PutInS3_briese(QPlumTask):
    """
    Task to put briese data in S3
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_briese(self.date)

    def output(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        return S3Target(self.date.strftime('s3://cvquantdata/csi/rawdata/Briese%Y%m%d.txt'), client=s3_client)
    
    def run(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        s3_client.put(self.input().path, self.output().path)

class PutInS3_canada(QPlumTask):
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

class PutInS3_cftc(QPlumTask):
    """
    Task to put cftc data in S3
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_cftc(self.date)

    def output(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        return S3Target(self.date.strftime('s3://cvquantdata/csi/rawdata/CFTC%Y%m%d.txt'), client=s3_client)
    
    def run(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        s3_client.put(self.input().path, self.output().path)

class PutInS3_econ(QPlumTask):
    """
    Task to put econ data in S3
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_econ(self.date)

    def output(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        return S3Target(self.date.strftime('s3://cvquantdata/csi/rawdata/Econ%Y%m%d.txt'), client=s3_client)
    
    def run(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        s3_client.put(self.input().path, self.output().path)

class PutInS3_findices(QPlumTask):
    """
    Task to put findices data in S3
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_findices(self.date)

    def output(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        return S3Target(self.date.strftime('s3://cvquantdata/csi/rawdata/f-indices.%Y%m%d.gz'), client=s3_client)
    
    def run(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        s3_client.put(self.input().path, self.output().path)

class PutInS3_funds(QPlumTask):
    """
    Task to put funds data in S3
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_funds(self.date)

    def output(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        return S3Target(self.date.strftime('s3://cvquantdata/csi/rawdata/funds.%Y%m%d.gz'), client=s3_client)
    
    def run(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        s3_client.put(self.input().path, self.output().path)

class PutInS3_futures(QPlumTask):
    """
    Task to put futures data in S3
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_futures(self.date)

    def output(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        return S3Target(self.date.strftime('s3://cvquantdata/csi/rawdata/futures.%Y%m%d.gz'), client=s3_client)
    
    def run(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        s3_client.put(self.input().path, self.output().path)

class PutInS3_indices(QPlumTask):
    """
    Task to put indices data in S3
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_indices(self.date)

    def output(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        return S3Target(self.date.strftime('s3://cvquantdata/csi/rawdata/indices.%Y%m%d.gz'), client=s3_client)
    
    def run(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        s3_client.put(self.input().path, self.output().path)

class PutInS3_usstocks(QPlumTask):
    """
    Task to put usstocks data in S3
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_usstocks(self.date)

    def output(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        return S3Target(self.date.strftime('s3://cvquantdata/csi/rawdata/us-stocks.%Y%m%d.gz'), client=s3_client)
    
    def run(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        s3_client.put(self.input().path, self.output().path)

class PutInS3_ukstocks(QPlumTask):
    """
    Task to put ukstocks data in S3
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_ukstocks(self.date)

    def output(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        return S3Target(self.date.strftime('s3://cvquantdata/csi/rawdata/uk-stocks.%Y%m%d.gz'), client=s3_client)
    
    def run(self):
        s3_client = S3Client(aws_access_key, aws_secret_key)
        s3_client.put(self.input().path, self.output().path)

class PutCSIinDB_futures(QPlumTask):
    """
    Task to put CSI data in database
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_futures(self.date)

    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('PutCSIinDB_futures.%Y%m%d.SUCCESS'))

    def run(self):
        futures = ['TU', 'FV', 'TY', 'US', 'NK', 'NIY', 'ES', 'SP', 'EMD', 'NQ', 'YM', 'AD',\
                   'BP', 'CD', 'CU1', 'JY', 'MP', 'NE2', 'SF', 'GC', 'SI', 'HG', 'PL', 'PA',\
                   'LH', 'ZW', 'ZC', 'ZS', 'ZM', 'ZL', 'EBS', 'EBM', 'EBL', 'SXE', 'FDX', \
                   'SMI', 'SXF', 'CGB', 'FFI', 'FLG', 'AEX', 'KC', 'CT', 'CC', 'SB', 'JTI', \
                   'JGB', 'JNI', 'SIN', 'SSG', 'HCE', 'HSI', 'ALS', 'YAP', 'MFX', 'KOS', 'VX', \
                   'JPYUSD', 'CADUSD', 'GBPUSD', 'EURUSD', 'AUDUSD', 'NZDUSD', 'CHFUSD', 'SEKUSD',\
                   'NOKUSD', 'TRYUSD', 'MXNUSD', 'ZARUSD', 'ILSUSD', 'SGDUSD', 'HKDUSD', 'TWDUSD',\
                   'INRUSD', 'BRLUSD']
        if push_file_to_db(self.input().path, futures):
            with open(self.output().path,'w') as f:
                f.write("Successfully put CSI data in DB for futures")

class PutCSIinDB_funds(QPlumTask):
    """
    Task to put CSI data in database
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_funds(self.date)

    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('PutCSIinDB_funds.%Y%m%d.SUCCESS'))

    def run(self):
        funds = ['AQRIX','AQMIX','QGMIX','SRPFX','ABRZX','VBLTX','VTSMX']
        if push_file_to_db(self.input().path, funds):
            with open(self.output().path,'w') as f:
                f.write("Successfully put CSI data in DB for funds")

class PutCSIinDB_indices(QPlumTask):
    """
    Task to put CSI data in database
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_indices(self.date)

    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('PutCSIinDB_indices.%Y%m%d.SUCCESS'))

    def run(self):
        indices = ['VIX','TCMP','SPX','NYA','COMP']
        if push_file_to_db(self.input().path, indices):
            with open(self.output().path,'w') as f:
                f.write("Successfully put CSI data in DB for indices")

class PutCSIinDB_findices(QPlumTask):
    """
    Task to put CSI data in database
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_findices(self.date)

    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('PutCSIinDB_findices.%Y%m%d.SUCCESS'))

    def run(self):
        findices = ['XMSW','^AXJO','^BSES','^BVSP','^FTSE','^GDAX','^GU15','^MXX',\
                    '^N500','^N150','^NZ50','^SSMI','^XMSC']
        if push_file_to_db(self.input().path, findices):
            with open(self.output().path,'w') as f:
                f.write("Successfully put CSI data in DB for f-indices")

class PutCSIinDB_usstocks(QPlumTask):
    """
    Task to put CSI data in database
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_usstocks(self.date)

    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('PutCSIinDB_usstocks.%Y%m%d.SUCCESS'))

    def run(self):
        usstocks = ['BND', 'BNDX', 'IEMG', 'LQD', 'MUB', 'SHV', 'TIP', 'VBR', 'VEA', 'VIG', 'VNQ',\
                    'VOE', 'VT', 'VTI', 'VTIP', 'VTV', 'VWO', 'VWOB', 'VXUS']
        if push_file_to_db(self.input().path, usstocks):
            with open(self.output().path,'w') as f:
                f.write("Successfully put CSI data in DB for us-stocks")

class FetchCSI_all(QPlumTask):
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

class PutInS3_all(QPlumTask):
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

class PutCSIinDB_all(QPlumTask):
    """
    Task to put all CSI data in database
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return PutCSIinDB_futures(self.date), PutCSIinDB_usstocks(self.date),\
               PutCSIinDB_findices(self.date), PutCSIinDB_indices(self.date),\
               PutCSIinDB_funds(self.date)

class AllReports(QPlumTask):
    """
    Task to trigger all base tasks
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_all(self.date), PutInS3_all(self.date), PutCSIinDB_futures(self.date)

if __name__ == '__main__':
    load_credentials()
    luigi.run(main_task_cls=AllReports)
