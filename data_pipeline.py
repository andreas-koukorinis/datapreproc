import ConfigParser
from datetime import date, timedelta, datetime
import json
import os
import subprocess
import sys
import traceback
import urllib2
import luigi
import MySQLdb
import pandas as pd
from luigi.contrib.ftp import RemoteTarget
from luigi.s3 import S3Target, S3Client
from data_cleaning.csi_scripts.daily_update import push_file_to_db, update_benchmarks
from data_cleaning.csi_scripts.update_last_trading_day import update_last_trading_day
from data_cleaning.quandl_scripts.daily_update_quandl import daily_update_quandl
from data_cleaning.wedbush.dump_statement_data import dump_statement_data
from data_cleaning.wedbush.dump_eod_estimated_data import dump_eod_estimated_data
from data_cleaning.wedbush.reconcile import reconcile
from data_cleaning.wedbush.inventory_management import manage_inventory
from data_cleaning.wedbush.pnl_demystification import demystify_pnl
sys.path.append('/home/cvdev/stratdev/')
from utility_scripts.generate_orders import get_desired_positions
from tasks import schedule_send_stats, schedule_workbench_update

data_path = '/apps/data/csi/'
log_path = '/apps/logs/luigi/'
wedbush_path = '/apps/wedbush/'
csi_ftp_server = 'ftp.csidata.com'
csi_ftp_port = 21
s3_cfg = '/home/cvdev/.s3cfg'

os.environ["HOME"] = "/home/cvdev/"

global csi_ftp_username, csi_ftp_password, aws_access_key, aws_secret_key, db, db_cursor

def db_connect():
    global db, db_cursor
    try:
        with open('/spare/local/credentials/readonly_credentials.txt') as f:
            credentials = [line.strip().split(':') for line in f.readlines()]
    except IOError:
        sys.exit('No credentials file found')
    try:
        for user_id,password in credentials:
            db = MySQLdb.connect(host='fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com', user=user_id, passwd=password, db='workbench')
            db_cursor = db.cursor(MySQLdb.cursors.DictCursor) 
    except MySQLdb.Error:
        sys.exit("Error in DB connection")

def db_close():
    cursor = db.cursor()
    cursor.close()
    del cursor
    db.close()

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
        s = "*Error in %s Task*\n"%(self.__class__.__name__)
        s += traceback_string
        payload = {"channel": "#datapipeline-errors", "username": "Luigi", "text": s}
        #req = urllib2.Request('https://hooks.slack.com/services/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')
        #response = urllib2.urlopen(req, json.dumps(payload))
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

class PutCsiInDb_futures(QPlumTask):
    """
    Task to put CSI data in database
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_futures(self.date)

    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('PutCsiInDb_futures.%Y%m%d.SUCCESS'))

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

class PutCsiInDb_funds(QPlumTask):
    """
    Task to put CSI data in database
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_funds(self.date)

    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('PutCsiInDb_funds.%Y%m%d.SUCCESS'))

    def run(self):
        funds = ['AQRIX','AQMIX','QGMIX','SRPFX','ABRZX','VBLTX','VTSMX']
        if push_file_to_db(self.input().path, funds):
            with open(self.output().path,'w') as f:
                f.write("Successfully put CSI data in DB for funds")

class PutCsiInDb_indices(QPlumTask):
    """
    Task to put CSI data in database
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_indices(self.date)

    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('PutCsiInDb_indices.%Y%m%d.SUCCESS'))

    def run(self):
        indices = ['VIX','TCMP','SPX','NYA','COMP']
        if push_file_to_db(self.input().path, indices):
            with open(self.output().path,'w') as f:
                f.write("Successfully put CSI data in DB for indices")

class PutCsiInDb_findices(QPlumTask):
    """
    Task to put Quandl data in database
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_findices(self.date)

    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('PutCsiInDb_findices.%Y%m%d.SUCCESS'))

    def run(self):
        findices = ['XMSW','^AXJO','^BSES','^BVSP','^FTSE','^GDAX','^GU15','^MXX',\
                    '^N500','^N150','^NZ50','^SSMI','^XMSC']
        if push_file_to_db(self.input().path, findices):
            with open(self.output().path,'w') as f:
                f.write("Successfully put CSI data in DB for f-indices")

class PutCsiInDb_usstocks(QPlumTask):
    """
    Task to put CSI data in database
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_usstocks(self.date)

    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('PutCsiInDb_usstocks.%Y%m%d.SUCCESS'))

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

class PutCsiInDb_all(QPlumTask):
    """
    Task to put all CSI data in database
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return PutCsiInDb_futures(self.date), PutCsiInDb_usstocks(self.date),\
               PutCsiInDb_findices(self.date), PutCsiInDb_indices(self.date),\
               PutCsiInDb_funds(self.date)
    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('PutCsiInDb_all.%Y%m%d.SUCCESS'))
    def run(self):
        with open(self.output().path,'w') as f:
            f.write("Successfully updated last trading day for all futures")

class UpdateLastTradingDay(QPlumTask):
    """
    Task to update last trading days for all products
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return PutCsiInDb_futures(self.date)
    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('UpdateLastTradingDay.%Y%m%d.SUCCESS'))
    def run(self):
        update_last_trading_day(self.date.strftime('%Y-%m-%d'))
        with open(self.output().path,'w') as f:
            f.write("Successfully updated last trading day for all futures")

class UpdateBenchmarks(QPlumTask):
    """
    Task to update benchmarks in workbench db
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return PutCsiInDb_all(self.date)
    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('UpdateBenchmarks.%Y%m%d.SUCCESS'))
    def run(self):
        update_benchmarks(['AQRIX','AQMIX','VTSMX','VBLTX'],self.date.strftime('%Y-%m-%d'))
        with open(self.output().path,'w') as f:
            f.write("Successfully updated benchmarks in workbench db")

class PutQuandlInDb(QPlumTask):
    """
    Task to put Quandl data in DB
    """
    date = luigi.DateParameter(default=date.today())
    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('PutQuandlInDb.%Y%m%d.SUCCESS'))
    def run(self):
        daily_update_quandl(self.date.strftime('%Y%m%d'))
        with open(self.output().path,'w') as f:
            f.write("Successfully put Quandl data in DB")

class SendStats(QPlumTask):
    """
    Task to send stats emails
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return PutCsiInDb_all(self.date)
    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('SendStats.%Y%m%d.SUCCESS'))
    def run(self):
        send_stats_configs = [{'config':"~/modeling/sample_strats/selected_strats/TRVP-aqrix-mimic.cfg"},\
                              {'config':"~/modeling/sample_strats/selected_strats/ACWAS_0.25MVO_0.25TRVP_0.25TRMSHC_0.25SMS.cfg"},\
                              {'config':"~/modeling/sample_strats/selected_strats/SMS_rb21_trend252_std21_63_252_corr252.cfg"},\
                              {'config':"~/modeling/sample_strats/selected_strats/MVO_rb21_ret252_std21_63_252_corr252.cfg"},\
                              {'config':"~/modeling/sample_strats/selected_strats/TRMSHC_rb21_std21_63_252_corr252.cfg"},\
                              {'config':"~/modeling/sample_strats/selected_strats/TRVP_rb21_std21_63_252_corr252.cfg"},\
                              {'config':"~/modeling/sample_strats_etfs/selected_strats/ETF-TRERCL_rb21.cfg"},\
                              {'config':"~/modeling/sample_strats_etfs/selected_strats/ETF-TRVP_rb21.cfg"},\
                              {'config':"~/modeling/sample_strats_etfs/selected_strats/ETF-TRMSHC_rb21.cfg"},\
                              {'config':"~/modeling/sample_strats_etfs/selected_strats/ETF-MVO_rb21.cfg"},\
                              {'config':"~/modeling/sample_strats_etfs/selected_strats/ETF-SMS_rb21.cfg"},\
                              {'config':"~/modeling/sample_strats_etfs/selected_strats/ETF-ACWAS_0.25MVO_0.25SMS_0.25TRMSHC_0.25TRVP.cfg"},\
                              {'config':"~/modeling/livetrading/strategies/t_avg.cfg",'start_date':'1995-01-01','name':'LiveTrading'}]
        for config in send_stats_configs:
            schedule_send_stats.apply_async((config,), serializer='json', expires=datetime.now()+timedelta(days=1))
        with open(self.output().path,'w') as f:
            f.write("Successfully scheduled sent stats")

class UpdateWorkbenchStats(QPlumTask):
    """
    Task to update workbench stats
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return PutCsiInDb_all(self.date),SendStats(self.date) # Doing this to ensure priority till we set up routing queues in Celery
    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('UpdateWorkbenchStats.%Y%m%d.SUCCESS'))
    def run(self):
        db_connect()
        query = "SELECT a.strat_id, a.config_path FROM strategy_static AS a INNER JOIN strategy_dyn AS b ON a.strat_id = b.strat_id WHERE b.to_update = TRUE"
        wb_strategies_df = pd.read_sql(query, con=db)
        db_close()
        for i in xrange(len(wb_strategies_df.index)):
            schedule_workbench_update.apply_async((wb_strategies_df.iloc[i]['config_path'], wb_strategies_df.iloc[i]['strat_id']), serializer='json', expires=datetime.now()+timedelta(days=1))
        with open(self.output().path,'w') as f:
            f.write("Successfully scheduled workbench updates")

class FetchWedbushFromSFTP(luigi.ExternalTask):
    """
    Task to fetch Wedbush statements from SFTP
    """
    date = luigi.DateParameter(default=date.today())
    def output(self):
        if os.path.isfile(wedbush_path+self.date.strftime('%Y%m%d/mny%Y%m%d.csv')) == False:
            wedbush_download_script_path = "/home/cvdev/datapreproc/data_cleaning/wedbush/download_statements.sh"
            subprocess.call(["bash",wedbush_download_script_path])
        return luigi.LocalTarget(wedbush_path+self.date.strftime('%Y%m%d/mny%Y%m%d.csv'))

class PutWedbushStmtInDb(QPlumTask):
    """
    Task to parse statemetns and put them in DB
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchWedbushFromSFTP(self.date)
    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('PutWedbushStmtInDb.%Y%m%d.SUCCESS'))    
    def run(self):
        if dump_statement_data(self.date.strftime('%Y%m%d')):
            with open(self.output().path,'w') as f:
                f.write("Successfully put statement in db")

class PutEODEstimatesInDb(QPlumTask):
    """
    Task to estimate EOD stats and put them in DB
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return PutWedbushStmtInDb(self.date), PutCsiInDb_all(self.date)
    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('PutEODEstimatesInDb.%Y%m%d.SUCCESS'))    
    def run(self):
        if dump_eod_estimated_data(self.date.strftime('%Y%m%d')):
            with open(self.output().path,'w') as f:
                f.write("Successfully put EOD estimates in db")

class ReconcileWedbush(QPlumTask):
    """
    Task to reconcile ours and Wedbush accounts
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return PutEODEstimatesInDb(self.date)
    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('ReconcileWedbush.%Y%m%d.SUCCESS'))    
    def run(self):
        if reconcile(self.date.strftime('%Y%m%d'), False):
            with open(self.output().path,'w') as f:
                f.write("Successfully Reconciled Wedbush")

class ManageInventory(QPlumTask):
    """
    Task to separate positions between inventory and strategy, store them in db
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return ReconcileWedbush(self.date)
    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('ManageInventory.%Y%m%d.SUCCESS'))    
    def run(self):
        if manage_inventory(self.date, '/home/cvdev/modeling/livetrading/strategies/t_avg.cfg', 'csi', 'future', None):
            with open(self.output().path,'w') as f:
                f.write("Successfully Managed Inventory")

class DemystifyPnl(QPlumTask):
    """
    Task to separate positions between inventory and strategy, store them in db
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return ManageInventory(self.date)
    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('DemystifyPnl.%Y%m%d.SUCCESS'))    
    def run(self):
        if demystify_pnl(self.date.strftime('%Y%m%d'), False):
            with open(self.output().path,'w') as f:
                f.write("Successfully Demystified Pnl")

class GenerateOrders(QPlumTask):
    """
    Task to separate positions between inventory and strategy, store them in db
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return DemystifyPnl(self.date)
    def output(self):
        return luigi.LocalTarget(log_path+self.date.strftime('GenerateOrders.%Y%m%d.SUCCESS'))    
    def run(self):
        if generate_orders('/home/cvdev/modeling/livetrading/strategies/t_avg.cfg', 'csi', 'future', '1995-01-01', self.date + timedelta(days=1), False, False):
            with open(self.output().path,'w') as f:
                f.write("Successfully Generated Orders")

class AllTasks(QPlumTask):
    """
    Task to trigger all base tasks
    """
    date = luigi.DateParameter(default=date.today())
    def requires(self):
        return FetchCSI_all(self.date), PutInS3_all(self.date), PutCsiInDb_all(self.date),\
               PutQuandlInDb(self.date), UpdateLastTradingDay(self.date), SendStats(self.date),\
               ReconcileWedbush(self.date), UpdateWorkbenchStats(self.date), UpdateBenchmarks(self.date)#, GenerateOrders(self.date)

if __name__ == '__main__':
    load_credentials()
    luigi.run(main_task_cls=AllTasks)
