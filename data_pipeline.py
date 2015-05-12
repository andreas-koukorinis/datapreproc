from datetime import date
import luigi
from luigi.contrib import ftp

#data_path = '/apps/data/csi/'
data_path = '/home/debi/data/'
csi_ftp_server = 'ftp.csidata.com'
csi_ftp_username = 'circulumvite'
csi_ftp_password = 'sv13tjmd'
csi_ftp_port = 21

class CheckFTP_canada(luigi.ExternalTask):
    """
    Task to check CSI FTP for today's data
    """
    date = luigi.DateParameter()
    def output(self):
        return ftp.RemoteTarget(self.date.strftime('canada.%Y%m%d.gz'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

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
        return ftp.RemoteTarget(self.date.strftime('f-indices.%Y%m%d.gz'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

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
        return ftp.RemoteTarget(self.date.strftime('funds.%Y%m%d.gz'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

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
        return ftp.RemoteTarget(self.date.strftime('futures.%Y%m%d.gz'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

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
        return ftp.RemoteTarget(self.date.strftime('indices.%Y%m%d.gz'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

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
        return ftp.RemoteTarget(self.date.strftime('uk-stocks.%Y%m%d.gz'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

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
        return ftp.RemoteTarget(self.date.strftime('us-stocks.%Y%m%d.gz'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

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
        return ftp.RemoteTarget(self.date.strftime('Briese%Y%m%d.txt'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

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
        return ftp.RemoteTarget(self.date.strftime('CFTC%Y%m%d.txt'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

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
        return ftp.RemoteTarget(self.date.strftime('Econ%Y%m%d.txt'), csi_ftp_server, username=csi_ftp_username, password=csi_ftp_password, port=csi_ftp_port) 

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

class AllReports(luigi.Task):
    """
    Task to trigger all base tasks
    """
    date = luigi.DateParameter(default=date.today())
    
    def requires(self):
        yield FetchCSI_all(self.date)


if __name__ == '__main__':
    luigi.run(main_task_cls=AllReports)