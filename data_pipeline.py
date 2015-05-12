from datetime import date
import luigi
from luigi.contrib import ftp

data_path = '/apps/data/csi/'

class CheckFTP(luigi.ExternalTask):
    """
    Task to check CSI FTP for today's data
    """
    date = luigi.DateParameter()
    def output(self):
        return ftp.RemoteTarget(self.date.strftime('futures.%Y%m%d.gz'), 'ftp.csidata.com', username='circulumvite', password='sv13tjmd', port=21) 

class FetchFTP(luigi.Task):
    """
    Task that downloads data from CSI FTP in given folder
    """
    date = luigi.DateParameter()    
    def requires(self):
        return CheckFTP(self.date)

    def output(self):
        return luigi.LocalTarget(data_path+self.date.strftime('futures.%Y%m%d.gz'))

    def run(self):
        f = self.input()
        f.get(self.output().path)

class AllReports(luigi.Task):
    """
    Task to trigger all base tasks
    """
    date = luigi.DateParameter(default=date.today())
    
    def requires(self):
        yield FetchFTP(self.date)

if __name__ == '__main__':
    luigi.run(main_task_cls=AllReports)