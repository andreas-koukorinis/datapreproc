from fabric.api import *

env.roledefs = {'workers':['54.164.22.228','54.172.28.157']}
env.user = 'cvdev'
env.key_filename = '/home/cvdev/.ssh/cvdev_rsa'

def update_code():
    code_dir = '/home/cvdev/datapreproc/'
    with cd(code_dir):
        run('git pull origin master')

def update_stratdev():
    code_dir = '/home/cvdev/stratdev/'
    with cd(code_dir):
        run('git pull origin beta')

def update_modeling():
    code_dir = '/home/cvdev/modeling/'
    with cd(code_dir):
        run('git pull origin master')

def restart_worker():
	code_dir = '/home/cvdev/datapreproc/'
    with cd(code_dir):
        run('source /apps/pythonenv/py2.7/bin/activate')
        run('supervisorctl restart celery-worker')