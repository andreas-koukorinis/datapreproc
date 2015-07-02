from fabric.api import *

env.roledefs = {'workers':['172.31.19.121','172.31.20.112']}
env.user = 'cvdev'
env.key_filename = '/home/cvdev/.ssh/cvdev.pem'

def update_code():
    code_dir = '/home/cvdev/datapreproc/'
    with cd(code_dir):
        run('git pull origin master', shell=False)

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
    env.user = 'ec2-user'
    env.key_filename = '/home/cvdev/.ssh/cvfif1.pem'
    with cd(code_dir):
        sudo('source /apps/pythonenv/py2.7/bin/activate', shell=False, user=root)
        sudo('supervisorctl restart celery-worker', shell=False, user=root)
