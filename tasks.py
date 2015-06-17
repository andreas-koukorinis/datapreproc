import os
from subprocess import call
from celery import Celery

try:
   with open('/spare/local/credentials/rabbitmq_credentials.txt') as f:
       user,password = f.readlines()[0].strip().split(':') 
except IOError:
    sys.exit('No credentials file found')

broker = 'amqp://%s:%s@54.173.156.25:5672/cvqhost'%(user,password)
backend = 'db+sqlite:////apps/logs/celery-results.db'
app = Celery('tasks', broker=broker, backend=backend)

@app.task
def send_stats(config):
    call(["source","/apps/pythonenv/py2.7/bin/activate"],stdout=open(os.devnull, 'w'))
    call(["cd","/home/cvdev/stratdev/"],stdout=open(os.devnull, 'w'))
    call(["git","pull","origin","beta"],stdout=open(os.devnull, 'w'))
    ret_code = call(["python", "utility_scripts/send_stats.py"] + config.split(),stdout=open(os.devnull, 'w'))
    return ret_code
