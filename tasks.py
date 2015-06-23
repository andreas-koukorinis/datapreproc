import os
import sys
from celery import Celery
from subprocess import Popen

try:
   with open('/spare/local/credentials/rabbitmq_credentials.txt') as f:
       user,password = f.readlines()[0].strip().split(':')
except IOError:
    sys.exit('No credentials file found')

broker = 'amqp://%s:%s@54.173.156.25:5672/cvqhost'%(user,password)
backend = 'db+sqlite:////apps/logs/celery-results.db'
app = Celery('tasks', broker=broker, backend=backend)

@app.task
def schedule_send_stats(config):
    sys.path.append('/home/cvdev/stratdev/utility_scripts/')
    from send_stats import send_stats
    send_stats(os.path.expanduser(config['config']),name=config.get('name',None),dontsend=False,sim_start_date=config.get('start_date',None),sim_end_date=config.get('end_date',None))
    return 0

@app.task
def schedule_workbench_update(config, strat_id):
    _config_file = os.path.expanduser(config)
    cmd = [_config_file, "-update", str(strat_id),"-db","workbench"]
    proc = Popen(['python', '-W', 'ignore', '/home/cvdev/stratdev/run_simulator.py'] + cmd, stdout=open(os.devnull,'w'))
    proc.communicate() 
    return cmd
