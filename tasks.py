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
def add(x, y):
    return x + y
