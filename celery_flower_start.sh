git pull origin master
credentials=$(head -n 1  /spare/local/credentials/rabbitmq_credentials.txt)
celery flower -A tasks --basic_auth=$credentials
