import sys

#RUN : python Backtester.py 2014-01-01 2014-08-20 ['ES','TY']
start_date = sys.args[1]
end_date = sys.args[2]
products = sys.args[3]

dispatcher = Dispatcher(start_date,end_date,products)
dispatcher.run()


