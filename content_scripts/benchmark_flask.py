#!/usr/bin/python
#Date: 25/06/2015
#version: V1
import sys
import json
from flask import jsonify, request, abort, make_response, Flask
import pandas as pd
from pandas import DataFrame
import numpy as np
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.mysql import BIGINT

# Flask Setup Code 

app = Flask(__name__)
try:
    with open('/spare/local/credentials/readonly_credentials.txt') as f:
        credentials = [line.strip().split(':') for line in f.readlines()]
except IOError:
    sys.exit('No credentials file found')
engine = create_engine('mysql://'+credentials[0][0]+':'+credentials[0][1]+'@fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com/workbench',echo=True)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# Class Definitions

class Benchmark(Base):
    __tablename__ = 'benchmark_static'
    benchmark_id = Column('benchmark_id', BIGINT(unsigned=True),  primary_key=True,  autoincrement=True)
    ticker = Column('ticker', String(16), unique=True,nullable=False)
    desc = Column('description', String(60))
    freq = Column('frequency', String(2), nullable=False)
    sector = Column('Sector', String(60))
    rating = Column('Rating', String(60))
    exchange = Column('Exchange', String(60))
    tradetime = Column('TradeTime', String(60)) 
    rawdataunit = Column('rawdataunit', String(10) )

    def __repr__(self):
        return "Debug: Benchmark"

class Benchmark_Returns(Base):
    __tablename__ = 'benchmark_daily_basic'
    __table_args__ = (UniqueConstraint('ticker','date',name='constraint_1'),
                        UniqueConstraint('benchmark_id','date',name='constraint_2'),
                        ForeignKeyConstraint(['ticker'],['benchmark_static.ticker']),
                        ForeignKeyConstraint(['benchmark_id'],['benchmark_static.benchmark_id']))
    benchmark_id = Column('benchmark_id', BIGINT(unsigned=True), primary_key=True)
    ticker = Column('ticker', String(16),nullable=False)
    rawdata = Column('rawdata', Float, nullable=False)
    date = Column('date', Date, primary_key=True)

    def __repr__(self):
        return "Debug: Benchmark_Returns"

class Benchmark_Returns_Generated(Base):
    __tablename__ = 'benchmark_daily'
    __table_args__ = (UniqueConstraint('ticker','date',name='constraint_1'),
                        UniqueConstraint('benchmark_id','date',name='constraint_2'),
                        ForeignKeyConstraint(['ticker'],['benchmark_static.ticker']),
                        ForeignKeyConstraint(['benchmark_id'],['benchmark_static.benchmark_id']))
    benchmark_id = Column('benchmark_id', BIGINT(unsigned=True), primary_key=True)
    ticker = Column('ticker', String(16),nullable=False)
    logreturn = Column('logreturn', Float)
    percent = Column('percent', Float)
    date = Column('date', Date, primary_key=True)

    def __repr__(self):
        return "Debug: Benchmark_Returns_Generated"

def getbenchmark(benchmark_ticker):
    i=session.query(cast(Benchmark_Returns_Generated.date, DATE) ,Benchmark_Returns_Generated.logreturn)\
        .filter(Benchmark_Returns_Generated.ticker==benchmark_ticker)
    df = DataFrame(i.all())
    if(df.shape[0]>0):
        benchmark_dict = {} 
        df.columns=['dates','logreturn']
        benchmark_dict['benchmark_dates'] = map(lambda x: x.strftime("%Y-%m-%d") , list(df['dates'].values))
        benchmark_dict['benchmark_log_returns'] = list(np.round(df['logreturn'].values, 5))
        #print df
        benchmark_frequency=session.query(Benchmark.freq).filter(Benchmark.ticker==benchmark_ticker).scalar()
        benchmark_rawdataunit=session.query(Benchmark.rawdataunit).filter(Benchmark.ticker==benchmark_ticker).scalar()
        #print benchmark_frequency
        #print benchmark_dict
        benchmark_dict['benchmark_name']=benchmark_ticker
        #benchmark_dict['benchmark_frequency']=benchmark_frequency # Dont need it now
        #benchmark_dict['benchmark_unit']=benchmark_rawdataunit # Dont need it now
        #print json.dumps(benchmark_dict)
        #return json.dumps(benchmark_dict)
        return benchmark_dict
