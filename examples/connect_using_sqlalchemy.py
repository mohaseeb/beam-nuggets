from __future__ import division, print_function
from sqlalchemy.engine.url import URL

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String

url = URL(drivername='sqlite', database='/tmp/months_db.sqlite')
SessionClass = sessionmaker(bind=create_engine(url))
sess = SessionClass()


