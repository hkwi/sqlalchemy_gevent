from setuptools import setup

setup(name="sqlalchemy_gevent",
	version = '0.1',
	packages=["sqlalchemy_gevent",],
	entry_points = ''' 
[sqlalchemy.dialects]
gevent_sqlite=sqlalchemy_gevent:SqliteDialect
gevent_mysql=sqlalchemy_gevent:MysqlDialect
gevent_postgresql=sqlalchemy_gevent:PostgresqlDialect
'''
)

