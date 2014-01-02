from sqlalchemy.engine import default
from sqlalchemy.dialects import registry
import sqlalchemy.dialects.sqlite
import gevent
import gevent.threadpool
import importlib

class FuncProxy(object):
	def __init__(self, func, threadpool):
		self.func = func
		self.threadpool = threadpool
	
	def __call__(self, *args, **kwargs):
		return self.threadpool.apply_e(BaseException, self.func, args, kwargs)

class Proxy(object):
	_inner = None
	_threadpool = None
	def __getattr__(self, name):
		obj = getattr(self._inner, name)
		if callable(obj) and not name.endswith("Error"):
			return FuncProxy(obj, self._threadpool)
		else:
			return obj

class ConnectionProxy(Proxy):
	def cursor(self):
		return type("CursorProxy", (Proxy,), {
			"_inner": self._threadpool.apply(self._inner.cursor, None, None),
			"_threadpool": self._threadpool })()

class DbapiProxy(Proxy):
	def connect(self, *args, **kwargs):
		return type("ConnectionProxy", (ConnectionProxy,), {
			"_inner": self._threadpool.apply(self._inner.connect, args, kwargs),
			"_threadpool": self._threadpool })()

class ProxyDialect(default.DefaultDialect):
	_inner = None
	_threadpool = None
	
	@classmethod
	def dbapi(cls):
		return type("DbapiProxy", (DbapiProxy,), {
			"_inner": cls._inner.dbapi(),
			"_threadpool": cls._threadpool })()

def dialect_maker(db, driver=None, threadpool=None):
	if driver is None:
		modname = "sqlalchemy.dialects.%s.base" % db
	else:
		modname = "sqlalchemy.dialects.%s.%s" % (db, driver)
	
	dialect = importlib.import_module(modname).dialect
	
	if threadpool is None:
		threadpool = gevent.get_hub().threadpool
	return type("GeventDialect_%s" % modname.replace(".","_"),
		(ProxyDialect, dialect), {
		"_inner": dialect,
		"_threadpool": threadpool })

SqliteDialect = dialect_maker("sqlite", None, gevent.threadpool.ThreadPool(1))
MysqlDialect = dialect_maker("mysql")
MysqlCymysqlDialect = dialect_maker("mysql", "cymysql")
MysqlGaerdbmsDialect = dialect_maker("mysql", "gaerdbms")
MysqlMysqlConnectorDialect = dialect_maker("mysql", "mysqlconnector")
MysqlMysqldbDialect = dialect_maker("mysql", "mysqldb")
MysqlOursqlDialect = dialect_maker("mysql", "oursql")
MysqlPymysqlDialect = dialect_maker("mysql", "pymysql")
MysqlPyodbcDialect = dialect_maker("mysql", "pyodbc")
MysqlZxjdbcDialect = dialect_maker("mysql", "zxjdbc")
PostgresqlDialect = dialect_maker("postgresql")
PostgresqlPsycopg2Dialect = dialect_maker("postgresql", "psycopg2")
PostgresqlPg8000Dialect = dialect_maker("postgresql", "pg8000")
PostgresqlPypostgresqlDialect = dialect_maker("postgresql", "pypostgresql")
PostgresqlZxjdbcDialect = dialect_maker("postgresql", "zxjdbc")
FirebirdDialect = dialect_maker("firebird")
FirebirdKinterbasdbDialect = dialect_maker("firebird", "kinterbasdb")
FirebirdFdbDialect = dialect_maker("firebird", "fdb")
DrizzleDialect = dialect_maker("drizzle")
DrizzleMysqldbDialect = dialect_maker("drizzle", "mysqldb")


def patch_all():
	registry.register("sqlite", "sqlalchemy_gevent", "SqliteDialect")
	registry.register("mysql", "sqlalchemy_gevent", "MySQLDialect")
	registry.register("postgresql", "sqlalchemy_gevent", "PostgresqlDialect")

