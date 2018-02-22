import sqlalchemy.engine
from sqlalchemy.dialects import registry
import sqlalchemy.dialects.sqlite
import gevent
import gevent.threadpool
import importlib
import functools
from sqlalchemy.engine import interfaces

def call_in_gevent(tp_factory):
	def wraps(func):
		if tp_factory is None:
			return func
		
		@functools.wraps(func)
		def proxy(*args, **kwargs):
			threadpool = tp_factory()
			return threadpool.apply_e(BaseException, func, args, kwargs)
		return proxy
	return wraps

class Proxy(object):
	_intercept = dict()
	
	def __init__(self, inner):
		self._inner = inner
	
	def __getattr__(self, name):
		obj = getattr(self._inner, name)
		if name in self._intercept:
			return self._intercept[name](obj)
		else:
			return obj

def cursor_proxy(tp_factory):
	g = call_in_gevent(tp_factory)
	ic = {k:g for k in ("callproc", "close", "execute", "executemany", "fetchone",
		"fetchmany", "fetchall", "nextset", "setinputsizes", "setoutputsize")}
	
	def proxy(func):
		@functools.wraps(func)
		def wraps(*args, **kwargs):
			cur = g(func)(*args, **kwargs)
			return type("CursorProxy", (Proxy,), {"_intercept":ic})(cur)
		return wraps
	return proxy

def connection_proxy(tp_factory):
	g = call_in_gevent(tp_factory)
	ic = {k:g for k in ("close", "commit", "rollback")}
	ic["cursor"] = cursor_proxy(tp_factory)
	
	def proxy(func):
		@functools.wraps(func)
		def wraps(*args, **kwargs):
			con = g(func)(*args, **kwargs)
			return type("ConnectionProxy", (Proxy,), {"_intercept":ic})(con)
		return wraps
	return proxy

def dbapi_proxy(tp_factory):
	g = call_in_gevent(tp_factory)
	ic = dict(connect= connection_proxy(tp_factory))
	return type("DbapiProxy", (Proxy,), {"_intercept":ic})

def dbapi_factory_proxy(tp_factory):
	def proxy(func):
		@functools.wraps(func)
		def wraps(*args, **kwargs):
			m = func(*args, **kwargs) # obtain dbapi module
			return dbapi_proxy(tp_factory)(m)
		return wraps
	return proxy

class DialectProxy(object):
	_tp_factory = None
	
	def __init__(self, inner):
		self._inner = inner
	
	def __getattr__(self, name):
		obj = getattr(self._inner, name)
		if name == "dbapi":
			return dbapi_factory_proxy(self._tp_factory)(obj)
		elif name == "get_dialect_cls":
			return lambda *args:self
		else:
			return obj

def dialect_init_wrap(tp_factory):
	def proxy(func):
		@functools.wraps(func)
		def wraps(self, *args, **kwargs):
			inner = call_in_gevent(tp_factory)(func)(*args, **kwargs)
			return type(self.__name__, (DialectProxy,), {"_tp_factory":staticmethod(tp_factory)})(inner)
		return wraps
	return proxy

single_pool = gevent.threadpool.ThreadPool(1)

def dialect_name(*args):
	return "".join([s[0].upper()+s[1:] for s in args if s])+"Dialect"

def dialect_maker(db, driver):
	class_name = dialect_name(db, driver)
	if driver is None:
		driver = "base"
	
	dialect = importlib.import_module("sqlalchemy.dialects.%s.%s" % (db, driver)).dialect
	
	tp_factory = lambda: gevent.get_hub().threadpool
	if db == "sqlite": # pysqlite dbapi connection requires single threaded
		tp_factory = lambda: single_pool
	
	return type(dialect.__name__, (DialectProxy,), {
		"_tp_factory":staticmethod(tp_factory),
		"__call__":dialect_init_wrap(tp_factory)(dialect)
	})(dialect)

bundled_drivers = {
	"drizzle":"mysqldb".split(),
	"firebird":"kinterbasdb fdb".split(),
	"mssql":"pyodbc adodbapi pymssql zxjdbc mxodbc".split(),
	"mysql":"mysqldb oursql pyodbc zxjdbc mysqlconnector pymysql gaerdbms cymysql".split(),
	"oracle":"cx_oracle zxjdbc".split(),
	"postgresql":"psycopg2 pg8000 pypostgresql zxjdbc".split(),
	"sqlite":"pysqlite".split(),
	"sybase":"pysybase pyodbc".split()
	}
for db, drivers in bundled_drivers.items():
	try:
		globals()[dialect_name(db)] = dialect_maker(db, None)
		registry.register("gevent_%s" % db, "sqlalchemy_gevent", dialect_name(db))
		for driver in drivers:
			globals()[dialect_name(db,driver)] = dialect_maker(db, driver)
			registry.register("gevent_%s.%s" % (db,driver), "sqlalchemy_gevent", dialect_name(db,driver))
	except ImportError:
		# drizzle was removed in sqlalchemy v1.0
		pass

def patch_all():
	for db, drivers in bundled_drivers.items():
		registry.register(db, "sqlalchemy_gevent", dialect_name(db))
		for driver in drivers:
			registry.register("%s.%s" % (db,driver), "sqlalchemy_gevent", dialect_name(db,driver))

