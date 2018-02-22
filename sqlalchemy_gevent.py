from sqlalchemy.engine import default
from sqlalchemy.dialects import registry
import sqlalchemy.dialects.sqlite
import gevent
import gevent.threadpool
import importlib
import functools

def call_in_gevent(tp_factory):
	def wraps(func):
		if tp_factory is None:
			return func
		
		@functools.wraps(func)
		def proxy(*arg, **kwargs):
			threadpool = tp_factory()
			return threadpool.apply_e(BaseException, func, args, kwargs)
		return proxy
	return wraps

def unproxy_call(func):
	@functools.wraps(func)
	def wraps(*args, **kwargs):
		f = lambda x: x.__inner if isinstance(x, Proxy) else x
		args = [f(a) for a in args]
		kwargs = {k:f(v) for k,v in kwargs.items()}
		return func(*args, **kwargs)
	return wraps

class Proxy(object):
	__dbapi_methods__ = tuple()
	
	def __init__(self, inner, tp_factory=None):
		self.__inner = inner
		self.__tp_factory = tp_factory
	
	def __getattr__(self, name):
		obj = getattr(self.__inner, name)
		if name in self.__dbapi_methods__:
			return call_in_gevent(self.__tp_factory)(obj)
		elif callable(obj):
			return call_in_gevent(self.__tp_factory)(unproxy_call(obj))
		else:
			return obj

class CursorProxy(Proxy):
	__dbapi_methods__ = ("callproc", "close", "execute", "executemany",
		"fetchone", "fetchmany", "fetchall", "nextset", "setinputsizes", "setoutputsize")

class ConnectionProxy(Proxy):
	__dbapi_methods__ = ("close", "commit", "rollback", "cursor")
	
	def cursor(self):
		obj = self.__inner.cursor()
		return CursorProxy(obj, self.__tp_factory)

single_pool = gevent.threadpool.ThreadPool(1)

def dialect_name(*args):
	return "".join([s[0].upper()+s[1:] for s in args if s])+"Dialect"

def dialect_maker(db, driver):
	class_name = dialect_name(db, driver)
	if driver is None:
		driver = "base"
	
	dialect = importlib.import_module("sqlalchemy.dialects.%s.%s" % (db, driver)).dialect
	
	def wrap_connect(func):
		@functools.wraps(func)
		def wraps(*args, **kwrags):
			con = func(*args, **kwargs)
			if db == "sqlite": # pysqlite dbapi connection requires single threaded
				p = ConnectionProxy(con, single)
			else:
				p = ConnectionProxy(con, lambda: gevent.get_hub().threadpool)
			return p
		return wraps
	
	def wrap_dbapi(func):
		@functools.wraps(func)
		def wrap(*args, **kwargs):
			m = func(*args, **kwrags)
			p = Proxy(m)
			p.connect = wrap_connect(m.connect)
			return p
		return wrap
	
	d = Proxy(dialect) # d is Dialect clazz
	if hasattr(dialect, "dbapi"):
		d.dbapi = wrap_dbapi(dialect.dbapi)
	return d

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

