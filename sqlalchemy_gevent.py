import inspect
from sqlalchemy.engine import default
from sqlalchemy.dialects import registry
import sqlalchemy.dialects.sqlite
import gevent
import gevent.threadpool
import importlib
import functools

def call_in_gevent(thread_pool_factory, return_wrap=lambda x:x):
	def capture(func):
		@functools.wraps(func)
		def call_func(*args, **kwargs):
			threadpool = thread_pool_factory()
			return return_wrap(threadpool.apply_e(BaseException, func, args, kwargs))
		return call_func
	return capture

def cursor_wrap(tp_factory):
	def wrap(obj):
		methods = ("callproc", "close", "execute", "executemany",
			"fetchone", "fetchmany", "fetchall", "nextset", "setinputsizes", "setoutputsize")
		for method in methods:
			setattr(obj, method, call_in_gevent(tp_factory)(getattr(obj, method)))
		return obj
	return wrap

def connection_wrap(tp_factory):
	def wrap(obj):
		methods = ("close", "commit", "rollback", "cursor")
		for method in methods:
			setattr(obj, method, call_in_gevent(tp_factory)(getattr(obj, method)))
		obj.cursor = call_in_gevent(tp_factory, cursor_wrap(tp_factory))(obj.cursor)
		return obj
	return wrap

def dbapi_wrap(tp_factory):
	def wrap(obj):
		obj.connect = call_in_gevent(tp_factory, connection_wrap(tp_factory))(obj.connect)
		return obj
	return wrap

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
	
	attrs = {}
	if hasattr(dialect, "dbapi"):
		attrs["dbapi"]= lambda: dbapi_wrap(tp_factory)(dialect.dbapi())
	
	return type(class_name, (dialect,), attrs)


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
	except ImportError as e:
		# drizzle was removed in sqlalchemy v1.0
		pass

def patch_all():
	for db, drivers in bundled_drivers.items():
		registry.register(db, "sqlalchemy_gevent", dialect_name(db))
		for driver in drivers:
			registry.register("%s.%s" % (db,driver), "sqlalchemy_gevent", dialect_name(db,driver))

