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
	_context = None
	def __getattr__(self, name):
		obj = getattr(self._inner, name)
		if name in self._context.get("methods",()):
			threadpool = self._context.get("threadpool", gevent.get_hub().threadpool)
			return FuncProxy(obj, threadpool)
		else:
			return obj

class ConnectionProxy(Proxy):
	def cursor(self):
		threadpool = self._context.get("threadpool", gevent.get_hub().threadpool)
		methods = ("callproc", "close", "execute", "executemany",
			"fetchone", "fetchmany", "fetchall", "nextset", "setinputsizes", "setoutputsize")
		return type("CursorProxy", (Proxy,), {
			"_inner": threadpool.apply(self._inner.cursor, None, None),
			"_context": dict(list(self._context.items())+[("methods", methods),]) })()

class DbapiProxy(Proxy):
	def connect(self, *args, **kwargs):
		threadpool = self._context.get("threadpool", gevent.get_hub().threadpool)
		if self._context.get("single_thread_connection"):
			threadpool = gevent.threadpool.ThreadPool(1)
		methods = ("close", "commit", "rollback", "cursor")
		return type("ConnectionProxy", (ConnectionProxy,), {
			"_inner": threadpool.apply(self._inner.connect, args, kwargs),
			"_context": dict(list(self._context.items())+[("methods", methods), ("threadpool", threadpool)]) })()

class ProxyDialect(default.DefaultDialect):
	_inner = None
	_context = None
	
	@classmethod
	def dbapi(cls):
		return type("DbapiProxy", (DbapiProxy,), {
			"_inner": cls._inner.dbapi(),
			"_context": cls._context })()

def dialect_name(*args):
	return "".join([s[0].upper()+s[1:] for s in args if s])+"Dialect"

def dialect_maker(db, driver):
	class_name = dialect_name(db, driver)
	if driver is None:
		driver = "base"
	
	dialect = importlib.import_module("sqlalchemy.dialects.%s.%s" % (db, driver)).dialect
	
	context = {}
	if db == "sqlite": # pysqlite dbapi connection requires single threaded
		context["single_thread_connection"] = True
	
	return type(class_name,
		(ProxyDialect, dialect), {
		"_inner": dialect,
		"_context": context })

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
		for driver in drivers:
			globals()[dialect_name(db,driver)] = dialect_maker(db, driver)
	except:
		# drizzle was removed in sqlalchemy v1.0
		pass

def patch_all():
	for db, drivers in bundled_drivers.items():
		registry.register(db, "sqlalchemy_gevent", dialect_name(db))
		for driver in drivers:
			registry.register("%s.%s" % (db,driver), "sqlalchemy_gevent", dialect_name(db,driver))

