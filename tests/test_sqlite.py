import os
import unittest
import threading
import uuid

opts = dict(
	MODE="direct"
)
opts.update(os.environ)

class Sqlite(unittest.TestCase):
	@unittest.skipIf(opts.get("MODE")!="direct", "runs if MODE=direct")
	def test_connect(self):
		import sqlalchemy
		e = sqlalchemy.create_engine("gevent_sqlite:///demo.db")
		self.do_sql(e)

	@unittest.skipIf(opts.get("MODE")!="patch", "runs if MODE=patch")
	def test_connect_patched(self):
		import sqlalchemy
		import sqlalchemy_gevent
		sqlalchemy_gevent.patch_all()
		e = sqlalchemy.create_engine("sqlite:///demo.db")
		self.do_sql(e)

	def do_sql(self, e):
		e.execute("CREATE TABLE a(k VARCHAR PRIMARY KEY, v VARCHAR)")
		try:
			e.execute("INSERT INTO a VALUES(?, ?)", (str(uuid.uuid4()), "OK"))
			r = e.execute("SELECT * FROM a").fetchone()
			assert r[1] == "OK"
		finally:
			e.execute("DROP TABLE a")

if __name__ == "__main__":
	unittest.main()
