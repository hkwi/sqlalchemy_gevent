import os
import unittest
import uuid

opts = dict(
	POSTGRES_PASSWORD=None,
	POSTGRES_HOST=None,
	MODE="direct"
)
opts.update(os.environ)

class Postgres(unittest.TestCase):
	@unittest.skipIf(opts.get("POSTGRES_HOST") is None, "POSTGRES_HOST required")
	@unittest.skipIf(opts.get("MODE")!="direct", "runs if MODE=direct")
	def test_connect(self):
		import sqlalchemy
		if opts["POSTGRES_PASSWORD"]:
			opts["cred"] = "postgres:{POSTGRES_PASSWORD:}".format(**opts)
		else:
			opts["cred"] = "postgres"
		
		e = sqlalchemy.create_engine("gevent_postgresql+psycopg2://{cred:}@{POSTGRES_HOST:}/postgres".format(**opts),
			isolation_level="AUTOCOMMIT")
		self.do_sql(e)
		self.do_uuid(e)

	@unittest.skipIf(opts.get("POSTGRES_HOST") is None, "POSTGRES_HOST required")
	@unittest.skipIf(opts.get("MODE")!="patch", "runs if MODE=patch")
	def test_connect_patched(self):
		import sqlalchemy
		import sqlalchemy_gevent
		sqlalchemy_gevent.patch_all()
		if opts["POSTGRES_PASSWORD"]:
			opts["cred"] = "postgres:{POSTGRES_PASSWORD:}".format(**opts)
		else:
			opts["cred"] = "postgres"
		
		e = sqlalchemy.create_engine("postgresql+psycopg2://{cred:}@{POSTGRES_HOST:}/postgres".format(**opts),
			pool_size=20)
		con = e.connect().execution_options(autocommit=True)
		self.do_sql(con)
		self.do_uuid(con)

	def do_sql(self, e):
		e.execute("CREATE TABLE a(k VARCHAR PRIMARY KEY, v VARCHAR)")
		try:
			e.execute("INSERT INTO a VALUES(%s, %s)", ("will_be_UUID", "OK"))
			r = e.execute("SELECT * FROM a").fetchone()
			assert r[1] == "OK"
		finally:
			e.execute("DROP TABLE a")

	def do_uuid(self, e):
		r = e.execute("SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid").fetchone()
		assert uuid.UUID('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11') == r[0]
		

if __name__ == "__main__":
	unittest.main()
