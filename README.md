sqlalchemy_gevent
=================

sqlalchemy dialect adaptor for gevent to work in non-blocking mode

```
import sqlalchemy
import sqlalchemy.dialects

# use case 1.
# Installing sqlalchemy_gevent via setuptools installs gevent_* schema
engine = sqlalchemy.create_engine("gevent_sqlite:///example.db")

# use case 2.
# override standard sqlite driver with gevent enabled driver explicitly.
sqlalchemy.dialects.registry.register("sqlite", "sqlalchemy_gevent", "SqliteDialect")
engine = sqlalchemy.create_engine("sqlite:///example.db")

# use case 3.
# monkey patching
import sqlalchemy_gevent
sqlalchemy_gevent.patch_all()
engine = sqlalchemy.create_engine("sqlite:///example.db")
```

