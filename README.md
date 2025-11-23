<!-- PROJECT BADGES -->
[![Python Version][python-shield]][python-url]
[![MIT License][license-shield]][license-url]
[![Coverage][coverage-shield]][coverage-url]

![OnlyMaps Logo](docs/source/onlymaps.png)


Onlymaps is a Python micro-ORM library that enables you to interact with a database
through plain SQL queries while it takes care of mapping the results back to Python
objects. More specifically, it provides:

- A minimal API that enables both sync and async query execution.
- Fine-grained type-hinting and validation with the help of [Pydantic](https://docs.pydantic.dev/latest/).
- Support for all major databases such as PostgreSQL, MySQL, MariaDB and MS SQL Server.
- Connection pooling via custom implementation.

## How to install 📦

You can install Onlymaps by running `pip install onlymaps`. If your virtual environment is missing any required database driver
packages, an exception with an appropriate message will be raised when trying to establish a connection for the first time, 
for example:

```
ImportError: Package `psycopg` not found. Please run `pip install onlymaps[psycopg]`.
```

## Documentation 📖

- [Connecting to a database](#connecting-to-a-database)
    + [Establishing a connection](#establishing-a-connection)
    + [Building the connection string](#building-the-connection-string)
    + [Using unsupported drivers](#using-unsupported-drivers)
    + [Enabling connection pooling](#enabling-connection-pooling)
- [Running queries](#running-queries)
    + [Query execution](#query-execution)
    + [Passing arguments to queries](#passing-arguments-to-queries)
    + [Query parameter wrappers](#query-parameter-wrappers)
    + [Managing transactions](#managing-transactions)
- [Mapping query results](#mapping-query-results)
    - [Single-column queries](#single-column-queries)
    - [Multi-column queries](#multi-column-queries)

### Connecting to a database

This chapter explains how to access the `Database` and `AsyncDatabase` APIs.

#### Establishing a connection

Onlymaps exposes a single entrypoint to its sync API, i.e. the function `onlymaps.connect`:

```python
from onlymaps import connect

with connect("postgresql://user:password@localhost:5432/mydb") as db:
    # Execute queries...
```

Similarly, `onlymaps.asyncio.connect` gives access to the `AsyncDatabase` API:

```python
from onlymaps.asyncio import connect

async with connect("postgresql://user:password@localhost:5432/mydb") as db:
    # Execute queries...
```

Using a `Database` object in a `with` statement manages opening and closing the underlying connection for you. However, you can always handle this yourself if you choose to do so:

```python
from onlymaps import connect, Database

db: Database = connect("postgresql://user:password@localhost:5432/mydb")

db.open()
# Execute queries...
db.close()
```

Since the sync and async APIs are identical, from now on we will be using the sync API for all examples. You only have to remeber that when using the async API, methods must be awaited:

```python
from onlymaps.asyncio import connect, AsyncDatabase

db: AsyncDatabase = connect("postgresql://user:password@localhost:5432/mydb")

await db.open()
# Execute queries...
await db.close()
```

#### Building the connection string

In order to connect to a database you must provide the `connect` function with a valid connection string, i.e. a string that has the following format:

```
{DB_TYPE}://{USERNAME}[:{PASSWORD}]@{HOST}:{PORT}/{DB_NAME}
```

The `{DB_TYPE}` placeholder can take any of the following values depending on the type of database you are trying to connect to:

- `postgresql`: PostgreSQL
- `mysql`: MySQL
- `mssql`: Microsoft SQL Server
- `mariadb`: MariaDB
- `sqlite`: SQLite. More specifically, when connecting to a SQLite database, your connection string must be formatted as such: `sqlite:///${DB_NAME}`.

#### Using unsupported drivers

Besides using a connection string, there is a second way of connecting to a database,
which also enables you to use a driver of your choice even if it is not "officially"
supported by Onlymaps. The way to do this is by providing `onlymaps.connect` with a
connection factory, i.e. a parameterless function that, when invoked, outputs a
database connection instance:

```python
from functools import partial

from onlymaps import connect
from pydbapiv2_compatible_driver import connect as pydbapiv2_connect

conn_factory = partial(
    pydbapiv2_connect,
    "<MY_CONN_STRING>",
    conn_arg_1=...,
    conn_arg_2=...,
)

db = connect(conn_factory)
```

You only have to remember that, for this to work, the factory function must output connection
instances that implement the `Connection` interface of the [Python Database API Specification v2.0](https://peps.python.org/pep-0249/).
Therefore, as long as a Python database driver package is compatible with the afformentioned protocol,
you can use it and it will work just fine!

#### Enabling connection pooling

Connection pooling can be activated by setting the `pooling` parameter to `True`:

```python
from onlymaps import connect

with connect(
    "postgresql://user:password@localhost:5432/mydb",
    pooling=True,
    max_pool_size=100
) as db:
    # Object `db` now uses a connection pool for query execution underneath.
```

While both a connection and a connection pool are thread-safe, the latter is
much more suitable for multithreaded applications as a single connection is
restricted from executing more than one query at a time. This means that if
two or more threads are sharing the same connection, they will be competing
with each other for query execution time. This is not an issue when using
a connection pool with an adequate number of connections, where each thread
can get its own connection from the pool.

The same is true for the async variants as well. Both the async connection and the
async connection pool can be considered reentrant, as long as they are not accessed
outside the event loop in which they were created. When it comes to their performance,
connection pooling is again the right choice for a heavily concurrent application,
for example an ASGI web server.


### Running queries

This chapter focuses on querying the database.

#### Query execution

The `Database` API includes five different methods for query execution:

1. `exec`: Executes a query and returns `None`.
2. `fetch_one_or_none`: Executes a query and returns either a single row or `None`.
3. `fetch_one`: Executes a query and returns a single row.
4. `fetch_many`: Executes a query and returns a list of rows.
5. `iter`: Executes a query and returns an iterator over rows.

Consider for example the following code:

```python
from typing import Any

users: list[Any] = db.fetch_many(..., "SELECT name, age FROM users")
```

The ellipsis `...` is used when you don't care about the type and just want to retrieve some rows.
However, in most cases you'd probably like to enforce a type on the result:

```python
from pydantic import BaseModel

class User(BaseModel):
    name: str
    age: int

users: list[User] = db.fetch_many(User, "SELECT name, age FROM users")
```

Even though the above example uses a pydantic model, you are not required to use one.
In fact, you can use any type you like as long as it matches the query result you are
expecting:

```python
users: list[tuple[str, int]] = db.fetch_many(tuple[str, int], "SELECT name, age FROM users")
```

Keep in mind, however, that `fetch_many` goes on to retrieve all possible rows from the database.
If that's something you don't want, then you should use a `LIMIT` clause in your SQL query,
otherwise you might run into memory issues if your database table is very large. If you must query
a very large table, method `iter` comes in handy as it enables you to fetch rows from the database
in distinct batches over many iterations:

```python
with db.iter(
    tuple[str, int],
    100, # This is the size of each batch.
    "SELECT name, age FROM users"
) as it:
    for batch in it:
        # Each batch contains data for 100 users.
```

#### Passing arguments to queries

Any argument that comes after the query, is meant to be used as a query parameter:

```python
row = db.fetch_one(tuple[int, str], "SELECT %s, %s", 1, "Hi!")

print(row) # Prints `(1, 'Hi!')`.
```

This is true for keyword arguments as well:

```python
row = db.fetch_one(tuple[int, str], "SELECT %(_id)s, %(msg)s", _id=1, msg="Hi!")

print(row) # Prints `(1, 'Hi!')`.
```

Though mixing positional and keyword arguments together is not allowed:
```python
# This raises a `ValueError` exception.
row_2 = db.fetch_one(tuple[int, str], "SELECT %s, %(msg)s", 1, msg="Hi!")
```

Note that in the above examples we use symbol `%s` for positional parameters and `%(<VAR>)s` for keyword parameters.
However, the exact parameter symbol you must use depends on the underlying database driver. For example,
the SQLite driver uses symbols `?` and `:<VAR>` respectively. In order to determine what parameter symbol
you should use, you can take a look at the documentation of the driver you are using.

#### Query parameter wrappers

Onlymaps does some parameter handling by default when it makes sense to do so. To give an example, `UUID` and `pydantic.BaseModel` type instances are always converted into strings when provided as query parameters, as this is the only type conversion that makes sense in this context. However, there exist certain cases where it is not so obvious how a parameter is meant to be used. Consider the following example:

```python
ids = [1, 2, 3, 4, 5]

rows = db.fetch_many(..., "SELECT * FROM my_table WHERE id = ANY(%s)", ids)
```

The `psycopg` driver is able to handle `list` type arguments just fine, and goes on to fetch
all rows in `my_table` whose id can be found within the list. But a list query parameter could
also be used in other ways, for example, being converted into a JSON string and inserted into the
database.

For this reason exists the `Json` parameter wrapper class, which indicates that
an argument is meant to be converted into a JSON string before being passed on to the driver, as
long as that's feasible:

```python
from onlymaps import Json

ids = [1, 2, 3, 4, 5]
kv_pairs = {"a": 1, "b": 2}

db.exec("INSERT INTO my_table (col_a, col_b) VALUES (%s, %s)", Json(ids), Json(kv_pairs))
```

Both `ids` and `kv_pairs` are converted into JSON-compatible strings, which are then inserted
into the table columns `col_a` and `col_b` respectively.

Other than `Json`, there exists one more parameter wrapper class, namely `Bulk`, which in turn indicates that the provided argument is to be executed as part of a bulk statement:

```python
from onlymaps import Bulk

rows = [[1, "a"], [2, "b"], [3, "c"]]

db.exec("INSERT INTO my_table (id, label) VALUES (%s, %s)", Bulk(rows))
```

As for how the bulk statement is actually being executed, that depends on the underlying driver being used.

#### Managing transactions

Onlymaps completely abstracts away the concepts of commit and rollback for the sake of simplicity. This means that if a query execution method call exits successfully, you should consider any changes done by the query committed to the database. On the other hand, if a method call raises an exception, even if said exception is raised during the result mapping stage, no changes are committed.

Nevertheless, sometimes you need to execute a bunch of queries together so that either all succeed or none does. In that case, the `Database` API provides a `transaction` context manager method:

```python
with db.transaction():
    db.exec(INSERT_ROW_INTO_TABLE_A_QUERY)
    db.exec(INSERT_ROW_INTO_TABLE_B_QUERY)
    db.exec(DELETE_ROW_FROM_TABLE_C_QUERY)
```

If an exception is raised before the transaction exits, any changes
done by any of the queries are discarded. Else, if the transaction
exits successfully, all changes are persisted.


### Mapping query results

In order to understand how rows are mapped to Python objects, we should first
make the distinction of querying a single column versus querying multiple columns.

##### Single-column queries

When querying a single column, the type you use must match the type of the column you are selecting.
If, for example, your query selects an integer id column, then you should use `int` as a type:

```python
ids: list[int] = db.fetch_many(int, "SELECT id FROM my_table")
```

The type of the column can be as simple as an integer, or as complex as a JSON
string with a predefined schema:

```python
from pydantic import BaseModel

class JsonColumn(BaseModel):
    id: int
    label: str
    metadata: str | None = None

json_rows: list[JsonColumn] = db.fetch_many(JsonColumn, "SELECT json_col FROM my_table")
```

In general, when querying a single column, the following types are supported:

- `bool`
- `int`
- `float`
- `str`
- `bytes`
- `uuid.UUID`
- `datetime.date`
- `datetime.datetime`
- `Enum`
- `tuple`
- `list`
- `set`
- `dict`
- `dataclasses.dataclass`
- `pydantic.dataclasses.dataclass`
- `pydantic.BaseModel`

##### Multi-column queries


Things are a bit different when querying multiple columns, as the type you must use
should always be some sorts of struct type which is able to contain more than one
type of data:

```python
ids: list[tuple] = db.fetch_many(tuple, "SELECT id, label FROM my_table")
```

The complete list of types supported when querying multiple columns is as follows:

- `tuple`
- `list`
- `set`
- `dict`
- `dataclasses.dataclass`
- `pydantic.dataclasses.dataclass`
- `pydantic.BaseModel`

This  above list of struct types can be further separated into two distinct categories:

- Container types: `tuple`, `list`, `set`.
- Model types: `dict`, `dataclasses.dataclass`, `pydantic.dataclasses.dataclass`, `pydanticBaseModel`

When using a container type, you lose all information regarding the column names.
If you wish to retain that, you should use a model type:

```python
rows: list[dict] = db.fetch_many(dict, "SELECT id, label FROM my_table")

print(rows.keys()) # This prints `dict_keys(['id', 'label'])`.
```

You can use whichever suits you best, as both container types and
model types can be parametrized in order to further validate the
type of values you are expecting:

```python

# This works!
ids1 = db.fetch_many(tuple[int, str], "SELECT id, label FROM my_table")

# And this works!
ids2 = db.fetch_many(dict[str, int | str], "SELECT id, label FROM my_table")

# But these two raise a `TypeError`.
ids3 = db.fetch_many(tuple[int, int], "SELECT id, label FROM my_table")
ids4 = db.fetch_many(dict[str, int], "SELECT id, label FROM my_table")
```


<!-- MARKDOWN LINKS & IMAGES -->
[python-shield]: https://img.shields.io/badge/python-3.11+-blue
[python-url]: https://www.python.org/downloads/release/python-390/
[license-shield]: https://img.shields.io/badge/license-MIT-red
[license-url]: https://github.com/manoss96/onlymaps/blob/main/LICENSE.txt
[coverage-shield]: https://coveralls.io/repos/github/manoss96/onlymaps/badge.svg?branch=main&service=github
[coverage-url]: https://coveralls.io/github/manoss96/onlymaps?branch=main
[docs-url]: https://github.com/manoss96/onlymaps?tab=readme-ov-file#documentation-
