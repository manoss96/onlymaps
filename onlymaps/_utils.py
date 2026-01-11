# Copyright (c) 2025 Manos Stoumpos
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

"""
This module contains all sorts of utility functions,
type aliases, and helper objects.
"""

import inspect
import re
import sqlite3
from functools import partial
from importlib import import_module
from types import ModuleType
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    ContextManager,
    Never,
    TypeVar,
    Union,
    cast,
)

from onlymaps._drivers import BaseDriver, Driver, driver_factory
from onlymaps._spec import (
    Database,
    PyDbAPIv2Connection,
    PyDbAPIv2Cursor,
    PyDbAPIv2Module,
)

if TYPE_CHECKING:

    import aiomysql
    import aiosqlite
    import duckdb
    import mariadb
    import oracledb
    import psycopg
    import pymssql
    import pymysql

    from onlymaps.asyncio._spec import (
        AsyncDatabase,
        AsyncPyDbAPIv2Connection,
        AsyncPyDbAPIv2Cursor,
        AsyncPyDbAPIv2Module,
    )

ConnInfo = tuple[Driver, str, int, str, str, str | None]

SafeCursor = Callable[[], ContextManager[PyDbAPIv2Cursor]]
AsyncSafeCursor = Callable[[], AsyncContextManager["AsyncPyDbAPIv2Cursor"]]
PyDbAPIv2ConnectionFactory = Callable[
    [],
    PyDbAPIv2Connection,
]
AsyncPyDbAPIv2ConnectionFactory = Callable[[], Awaitable["AsyncPyDbAPIv2Connection"]]

T = TypeVar("T")
C = TypeVar("C", bound=Callable)
D = TypeVar("D", bound=Union[Database, "AsyncDatabase"])


async def _empty_async_iter_factory() -> AsyncIterator[Never]:  # pragma: no cover
    """
    A empty async iterator factory.
    """
    if False:  # pylint: disable=using-constant-test
        yield


EMPTY_ITER = iter(())
ASYNC_EMPTY_ITER = _empty_async_iter_factory()


class Error:
    """
    A class containing all sorts of exceptions.
    """

    # `Database`-related errors.
    DbOpenConnection = RuntimeError("Connection is already open.")
    DbClosedConnection = RuntimeError(
        "Connection is closed. Call `open` to establish a new connection."
    )

    # `Connection`-related errors.
    ConnActiveIteratorSameCtx = RuntimeError(
        "There exists an active iterator in this execution context."
    )
    ConnActiveTransactionSameCtx = RuntimeError(
        "There exists an active transaction in this execution context."
    )

    # `ConnectionPool`-related errors.
    PoolNoAvailableConnections = TimeoutError(
        "No available connections in pool. Wait timeout reached."
    )
    PoolIteratorNotAllowed = RuntimeError("Nested iterations are not allowed.")

    @staticmethod
    def create_async_not_supported_error(
        name: str,
    ) -> NotImplementedError:  # pragma: no cover
        """
        Returns a `NotImplementedError` with an appropriate message.

        :param str name: The name of the database for which async query execution
            is not yet supported.
        """
        return NotImplementedError(f"Async is not currently supported for {name}.")


_RE_CONN_STR = re.compile(
    r"^(?:(?:([a-z]+)://"
    "|"
    r"([a-z]+)://(?:(\S+?)(?::(\S+?))?@)?([\w\.]+):(\d+))"
    r"(?:/(\S+)))$"
)


async def co_exec(fn: Callable[..., T | Awaitable[T]], *args: Any, **kwargs: Any) -> T:
    """
    Executes the provided function whether it is an ordinary
    function or a coroutine, and returns its result.

    :param `... -> T | Awaitable[T]` The function/coroutine
        to be executed.
    :param Any *args: Any positional arguments for `fn`.
    :param Any **kwargs: Any keyword arguments for `fn`.
    """
    return (
        await result if inspect.isawaitable(result := fn(*args, **kwargs)) else result
    )


def require(assertion: Callable[[D], None | Awaitable[None]]) -> Callable[[C], C]:
    """
    A decorator function that first invokes `assert_is_open`
    before passing through to the decorated function.

    :param Callable fn: The function to be decorated. Must be
        a `Database` method.

    :raises RuntimeError: A connection has not been established.
    """

    def decorator(fn: C) -> C:

        def sync_wrapper(db: D, /, *args: Any, **kwargs: Any) -> Any:
            assertion(db)
            return fn(db, *args, **kwargs)

        async def async_wrapper(db: D, /, *args: Any, **kwargs: Any) -> Any:
            assertion(db)
            return await fn(db, *args, **kwargs)

        wrapper = async_wrapper if inspect.iscoroutinefunction(fn) else sync_wrapper

        return cast(C, wrapper)

    return decorator


def assert_is_open(db: Union[Database, "AsyncDatabase"]) -> None:
    """
    A function that ensures an underlying connection
    has been established before interacting with the database.

    :param `Database` db: A `Database` instance.

    :raises RuntimeError: A connection has not been established.
    """
    if not db.is_open:
        raise Error.DbClosedConnection


# Function `assert_is_open` as a decorator.
require_open = require(assert_is_open)


def decompose_conn_str(conn_str: str) -> ConnInfo:
    """
    Decomposes the provided connection string into its constituent
    parts and returns them as a tuple.

    :param str conn_str: A connection string.

    :raises ValueError: The connection string is not valid.
    """
    if m := _RE_CONN_STR.match(conn_str):

        db_only_driver, driver, user, password, host, port, db = m.groups()

        if db_only_driver:
            driver = db_only_driver
            host = ""
            port = 0
            user = ""
            password = ""

        try:
            driver = Driver(driver)
        except ValueError as exc:
            all_drivers = (d for d in Driver if d != Driver.UNKNOWN)
            raise ValueError(
                f"Invalid driver: `{driver}`. Available drivers: {', '.join(all_drivers)}"
            ) from exc
        return (driver, host, int(port), db, user, password)

    raise ValueError(f"Invalid connection string: `{conn_str}`.")


def try_import_module(name: str) -> ModuleType:
    """
    Tries to import a module, and if successful,
    returns a copy of it.

    :param str name: The name of the module

    :raises ImportError: The module was not found.
    """
    try:
        module = import_module(name)

        module_copy = ModuleType("mymodule_copy")

        for attr_name, value in module.__dict__.items():
            setattr(module_copy, attr_name, value)

        return module_copy
    except ImportError as exc:
        raise ImportError(
            f"Package `{name}` not found. Please run `pip install onlymaps[{name}]`."
        ) from exc


def get_pydbapiv2_conn_factory_and_driver(
    conn_str: str, /, pooling: bool, connect_timeout: int, **kwargs: Any
) -> tuple[PyDbAPIv2ConnectionFactory, BaseDriver]:
    """
    A function that given a connection string and various other
    arguments returns a tuple containing the following:

    1. A factory function used to create connection instances that
        comfort to the Python Database API Specification v2.0.

    2. A `BaseDriver` instance representing the connection instance's
        underlyng driver.

    :param str conn_str: The connection string. A connection
        string must be formatted as such:
        `<DRIVER>://<USER>[:<PASSWORD>]@<HOST>:<PORT>/<DATABASE>`
    :param bool pooling: Indicates whether to enable connection
        pooling or not.
    :param int connect_timeout: The maximum number of seconds
        to block while waiting for a connection to be established,
        before raising a `TimeoutError`.
    :param Any **kwargs: Any other keyword arguments that are to
        be provided to the underlying connection function. These
        keyword arguments are specific to the driver that is being
        used so as to establish a connection to the database.

    :raises ValueError: A forbidden keyword argument was given
    """

    if "autocommit" in kwargs:
        raise ValueError("Forbidden connection argument: `autocommit`")
    for argname in kwargs:
        if "pool" in argname:
            raise ValueError(f"Forbidden connection argument: `{argname}`")

    (driver, host, port, database, user, password) = decompose_conn_str(conn_str)

    module: PyDbAPIv2Module

    match driver:
        case Driver.POSTGRES:

            if TYPE_CHECKING:
                module = psycopg
            else:
                module = try_import_module("psycopg")

            kwargs |= {
                "host": host,
                "port": port,
                "dbname": database,
                "user": user,
                "password": password,
                "connect_timeout": connect_timeout,
                "autocommit": False,
            }

        case Driver.MY_SQL:

            if TYPE_CHECKING:
                module = pymysql
            else:
                module = try_import_module("pymysql")

            # NOTE: If set to `False` then all string values
            #       are given as bytes.
            use_unicode = kwargs.pop("use_unicode", True)

            kwargs |= {
                "host": host,
                "port": port,
                "database": database,
                "user": user,
                "password": password if password else "",
                "connect_timeout": connect_timeout,
                "use_unicode": use_unicode,
                "autocommit": False,
            }

        case Driver.SQL_SERVER:

            if TYPE_CHECKING:
                # Cast to `PyDbAPIv2Module` due to the fact that method
                # `pymssql.Cursor.executemany` has its `params` argument
                # type hinted as `Sequence[str]`, thereby violating the
                # `PyDbAPIv2Cursor` protocol.
                module = cast(PyDbAPIv2Module, pymssql)
            else:
                module = try_import_module("pymssql")

            if "timeout" in kwargs:
                raise ValueError(
                    "Please use argument `connect_timeout` instead of `timeout`."
                )

            kwargs |= {
                "server": f"{host}:{port}",
                "database": database,
                "user": user,
                "password": password,
                "timeout": connect_timeout,
                "autocommit": False,
            }

        case Driver.MARIA_DB:

            if TYPE_CHECKING:
                module = mariadb
            else:
                module = try_import_module("mariadb")

            kwargs |= {
                "host": host,
                "port": port,
                "db": database,
                "user": user,
                "password": password,
                "connect_timeout": connect_timeout,
                "autocommit": False,
            }

        case Driver.ORACLE_DB:

            if TYPE_CHECKING:
                module = oracledb
            else:
                module = try_import_module("oracledb")

            if "tcp_connect_timeout" in kwargs:
                raise ValueError(
                    "Please use argument `connect_timeout` instead of `tcp_connect_timeout`."
                )

            kwargs |= {
                "host": host,
                "port": port,
                "service_name": database,
                "user": user,
                "password": password,
                "tcp_connect_timeout": connect_timeout,
            }

        case Driver.SQL_LITE:

            if TYPE_CHECKING:
                module = sqlite3
            else:
                module = try_import_module("sqlite3")

            kwargs |= {
                "database": database,
                # In case a connection pool is being used, it is safe
                # to disable `check_same_thread` since a single connection
                # may be occupied by a single thread at all times.
                "check_same_thread": kwargs.pop("check_same_thread", not pooling),
                "isolation_level": "DEFERRED",
            }

        case Driver.DUCK_DB:

            if TYPE_CHECKING:
                module = duckdb
            else:
                module = try_import_module("duckdb")

            kwargs |= {"database": database}

    conn_factory: PyDbAPIv2ConnectionFactory = partial(module.connect, **kwargs)
    driver_instance = driver_factory(
        driver, module.apilevel, module.threadsafety, module.paramstyle
    )

    return conn_factory, driver_instance


def get_async_pydbapiv2_conn_factory_and_driver(
    conn_str: str, /, pooling: bool, connect_timeout: int, **kwargs: Any
) -> tuple["AsyncPyDbAPIv2ConnectionFactory", BaseDriver]:
    """
    A function that given a connection string and various other
    arguments returns a tuple containing the following:

    1. An async factory function used to create connection instances
        that comfort to the Python Database API Specification v2.0.

    2. A `BaseDriver` instance representing the connection instance's
        underlyng driver.

    :param str conn_str: The connection string. A connection
        string must be formatted as such:
        `<DRIVER>://<USER>[:<PASSWORD>]@<HOST>:<PORT>/<DATABASE>`
    :param bool pooling: Indicates whether to enable connection
        pooling or not.
    :param int connect_timeout: The maximum number of seconds
        to block while waiting for a connection to be established,
        before raising a `TimeoutError`.
    :param Any **kwargs: Any other keyword arguments that are to
        be provided to the underlying connection function. These
        keyword arguments are specific to the driver that is being
        used so as to establish a connection to the database.

    :raises ValueError: A forbidden keyword argument was given
    """

    if "autocommit" in kwargs:
        raise ValueError("Forbidden connection argument: `autocommit`")
    for argname in kwargs:
        if "pool" in argname:
            raise ValueError(f"Forbidden connection argument: `{argname}`")

    (driver, host, port, database, user, password) = decompose_conn_str(conn_str)

    module: "AsyncPyDbAPIv2Module"

    match driver:
        case Driver.POSTGRES:

            if TYPE_CHECKING:
                _psycopg = psycopg
            else:
                _psycopg = try_import_module("psycopg")

            setattr(_psycopg, "connect", _psycopg.AsyncConnection.connect)
            module = cast("AsyncPyDbAPIv2Module", _psycopg)

            kwargs |= {
                "host": host,
                "port": port,
                "dbname": database,
                "user": user,
                "password": password,
                "connect_timeout": connect_timeout,
                "autocommit": False,
            }

        case Driver.MY_SQL | Driver.MARIA_DB:

            if TYPE_CHECKING:
                module = aiomysql
                _pymysql = pymysql
            else:
                module = try_import_module("aiomysql")
                _pymysql = try_import_module("pymysql")

            setattr(module, "apilevel", _pymysql.apilevel)
            setattr(module, "paramstyle", _pymysql.paramstyle)

            # NOTE: If set to `False` then all string values
            #       are given as bytes.
            use_unicode = kwargs.pop("use_unicode", True)

            kwargs |= {
                "host": host,
                "port": port,
                "db": database,
                "user": user,
                "password": password if password else "",
                "connect_timeout": connect_timeout,
                "use_unicode": use_unicode,
                "autocommit": False,
            }

        case Driver.ORACLE_DB:

            if TYPE_CHECKING:
                _oracledb = oracledb
            else:
                _oracledb = try_import_module("oracledb")

            if "tcp_connect_timeout" in kwargs:
                raise ValueError(
                    "Please use argument `connect_timeout` instead of `tcp_connect_timeout`."
                )

            setattr(_oracledb, "connect", _oracledb.connect_async)
            module = cast("AsyncPyDbAPIv2Module", _oracledb)

            kwargs |= {
                "host": host,
                "port": port,
                "service_name": database,
                "user": user,
                "password": password,
                "tcp_connect_timeout": connect_timeout,
            }

        case Driver.SQL_LITE:

            if TYPE_CHECKING:
                _aiosqlite = aiosqlite
            else:
                _aiosqlite = try_import_module("aiosqlite")

            setattr(_aiosqlite, "apilevel", sqlite3.apilevel)

            module = cast("AsyncPyDbAPIv2Module", _aiosqlite)

            kwargs |= {
                "database": database,
                # In case a connection pool is being used, it is safe
                # to disable `check_same_thread` since a single connection
                # may be occupied by a single thread at all times.
                "check_same_thread": kwargs.pop("check_same_thread", not pooling),
                "isolation_level": "DEFERRED",
            }

        case Driver.SQL_SERVER:  # pragma: no cover
            raise Error.create_async_not_supported_error("MS SQL Server")

        case Driver.DUCK_DB:  # pragma: no cover
            raise Error.create_async_not_supported_error("DuckDB")

        case Driver.UNKNOWN:  # pragma: no cover
            raise ValueError(f"Unknown driver: `{driver}`")

    conn_factory = partial(module.connect, **kwargs)
    driver_instance = driver_factory(driver, module.apilevel, 0, module.paramstyle)

    return conn_factory, driver_instance
