# Copyright (c) 2025 Manos Stoumpos
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

"""
This module contains fixtures used to connect to a database.
"""

from typing import Any, Iterator

import pytest

from onlymaps._connection import Connection

# <include:from onlymaps._utils import co_exec>
from onlymaps._drivers import Driver
from onlymaps._pool import ConnectionPool
from onlymaps._spec import Database, PyDbAPIv2Connection
from onlymaps._utils import PyDbAPIv2ConnectionFactory
from tests.utils import (
    CONNECT_TIMEOUT,
    MAX_POOL_SIZE,
    MIN_POOL_SIZE,
    POOL_WAIT_TIMEOUT,
    DbContainer,
    SqliteContainer,
    get_conn_str_and_kwargs_from_container,
    get_request_param,
)


@pytest.fixture(scope="function")
def connection(  # <async>
    request: pytest.FixtureRequest, db_container: DbContainer
) -> Iterator[Connection]:
    """
    Yields a `Connection` instance connected to a `DbContainer`.
    """

    param = get_request_param(request)

    conn_str, kwargs = get_conn_str_and_kwargs_from_container(db_container)

    conn = Connection.from_conn_str(conn_str, **kwargs)

    if param == Driver.UNKNOWN:
        conn_factory = getattr(conn, "_Connection__conn_factory")
        conn = Connection(conn_factory)

    conn.open()  # <await>

    yield conn

    if conn.is_open:
        conn.close()  # <await>


@pytest.fixture(scope="function")
def pool(  # <async>
    request: pytest.FixtureRequest, db_container: DbContainer
) -> Iterator[ConnectionPool]:
    """
    Yields a `ConnectionPool` instance connected to a `DbContainer`.
    """

    driver = get_request_param(request)

    conn_str, kwargs = get_conn_str_and_kwargs_from_container(db_container)

    pool_kwargs: dict[str, Any] = {
        "min_pool_size": MIN_POOL_SIZE,
        "max_pool_size": MAX_POOL_SIZE,
        "wait_timeout": POOL_WAIT_TIMEOUT,
    }

    conn = ConnectionPool.from_conn_str(conn_str, **pool_kwargs, **kwargs)

    if driver == Driver.UNKNOWN:
        conn_factory: PyDbAPIv2ConnectionFactory = getattr(
            conn, "_ConnectionPool__conn_factory"
        )
        conn = ConnectionPool(conn_factory, **pool_kwargs)

    conn.open()  # <await>

    yield conn

    if conn.is_open:
        conn.close()  # <await>


@pytest.fixture(scope="function")
def big_pool(pool: ConnectionPool) -> Iterator[ConnectionPool]:  # <async>
    """
    Yields a `ConnectionPool` instance connected to a `DbContainer`,
    that has a maximum of 100 connections and no wait timeout.
    """
    conn_factory = getattr(pool, "_ConnectionPool__conn_factory")
    driver = getattr(pool, "_ConnectionPool__driver")

    with ConnectionPool(  # <async>
        conn_factory,
        driver=driver,
        min_pool_size=MIN_POOL_SIZE,
        max_pool_size=100,
        wait_timeout=None,
    ) as conn:
        yield conn


@pytest.fixture(scope="function")
def small_pool(pool: ConnectionPool) -> Iterator[ConnectionPool]:  # <async>
    """
    Yields a `ConnectionPool` instance connected to a `DbContainer`,
    that has a single connection.
    """
    conn_factory = getattr(pool, "_ConnectionPool__conn_factory")
    driver = getattr(pool, "_ConnectionPool__driver")

    with ConnectionPool(  # <async>
        conn_factory,
        driver=driver,
        min_pool_size=MIN_POOL_SIZE,
        max_pool_size=1,
        wait_timeout=None,
    ) as conn:
        yield conn


# NOTE: An additional `Connection` fixture.
connection_B = connection


@pytest.fixture(scope="function")
def dbapiv2(connection: Connection) -> Iterator[PyDbAPIv2Connection]:  # <async>
    """
    Returns a `Connection` instance's underlying DB API v2.0 connection object.
    """
    conn_factory = getattr(connection, "_Connection__conn_factory")
    conn: PyDbAPIv2Connection = conn_factory()  # <await>
    yield conn

    try:
        conn.close()  # <replace:await co_exec(conn.close)>
    except:
        pass


@pytest.fixture(scope="function")
def db(  # <async>
    request: pytest.FixtureRequest,
    db_container: DbContainer,
    pooling: bool,
    # <include:connection: AsyncConnection, pool: AsyncConnectionPool>
) -> Database:
    """
    Returns a `Database` API instance.
    """
    # <ignore>
    if pooling:
        return request.getfixturevalue("pool")
    return request.getfixturevalue("connection")
    # <ignore>
    # <include:return pool if pooling else connection>
