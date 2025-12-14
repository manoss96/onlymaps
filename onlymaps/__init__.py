# Copyright (c) 2025 Manos Stoumpos
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

"""
This module includes the `connect` method which is the package's
entrypoint so as to interact with a database.
"""

from functools import partial
from typing import Any as _Any
from typing import Callable

# <ignore>
try:
    # NOTE: Prevents partially intialized module error.
    import onlymaps.asyncio as _
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "Generate the async API by running `python gen_async.py`."
    ) from exc
# <ignore>

from onlymaps._connection import Connection
from onlymaps._params import Bulk, Json
from onlymaps._pool import ConnectionPool

# Part of the public API.
from onlymaps._spec import Database
from onlymaps._utils import PyDbAPIv2ConnectionFactory


def connect(
    conn: str | PyDbAPIv2ConnectionFactory,
    /,
    *,
    connect_timeout: int = 5,
    pooling: bool = False,
    min_pool_size: int = 0,
    max_pool_size: int | None = None,
    pool_wait_timeout: float | None = None,
    **kwargs: _Any,
) -> Database:
    """
    Returns a `Database` object that can be used to connect and
    interact with the database through either a single connection
    or a connection pool.

    :param str | `() -> PyDbAPIv2Connection` conn: Either A
        connection string or a connection factory function.
    :param int connect_timeout: The maximum number of seconds
        to block while waiting for a connection to be established,
        before raising a `TimeoutError`.
    :param bool pooling: Indicates whether to enable connection
        pooling or not.
    :param int min_pool_size: If pooling is enabled, determines
        the minimum number of connections that are managed by the
        pool at all times.
    :param int | None max_pool_size: If pooling is enabled, determines
        the maximum number of connections allowed to be managed by the
        pool. If set to `None`, then no restriction is imposed upon this
        number.
    :param float | None pool_wait_timeout: The maximum number of seconds
        to block while waiting for a connection in the pool to become available,
        after which time a  `TimeoutError` will be raised. If set to `None`,
        then blocks indefinitely until a connection becomes available.
    :param Any **kwargs: Any further keyword arguments are forwarded to the
        connection's underlying driver, e.g. `psycopg`, `pymysql`, etc...
    """
    factory: Callable[..., Database]

    match conn:
        case str() if pooling:
            factory = partial(
                ConnectionPool.from_conn_str,
                connect_timeout=connect_timeout,
                min_pool_size=min_pool_size,
                max_pool_size=max_pool_size,
                wait_timeout=pool_wait_timeout,
                **kwargs,
            )
        case str():
            factory = partial(
                Connection.from_conn_str, connect_timeout=connect_timeout, **kwargs
            )
        case _ if callable(conn) and pooling:
            factory = partial(
                ConnectionPool,
                min_pool_size=min_pool_size,
                max_pool_size=max_pool_size,
                wait_timeout=pool_wait_timeout,
            )
        case _ if callable(conn):
            factory = Connection
        case _:
            raise ValueError(
                "Neither a connection string nor a connection factory function was provided."
            )

    return factory(conn)
