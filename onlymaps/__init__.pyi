# Copyright (c) 2025 Manos Stoumpos
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

"""
This is a stub file for `__init__.py`.
"""

from typing import Any, Literal, overload

from onlymaps._params import Bulk, Json
from onlymaps._spec import Database
from onlymaps._utils import PyDbAPIv2ConnectionFactory

__all__ = ["connect", "Bulk", "Json", "Database"]

@overload
def connect(
    conn_str: str,
    /,
    *,
    connect_timeout: int = 5,
    pooling: Literal[False] = False,
    **kwargs: Any,
) -> Database:
    """
    Returns a `Database` object that can be used to connect and
    interact with the database through a single connection.

    :param str conn_str: A connection string used to establish a
        connection to the database. A connection string must be formatted
        as such: `<DRIVER>://<USER>[:<PASSWORD>]@<HOST>:<PORT>/<DATABASE>`.
    :param int connect_timeout: The maximum number of seconds
        to block while waiting for a connection to be established,
        before raising a `TimeoutError`.
    :param `False` pooling: A value of `False` indicates that connection pooling
        is disabled. Set this value to `True` to enable it.
    :param Any **kwargs: Any further keyword arguments are forwarded to the
        connection's underlying driver, e.g. `psycopg`, `pymysql`, etc...
    """

@overload
def connect(
    conn_str: str,
    /,
    *,
    connect_timeout: int = 5,
    pooling: Literal[True] = True,
    min_pool_size: int = 0,
    max_pool_size: int | None = None,
    pool_wait_timeout: float | None = None,
    **kwargs: Any,
) -> Database:
    """
    Returns a `Database` object that can be used to connect
    and interact with the database through a connection pool.

    :param str conn_str: A connection string used to establish
        a connection to the database. A connection string must
        be formatted as such: `<DRIVER>://<USER>[:<PASSWORD>]@<HOST>:<PORT>/<DATABASE>`
    :param int connect_timeout: The maximum number of seconds
        to block while waiting for a connection to be established,
        before raising a `TimeoutError`.
    :param `False` pooling: A value of `True` indicates that connection pooling
        is enabled. Set this value to `False` to disable it.
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

@overload
def connect(
    conn_factory: PyDbAPIv2ConnectionFactory,
    /,
    *,
    pooling: Literal[False] = False,
    **kwargs: Any,
) -> Database:
    """
    Returns a `Database` object that can be used to connect and
    interact with the database through a single connection.

    :param `() -> PyDbAPIv2Connection` conn_factory: A parameterless
        function that outputs a DB API v2 compatible connection instance
        when invoked.
    :param `False` pooling: A value of `False` indicates that connection pooling
        is disabled. Set this value to `True` to enable it.
    """

@overload
def connect(
    conn_factory: PyDbAPIv2ConnectionFactory,
    /,
    *,
    pooling: Literal[True] = True,
    min_pool_size: int = 0,
    max_pool_size: int | None = None,
    pool_wait_timeout: float | None = None,
) -> Database:
    """
    Returns a `Database` object that can be used to connect
    and interact with the database through a connection pool.

    :param `() -> PyDbAPIv2Connection` conn_factory: A parameterless
        function that outputs a DB API v2 compatible connection instance
        when invoked.
    :param `False` pooling: A value of `True` indicates that connection pooling
        is enabled. Set this value to `False` to disable it.
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
    """
