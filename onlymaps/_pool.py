# Copyright (c) 2025 Manos Stoumpos
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

"""
This module contains the `ConnectionPool` class, which is used
to maintain a pool of multiple database connections in the form
of `Connection` instances.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from queue import Empty  # <replace:from asyncio import QueueEmpty as Empty>
from queue import Queue  # <replace:from asyncio import Queue>

# fmt: off
# isort: off
from threading import Lock, get_ident  # <replace:from asyncio import Lock, current_task>
# isort: on
# fmt: on
from time import sleep  # <replace:from asyncio import sleep, wait_for>
from types import EllipsisType
from typing import Any, Iterator, Self, TypeVar

from onlymaps._connection import Connection
from onlymaps._drivers import BaseDriver, Driver, UnknownDriver
from onlymaps._types import QueryString
from onlymaps._utils import (
    Error,
    PyDbAPIv2ConnectionFactory,
    assert_is_open,
    get_pydbapiv2_conn_factory_and_driver,
    require_open,
)

__all__ = ["ConnectionPool"]


T = TypeVar("T")


class ConnectionPool:
    """
    This class represents a connection pool.
    """

    def __init__(
        self,
        conn_factory: PyDbAPIv2ConnectionFactory,
        driver: BaseDriver | None = None,
        min_pool_size: int = 0,
        max_pool_size: int | None = None,
        wait_timeout: float | None = None,
    ):
        """
        This class represents a connection pool.

        :param conn_factory: A parameterless function capable of spawning
            `PyDbAPIv2Connection` instances.
        :param `BaseDriver` | None driver: The connection's underlying driver.
            Defaults to `None` if unknown or not supported.
        :param int min_pool_size: The minimum number of connections that are
            managed by this pool instance at all times.
        :param int | None max_pool_size: Maximum number of connections allowed.
        :param float | None wait_timeout: The number of seconds to wait for a connection
            in the pool to become available.
        """
        self.__conn_factory = conn_factory
        self.__driver = driver if driver else UnknownDriver.create()
        self.__min_pool_size = min_pool_size
        self.__max_pool_size = max_pool_size
        self.__wait_timeout = wait_timeout

        self.__pool = Queue[Connection](max_pool_size if max_pool_size else 0)
        self.__lock = Lock()
        self.__is_open = False
        self.__current_connections = 0
        self.__num_currently_checked = 0
        self.__iterator_ctx_ids: set[int] = set()

        # NOTE: A context connection is used so as to implement transactions
        #       on a connection pool level, in the context of which each statement
        #       within the transaction must be executed by the same connection.
        #       This works on both a per-thread level for sync pools, as well as on
        #       a per-task level for async pools.
        self.__ctx_conn = ContextVar[Connection | None]("ctx_conn", default=None)

    @property
    def driver(self) -> Driver:
        """
        The connection pool's underlying driver.
        """
        return self.__driver.tag

    @property
    def is_open(self) -> bool:
        """
        Indicates whether a connection to the database
        has been established or not.
        """
        # NOTE: If `ctx_conn` is not `None` then this indicates that
        #       the calling thread is currently mid transaction, in
        #       which case we should let it consider the connection
        #       open, even if the pool has been set to closed, so that
        #       it may finish its transaction. Method `close` will block
        #       until the transaction is over and the connection is put
        #       back into the pool, at which point it will be properly
        #       closed.
        return self.__is_open or self.__ctx_conn.get() is not None

    @property
    def __context_id(self) -> int:
        """
        Returns an integer uniquely identifying the
        current execution context.
        """
        return get_ident()  # <replace:return id(current_task())>

    @classmethod
    def from_conn_str(
        cls,
        conn_str: str,
        /,
        *,
        connect_timeout: int,
        min_pool_size: int = 0,
        max_pool_size: int | None = None,
        wait_timeout: float | None = None,
        **kwargs: Any,
    ) -> Self:
        """
        Creates and returns a connection based on the provided
        connection string.

        :param str conn_str: The connection string. A connection
            string must be formatted as such:

            `<DRIVER>://<USER>[:<PASSWORD>]@<HOST>:<PORT>/<DATABASE>`
        :param int connect_timeout: The maximum number of seconds
            to block while waiting for a connection to be established,
            before raising a `TimeoutError`.
        :param int min_pool_size: The minimum number of connections that are
            managed by this pool instance at all times.
        :param int | None max_pool_size: The maximum number of connections allowed.
        :param float | None wait_timeout: The number of seconds to wait for a connection
            in the pool to become available.
        :param Any **kwargs: Any other keyword arguments that are to
            be provided to the underlying connection function. These
            keyword arguments are specific to the driver that is being
            used so as to establish a connection to the database.
        """
        conn_factory, driver = get_pydbapiv2_conn_factory_and_driver(
            conn_str,
            True,
            connect_timeout,
            **kwargs,
        )
        return cls(
            conn_factory,
            driver,
            min_pool_size=min_pool_size,
            max_pool_size=max_pool_size,
            wait_timeout=wait_timeout,
        )

    def __enter__(self) -> Self:  # <async>
        """
        Opens a connection to the database and returns
        the instance itself.
        """
        self.open()  # <await>
        return self

    def __exit__(self, *_: tuple[Any, ...]) -> None:  # <async>
        """
        Closes the connection to the database.
        """
        if self.is_open:
            self.close()  # <await>

    def __create_connection(self) -> Connection:  # <async>
        """
        Creates and returns a new connection.
        """
        conn = Connection(
            self.__conn_factory,
            self.__driver,
            # NOTE: Set to `True` so that even if a connection has been
            #       closed from the database's side, a new connection
            #       is re-established automatically, else there is the
            #       danger of the pool filling up with dead connections.
            handle_broken_conn=True,
        )
        conn.open()  # <await>
        return conn

    def __try_create_connection(self) -> Connection | None:  # <async>
        """
        Creates and returns a new connection if the current total
        number of connection allows it, else returns `None`.
        """
        create_conn = False

        with self.__lock:  # <async>
            if (
                self.__max_pool_size is None
                or self.__current_connections < self.__max_pool_size
            ):
                create_conn = True
                self.__current_connections += 1

        if not create_conn:
            return None

        try:
            return self.__create_connection()  # <await>
        except:
            # Not sure whether a lock is needed here,
            # though better safe than sorry!
            with self.__lock:  # <async>
                self.__current_connections -= 1
            raise

    def __getconn(self) -> Connection:  # <async>
        """
        Acquires a connection from the pool.

        NOTE: In case there are no connections available, this method
        will block at most `wait_timeout` seconds, at which point an
        error will be raised if no available connections exist.
        """
        try:
            # Try fetching a connection from the queue.
            return self.__pool.get_nowait()
        except Empty:
            # If the queue is currently empty then try
            # creating a new connection.
            if conn := self.__try_create_connection():  # <await>
                return conn
            # If that failed, then wait for a connection
            # to be available.
            try:
                # <ignore>
                return self.__pool.get(block=True, timeout=self.__wait_timeout)
                # <ignore>
                # <include:return await wait_for(self.__pool.get(), self.__wait_timeout)>
            except Empty as exc:  # <replace:except TimeoutError as exc:>
                raise Error.PoolNoAvailableConnections from exc

    @contextmanager
    def __connection(self) -> Iterator[Connection]:  # <async>
        """
        Yields either a new connection from the pool,
        or a connection from the current context if
        in the middle of a transaction.
        """

        if ctx_conn := self.__ctx_conn.get():
            conn = ctx_conn
        else:
            conn = self.__getconn()  # <await>

        if not ctx_conn:
            with self.__lock:  # <async>
                self.__num_currently_checked += 1

        try:
            yield conn
        finally:
            # Put connection back only if not coming from context,
            # in which case it will be put back once the transaction
            # is over.
            if not ctx_conn:
                # NOTE: No problem calling `put_nowait` since the number
                #       of currently checked connections is never supposed
                #       to be greater than the queue's max capaciy.
                self.__pool.put_nowait(conn)
                with self.__lock:  # <async>
                    self.__num_currently_checked -= 1

    @require_open
    def exec(self, sql: QueryString, /, *args: Any, **kwargs: Any) -> None:  # <async>
        """
        Executes the given SQL query.

        :param QueryString sql: The SQL query to be executed.
        :param tuple[Any, ...] params: A tuple containing positional
            parameters for the query.
        :param dict[str, Any] kwparams: A dictionary containing keyword
            parameters for the query.

        :raises RuntimeError: Connection is not open.
        """
        with self.__connection() as conn:  # <async>
            conn.exec(sql, *args, **kwargs)  # <await>

    @require_open
    def fetch_one_or_none(  # <async>
        self, t: type[T] | EllipsisType, sql: QueryString, /, *args: Any, **kwargs: Any
    ) -> T | None:
        """
        Executes the query and returns a single row object of type `T`,
        if the query resulted in such object, else returns `None`.

        :param `T` | ellipsis t: The type to which the query result is mapped.
            If an `ellipsis` is provided, then no type check/cast occurs.
        :param QueryString sql: The SQL query to be executed.
        :param tuple[Any, ...] params: A tuple containing positional
            parameters for the query.
        :param dict[str, Any] kwparams: A dictionary containing keyword
            parameters for the query.

        :raises RuntimeError: Connection is not open.
        """
        with self.__connection() as conn:  # <async>
            return conn.fetch_one_or_none(t, sql, *args, **kwargs)  # <await>

    @require_open
    def fetch_one(  # <async>
        self, t: type[T] | EllipsisType, sql: QueryString, /, *args: Any, **kwargs: Any
    ) -> T:
        """
        Executes the query and returns a single row object of type `T`.

        :param `T` | ellipsis t: The type to which the query result is mapped.
            If an `ellipsis` is provided, then no type check/cast occurs.
        :param QueryString sql: The SQL query to be executed.
        :param tuple[Any, ...] params: A tuple containing positional
            parameters for the query.
        :param dict[str, Any] kwparams: A dictionary containing keyword
            parameters for the query.

        :raises ValueError: No row object was found to return.
        :raises RuntimeError: Connection is not open.
        """
        with self.__connection() as conn:  # <async>
            return conn.fetch_one(t, sql, *args, **kwargs)  # <await>

    @require_open
    def fetch_many(  # <async>
        self, t: type[T] | EllipsisType, sql: QueryString, /, *args: Any, **kwargs: Any
    ) -> list[T]:
        """
        Executes the query and returns a a list of row objects of type `T`.

        :param `T` | ellipsis t: The type to which the query result is mapped.
            If an `ellipsis` is provided, then no type check/cast occurs.
        :param QueryString sql: The SQL query to be executed.
        :param tuple[Any, ...] params: A tuple containing positional
            parameters for the query.
        :param dict[str, Any] kwparams: A dictionary containing keyword
            parameters for the query.

        :raises RuntimeError: Connection is not open.
        """
        with self.__connection() as conn:  # <async>
            return conn.fetch_many(t, sql, *args, **kwargs)  # <await>

    @contextmanager
    def iter(  # <async>
        self,
        t: type[T] | EllipsisType,
        size: int,
        sql: QueryString,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Iterator[Iterator[list[T]]]:  # <async>  # <async>
        """
        Executes the query and returns an iterator on batches of row objects
        of type `T`. Each batch of rows is loaded into memory during the
        iteration.

        NOTE: A connection is taken from the pool at the start of
        the `with` statement and is only returned back to the loop
        after the statement exits or is interrupted.

        :param `T` | ellipsis t: The type to which the query result is mapped.
            If an `ellipsis` is provided, then no type check/cast occurs.
        :param int size: The number of rows each batch contains.
        :param str sql: The SQL query to be executed.
        :param tuple[Any, ...] params: A tuple containing positional
            parameters for the query.
        :param dict[str, Any] kwparams: A dictionary containing keyword
            parameters for the query.

        :raises RuntimeError: Connection is not open.
        """
        assert_is_open(self)

        # NOTE: This is a safeguard against a thread
        #       being blocked due to nested iterations.
        if (ctx_id := self.__context_id) in self.__iterator_ctx_ids:
            raise Error.PoolIteratorNotAllowed

        with (  # <async>
            self.__connection() as conn,
            conn.iter(t, size, sql, *args, **kwargs) as it,
        ):
            try:
                self.__iterator_ctx_ids.add(ctx_id)
                yield it
            finally:
                self.__iterator_ctx_ids.remove(ctx_id)

    @contextmanager
    def transaction(self) -> Iterator[None]:  # <async>
        """
        Opens a trasnaction so that any changes caused by any
        queries executed under said transaction are either all
        committed together after the transaction successfully exits,
        or none of them are if an exception is raised during the
        transaction.

        NOTE: A connection is taken from the pool at the start of
        the `with` statement and is only returned back to the loop
        after the statement exits or is interrupted.

        :raises RuntimeError: Connection is not open.
        """
        assert_is_open(self)
        with self.__connection() as conn, conn.transaction():  # <async>
            self.__ctx_conn.set(conn)
            try:
                yield
            finally:
                # NOTE: Make sure to set the context back to `None` so
                #       that the connection is put back into the pool.
                self.__ctx_conn.set(None)

    def open(self) -> None:  # <async>
        """
        Establishes a connection to the database.

        :raises RuntimeError: Connection is already open.
        """
        with self.__lock:  # <async>
            if self.__is_open:
                raise Error.DbOpenConnection
            self.__is_open = True

        for _ in range(self.__min_pool_size):
            try:
                conn = self.__create_connection()  # <await>
            except:
                self.close()  # <await>
                raise
            self.__pool.put(conn)  # <await>
            self.__current_connections += 1

    def close(self) -> None:  # <async>
        """
        Closes the connection pool.

        NOTE: This function will block until all connections have completed
            their work and have been properly closed.

        :raises RuntimeError: Connection is not open.
        """
        with self.__lock:  # <async>
            if not self.__is_open:
                raise Error.DbClosedConnection
            self.__is_open = False

        # Wait for all connections to be put back into the queue.
        while self.__num_currently_checked > 0:
            # NOTE: Use `asyncio.sleep` in async pool so as to
            #       not block the main thread.
            sleep(0.5)  # <await>

        while not self.__pool.empty():
            conn = self.__pool.get()  # <await>
            try:
                # This may raise an exception if, for whatever reason,
                # a connection has already been closed, in which case
                # just move on to the next connection.
                conn.close()  # <await>
            except:  # pylint: disable=bare-except
                pass

        self.__current_connections = 0
        self.__num_currently_checked = 0
