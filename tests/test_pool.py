# Copyright (c) 2025 Manos Stoumpos
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

"""
This module contains tests related to the `ConnectionPool` class.
"""

from random import random
from threading import Lock  # <replace:from asyncio import Lock>
from time import sleep  # <replace:from asyncio import sleep>
from typing import Any, Callable

import pytest

from onlymaps._connection import Connection
from onlymaps._drivers import Driver
from onlymaps._pool import ConnectionPool
from onlymaps._utils import Error
from tests.fixtures.connections import big_pool, connection, pool, small_pool
from tests.fixtures.executors import Executor
from tests.utils import DRIVERS, MAX_POOL_SIZE, SQL

# NOTE: Exclude certain drivers from async tests.
# <include:DRIVERS = [d for d in DRIVERS if d not in {Driver.SQL_SERVER, Driver.DUCK_DB}]>


@pytest.mark.parametrize("pool", DRIVERS, indirect=True)
class TestConnectionPool:  # <replace:class TestAsyncConnectionPool:>
    """
    Tests the `ConnectionPool` class.
    """

    def test_connection_pool_transaction_on_uncommitted_changes_check_by_pool(  # <async>
        self, pool: ConnectionPool, executor: Executor, uid: int
    ) -> None:
        """
        Tests whether a connection from a pool is unable to see changes
        by another connection from the same pool while the latter is still
        in transaction mode.
        """

        result: int | None | Exception = uid
        continue_counter = 0

        def fn_1() -> None:  # <async>
            nonlocal continue_counter
            with pool.transaction():  # <async>
                try:
                    pool.exec(SQL.INSERT_INTO_TEST_TABLE(pool.driver), uid)  # <await>
                finally:
                    continue_counter = 1
                while continue_counter < 2:
                    sleep(0.1)  # <await>

        def fn_2() -> None:  # <async>
            nonlocal continue_counter, result
            while continue_counter < 1:
                sleep(0.1)  # <await>
            try:
                result = pool.fetch_one_or_none(  # <await>
                    int, SQL.SELECT_FROM_TEST_TABLE(pool.driver), uid
                )
            except Exception as exc:
                result = exc
            finally:
                continue_counter = 2

        executor.submit(fn_1)
        executor.submit(fn_2)
        executor.wait()  # <await>

        # SQL Server implements READ COMMITTED isolation via
        # locking, not MVCC, therefore the query should fail
        # instead due to the lock held on the table.
        if pool.driver == Driver.SQL_SERVER:
            assert isinstance(result, Exception)
        else:
            assert result is None

    def test_connection_pool_on_max_pool_size(  # <async>
        self, pool: ConnectionPool, executor: Executor
    ) -> None:
        """
        Tests whether no more than `MAX_POOL_SIZE` pool connections
        can be occupied at the same time.
        """

        continue_counter = 0
        continue_flag = False

        lock = Lock()

        def occupy_connection() -> None:  # <async>
            nonlocal continue_counter
            with pool.transaction():  # <async>
                with lock:  # <async>
                    continue_counter += 1
                while not continue_flag:
                    sleep(0.1)  # <await>

        # Create a task for each connection
        # and have it execute `occupy_connection`.
        for _ in range(MAX_POOL_SIZE):
            executor.submit(occupy_connection)

        # Wait until all tasks have started a transaction,
        # thereby occupying a conection.
        while continue_counter < MAX_POOL_SIZE:
            sleep(0.1)  # <await>

        # At this point all connections must be occupied.
        with pytest.raises(TimeoutError) as exc_info:
            try:
                pool.exec("SELECT NULL")  # <await>
            finally:
                continue_flag = True

        assert exc_info.value == Error.PoolNoAvailableConnections

    def test_connection_pool_on_nested_iter_runtime_error(  # <async>
        self,
        pool: ConnectionPool,
    ) -> None:
        """
        Tests whether a `RuntimeError` is raised if trying to
        to perform nested iterations.
        """
        with pool.iter(..., 1, "SELECT 1"):  # <async>
            with pytest.raises(RuntimeError) as exc_info:
                with pool.iter(..., 1, "SELECT 1"):  # <async>
                    pass

        assert exc_info.value == Error.PoolIteratorNotAllowed

    def test_connection_pool_on_multiple_iter_allowed(  # <async>
        self, pool: ConnectionPool, executor: Executor
    ) -> None:
        """
        Tests whether multiple concurrent iterations are
        allowed.
        """

        num_iters = 10

        expected_result: list[int] = []
        actual_result: list[int] = []

        def perform_iter() -> None:  # <async>
            with pool.iter(int, 1, "SELECT 1") as it:  # <async>
                for batch in it:  # <async>
                    actual_result.append(batch[0])

        for _ in range(num_iters):
            expected_result.append(1)
            executor.submit(perform_iter)

        executor.wait()  # <await>

        assert actual_result == expected_result

    def test_connection_pool_on_blocking_during_close(  # <async>
        self, pool: ConnectionPool, connection: Connection, executor: Executor, uid: int
    ) -> None:
        """
        Tests whether the connection pool waits for all connections
        to finish their work when closing.
        """

        continue_flag = False

        def fn() -> None:  # <async>
            nonlocal continue_flag
            with pool.transaction():  # <async>
                continue_flag = True
                sleep(2)  # <await>
                pool.exec(SQL.INSERT_INTO_TEST_TABLE(pool.driver), uid)  # <await>

        executor.submit(fn)

        while True:
            sleep(0.1)  # <await>
            if continue_flag:
                break

        pool.close()  # <await>

        assert (
            connection.fetch_one_or_none(  # <await>
                int, SQL.SELECT_FROM_TEST_TABLE(pool.driver), uid
            )
            == uid
        )


@pytest.mark.parametrize(
    "small_pool",
    # NOTE: Do not test on certain drivers as there is not
    #       an easy way to either explicitly or implicitly
    #       force the DB to close the connection from within
    #       the same connection.
    [
        driver
        for driver in DRIVERS
        if driver
        not in {Driver.SQL_SERVER, Driver.ORACLE_DB, Driver.SQL_LITE, Driver.DUCK_DB}
    ],
    indirect=True,
)
def test_connection_pool_on_broken_connections(  # <async>
    small_pool: ConnectionPool,
) -> None:
    """
    Tests the connection pool on broken connections.
    """

    # Have the database close the connection by setting
    # a very small idle session timeout.
    small_pool.exec(SQL.KILL_CONNECTION(small_pool.driver))  # <await>

    # Sleep for a bit in case connection is killed via
    # idle session timeout.
    sleep(1.5)  # <await>

    # Next query should succeed even if the pool's single
    # connection has been closed.
    assert small_pool.fetch_one(int, "SELECT 1") == 1  # <await>


@pytest.mark.parametrize("big_pool", DRIVERS, indirect=True)
@pytest.mark.filterwarnings("ignore")
def test_connection_pool_on_chaos_monkey(  # <async>
    big_pool: ConnectionPool,
    connection: Connection,
    executor: Executor,
    uid_factory: Callable[[], int],
    monkeypatch: Any,
) -> None:
    """
    Tests the connection pool under several simultaneous conditions:

    1. Multiple concurrent connection requests.
    2. Failing connection instantiations.
    3. Failing query executions.
    4. Closing the pool while connections are checked.
    """

    num_tasks = 1000
    continue_flag = False
    query = SQL.INSERT_INTO_TEST_TABLE(big_pool.driver)

    def close_pool() -> None:  # <async>
        """
        Close the connection pool after a while
        """
        nonlocal continue_flag
        sleep(2.5)  # <await>
        big_pool.close()  # <await>
        continue_flag = True

    original_conn_init = Connection.__init__
    original_conn_exec = Connection.exec

    uids: list[int] = []

    def mock_conn_init(*args: Any, **kwargs: Any) -> None:
        """
        Mock the `Connection` constructor to simulate some
        connections failing to be created.
        """
        if random() < 0.1:
            raise Exception()
        original_conn_init(*args, **kwargs)

    def mock_conn_exec(*args: Any, **kwargs: Any) -> None:  # <async>
        """
        Mock `Connection.exec` to simulate some
        connections failing to execute their query.
        """
        if random() < 0.2:
            raise Exception()
        uid = args[-1]
        uids.append(uid)
        original_conn_exec(*args, **kwargs)  # <await>

    monkeypatch.setattr(
        "onlymaps._connection.Connection.__init__",
        # NOTE: Have some connections fail during instantiation.
        mock_conn_init,
    )
    monkeypatch.setattr(
        "onlymaps._connection.Connection.exec",
        # NOTE: Have some connections fail during query execution.
        mock_conn_exec,
    )

    # Start all tasks.
    for _ in range(num_tasks):
        uid = uid_factory()
        executor.submit(big_pool.exec, query, uid)
    executor.submit(close_pool)

    max_big_pool_size = getattr(big_pool, f"_{ConnectionPool.__name__}__max_pool_size")

    while not continue_flag:
        num_currently_checked = getattr(
            big_pool, f"_{ConnectionPool.__name__}__num_currently_checked"
        )
        num_current_connections = getattr(
            big_pool, f"_{ConnectionPool.__name__}__current_connections"
        )
        # Assert number of currently checked connections and total number of connection
        # is valid at all times.
        assert num_currently_checked <= num_current_connections <= max_big_pool_size
        sleep(0.1)  # <await>

    executor.wait()  # <await>

    # Assert all UIDs were inserted into the database successfully.
    fetched_uids = connection.fetch_many(  # <await>
        int, SQL.SELECT_MANY_FROM_TEST_TABLE(connection.driver, len(uids)), *uids
    )

    assert 0 < len(uids) <= num_tasks
    assert sorted(uids) == sorted(fetched_uids)
