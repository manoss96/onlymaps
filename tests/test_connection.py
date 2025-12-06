# Copyright (c) 2025 Manos Stoumpos
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

"""
This module contains tests related to the `Connection` class.
"""

from random import random
from time import sleep  # <replace:from asyncio import sleep>
from typing import Any, Callable

import pytest

from onlymaps._connection import Connection
from onlymaps._utils import Error
from tests.fixtures.connections import connection, connection_B, dbapiv2
from tests.fixtures.executors import Executor
from tests.utils import DRIVERS, SQL

# <include:from tests.utils import Driver>

# NOTE: Do not incude SQL Server / DuckDB for async tests.
# <include:DRIVERS = [d for d in DRIVERS if d not in {Driver.SQL_SERVER, Driver.DUCK_DB}]>


@pytest.mark.parametrize("connection", DRIVERS, indirect=True)
class TestConnection:  # <replace:class TestAsyncConnection:>
    """
    Tests the `Connection` class.
    """

    @pytest.mark.parametrize(
        "method, args",
        [
            ("exec", ("SELECT 1",)),
            ("fetch_one", (..., "SELECT 1")),
            ("fetch_one_or_none", (..., "SELECT 1")),
            ("fetch_many", (..., "SELECT 1")),
            ("iter", (..., 1, "SELECT 1")),
            ("transaction", tuple()),
        ],
    )
    def test_connection_on_active_iter_runtime_error(  # <async>
        self, connection: Connection, method: str, args: tuple
    ) -> None:
        """
        Tests whether a `RuntimeError` is raised when calling
        a `Connection` method while an iterator is active in
        the same execution context.
        """

        with connection.iter(..., 1, "SELECT 1") as _:  # <async>

            conn_method = getattr(connection, method)

            with pytest.raises(RuntimeError) as exc_info:
                if method in {"iter", "transaction"}:
                    with conn_method(*args) as _:  # <async>
                        pass
                else:
                    _ = conn_method(*args)  # <await>

            assert exc_info.value == Error.ConnActiveIteratorSameCtx

    def test_connection_on_active_transaction_runtime_error(  # <async>
        self, connection: Connection
    ) -> None:
        """
        Tests whether a `RuntimeError` is raised when attempting
        to open a transaction while a transaction is already open
        in the current execution context.
        """

        with connection.transaction():  # <async>

            with pytest.raises(RuntimeError) as exc_info:
                with connection.transaction():  # <async>
                    pass

        assert exc_info.value == Error.ConnActiveTransactionSameCtx

    @pytest.mark.parametrize(
        "ctx_method, ctx_args",
        [
            ("iter", (..., 1, "SELECT 1")),
            ("transaction", tuple()),
        ],
    )
    @pytest.mark.parametrize(
        "blocked_method, blocked_args",
        [
            ("exec", ("SELECT 1",)),
            ("fetch_one", (..., "SELECT 1")),
            ("fetch_one_or_none", (..., "SELECT 1")),
            ("fetch_many", (..., "SELECT 1")),
            ("iter", (..., 1, "SELECT 1")),
            ("transaction", tuple()),
        ],
    )
    def test_connection_on_active_transaction_or_iteration_blocking_other_threads(  # <async>
        self,
        connection: Connection,
        executor: Executor,
        ctx_method: str,
        ctx_args: tuple,
        blocked_method: str,
        blocked_args: tuple,
    ) -> None:
        """
        Tests whether an active transaction blocks query executions
        in other execution contexts, i.e. threads/tasks.
        """

        ctx = getattr(connection, ctx_method)
        blocked = getattr(connection, blocked_method)

        result: int = 0
        continue_flag: bool = False

        def open_transaction_or_iter() -> None:  # <async>
            nonlocal continue_flag, result
            try:
                with ctx(*ctx_args) as _:  # <async>
                    continue_flag = True
                    sleep(1)  # <await>
                    result += 1
            finally:
                continue_flag = True

        def run_query() -> None:  # <async>
            nonlocal continue_flag, result

            while not continue_flag:
                sleep(0.1)  # <await>

            if blocked_method in {"iter", "transaction"}:
                with blocked(*blocked_args) as _:  # <async>
                    pass
            else:
                blocked(*blocked_args)  # <await>

            result += 1

        executor.submit(open_transaction_or_iter)
        executor.submit(run_query)

        executor.wait()  # <await>

        assert result == 2

    @pytest.mark.parametrize(
        "method, args",
        [
            ("iter", (..., 1, "SELECT 1")),
            ("transaction", tuple()),
        ],
    )
    def test_connection_on_open_runtime_error_due_to_active_transaction_or_iterator(  # <async>
        self, connection: Connection, method: str, args: tuple
    ) -> None:
        """
        Tests whether a `RuntimeError` is raised when attempting
        to open an already open connection while either a transaction
        or an iterator is active in the current execution context.
        """

        conn_method = getattr(connection, method)

        with conn_method(*args) as _:  # <async>

            with pytest.raises(RuntimeError) as exc_info:
                connection.open()  # <await>

        assert exc_info.value == Error.DbOpenConnection

    @pytest.mark.parametrize(
        "method, args, error",
        [
            ("iter", (..., 1, "SELECT 1"), Error.ConnActiveIteratorSameCtx),
            ("transaction", tuple(), Error.ConnActiveTransactionSameCtx),
        ],
    )
    def test_connection_on_close_runtime_error_due_to_active_transaction_or_iterator(  # <async>
        self, connection: Connection, method: str, args: tuple, error: RuntimeError
    ) -> None:
        """
        Tests whether a `RuntimeError` is raised when attempting
        to close a connection while either a transaction or an
        iterator is active in the current execution context.
        """

        conn_method = getattr(connection, method)

        with conn_method(*args) as _:  # <async>

            with pytest.raises(RuntimeError) as exc_info:
                connection.close()  # <await>

        assert exc_info.value == error

    @pytest.mark.parametrize(
        "method, args",
        [
            ("iter", (..., 1, "SELECT 1")),
            ("transaction", tuple()),
        ],
    )
    def test_connection_on_blocking_during_close(  # <async>
        self,
        connection: Connection,
        connection_B: Connection,
        executor: Executor,
        uid: int,
        method: str,
        args: tuple,
    ) -> None:
        """
        Tests whether the `close` method blocks if there
        is either an open transaction or iteration in
        another execution context.
        """

        conn_method = getattr(connection, method)

        continue_flag = False

        def fn() -> None:  # <async>
            nonlocal continue_flag
            with conn_method(*args) as _:  # <async>
                continue_flag = True
                sleep(2)  # <await>
                # NOTE: Use `connection_B` since `exec` can't be executed
                #       if `connection` is in the middle of an iteration.
                connection_B.exec(  # <await>
                    SQL.INSERT_INTO_TEST_TABLE(connection.driver), uid
                )

        executor.submit(fn)

        while not continue_flag:
            sleep(0.2)  # <await>
            continue

        connection.close()  # <await>

        assert (
            connection_B.fetch_one_or_none(  # <await>
                int, SQL.SELECT_FROM_TEST_TABLE(connection_B.driver), uid
            )
            == uid
        )

    @pytest.mark.filterwarnings("ignore")
    def test_connection_on_chaos_monkey(  # <async>
        self,
        connection: Connection,
        connection_B: Connection,
        executor: Executor,
        uid_factory: Callable[[], int],
        monkeypatch: Any,
    ) -> None:
        """
        Tests the connection under several simultaneous conditions:

        1. Multiple concurrent connection requests.
        2. Failing query executions.
        3. Closing the connection while queries are being executed.
        """

        num_tasks = 1000
        query = SQL.INSERT_INTO_TEST_TABLE(connection.driver)

        def close_conn() -> None:  # <async>
            """
            Close the connection pool after a while
            """
            sleep(2.5)  # <await>
            connection.close()  # <await>

        original_conn_exec = Connection.exec

        uids: list[int] = []

        def mock_conn_exec(*args: Any, **kwargs: Any) -> None:  # <async>
            """
            Mock `Connection.exec` to simulate some
            connections failing to execute their query.
            """
            if random() < 0.2:
                raise Exception()

            original_conn_exec(*args, **kwargs)  # <await>

            # Only add uid if `original_conn_exec` did
            # not raise an exception, probably due to
            # the underlying connection being closed.
            uid = args[-1]
            uids.append(uid)

        monkeypatch.setattr(
            "onlymaps._connection.Connection.exec",
            # NOTE: Have some connections fail during query execution.
            mock_conn_exec,
        )

        # Start all tasks.
        for _ in range(num_tasks):
            uid = uid_factory()
            executor.submit(connection.exec, query, uid)
        executor.submit(close_conn)

        executor.wait()  # <await>

        # Assert all UIDs were inserted into the database successfully.
        fetched_uids = connection_B.fetch_many(  # <await>
            int, SQL.SELECT_MANY_FROM_TEST_TABLE(connection.driver, len(uids)), *uids
        )

        assert 0 < len(uids) <= num_tasks
        assert sorted(uids) == sorted(fetched_uids)
