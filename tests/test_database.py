# Copyright (c) 2025 Manos Stoumpos
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

"""
This module contains tests related to the `Database` interface.
"""

from functools import partial
from threading import Lock  # <replace:from asyncio import Lock>
from time import sleep  # <replace:from asyncio import sleep>
from typing import Any, Callable, ContextManager, Iterator

import pytest

from onlymaps._connection import Connection
from onlymaps._drivers import Driver
from onlymaps._params import Bulk
from onlymaps._pool import ConnectionPool
from onlymaps._spec import Database, PyDbAPIv2Connection
from onlymaps._utils import Error, PyDbAPIv2ConnectionFactory
from tests.fixtures.connections import connection, db, dbapiv2
from tests.fixtures.executors import Executor
from tests.utils import (
    DRIVERS,
    ROW_VALUES,
    SCALAR_VALUES,
    SQL,
    get_row_type,
    get_row_values,
    handle_scalar_param,
)

# NOTE: Do not incude SQL Server / DuckDB for async tests.
# <include:DRIVERS = [d for d in DRIVERS if d not in {Driver.SQL_SERVER, Driver.DUCK_DB}]>


@pytest.mark.parametrize("pooling", [False, True])
@pytest.mark.parametrize("db", DRIVERS, indirect=True)
class TestDatabase:  # <replace:class TestAsyncDatabase:>
    """
    Tests the `Database` API.
    """

    def test_database_on_open_connection_runtime_error(  # <async>
        self, db: Database
    ) -> None:
        """
        Tests whether a `RuntimeError` is raised when opening
        an already open connection.
        """
        with pytest.raises(RuntimeError) as exc_info:
            db.open()  # <await>

        assert exc_info.value == Error.DbOpenConnection

    @pytest.mark.parametrize(
        "method, args",
        [
            ("exec", ("SELECT 1",)),
            ("fetch_one", (..., "SELECT 1")),
            ("fetch_one_or_none", (..., "SELECT 1")),
            ("fetch_many", (..., "SELECT 1")),
            ("iter", (..., 1, "SELECT 1")),
            ("transaction", tuple()),
            ("close", tuple()),
        ],
    )
    def test_database_on_closed_connection_runtime_error(  # <async>
        self, db: Database, method: str, args: tuple
    ) -> None:
        """
        Tests whether a `RuntimeError` is raised when executing
        a query with a non-open connection.
        """

        db_method = getattr(db, method)

        db.close()  # <await>

        with pytest.raises(RuntimeError) as exc_info:
            if method in {"iter", "transaction"}:
                with db_method(*args) as _:  # <async>
                    pass
            else:
                _ = db_method(*args)  # <await>

        assert exc_info.value == Error.DbClosedConnection

    def test_database_exec(self, db: Database) -> None:  # <async>
        """
        Tests whether `exec` executes a SQL query.
        """
        # fmt: off
        assert db.exec( # type: ignore # <await>
            SQL.SELECT_SINGLE_SCALAR(db.driver), 1
        ) is None
        # fmt: on

    def test_database_exec_on_bulk(  # <async>
        self, db: Database, uid_factory: Callable[[], int]
    ) -> None:
        """
        Tests whether `exec` executes a SQL query in bulk mode.
        """
        NUM_ELEMENTS = 5
        uids = [uid_factory() for _ in range(NUM_ELEMENTS)]
        batch = Bulk([[uid] for uid in uids])
        db.exec(SQL.INSERT_INTO_TEST_TABLE(db.driver), batch)  # <await>
        assert (
            db.fetch_many(  # <await>
                int, SQL.SELECT_MANY_FROM_TEST_TABLE(db.driver, NUM_ELEMENTS), *uids
            )
            == uids
        )

    @pytest.mark.parametrize("scalar", SCALAR_VALUES)
    def test_database_fetch_one_on_scalar(  # <async>
        self, db: Database, scalar: Any
    ) -> None:
        """
        Tests whether `fetch_one` retrieves a scalar value.
        """
        assert (
            db.fetch_one(  # <await>
                type(scalar),
                SQL.SELECT_SINGLE_SCALAR(db.driver),
                handle_scalar_param(scalar),
            )
            == scalar
        )

    @pytest.mark.parametrize("row", ROW_VALUES)
    def test_database_fetch_one_on_row(self, db: Database, row: Any) -> None:  # <async>
        """
        Tests whether `fetch_one` retrieves a row.
        """
        assert (
            db.fetch_one(  # <await>
                get_row_type(row),
                SQL.SELECT_SINGLE_ROW(db.driver),
                *get_row_values(row),
            )
            == row
        )

    def test_database_fetch_one_on_not_exists(self, db: Database) -> None:  # <async>
        """
        Tests whether `fetch_one` raises a `ValueError` exception
        whenever query results in no rows being fetched.
        """
        with pytest.raises(ValueError):
            db.fetch_one(int, SQL.SELECT_NONE)  # <await>

    @pytest.mark.parametrize("scalar", SCALAR_VALUES)
    def test_database_fetch_one_or_none_on_scalar_exists(  # <async>
        self, db: Database, scalar: Any
    ) -> None:
        """
        Tests whether `fetch_one_or_none` retrieves a scalar value.
        """
        assert (
            db.fetch_one_or_none(  # <await>
                type(scalar),
                SQL.SELECT_SINGLE_SCALAR(db.driver),
                handle_scalar_param(scalar),
            )
            == scalar
        )

    @pytest.mark.parametrize("row", ROW_VALUES)
    def test_database_fetch_one_or_none_on_row_exists(  # <async>
        self, db: Database, row: Any
    ) -> None:
        """
        Tests whether `fetch_one_or_none` retrieves a row.
        """
        assert (
            db.fetch_one_or_none(  # type: ignore # <await>
                get_row_type(row),
                SQL.SELECT_SINGLE_ROW(db.driver),
                *get_row_values(row),
            )
            == row
        )

    def test_database_fetch_one_or_none_on_not_exists(  # <async>
        self, db: Database
    ) -> None:
        """
        Tests whether `fetch_one_or_none` returns `None` when
        there are no values to be returned.
        """
        assert db.fetch_one_or_none(int, SQL.SELECT_NONE) is None  # <await>

    @pytest.mark.parametrize("scalar", SCALAR_VALUES)
    def test_database_fetch_many_on_scalar(  # <async>
        self, db: Database, scalar: Any
    ) -> None:
        """
        Tests whether `fetch_many` retrieves a list of scalar values.
        """
        assert db.fetch_many(  # <await>
            type(scalar),
            SQL.SELECT_MULTIPLE_SCALAR(db.driver),
            scalar=handle_scalar_param(scalar),
        ) == [scalar, scalar]

    def test_database_fetch_many_on_not_exists(self, db: Database) -> None:  # <async>
        """
        Tests whether `fetch_many` retrieves an empty list when
        there are no values to be returned.
        """
        assert db.fetch_many(int, SQL.SELECT_NONE) == []  # <await>

    @pytest.mark.parametrize("row", ROW_VALUES)
    def test_database_fetch_many_on_row(  # <async>
        self, db: Database, row: Any
    ) -> None:
        """
        Tests whether `fetch_many` retrieves a list of rows.
        """
        params = get_row_values(row)
        assert db.fetch_many(  # <await>
            get_row_type(row), SQL.SELECT_MULTIPLE_ROW(db.driver), *params, *params
        ) == [row, row]

    @pytest.mark.parametrize("scalar", SCALAR_VALUES)
    @pytest.mark.parametrize("size", [1, 2])
    def test_database_iter_on_scalar(  # <async>
        self, db: Database, scalar: Any, size: int
    ) -> None:
        """
        Tests whether `iter` yields an iterator over predeterminedly-sized
        batches of retrieved scalar values.
        """
        with db.iter(  # <async>
            type(scalar),
            size,
            SQL.SELECT_MULTIPLE_SCALAR(db.driver),
            scalar=handle_scalar_param(scalar),
        ) as it:
            for batch in it:  # <async>
                assert batch == size * [scalar]

    @pytest.mark.parametrize("row", ROW_VALUES)
    @pytest.mark.parametrize("size", [1, 2])
    def test_database_iter_on_row(  # <async>
        self, db: Database, row: Any, size: int
    ) -> None:
        """
        Tests whether `iter` yields an iterator over predeterminedly-sized
        batches of retrieved rows.
        """
        params = get_row_values(row)
        it_ctx: ContextManager[Iterator[list]] = db.iter(
            get_row_type(row),
            size,
            SQL.SELECT_MULTIPLE_ROW(db.driver),
            *params,
            *params,
        )
        with it_ctx as it:  # <async>
            for batch in it:  # <async>
                assert batch == size * [row]

    def test_database_transaction_multiple_queries(  # <async>
        self, db: Database, uid: int
    ) -> None:
        """
        Tests whether another query within the same transaction
        can view any changes so far.
        """
        with db.transaction():  # <async>
            db.exec(SQL.INSERT_INTO_TEST_TABLE(db.driver), uid)  # <await>
            assert (
                db.fetch_one_or_none(  # <await>
                    int, SQL.SELECT_FROM_TEST_TABLE(db.driver), uid
                )
                == uid
            )

    def test_database_transaction_on_uncommitted_changes(  # <async>
        self, db: Database, connection_B: Connection, uid: int
    ) -> None:
        """
        Tests whether connection A is unable to see changes done by
        connection B while the latter is still in transaction mode.
        """
        with db.transaction():  # <async>
            db.exec(SQL.INSERT_INTO_TEST_TABLE(db.driver), uid)  # <await>
            # SQL Server implements READ COMMITTED isolation via
            # locking, not MVCC, therefore the query should fail
            # instead due to the lock held on the table.
            if db.driver == Driver.SQL_SERVER:
                # NOTE: Sleep for a little bit to ensure the database
                #       has locked the table, as sometimes not waiting
                #       might cause this test to fail.
                sleep(0.5)  # <await>
                with pytest.raises(Exception):
                    connection_B.fetch_one_or_none(  # <await>
                        int, SQL.SELECT_FROM_TEST_TABLE(db.driver), uid
                    )
            else:
                assert (
                    connection_B.fetch_one_or_none(  # <await>
                        int, SQL.SELECT_FROM_TEST_TABLE(db.driver), uid
                    )
                    is None
                )

    def test_database_transaction_on_committed_changes(  # <async>
        self, db: Database, connection: Connection, uid: int
    ) -> None:
        """
        Tests whether connection A is able to see changes done by
        connection B after the latter has exited transaction mode.
        """
        with db.transaction():  # <async>
            db.exec(SQL.INSERT_INTO_TEST_TABLE(db.driver), uid)  # <await>

        assert (
            connection.fetch_one_or_none(  # <await>
                int, SQL.SELECT_FROM_TEST_TABLE(db.driver), uid
            )
            == uid
        )

    def test_database_transaction_on_exception_raised(  # <async>
        self, db: Database, uid: int
    ) -> None:
        """
        Tests whether changes are not committed if an exception
        is raised in the middle of a transaction.
        """
        try:
            with db.transaction():  # <async>
                db.exec(SQL.INSERT_INTO_TEST_TABLE(db.driver), uid)  # <await>
                raise Exception()
        except:
            pass

        assert (
            db.fetch_one_or_none(  # <await>
                int, SQL.SELECT_FROM_TEST_TABLE(db.driver), uid
            )
            is None
        )

    def test_database_transaction_on_beginning_another_transaction(  # <async>
        self, db: Database
    ) -> None:
        """
        Tests whether a `RuntimeError` exception is raised
        if a transaction is started in the middle of another
        transaction.
        """
        with db.transaction():  # <async>
            with pytest.raises(RuntimeError) as exc_info:
                with db.transaction():  # <async>
                    pass

        assert exc_info.value == Error.ConnActiveTransactionSameCtx

    def test_database_on_concurrent_close(  # <async>
        self, db: Database, executor: Executor
    ) -> None:
        """
        Tests whether only one thread/task can successfully
        close a database.
        """

        lock = Lock()
        error_counter = 0

        def close_pool() -> None:  # <async>
            nonlocal error_counter
            try:
                db.close()  # <await>
            except RuntimeError:
                with lock:  # <async>
                    error_counter += 1

        num_tasks = 10

        for _ in range(num_tasks):
            executor.submit(close_pool)

        executor.wait()  # <await>

        assert error_counter == num_tasks - 1


@pytest.mark.parametrize("connection", [Driver.POSTGRES], indirect=True)
def test_database_fetch_on_connection_rollback(  # <async>
    connection: Connection, uid: int
) -> None:
    """
    Tests whether any changes are rolled back if an exception
    is raised during the execution of a query.

    NOTE: This test is only executed on `Postgres`
    due to the `INSERT RETURN` command.
    """

    # Insert `uid` into the database, though deliberately cause the statement
    # to fail by using the wrong type for the returning IDs. This means that
    # the query gets executed successfully though an exception is raised during
    # the mapping phase.
    try:
        connection.fetch_one(  # <await>
            str, SQL.INSERT_INTO_TEST_TABLE(connection.driver, returning_id=True), uid
        )
    except:
        pass

    # Next, simply execute a successful query.
    # This is done to enforce a commit after
    # the exception was raised.
    connection.exec("SELECT 1")  # <await>

    # Assert that `uid` has not been inserted into the database.
    assert (
        connection.fetch_one_or_none(  # <await>
            int, SQL.SELECT_FROM_TEST_TABLE(connection.driver), uid
        )
        is None
    )


@pytest.mark.parametrize(
    "db_factory", [Connection, partial(ConnectionPool, min_pool_size=1)]
)
def test_database_on_failed_open(  # <async>
    db_factory: Callable[[PyDbAPIv2ConnectionFactory], Database]
) -> None:
    """
    Tests whether only one thread/task can successfully
    open a connection pool.
    """

    class PyDbFactoryError(Exception):
        """
        A custom exception class.
        """

    def pydb_factory() -> PyDbAPIv2Connection:  # <async>
        raise PyDbFactoryError()

    db = db_factory(pydb_factory)

    with pytest.raises(PyDbFactoryError):
        db.open()  # <await>

    assert db.is_open is False


@pytest.mark.parametrize("dbapiv2", DRIVERS, indirect=True)
@pytest.mark.parametrize("db_factory", [Connection, ConnectionPool])
def test_database_on_concurrent_open(  # <async>
    dbapiv2: PyDbAPIv2Connection,
    db_factory: Callable[[PyDbAPIv2ConnectionFactory], Database],
    executor: Executor,
) -> None:
    """
    Tests whether only one thread/task can successfully
    open a connection pool.
    """

    def pydb_factory() -> PyDbAPIv2Connection:  # <async>
        return dbapiv2

    db = db_factory(pydb_factory)

    lock = Lock()
    error_counter = 0

    def open_pool() -> None:  # <async>
        nonlocal error_counter
        try:
            db.open()  # <await>
        except RuntimeError:
            with lock:  # <async>
                error_counter += 1

    num_tasks = 10

    for _ in range(num_tasks):
        executor.submit(open_pool)

    executor.wait()  # <await>

    assert error_counter == num_tasks - 1


@pytest.mark.parametrize("connection", [Driver.POSTGRES], indirect=True)
def test_database_iter_on_connection_rollback(  # <async>
    connection: Connection, uid: int
) -> None:
    """
    Tests whether any changes are rolled back if an exception
    is raised during an iteration.

    NOTE: This test is only executed on `Postgres`
    due to the `INSERT RETURN` command.
    """

    # Fetch an iterator then raise an Exception.
    try:
        with connection.iter(  # <async>
            int,
            1,
            SQL.INSERT_INTO_TEST_TABLE(connection.driver, returning_id=True),
            uid,
        ) as _:
            raise Exception()
    except:
        pass

    # Next, simply execute a successful query.
    # This is done to enforce a commit after
    # the exception was raised.
    connection.exec("SELECT 1")  # <await>

    # Assert that `uid` has not been inserted into the database.
    assert (
        connection.fetch_one_or_none(  # <await>
            int, SQL.SELECT_FROM_TEST_TABLE(connection.driver), uid
        )
        is None
    )


@pytest.mark.parametrize("connection", [Driver.POSTGRES], indirect=True)
def test_database_iter_on_commit(connection: Connection, uid: int) -> None:  # <async>
    """
    Tests whether any changes are committed even if
    no rows are actually fetched, as long as the
    context exits successfully.

    NOTE: This test is only executed on `Postgres`
    due to the `INSERT RETURN` command.
    """

    # Fetch an iterator then do nothing.
    with connection.iter(  # <async>
        int, 1, SQL.INSERT_INTO_TEST_TABLE(connection.driver, returning_id=True), uid
    ) as _:
        pass

    # Assert that `uid` has been inserted into the database.
    assert (
        connection.fetch_one_or_none(  # <await>
            int, SQL.SELECT_FROM_TEST_TABLE(connection.driver), uid
        )
        == uid
    )
