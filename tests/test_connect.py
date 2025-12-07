# Copyright (c) 2025 Manos Stoumpos
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

"""
This module contains tests related to the `connect` function,
i.e. the package's entrypoint.
"""

import pytest

from onlymaps import connect  # <replace:from onlymaps.asyncio import connect>
from onlymaps._connection import Connection
from onlymaps._drivers import Driver
from onlymaps._pool import ConnectionPool
from onlymaps._spec import PyDbAPIv2Connection
from tests.fixtures.connections import db, dbapiv2
from tests.fixtures.containers import db_container
from tests.utils import DRIVERS, DbContainer, get_conn_str_and_kwargs_from_container

# NOTE: Do not incude SQL Server for async tests.
# <include:DRIVERS = [d for d in DRIVERS if d != Driver.SQL_SERVER]>


@pytest.mark.parametrize("db_container", DRIVERS, indirect=True)
class TestConnect:  # <replace:class TestAsyncConnect:>
    """
    Tests the `connect` function.
    """

    def test_connect_via_conn_str(self, db_container: DbContainer) -> None:  # <async>
        """
        Tests whether a connection can be successfully established
        via a connection string.
        """

        conn_str, kwargs = get_conn_str_and_kwargs_from_container(db_container)

        db = connect(conn_str, **kwargs)

        db.open()  # <await>

        db.close()  # <await>

    def test_connect_via_conn_str_as_ctx(  # <async>
        self, db_container: DbContainer
    ) -> None:
        """
        Tests whether a connection can be successfully established
        as a context manager via a connection string.
        """

        conn_str, kwargs = get_conn_str_and_kwargs_from_container(db_container)

        with connect(conn_str, **kwargs) as _:  # <async>
            pass

    def test_connect_via_conn_str_is_connection(  # <async>
        self, db_container: DbContainer
    ) -> None:
        """
        Tests whether calling the `connect` function returns a
        `Connection` instance by default when providing a connection
        string.
        """

        conn_str, kwargs = get_conn_str_and_kwargs_from_container(db_container)

        db = connect(conn_str, **kwargs)
        assert isinstance(db, Connection)

    def test_connect_via_conn_str_is_connection_pool(  # <async>
        self, db_container: DbContainer
    ) -> None:
        """
        Tests whether calling the `connect` function returns a
        `ConnectionPool` instance by default when providing a
        connection string.
        """

        conn_str, kwargs = get_conn_str_and_kwargs_from_container(db_container)
        db = connect(conn_str, pooling=True, **kwargs)
        assert isinstance(db, ConnectionPool)

    def test_connect_via_conn_factory(  # <async>
        self, db_container: DbContainer, dbapiv2: PyDbAPIv2Connection
    ) -> None:
        """
        Tests whether a connection can be successfully established
        via a connection factory.
        """

        def conn_factory() -> PyDbAPIv2Connection:  # <async>
            return dbapiv2  # pragma: no cover

        db = connect(conn_factory)

        db.open()  # <await>

        db.close()  # <await>

    def test_connect_via_conn_factory_as_ctx(  # <async>
        self, db_container: DbContainer, dbapiv2: PyDbAPIv2Connection
    ) -> None:
        """
        Tests whether a connection can be successfully established
        as a context manager via a connection factory.
        """

        def conn_factory() -> PyDbAPIv2Connection:  # <async>
            return dbapiv2  # pragma: no cover

        with connect(conn_factory) as _:  # <async>
            pass

    def test_connect_via_conn_factory_is_connection(  # <async>
        self, db_container: DbContainer, dbapiv2: PyDbAPIv2Connection
    ) -> None:
        """
        Tests whether calling the `connect` function returns a
        `Connection` instance by default when providing a connection
        factory.
        """

        def conn_factory() -> PyDbAPIv2Connection:  # <async>
            return dbapiv2  # pragma: no cover

        db = connect(conn_factory)
        assert isinstance(db, Connection)

    def test_connect_via_conn_factory_is_connection_pool(  # <async>
        self, db_container: DbContainer, dbapiv2: PyDbAPIv2Connection
    ) -> None:
        """
        Tests whether calling the `connect` function returns a
        `ConnectionPool` instance by default when providing a
        connection factory.
        """

        def conn_factory() -> PyDbAPIv2Connection:  # <async>
            return dbapiv2  # pragma: no cover

        db = connect(conn_factory, pooling=True)
        assert isinstance(db, ConnectionPool)

    def test_connect_on_value_error(self, db_container: DbContainer) -> None:  # <async>
        """
        Tests whether a `ValueError` exception is raised when providing
        the `connect` function with neither a connection string nor a
        callable.
        """

        with pytest.raises(ValueError):
            _ = connect(1)  # type: ignore

    def test_connect_on_set_autocommit_argument(  # <async>
        self, db_container: DbContainer
    ) -> None:
        """
        Tests whether a `ValueError` exception is raised when providing
        the `connect` function with argument `autocommit`.
        """

        conn_str, _ = get_conn_str_and_kwargs_from_container(db_container)

        with pytest.raises(ValueError):
            with connect(conn_str, autocommit=True) as _:  # <async>
                pass

    def test_connect_on_set_pool_argument(  # <async>
        self, db_container: DbContainer
    ) -> None:
        """
        Tests whether a `ValueError` exception is raised when providing
        the `connect` function with an unknown pool argument.
        """

        conn_str, _ = get_conn_str_and_kwargs_from_container(db_container)

        with pytest.raises(ValueError):
            with connect(conn_str, some_pool_arg=3) as _:  # <async>
                pass


# <ignore>
@pytest.mark.parametrize("db_container", [Driver.SQL_SERVER], indirect=True)
def test_connect_on_sql_server_set_timeout_argument(
    db_container: DbContainer,
) -> None:
    """
    Tests whether a `ValueError` exception is raised when providing
    the `connect` function with a `timeout` argument while using the
    `SQL_Server` driver.
    """

    conn_str, _ = get_conn_str_and_kwargs_from_container(db_container)

    with pytest.raises(ValueError):
        with connect(conn_str, timeout=2) as _:
            pass


# <ignore>
