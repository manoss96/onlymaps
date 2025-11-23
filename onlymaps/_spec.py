# Copyright (c) 2025 Manos Stoumpos
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

"""
This module contains the Python Database API v2.0 protocol classes
on which this package is based, as well as the protocol for the
public API this package provides so as to interact with a database.
"""

from contextlib import contextmanager

# <include:from typing import Awaitable>
from types import EllipsisType
from typing import Any, Iterator, Protocol, Self, Sequence, TypeVar

from typing_extensions import overload

from onlymaps._drivers import Driver
from onlymaps._types import QueryString

T = TypeVar("T")


class PyDbAPIv2Cursor(Protocol):
    """
    The Python Database API v2.0 `Cursor` class specification protocol.

    See: https://peps.python.org/pep-0249/#cursor-objects
    """

    @property
    def description(
        self,
    ) -> tuple[tuple[str, None, None, None, None, None, None], ...] | Any:
        """
        See: https://peps.python.org/pep-0249/#description
        """

    def execute(  # <async>
        self,
        operation: QueryString,
        params: dict[str, Any] | Sequence[Any] = ...,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Self | None:
        """
        See: https://peps.python.org/pep-0249/#id20
        """

    def executemany(  # <async>
        self,
        operation: QueryString,
        params: Sequence[Sequence[Any] | dict[str, Any]],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Self | None:
        """
        See: https://peps.python.org/pep-0249/#executemany
        """

    def fetchmany(self, size: int = 0) -> list[Any] | None:  # <async>
        """
        See: https://peps.python.org/pep-0249/#fetchmany
        """

    def close(self) -> bool | None:  # <async>
        """
        See: https://peps.python.org/pep-0249/#Cursor.close
        """


class PyDbAPIv2Connection(Protocol):
    """
    The Python Database API v2.0 `Connection` class specification protocol.

    See: https://peps.python.org/pep-0249/#connection-objects
    """

    def close(self) -> None:  # <replace:def close(self) -> None | Awaitable[None]:>
        """
        See: https://peps.python.org/pep-0249/#Connection.close
        """

    def commit(self) -> None:  # <async>
        """
        See: https://peps.python.org/pep-0249/#commit
        """

    def rollback(self) -> None:  # <async>
        """
        See: https://peps.python.org/pep-0249/#rollback
        """

    # fmt: off
    def cursor(
        self, *args: Any, **kwargs: Any
    ) -> PyDbAPIv2Cursor: # <replace:) -> AsyncPyDbAPIv2Cursor | Awaitable[AsyncPyDbAPIv2Cursor]:>
    # fmt: on
        """
        See: https://peps.python.org/pep-0249/#cursor
        """


class PyDbAPIv2Module(Protocol):
    """
    The Python Database API v2.0 module specification protocol.

    See: https://peps.python.org/pep-0249/#module-interface
    """

    apilevel: str
    paramstyle: str

    # <ignore>
    @property
    def threadsafety(self) -> int | bool:
        """
        See: https://peps.python.org/pep-0249/#threadsafety
        """

    # <ignore>

    def connect(  # <async> pylint: disable=no-self-argument
        *args: Any, **kwargs: Any
    ) -> PyDbAPIv2Connection:
        """
        See: https://peps.python.org/pep-0249/#connect
        """


class Database(Protocol):
    """
    This is the OnlyMaps public API for database interaction.
    """

    @property
    def driver(self) -> Driver:
        """
        The connection's underlying driver.
        """

    @property
    def is_open(self) -> bool:
        """
        Indicates whether a connection to the database
        has been established or not.
        """

    @overload
    def exec(  # <async>
        self,
        sql: QueryString,
        /,
        *args: Any,
    ) -> None:
        """
        Executes the given SQL query.

        :param QueryString sql: The SQL query to be executed.
        :param Any *args: A sequence of positional arguments to be used
            as query parameters.

        :raises RuntimeError: Connection is not open.
        """

    @overload
    def exec(  # <async>
        self,
        sql: QueryString,
        /,
        **kwargs: Any,
    ) -> None:
        """
        Executes the given SQL query.

        :param QueryString sql: The SQL query to be executed.
        :param Any **kwargs: A sequence of keyword arguments to be used
            as query parameters.

        :raises RuntimeError: Connection is not open.
        """

    @overload
    def fetch_one_or_none(  # <async>
        self,
        t: type[T],
        sql: QueryString,
        /,
        *args: Any,
    ) -> T | None:
        """
        Executes the query and returns a single row object of type `T`,
        if the query resulted in such object, else returns `None`.

        :param `T` t: The type to which the query result is mapped.
        :param str sql: The SQL query to be executed.
        :param Any *args: A sequence of positional arguments to be used
            as query parameters.

        :raises RuntimeError: Connection is not open.
        """

    @overload
    def fetch_one_or_none(  # <async>
        self,
        t: type[T],
        sql: QueryString,
        /,
        **kwargs: Any,
    ) -> T | None:
        """
        Executes the query and returns a single row object of type `T`,
        if the query resulted in such object, else returns `None`.

        :param `T` t: The type to which the query result is mapped.
        :param str sql: The SQL query to be executed.
        :param Any **kwargs: A sequence of keyword arguments to be used
            as query parameters.

        :raises RuntimeError: Connection is not open.
        """

    @overload
    def fetch_one_or_none(  # <async>
        self,
        t: EllipsisType,
        sql: QueryString,
        /,
        *args: Any,
    ) -> Any | None:
        """
        Executes the query and returns a single row object if the
        query resulted in such object, else returns `None`.

        :param ellipsis t: Using ellipsis in place of `t` results in
            no type check or type casting occurring.
        :param str sql: The SQL query to be executed.
        :param Any *args: A sequence of positional arguments to be used
            as query parameters.

        :raises RuntimeError: Connection is not open.
        """

    @overload
    def fetch_one_or_none(  # <async>
        self,
        t: EllipsisType,
        sql: QueryString,
        /,
        **kwargs: Any,
    ) -> Any | None:
        """
        Executes the query and returns a single row object if the
        query resulted in such object, else returns `None`.

        :param ellipsis t: Using ellipsis in place of `t` results in
            no type check or type casting occurring.
        :param str sql: The SQL query to be executed.
        :param Any **kwargs: A sequence of keyword arguments to be used
            as query parameters.

        :raises RuntimeError: Connection is not open.
        """

    @overload
    def fetch_one(  # <async>
        self,
        t: type[T],
        sql: QueryString,
        /,
        *args: Any,
    ) -> T:
        """
        Executes the query and returns a single row object of type `T`.

        :param `T` t: The type to which the query result is mapped.
        :param str sql: The SQL query to be executed.
        :param Any *args: A sequence of positional arguments to be used
            as query parameters.

        :raises ValueError: No row object was found to return.
        :raises RuntimeError: Connection is not open.
        """

    @overload
    def fetch_one(  # <async>
        self,
        t: type[T],
        sql: QueryString,
        /,
        **kwargs: Any,
    ) -> T:
        """
        Executes the query and returns a single row object of type `T`.

        :param `T` t: The type to which the query result is mapped.
        :param str sql: The SQL query to be executed.
        :param Any **kwargs: A sequence of keyword arguments to be used
            as query parameters.

        :raises ValueError: No row object was found to return.
        :raises RuntimeError: Connection is not open.
        """

    @overload
    def fetch_one(  # <async>
        self,
        t: EllipsisType,
        sql: QueryString,
        /,
        *args: Any,
    ) -> Any:
        """
        Executes the query and returns a single row object.

        :param ellipsis t: Using ellipsis in place of `t` results in
            no type check or type casting occurring.
        :param str sql: The SQL query to be executed.
        :param Any *args: A sequence of positional arguments to be used
            as query parameters.

        :raises ValueError: No row object was found to return.
        :raises RuntimeError: Connection is not open.
        """

    @overload
    def fetch_one(  # <async>
        self,
        t: EllipsisType,
        sql: QueryString,
        /,
        **kwargs: Any,
    ) -> Any:
        """
        Executes the query and returns a single row object.

        :param ellipsis t: Using ellipsis in place of `t` results in
            no type check or type casting occurring.
        :param QueryString sql: The SQL query to be executed.
        :param Any **kwargs: A sequence of keyword arguments to be used
            as query parameters.

        :raises ValueError: No row object was found to return.
        :raises RuntimeError: Connection is not open.
        """

    @overload
    def fetch_many(  # <async>
        self,
        t: type[T],
        sql: QueryString,
        /,
        *args: Any,
    ) -> list[T]:
        """
        Executes the query and returns a a list of row objects of type `T`.

        :param `T` t: The type to which the query result is mapped.
        :param QueryString sql: The SQL query to be executed.
        :param Any *args: A sequence of positional arguments to be used
            as query parameters.

        :raises RuntimeError: Connection is not open.
        """

    @overload
    def fetch_many(  # <async>
        self,
        t: type[T],
        sql: QueryString,
        /,
        **kwargs: Any,
    ) -> list[T]:
        """
        Executes the query and returns a a list of row objects of type `T`.

        :param `T` t: The type to which the query result is mapped.
        :param QueryString sql: The SQL query to be executed.
        :param Any **kwargs: A sequence of keyword arguments to be used
            as query parameters.

        :raises RuntimeError: Connection is not open.
        """

    @overload
    def fetch_many(  # <async>
        self,
        t: EllipsisType,
        sql: QueryString,
        /,
        *args: Any,
    ) -> list[Any]:
        """
        Executes the query and returns a a list of row objects.

        :param ellipsis t: Using ellipsis in place of `t` results in
            no type check or type casting occurring.
        :param str sql: The SQL query to be executed.
        :param Any *args: A sequence of positional arguments to be used
            as query parameters.

        :raises RuntimeError: Connection is not open.
        """

    @overload
    def fetch_many(  # <async>
        self,
        t: EllipsisType,
        sql: QueryString,
        /,
        **kwargs: Any,
    ) -> list[Any]:
        """
        Executes the query and returns a a list of row objects.

        :param ellipsis t: Using ellipsis in place of `t` results in
            no type check or type casting occurring.
        :param str sql: The SQL query to be executed.
        :param Any **kwargs: A sequence of keyword arguments to be used
            as query parameters.

        :raises RuntimeError: Connection is not open.
        """

    @overload
    @contextmanager
    def iter(
        self,
        t: type[T],
        size: int,
        sql: QueryString,
        /,
        *args: Any,
    ) -> Iterator[Iterator[list[T]]]:
        """
        Executes the query and returns an iterator on batches of row objects
        of type `T`. Each batch of rows is loaded into memory during the
        iteration.

        :param `T` t: The type to which the query result is mapped.
        :param int size: The number of rows each batch contains.
        :param str sql: The SQL query to be executed.
        :param Any *args: A sequence of positional arguments to be used
            as query parameters.

        :raises RuntimeError: Connection is not open.
        """

    @overload
    @contextmanager
    def iter(
        self,
        t: type[T],
        size: int,
        sql: QueryString,
        /,
        **kwargs: Any,
    ) -> Iterator[Iterator[list[T]]]:
        """
        Executes the query and returns an iterator on batches of row objects
        of type `T`. Each batch of rows is loaded into memory during the
        iteration.

        :param `T` t: The type to which the query result is mapped.
        :param int size: The number of rows each batch contains.
        :param str sql: The SQL query to be executed.
        :param Any **kwargs: A sequence of keyword arguments to be used
            as query parameters.

        :raises RuntimeError: Connection is not open.
        """

    @overload
    @contextmanager
    def iter(
        self,
        t: EllipsisType,
        size: int,
        sql: QueryString,
        /,
        *args: Any,
    ) -> Iterator[Iterator[list[Any]]]:
        """
        Executes the query and returns an iterator on batches of row objects
        of type `T`. Each batch of rows is loaded into memory during the
        iteration.

        :param ellipsis t: Using ellipsis in place of `t` results in
            neither type validation nor type casting occurring.
        :param int size: The number of rows each batch contains.
        :param str sql: The SQL query to be executed.
        :param Any *args: A sequence of positional arguments to be used
            as query parameters.

        :raises RuntimeError: Connection is not open.
        """

    @overload
    @contextmanager
    def iter(
        self,
        t: EllipsisType,
        size: int,
        sql: QueryString,
        /,
        **kwargs: Any,
    ) -> Iterator[Iterator[list[Any]]]:
        """
        Executes the query and returns an iterator on batches of row objects
        of type `T`. Each batch of rows is loaded into memory during the
        iteration.

        :param ellipsis t: Using ellipsis in place of `t` results in
            neither type validation nor type casting occurring.
        :param int size: The number of rows each batch contains.
        :param str sql: The SQL query to be executed.
        :param Any **kwargs: A sequence of keyword arguments to be used
            as query parameters.

        :raises RuntimeError: Connection is not open.
        """

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """
        Opens a trasnaction so that any changes caused by any
        queries executed under said transaction are either all
        committed together after the transaction successfully exits,
        or none of them are if an exception is raised during the
        transaction.

        :raises RuntimeError: Connection is not open.
        """

    def open(self) -> None:  # <async>
        """
        Establishes a connection to the database.

        :raises RuntimeError: Connection is already open.
        """

    def close(self) -> None:  # <async>
        """
        Closes the underlying connection to the database.

        :raises RuntimeError: Connection is not open.
        """

    def __enter__(self) -> Self:  # <async>
        """
        Opens a connection to the database and returns
        the instance itself.
        """

    def __exit__(self, *_: tuple[Any, ...]) -> None:  # <async>
        """
        Closes the connection to the database.
        """
