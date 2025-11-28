# Copyright (c) 2025 Manos Stoumpos
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

"""
This module contains the `Query` class which represents
a single SQL query to the database.
"""

import json
from collections.abc import Hashable
from contextlib import contextmanager
from types import EllipsisType, NoneType
from typing import Any, Callable, Iterator, TypeVar, cast, get_origin  # <async>

from pydantic import ValidationError

from onlymaps._drivers import BaseDriver
from onlymaps._spec import PyDbAPIv2Cursor
from onlymaps._types import STRICT_MODE, is_model_class, is_same_type
from onlymaps._utils import QueryString, SafeCursor

# isort: off
from onlymaps._utils import (
    EMPTY_ITER,  # <replace:ASYNC_EMPTY_ITER,>
)

# isort: on

from onlymaps._params import Bulk

T = TypeVar("T")
C = TypeVar("C", bound=Callable)


class Query:
    """
    A helper class used for querying the database.
    """

    def __init__(self, driver: BaseDriver, safe_cursor: SafeCursor) -> None:
        """
        A helper class used for querying the database.

        :param `BaseDriver` | None driver: The connection's underlying driver.
        :param `SafeCursor` lazy_cursor: A function, that when called,
            outputs a `PyDbAPIv2Cursor` context manager used to safely
            interact with the database.
        """
        self.__driver = driver
        self.__safe_cursor = safe_cursor

    def exec(  # <async>
        self,
        sql: QueryString,
        params: tuple[Any, ...],
        kwparams: dict[str, Any],
        /,
    ) -> None:
        """
        Executes the query.

        :param str sql: The SQL query to be executed.
        :param tuple[Any, ...] params: A tuple containing positional
            parameters for the query.
        :param dict[str, Any] kwparams: A dictionary containing keyword
            parameters for the query.
        """
        with self.iter(  # <async>
            NoneType, 0, sql, params, kwparams, allow_bulk=True
        ) as _:
            return None

    def one_or_none(  # <async>
        self,
        t: type[T] | EllipsisType,
        sql: QueryString,
        params: tuple[Any, ...],
        kwparams: dict[str, Any],
        /,
    ) -> T | Any | None:
        """
        Executes the query and returns a single row object of type `T`,
        if the query resulted in such object, else returns `None`.

        :param `T` | ellipsis t: The type to which the query result is mapped.
            If an `ellipsis` is provided, then neither type validation nor
            type casting occurs.
        :param str sql: The SQL query to be executed.
        :param tuple[Any, ...] params: A tuple containing positional
            parameters for the query.
        :param dict[str, Any] kwparams: A dictionary containing keyword
            parameters for the query.
        """
        with self.iter(t, 1, sql, params, kwparams) as it:  # <async>
            for batch in it:  # <async>
                return batch[0]
            return None

    def one(  # <async>
        self,
        t: type[T] | EllipsisType,
        sql: QueryString,
        params: tuple[Any, ...],
        kwparams: dict[str, Any],
        /,
    ) -> T | Any:
        """
        Executes the query and returns a single row object of type `T`.

        :param `T` | ellipsis t: The type to which the query result is mapped.
            If an `ellipsis` is provided, then neither type validation nor
            type casting occurs.
        :param str sql: The SQL query to be executed.
        :param tuple[Any, ...] params: A tuple containing positional
            parameters for the query.
        :param dict[str, Any] kwparams: A dictionary containing keyword
            parameters for the query.

        :raises ValueError: No row object was found to return.
        """
        if (obj := self.one_or_none(t, sql, params, kwparams)) is not None:  # <await>
            return obj
        raise ValueError("No row object to return.")

    def many(  # <async>
        self,
        t: type[T] | EllipsisType,
        sql: QueryString,
        params: tuple[Any, ...],
        kwparams: dict[str, Any],
        /,
    ) -> list[T] | list[Any]:
        """
        Executes the query and returns a a list of row objects of type `T`.

        :param `T` | ellipsis t: The type to which the query result is mapped.
            If an `ellipsis` is provided, then neither type validation nor
            type casting occurs.
        :param str sql: The SQL query to be executed.
        :param tuple[Any, ...] params: A tuple containing positional
            parameters for the query.
        :param dict[str, Any] kwparams: A dictionary containing keyword
            parameters for the query.
        """
        with self.iter(t, 128, sql, params, kwparams) as it:  # <async>
            # fmt: off
            return [
                row
                for batch in it # <async>
                for row in batch
            ]
            # fmt: on

    def __handle_params(
        self, params: tuple[Any, ...], kwparams: dict[str, Any], allow_bulk: bool
    ) -> tuple[Any, bool]:
        """
        Examines the provided SQL query parameters for inconsistencies
        and returns the preprocessed set of parameters to be injected
        into the query, along with a flag indicating whether said parameters
        are to be used as part of a bulk statement.

        :param `T` | ellipsis t: The type to which the query result is mapped.
            If an `ellipsis` is provided, then neither type validation nor
            type casting occurs.
        :param dict[str, Any] kwparams: Any keyword parmeters provided.
        :param bool allow_bulk: A flag indicating whether bulk statements
            are allowed.

        :raises ValueError: Both positional and keyword parameters were provided.
        :raises ValueError: Bulk statements are not allowed.
        :raises ValueError: One or more parameters were provided along with a `Bulk` param.
        """

        if params and kwparams:
            raise ValueError("Cannot include both positional and keyword parameters.")

        is_bulk = False

        def handle_param(param: Any) -> Any:
            nonlocal is_bulk
            match param:
                case Bulk():
                    if not allow_bulk:
                        raise ValueError("Use method `exec` for bulk statements.")
                    is_bulk = True
                    return param.value
                case _ if is_bulk:
                    raise ValueError(
                        "Cannot provide additional parameters in `_bulk` mode."
                    )
                case _:
                    return self.__driver.handle_sql_param(param)

        sql_params: tuple[Any, ...] | dict[str, Any]

        if params:
            sql_params = tuple(handle_param(p) for p in params)
            if is_bulk:
                sql_params = sql_params[0]
        else:
            sql_params = dict(
                zip(kwparams.keys(), map(handle_param, kwparams.values()))
            )

        return sql_params, is_bulk

    @contextmanager
    def iter(  # <async>
        self,
        t: type[T] | EllipsisType,
        size: int,
        sql: QueryString,
        params: tuple[Any, ...],
        kwparams: dict[str, Any],
        /,
        *,
        allow_bulk: bool = False,
    ) -> Iterator[Iterator[list[T]] | Iterator[list[Any]]]:
        """
        Executes the query and returns an iterator on batches of row objects
        of type `T`. Each batch of rows is loaded into memory during the
        iteration.

        :param `T` | ellipsis t: The type to which the query result is mapped.
            If an `ellipsis` is provided, then neither type validation nor
            type casting occurs.
        :param int size: The number of rows each batch contains.
        :param str sql: The SQL query to be executed.
        :param tuple[Any, ...] params: A tuple containing positional
            parameters for the query.
        :param dict[str, Any] kwparams: A dictionary containing keyword
            parameters for the query.
        :param bool allow_bulk: Whether to allow bulk parameters or not.

        :raises TypeError: `t` is not a type.
        :raises ValueError: Both positional and keyword arguments have been provided.
        :raises TypeError: The result cannot be successfully cast to type `t`.
        """

        _type: type[T] | Any = Any if isinstance(t, EllipsisType) else t

        if not (isinstance(_type, type) or get_origin(_type)):
            raise TypeError(f"`{_type}` is not a type!")

        sql_params, is_bulk = self.__handle_params(params, kwparams, allow_bulk)

        with self.__safe_cursor() as cursor:  # <async>
            if is_bulk:
                cursor.executemany(sql, sql_params)  # <await>
            else:
                cursor.execute(sql, sql_params)  # <await>

            if cursor.description and _type is not NoneType:
                adapter, map_to_original = self.__driver.handle_sql_result_type(
                    cast(Hashable, _type)
                )

                def deser(obj: Any) -> T:
                    try:
                        parsed = adapter.validate_python(obj, strict=STRICT_MODE)
                        return cast(T, map_to_original(parsed))
                    except ValidationError as exc:
                        err = exc.errors()[0]
                        err_msg = err["msg"]
                        err_input = repr(err["input"])
                        raise TypeError(f"{err_msg}, got `{err_input}`.") from exc

                is_model = is_model_class(_type)

                if len(cursor.description) == 1:

                    def handle_row(row: tuple[Any, ...]) -> T:
                        """
                        This function handles a row comprised of a single value.

                        - Model type: The row's value is presumed to be a JSON string
                            and therefore loaded into a python dictionary which is then
                            provided to the deserialization function.

                        - Non-model type: The row's value is given to the deserialization
                            function as-is.
                        """
                        val = row[0]
                        try:
                            obj = json.loads(val) if is_model else val
                        except (json.JSONDecodeError, TypeError) as exc:
                            raise TypeError(f"Invalid JSON value: `{exc}`.") from exc
                        return deser(obj)

                else:
                    colnames = [
                        self.__driver.std_colname(i, col[0])
                        for i, col in enumerate(cursor.description)
                    ]

                    as_dict = is_model or is_same_type(_type, dict)

                    def handle_row(row: tuple[Any, ...]) -> T:
                        """
                        This function handles a row comprised of more than one value.

                        - Model type: The row's values are zipped together with the row's
                            column names and provided as a dictionary to the deserialization
                            function.

                        - Non-model type: The whole row is given to the deserialization
                            function as-is in the form of a tuple.
                        """
                        return deser(dict(zip(colnames, row)) if as_dict else row)

                def iterate_cursor(  # <async>
                    cursor: PyDbAPIv2Cursor,
                ) -> Iterator[list[T]]:
                    while result := cursor.fetchmany(size):  # <await>
                        yield [handle_row(row) for row in result]

                yield iterate_cursor(cursor)

            else:
                yield EMPTY_ITER  # <replace:yield ASYNC_EMPTY_ITER>
