# Copyright (c) 2025 Manos Stoumpos
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

"""
This module contains all database driver classes that are used so as to
handler driver-specific issues that mostly relate to the types of SQL query
parameters and their results.
"""

import json
from abc import ABC, abstractmethod
from dataclasses import is_dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum, StrEnum
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Callable, Self, TypeVar, final
from uuid import UUID

from pydantic import BaseModel, TypeAdapter
from pydantic.dataclasses import is_pydantic_dataclass
from pydantic_core import to_jsonable_python
from typing_extensions import override

from onlymaps._params import Json
from onlymaps._types import OnlymapsBool, OnlymapsBytes, OnlymapsType

if TYPE_CHECKING:
    import oracledb
else:
    try:
        import oracledb
    except ImportError:
        pass

T = TypeVar("T")


class Driver(StrEnum):
    """
    The Database driver Enum class.
    """

    POSTGRES = "postgresql"
    MY_SQL = "mysql"
    SQL_SERVER = "mssql"
    MARIA_DB = "mariadb"
    ORACLE_DB = "oracledb"
    SQL_LITE = "sqlite"
    DUCK_DB = "duckdb"
    SNOWFLAKE = "snowflake"
    UNKNOWN = "?"


class ParamStyle(StrEnum):
    """
    A string enum for query paramstyle.
    """

    QMARK = "qmark"
    NUMERIC = "numeric"
    NAMED = "named"
    FORMAT = "format"
    PYFORMAT = "pyformat"
    UNKNOWN = "unknown"


class BaseDriver(ABC):
    """
    This is the base driver class.
    """

    def __init__(self, apilevel: str, threadsafety: int, paramstyle: ParamStyle):
        """
        Instantiates a `BaseDriver`.

        :param str apilevel: Must be `"2.0"`.
        :param int threadsafety: An integer indicating the
            driver's thread safety as dictated by DB API v2.
        :param ParamStyle threadsafety: A string indicating the
            driver's parameter style as dictated by DB API v2.
        """
        super().__init__()
        assert apilevel == "2.0"
        self.__threadsafety = threadsafety
        self.__paramstyle = paramstyle

    @property
    def threadsafety(self) -> int:
        """
        See: https://peps.python.org/pep-0249/#threadsafety
        """
        return self.__threadsafety  # pragma: no cover

    @property
    def paramstyle(self) -> ParamStyle:
        """
        See: https://peps.python.org/pep-0249/#paramstyle
        """
        return self.__paramstyle  # pragma: no cover

    @property
    @abstractmethod
    def tag(self) -> Driver:
        """
        The driver's type.
        """

    def init_connection(self, conn: Any) -> Any:
        """
        This function is used to perform any driver-specific
        initialization logic right after the instantiation of
        a PyDbAPIv2 connection. Returns the now-initialized
        connection.

        :param Any conn: A PyDbAPIv2-compatible connection object.
        """
        return conn

    def init_transaction(self, _: Any) -> None:
        """
        This function is used to perform any driver-specific
        initialization logic right before the execution of a
        transaction.

        :param Any conn: A PyDbAPIv2-compatible connection object.
        """
        return None

    def handle_sql_param(self, param: Any) -> Any:
        """
        Some Python types are not supported by certain drivers. This function
        handles these cases and maps parameters of these types to a type that
        is supported.

        :param Any param: An SQL query parameter.
        """
        match param:
            case UUID():
                return str(param)
            case Json():
                return json.dumps(
                    to_jsonable_python(param.value), separators=(",", ":")
                )
            # NOTE: Model instances are always converted to JSON strings,
            #       no matter the value of `to_json`.
            case BaseModel():
                return param.model_dump_json()
            case _ if is_dataclass(param) or is_pydantic_dataclass(param):
                return json.dumps(
                    to_jsonable_python(param.__dict__), separators=(",", ":")
                )
            case _:
                return param

    def handle_sql_result_type(self, t: type) -> type:
        """
        Some SQL query results are returned in different formats by certain
        drivers. This can be overridden by a driver class so as to handle
        these cases.

        :param type t: The type that is to be mapped.
        """
        return t

    # NOTE: Add LRU caching as type mapping can be expensive,
    #       especially for deeply nested model types.
    @final
    @lru_cache(maxsize=256)
    def get_adapter_and_inverse_map(
        self, t: type[T]
    ) -> tuple[TypeAdapter, Callable[[Any], T]]:
        """
        Given some type[T] `t`, this function returns a tuple containing
        two objects:

        1. A `TypeAdapter[S]` object that can be used to validate the SQL
            query result on type `S`. Note that type `S` might differ from
            `T` due to custom mappings.

        2. A function that given the object parsed by `TypeAdapter[S]`,
           returns a new object mapped to its original type `T`.

        :param type t: The type that is to be mapped.
        """
        custom_type, inverse_map = OnlymapsType.factory(
            t=t, field_type_mapper=self.handle_sql_result_type
        )
        return (TypeAdapter(custom_type), inverse_map)

    @staticmethod
    @abstractmethod
    def std_colname(idx: int, colname: str) -> str:
        """
        Assigns a unique column name to the provided column
        name if it was not explicitly set by the user.

        :param int idx: The column's index.
        :param list[str] colnames: The column name to be fixed.
        """


class PostgresDriver(BaseDriver):
    """
    This class represents the underlying driver to a PostgreSQL
    database and is used to handle driver-specific issues.
    """

    @property
    def tag(self) -> Driver:
        """
        The driver's type.
        """
        return Driver.POSTGRES

    @staticmethod
    def std_colname(idx: int, colname: str) -> str:
        """
        Assigns a unique column name to the provided column
        name if it was not explicitly set by the user.

        :param int idx: The column's index.
        :param list[str] colnames: The column name to be fixed.
        """
        return colname if colname != "?column?" else f"c{idx}"


class MySqlDriver(BaseDriver):
    """
    This class represents the underlying driver to a MySQL
    database and is used to handle driver-specific issues.
    """

    @property
    def tag(self) -> Driver:
        """
        The driver's type.
        """
        return Driver.MY_SQL

    @override
    def handle_sql_param(self, param: Any) -> Any:
        """
        Some Python types are not supported by certain drivers. This function
        handles these cases and maps parameters of these types to a type that
        is supported.

        :param Any param: An SQL query parameter.
        """
        match param:
            # NOTE: Since MySQL supports an `Enum` type that is strictly
            #       a string data type, all enums are converted into strings
            #       by default. Therefore, enums should be handled manually.
            case Enum():
                return param.value
            case _:
                return super().handle_sql_param(param)

    @override
    def handle_sql_result_type(self, t: type) -> type:
        """
        Maps `bool` types to `OnlymapsBool`.

        :param type t: The type that is to be mapped.
        """
        if t is bool:
            return OnlymapsBool
        return super().handle_sql_result_type(t)

    @staticmethod
    def std_colname(idx: int, colname: str) -> str:
        """
        Assigns a unique column name to the provided column
        name if it was not explicitly set by the user.

        :param int idx: The column's index.
        :param list[str] colnames: The column name to be fixed.
        """
        # NOTE: Due to the MySQL driver returning the expression itself
        #       as the column name if one was not provided, there is
        #       no way to know whether a name was provided or not.
        return colname


class SqlServerDriver(BaseDriver):
    """
    This class represents the underlying driver to a Microsoft
    SQL Server database and is used to handle driver-specific issues.
    """

    @property
    def tag(self) -> Driver:
        """
        The driver's type.
        """
        return Driver.SQL_SERVER

    @override
    def handle_sql_result_type(self, t: type) -> type:
        """
        Maps `bool` types to `OnlymapsBool`.

        :param type t: The type that is to be mapped.
        """
        if t is bool:
            return OnlymapsBool
        return super().handle_sql_result_type(t)

    @staticmethod
    def std_colname(idx: int, colname: str) -> str:
        """
        Assigns a unique column name to the provided column
        name if it was not explicitly set by the user.

        :param int idx: The column's index.
        :param list[str] colnames: The column name to be fixed.
        """
        return colname if colname != "" else f"c{idx}"


class MariaDbDriver(BaseDriver):
    """
    This class represents the underlying driver to a MariaDB
    database and is used to handle driver-specific issues.
    """

    @property
    def tag(self) -> Driver:
        """
        The driver's type.
        """
        return Driver.MARIA_DB

    @override
    def handle_sql_param(self, param: Any) -> Any:
        """
        Some Python types are not supported by certain drivers. This function
        handles these cases and maps parameters of these types to a type that
        is supported.

        :param Any param: An SQL query parameter.
        """
        match param:
            # NOTE: Actually only needed for the async mariadb driver.
            case Enum():
                return param.value
            case _:
                return super().handle_sql_param(param)

    @override
    def handle_sql_result_type(self, t: type) -> type:
        """
        Maps `bool` types to `OnlymapsBool`.

        :param type t: The type that is to be mapped.
        """
        if t is bool:
            return OnlymapsBool
        return super().handle_sql_result_type(t)

    @staticmethod
    def std_colname(idx: int, colname: str) -> str:
        """
        Assigns a unique column name to the provided column
        name if it was not explicitly set by the user.

        :param int idx: The column's index.
        :param list[str] colnames: The column name to be fixed.
        """
        return colname if colname != "?" else f"c{idx}"


class OracleDbDriver(BaseDriver):
    """
    This class represents the underlying driver to an Oracle
    database and is used to handle driver-specific issues.
    """

    @property
    def tag(self) -> Driver:
        """
        The driver's type.
        """
        return Driver.ORACLE_DB

    @override
    def init_connection(self, conn: Any) -> Any:
        """
        This function is used to perform any driver-specific
        initialization logic right after the instantiation of
        a PyDbAPIv2 connection. Returns the now-initialized
        connection.

        :param Any conn: A PyDbAPIv2-compatible connection object.
        """

        def query_param_handler(
            cursor: oracledb.Cursor, value: Any, num_elements: int
        ) -> oracledb.Var | None:
            """
            Handles certain types of query parameters so that
            no information is lost.
            """
            match value:
                # Force `BINARY_DOUBLE` TYPE even if the decimal part is zero.
                case float():
                    return cursor.var(
                        oracledb.DB_TYPE_BINARY_DOUBLE, arraysize=num_elements
                    )
                # Force `TIMESTAMP` type if datetime has microseconds.
                case datetime() if value.microsecond > 0:
                    return cursor.var(
                        oracledb.DB_TYPE_TIMESTAMP, arraysize=num_elements
                    )
            return None

        setattr(conn, "inputtypehandler", query_param_handler)
        return conn

    @staticmethod
    def std_colname(idx: int, colname: str) -> str:
        """
        Assigns a unique column name to the provided column
        name if it was not explicitly set by the user.

        :param int idx: The column's index.
        :param list[str] colnames: The column name to be fixed.
        """
        # NOTE: Due to the OracleDB driver returning the expression itself
        #       as the column name if one was not provided, there is
        #       no way to know whether a name was provided or not.
        # NOTE: Since all column names are converted to uppercase by default,
        #       we should instead convert them back to lowercase as that is
        #       the most common option for model fields.
        return colname.lower()


class SqlLiteDriver(BaseDriver):
    """
    This class represents the underlying driver to an SQLite
    database and is used to handle driver-specific issues.
    """

    @property
    def tag(self) -> Driver:
        """
        The driver's type.
        """
        return Driver.SQL_LITE

    @override
    def handle_sql_param(self, param: Any) -> Any:
        """
        Some Python types are not supported by certain drivers. This function
        handles these cases and maps parameters of these types to a type that
        is supported.

        :param Any param: An SQL query parameter.
        """
        match param:
            # Sqlite3 driver cannot handle `Decimal` objects.
            case Decimal():
                return str(param)
            case _:
                return super().handle_sql_param(param)

    @override
    def handle_sql_result_type(self, t: type) -> type:
        """
        Maps `bool` types to `OnlymapsBool`.

        :param type t: The type that is to be mapped.
        """
        if t is bool:
            return OnlymapsBool
        return super().handle_sql_result_type(t)

    @staticmethod
    def std_colname(idx: int, colname: str) -> str:
        """
        Assigns a unique column name to the provided column
        name if it was not explicitly set by the user.

        :param int idx: The column's index.
        :param list[str] colnames: The column name to be fixed.
        """
        # NOTE: Due to the DuckDB driver returning the expression itself
        #       as the column name if one was not provided, there is
        #       no way to know whether a name was provided or not.
        return colname if colname != "?" else f"c{idx}"


class DuckDbDriver(BaseDriver):
    """
    This class represents the underlying driver to a DuckDb
    database and is used to handle driver-specific issues.
    """

    @property
    def tag(self) -> Driver:
        """
        The driver's type.
        """
        return Driver.DUCK_DB

    @override
    def init_connection(self, conn: Any) -> Any:
        """
        This function is used to perform any driver-specific
        initialization logic right after the instantiation of
        a PyDbAPIv2 connection. Returns the now-initialized
        connection.

        :param Any conn: A PyDbAPIv2-compatible connection object.

        :NOTE: Due to the way `DuckDb` works, there is no distinction between
            a connection and a cursor, with the connection class taking on the
            responsibility of both. This means that method `cursor` on the connection
            class actually returns a connection object as well. Seeing that calling
            `cursor` for every query execution carries some performance overhead,
            as well as certain issues regarding transaction isolation, this function
            defines certain classes in order to be able to integrate DuckDb connections
            with the existing connection/cursor based API without having any issues.
        """

        class DuckDbCursorWrapper:
            """
            This is a wrapper class for the DuckDb connection that
            is to be handled as a cursor. All member access delegates
            to the underlying DuckDb connection, except for `close`,
            which goes on to do nothing, as `cursor.close()` should not
            result in the DuckDb connection being closed.
            """

            def __init__(self, conn: Any) -> None:
                self.__conn = conn

            def __getattr__(self, name: str) -> Any:
                match name:
                    case "close":
                        return lambda: None
                    case _:
                        return getattr(self.__conn, name)

        class DuckDbConnectionWrapper:
            """
            This is a wrapper class for the DuckDb connection that
            is to be handled as an actual connection. All member
            access delegates to the underlying DuckDb connection,
            except for `cursor`, which goes on to return the underlying
            connection wrapped within `DuckDbCursorWrapper`.
            result in the DuckDb connection being closed.
            """

            def __init__(self, conn: Any) -> None:
                self.__conn = conn

            def __getattr__(self, name: str) -> Any:
                match name:
                    case "cursor":
                        return lambda: DuckDbCursorWrapper(self.__conn)
                    case _:
                        return getattr(self.__conn, name)

        return DuckDbConnectionWrapper(conn)

    @override
    def init_transaction(self, conn: Any) -> None:
        """
        This function is used to perform any driver-specific
        initialization logic right before the execution of a
        transaction.

        :param Any conn: A PyDbAPIv2-compatible connection object.
        """
        begin: Callable[[], None] = getattr(conn, "begin")
        begin()

    @staticmethod
    def std_colname(idx: int, colname: str) -> str:
        """
        Assigns a unique column name to the provided column
        name if it was not explicitly set by the user.

        :param int idx: The column's index.
        :param list[str] colnames: The column name to be fixed.
        """
        return colname


class SnowflakeDriver(BaseDriver):
    """
    This class represents the connection's underlying driver
    and is used to handle driver-specific issues.
    """

    @property
    def tag(self) -> Driver:
        """
        The driver's type.
        """
        return Driver.SNOWFLAKE

    @override
    def handle_sql_param(self, param: Any) -> Any:
        """
        Some Python types are not supported by certain drivers. This function
        handles these cases and maps parameters of these types to a type that
        is supported.

        :param Any param: An SQL query parameter.
        """
        match param:
            case Enum():
                return param.value
            case _:
                return super().handle_sql_param(param)

    @staticmethod
    def std_colname(idx: int, colname: str) -> str:
        """
        Assigns a unique column name to the provided column
        name if it was not explicitly set by the user.

        :param int idx: The column's index.
        :param list[str] colnames: The column name to be fixed.
        """
        # NOTE: Due to the Snowflake driver returning the expression itself
        #       as the column name if one was not provided, there is
        #       no way to know whether a name was provided or not.
        # NOTE: Since all column names are converted to uppercase by default,
        #       we should instead convert them back to lowercase as that is
        #       the most common option for model fields.
        return colname.lower()


class UnknownDriver(BaseDriver):
    """
    This class represents the connection's underlying driver
    and is used to handle driver-specific issues.
    """

    @property
    def tag(self) -> Driver:
        """
        The driver's type.
        """
        return Driver.UNKNOWN

    @classmethod
    def create(cls) -> Self:
        """
        Returns an `UnknownDriver` instance.
        """
        return cls(apilevel="2.0", threadsafety=0, paramstyle=ParamStyle.UNKNOWN)

    @staticmethod
    def std_colname(idx: int, colname: str) -> str:
        """
        Assigns a unique column name to the provided column
        name if it was not explicitly set by the user.

        :param int idx: The column's index.
        :param list[str] colnames: The column name to be fixed.
        """
        return colname


def driver_factory(
    driver: Driver,
    apilevel: str,
    threadsafety: int,
    paramstyle: str,
) -> BaseDriver:
    """
    A factory function that given a `Driver` value
    returns the corresponding `Driver` instance.

    :param `Driver` driver: A `Driver` enum value.
    """

    factory: Callable[..., BaseDriver]

    match driver:
        case Driver.POSTGRES:
            factory = PostgresDriver
        case Driver.MY_SQL:
            factory = MySqlDriver
        case Driver.SQL_SERVER:
            factory = SqlServerDriver
        case Driver.MARIA_DB:
            factory = MariaDbDriver
        case Driver.ORACLE_DB:
            factory = OracleDbDriver
        case Driver.SQL_LITE:
            factory = SqlLiteDriver
        case Driver.DUCK_DB:
            factory = DuckDbDriver
        case Driver.SNOWFLAKE:
            factory = SnowflakeDriver
        case Driver.UNKNOWN:  # pragma: no cover
            factory = UnknownDriver

    return factory(apilevel, int(threadsafety), ParamStyle(paramstyle))
