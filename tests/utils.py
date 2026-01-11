# Copyright (c) 2025 Manos Stoumpos
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

"""
This module contains various utility functions and classes
that are to be used for testing purposes.
"""

from dataclasses import is_dataclass, make_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import IntEnum, StrEnum
from typing import Any, TypeAlias, Union
from uuid import UUID

from pydantic import BaseModel, create_model
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic.dataclasses import is_pydantic_dataclass
from pytest import FixtureRequest
from testcontainers.core.container import DockerContainer
from testcontainers.mssql import SqlServerContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.oracle import OracleDbContainer
from testcontainers.postgres import PostgresContainer

from onlymaps._drivers import Driver
from onlymaps._params import Json
from onlymaps._types import DataclassInstance, ModelClass, ModelClassType

CONNECT_TIMEOUT = 2
MIN_POOL_SIZE = 1
MAX_POOL_SIZE = 3
POOL_WAIT_TIMEOUT = 1

DRIVERS = [
    Driver.POSTGRES,
    Driver.MY_SQL,
    Driver.MARIA_DB,
    Driver.SQL_SERVER,
    Driver.ORACLE_DB,
    Driver.SQL_LITE,
    Driver.DUCK_DB,
    Driver.UNKNOWN,
]


class MyIntEnum(IntEnum):
    """
    An integer-based enum class.
    """

    A = 0


class MyStrEnum(StrEnum):
    """
    A string-based enum class.
    """

    A = "a"


SCALAR_TYPE = Union[
    bool,
    int,
    float,
    Decimal,
    str,
    bytes,
    UUID,
    date,
    datetime,
    MyIntEnum,
    MyStrEnum,
    tuple,
    list,
    set,
    dict,
    ModelClass,
]

ROW_TYPE = Union[tuple, list, set, dict, ModelClass]


def handle_scalar_param(scalar: Any) -> Any | Json:
    """
    Maps the given scalar to an SQL param valid type
    by wrapping it in a `Json` instance if its type
    cannot be handled by database drivers.
    """
    match scalar:
        case tuple() | list() | set() | dict():
            return Json(scalar)
        case _:
            return scalar


def get_row_type(row: Any) -> type:
    """
    Returns the type of the row appropriately
    parametrized when applicable.
    """
    match row:
        case tuple():
            return tuple[*SCALAR_TYPES]  # type: ignore # pragma: no cover
        case list():
            return list[Union[*SCALAR_TYPES]]  # type: ignore # pragma: no cover
        case set():
            return set[Union[*SCALAR_TYPES]]  # type: ignore # pragma: no cover
        case dict():
            return dict[str, int]  # pragma: no cover
        case _:
            return type(row)


def get_row_values(row: Any) -> tuple[Any, ...]:
    """
    Returns the sclar values of the row appropriately handled.
    """
    match row:
        case tuple() | list() | set():
            return tuple(handle_scalar_param(scalar) for scalar in row)
        case dict():
            return tuple(handle_scalar_param(scalar) for scalar in row.values())
        case BaseModel():
            return tuple(
                handle_scalar_param(scalar) for scalar in row.model_dump().values()
            )
        case _ if is_dataclass(row) or is_pydantic_dataclass(row):
            return tuple(
                handle_scalar_param(scalar) for scalar in row.__dict__.values()
            )
        case _:  # pragma: no cover
            raise ValueError(f"Invalid row type: `{type(row)}`")


def _build_values() -> tuple[list[SCALAR_TYPE], list[ROW_TYPE], type[BaseModel]]:
    """
    Builds and returns a tuple containing two lists of test values,
    one for scalar values and another for rows.
    """

    def create_dataclass_from_dict(name: str, d: dict) -> type:
        return make_dataclass(
            name, [(field_name, type(val)) for field_name, val in d.items()]
        )

    def create_pydantic_dataclass_from_dataclass(
        name: str, dc: ModelClassType
    ) -> type[ModelClass]:
        pydc = pydantic_dataclass(dc)
        pydc.__name__ = name
        pydc.__qualname__ = f"{'.'.join(pydc.__qualname__.split('.')[:-1])}{name}"
        return pydc

    def create_pydantic_model_from_pydantic_dataclass(
        name: str, pydc: ModelClassType
    ) -> type[BaseModel]:
        assert hasattr(pydc, "__pydantic_fields__")
        fields: dict[str, Any] = {
            name: (field.annotation) for name, field in pydc.__pydantic_fields__.items()
        }
        return create_model(name, **fields)

    SCALAR_DICT = {"a": 1, "b": 2}
    ScalarDataclass = create_dataclass_from_dict("ScalarDataclass", SCALAR_DICT)
    ScalarPydanticDataclass = create_pydantic_dataclass_from_dataclass(
        "ScalarPydanticDataclass", ScalarDataclass
    )
    ScalarPydanticModel = create_pydantic_model_from_pydantic_dataclass(
        "ScalarPydanticModel", ScalarPydanticDataclass
    )

    SCALAR_VALUES = [
        True,
        1,
        1.0,
        Decimal("1.0"),
        "Hello",
        b"Hello",
        UUID("3d751e08-dd90-4045-b1ee-7cfd392618a6"),
        date(1970, 1, 1),
        # NOTE: Set 1000 microseconds, i.e. 1 millisecond
        #       due to some databased not supporting microseconds.
        datetime(1970, 1, 1, 1, 1, 1, 1000),
        MyIntEnum.A,
        MyStrEnum.A,
        (1, 2),
        [1, 2],
        {1, 2},
        SCALAR_DICT,
        ScalarDataclass(**SCALAR_DICT),
        ScalarPydanticDataclass(**SCALAR_DICT),
        ScalarPydanticModel(**SCALAR_DICT),
    ]

    ROW_DICT = {
        f"{type(scalar).__name__.lower()}_field": scalar for scalar in SCALAR_VALUES
    }
    RowDataclass = create_dataclass_from_dict("RowDataclass", ROW_DICT)
    RowPydanticDataclass = create_pydantic_dataclass_from_dataclass(
        "RowPydanticDataClass", RowDataclass
    )
    RowPydanticModel = create_pydantic_model_from_pydantic_dataclass(
        "RowPydanticModel", RowPydanticDataclass
    )

    ROW_VALUES = [
        tuple(SCALAR_VALUES),
        # NOTE: Do not try for `list`, `set` and `dict` as due to
        #       `Union` types the deserialized values may differ from
        #       the original.
        # list(SCALAR_VALUES),
        # set(SCALAR_VALUES)
        # ROW_DICT,
        RowDataclass(**ROW_DICT),
        RowPydanticDataclass(**ROW_DICT),
        RowPydanticModel(**ROW_DICT),
    ]

    return (SCALAR_VALUES, ROW_VALUES, RowPydanticModel)


SCALAR_VALUES, ROW_VALUES, RowPydanticModel = _build_values()

SCALAR_TYPES: list[type] = [type(scalar) for scalar in SCALAR_VALUES]

ScalarDataclass: type[DataclassInstance] = SCALAR_TYPES[-3]
ScalarPydanticDataclass: ModelClassType = SCALAR_TYPES[-2]
ScalarPydanticModel: type[BaseModel] = SCALAR_TYPES[-1]


class SqliteContainer(BaseModel):
    """
    A pseudo Docker container class for SQL Lite.
    """

    dbname: str


class DuckDbContainer(BaseModel):
    """
    A pseudo Docker container class for DuckDB.
    """

    dbname: str


# NOTE: Define custom MariaDB container since
#       it is not supported by TestContainers yet.
class MariaDbContainer(DockerContainer):
    """
    A MariaDB Docker container class.
    """

    def __init__(self, image: str, env: dict[str, str]) -> None:
        env["MARIADB_ALLOW_EMPTY_ROOT_PASSWORD"] = "1"
        super().__init__(image, env=env)
        self.port = 3306
        self.username = env.get("MARIADB_USER")
        self.password = env.get("MARIADB_PASSWORD")
        self.dbname = env.get("MARIADB_DATABASE")
        self.with_exposed_ports(self.port)


DbContainer: TypeAlias = (
    PostgresContainer
    | MySqlContainer
    | MariaDbContainer
    | SqlServerContainer
    | OracleDbContainer
    | SqliteContainer
    | DuckDbContainer
)


def get_conn_str_and_kwargs_from_container(
    container: DbContainer,
) -> tuple[str, dict[str, Any]]:
    """
    Given a database Docker container, constructs and returns
    a tuple containing the following:

    1. A connection string that can be used to connect to said
        container's database.
    2. A dictionary containing additional keyword arguments to
        be passed to the `connect` function.

    :param `DbContainer` container: A Docker container instance.
    """

    kwargs: dict[str, Any] = {"connect_timeout": CONNECT_TIMEOUT}

    if isinstance(container, SqliteContainer):
        kwargs |= {
            # NOTE: Set this to `False` so as to be able to use
            #       this connection instance in a separate thread
            #       when connected to an sqlite database.
            "check_same_thread": False,
            # NOTE: In the case of Sqlite use an extremely large
            #       timeout so as not to get a locked database error.
            #       See: https://docs.python.org/3/library/sqlite3.html#sqlite3.connect
            "timeout": 10000,
        }
        return f"{Driver.SQL_LITE}:///{container.dbname}", kwargs
    if isinstance(container, DuckDbContainer):
        return f"{Driver.DUCK_DB}:///{container.dbname}", kwargs

    # NOTE: Use `127.0.0.1` instead of `localhost` as some
    #       drivers treat `localhost` as an indication to
    #       establish a connection via a unix socket, which
    #       causes tests to fail for `mariadb` driver in CI.
    host = getattr(container, "host", "127.0.0.1")
    port = container.get_exposed_port(container.port)
    db = container.dbname
    user = container.username
    password = container.password

    match container:
        case PostgresContainer():
            driver = Driver.POSTGRES
        case MySqlContainer():
            driver = Driver.MY_SQL
        case MariaDbContainer():
            driver = Driver.MARIA_DB
        case OracleDbContainer():
            driver = Driver.ORACLE_DB
        case SqlServerContainer():
            driver = Driver.SQL_SERVER
        case _:  # pragma: no cover
            raise ValueError(f"Invalid container: `{container}`.")

    return f"{driver}://{user}:{password}@{host}:{port}/{db}", kwargs


def get_request_param(request: FixtureRequest) -> Any | None:
    """
    Recursively goes through the request and its parent
    in order to find the parameter associated with the
    fixture request.
    """
    # NOTE: If request is a subrequest then fetch the parent request
    #       so as to access any parameters.
    while parent := getattr(request, "_parent_request", None):
        if param := getattr(request, "param", None):
            return param
        request = parent
    return None


class SQL:
    """
    A helper class for building SQL queries.
    """

    TEST_TABLE = "test_table"
    SELECT_NONE = "SELECT 1 WHERE 1 IS NULL"
    CREATE_TEST_TABLE = f"CREATE TABLE {TEST_TABLE} (id INT PRIMARY KEY)"

    @staticmethod
    def placeholder(driver: Driver, n: int | None = None) -> str:
        """
        Returns a positional placeholder based on the provided driver.

        :param int | None n: An integer that, if provided, is used in
            the Oracle DB placeholder.
        """
        match driver:
            case Driver.ORACLE_DB:
                return f":{n if n is not None else 0}"
            case Driver.SQL_LITE | Driver.DUCK_DB:
                return "?"
            case _:
                return "%s"

    @staticmethod
    def kw_placeholder(driver: Driver, n: int | None = None) -> str:
        """
        Returns a keyword placeholder based on the provided driver.
        """
        match driver:
            case Driver.ORACLE_DB | Driver.SQL_LITE:
                return f":scalar{n if n is not None else ''}"
            case Driver.DUCK_DB:
                return f"$scalar{n if n is not None else ''}"
            case _:
                return f"%(scalar{n if n is not None else ''})s"

    @classmethod
    def SELECT_SINGLE_SCALAR(cls, driver: Driver) -> str:
        """
        Query to select a single scalar.
        """
        placeholder = cls.placeholder(driver)
        return f"SELECT {placeholder}"

    @classmethod
    def SELECT_SINGLE_ROW(cls, driver: Driver) -> str:
        """
        Query to select a single row with column names.
        """
        query = "SELECT "
        for i, field_name in enumerate(RowPydanticModel.model_fields):
            query += f"{cls.placeholder(driver, i)} AS {field_name},"
        return query.removesuffix(",")

    @classmethod
    def SELECT_MULTIPLE_SCALAR(cls, driver: Driver) -> str:
        """
        Query to select multiple scalars.
        """
        placeholder = cls.kw_placeholder(driver)
        return f"SELECT {placeholder} UNION ALL SELECT {placeholder}"

    @classmethod
    def SELECT_MULTIPLE_ROW(cls, driver: Driver) -> str:
        """
        Query to select multiple rows with column names.
        """

        idx = 0

        def build_query() -> str:
            nonlocal idx
            query = "SELECT "
            for field_name in RowPydanticModel.model_fields:
                query += f"{cls.placeholder(driver, idx)} AS {field_name},"
                idx += 1
            return query.removesuffix(",")

        query1 = build_query()
        query2 = build_query()

        return f"{query1} UNION ALL {query2}"

    @classmethod
    def SELECT_SINGLE_ROW_WITH_INT_COL_NAMES(
        cls, driver: Driver, num_placeholders: int
    ) -> str:
        """
        Query to select a single row with column names
        following the `cN` pattern.
        """
        query = "SELECT "
        for i in range(num_placeholders):
            query += f"{cls.placeholder(driver, i)} AS c{i},"
        return query.removesuffix(",")

    @classmethod
    def INSERT_INTO_TEST_TABLE(cls, driver: Driver, returning_id: bool = False) -> str:
        """
        Query to insert row into the test table.
        """
        placeholder = cls.placeholder(driver)
        query = f"INSERT INTO {cls.TEST_TABLE} VALUES ({placeholder})"
        if returning_id:
            assert driver == Driver.POSTGRES
            query += " RETURNING id"
        return query

    @classmethod
    def SELECT_FROM_TEST_TABLE(cls, driver: Driver) -> str:
        """
        Query to select one row from the test table.
        """
        placeholder = cls.placeholder(driver)
        return f"SELECT id FROM {cls.TEST_TABLE} WHERE id = {placeholder}"

    @classmethod
    def SELECT_MANY_FROM_TEST_TABLE(cls, driver: Driver, num_elements: int) -> str:
        """
        Query to select many rows from the test table.
        """
        template = ",".join(cls.placeholder(driver, i) for i in range(num_elements))
        return f"SELECT id FROM {cls.TEST_TABLE} WHERE id IN ({template})"

    @classmethod
    def KILL_CONNECTION(cls, driver: Driver) -> str:
        """
        Query to kill a connetion via timeout.
        """
        match driver:
            case Driver.POSTGRES | Driver.UNKNOWN:
                return "SET idle_session_timeout = '1ms'"
            case Driver.MY_SQL | Driver.MARIA_DB:
                return "SET SESSION wait_timeout = 1"
            case _:  # pragma: no cover
                raise ValueError(f"Invalid driver: {driver}")
