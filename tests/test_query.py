# Copyright (c) 2025 Manos Stoumpos
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

"""
This module contains tests related to testing various querying aspects,
"""

import inspect
import json
import typing
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal, Optional, Union
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from onlymaps._drivers import Driver
from onlymaps._params import Bulk, Json
from onlymaps._spec import Database
from tests.fixtures.connections import connection, db
from tests.utils import (
    DRIVERS,
    SCALAR_TYPES,
    SCALAR_VALUES,
    SQL,
    MyIntEnum,
    MyStrEnum,
    ScalarDataclass,
    ScalarPydanticDataclass,
    ScalarPydanticModel,
    get_row_values,
    handle_scalar_param,
)

# NOTE: Do not incude SQL Server for async tests.
# <include:DRIVERS = [d for d in DRIVERS if d != Driver.SQL_SERVER]>


@pytest.mark.parametrize("pooling", [False, True])
@pytest.mark.parametrize("db", DRIVERS, indirect=True)
class TestQuery:  # <replace:class TestAsyncQuery:>
    """
    Tests the `Query` class by testing against various querying-related cases.
    """

    def test_query_on_ellipsis_type(self, db: Database) -> None:  # <async>
        """
        Tests whether a query can be performed successfully by providing
        ellipsis as a type.
        """
        val = 1
        query = SQL.SELECT_SINGLE_SCALAR(db.driver)
        assert db.fetch_one(..., query, val) == val  # <await>

    @pytest.mark.parametrize("scalar", SCALAR_VALUES)
    @pytest.mark.parametrize("t", SCALAR_TYPES)
    @typing.no_type_check
    def test_query_on_type_mapping(  # <async>
        self, db: Database, scalar: Any, t: type
    ) -> None:
        """
        Tests SQL result type casting to from every type to every type.
        """

        # NOTE: Skip this test for sync MariaDB as there seems
        #       to be an issue with the `mariadb` driver causing
        #       a segmentation fault error.
        if db.driver == Driver.MARIA_DB and not inspect.iscoroutinefunction(db.exec):
            pytest.skip(f"Skipping test for `{db.driver}`.")

        query = SQL.SELECT_SINGLE_SCALAR(db.driver)
        param = handle_scalar_param(scalar)

        try:
            result: Any = db.fetch_one(t, query, param)  # <await>
        except Exception as e:
            result = e

        # A value of type `T` should always be castable to type `T`.
        if t is type(scalar):
            assert result == scalar
        else:
            match scalar:
                # NOTE: Due to several databases not providing a dedicated BOOL type,
                #       a `TypeError` won't be raised when casting bools to integer/floats.
                case bool() if (
                    (t in {int, float, Decimal})
                    and db.driver
                    in {
                        Driver.MY_SQL,
                        Driver.MARIA_DB,
                        Driver.SQL_SERVER,
                        Driver.SQL_LITE,
                    }
                ):
                    assert result == t(scalar)
                # NOTE: Make sure scalar is not `bool` since `bool` is a subclass of `int`.
                case int() if (
                    not isinstance(scalar, bool)
                    and (
                        (
                            t is bool
                            and db.driver
                            in {
                                Driver.MY_SQL,
                                Driver.MARIA_DB,
                                Driver.SQL_SERVER,
                                Driver.SQL_LITE,
                            }
                        )
                        or t in {float, Decimal}
                    )
                ):
                    assert result == t(scalar)
                case float() if t is Decimal:
                    assert result == t(scalar)
                case Decimal() if (
                    # `oracledb` driver allows for `Decimal` to `int`
                    # conversion.
                    (db.driver == Driver.ORACLE_DB and t is int)
                    or
                    # Drivers that can natively handle `Decimal` types,
                    # allow for `Decimal` to `float` conversion.
                    (db.driver != Driver.SQL_LITE and t is float)
                    or
                    # Certain drivers can't handle `Decimal` types,
                    # so they are converted into strings.
                    (db.driver == Driver.SQL_LITE and t is str)
                ):
                    assert result == t(scalar)
                # Those drivers who handle `Decimal` objects as strings
                # must account for the `bytes` case as well.
                case Decimal() if db.driver == Driver.SQL_LITE and t is bytes:
                    assert result == str(scalar).encode("utf-8")
                # Strings can be cast to bytes due to `OnlymapsBytes`.
                case str() if t is bytes:
                    assert result == str(scalar).encode("utf-8")
                # bytes can be cast to strings if `utf-8`-decodable.
                case bytes() if t is str:
                    assert result == bytes(scalar).decode("utf-8")
                case UUID() if t is str:
                    assert result == t(scalar)
                # Since UUIDs can be cast to strings,
                # they can in turn be cast to bytes.
                case UUID() if t is bytes:
                    assert result == str(scalar).encode("utf-8")
                # Certain database drivers return strings in place of dates.
                case date() | datetime() if t is str and db.driver in {
                    Driver.MY_SQL,
                    Driver.SQL_SERVER,
                    Driver.SQL_LITE,
                    # `Driver.MARIA_DB` may not be needed for sync `mariadb` package.
                    Driver.MARIA_DB,
                }:
                    if isinstance(scalar, datetime):
                        fixed_scalar = scalar.strftime("%Y-%m-%d %H:%M:%S.%f")
                        if db.driver == Driver.SQL_SERVER:
                            # NOTE: SQL server supports milliseconds not microseconds.
                            fixed_scalar = fixed_scalar[:-3]
                    else:
                        fixed_scalar = t(scalar)
                    assert result == fixed_scalar
                # Therefore if dates are strings, these strings can also be cast to bytes.
                case date() | datetime() if t is bytes and db.driver in {
                    Driver.MY_SQL,
                    Driver.SQL_SERVER,
                    Driver.SQL_LITE,
                    # `Driver.MARIA_DB` may not be needed for sync `mariadb` package.
                    Driver.MARIA_DB,
                }:
                    if isinstance(scalar, datetime):
                        fixed_scalar = scalar.strftime("%Y-%m-%d %H:%M:%S.%f")
                        if db.driver == Driver.SQL_SERVER:
                            # NOTE: SQL server supports milliseconds not microseconds.
                            fixed_scalar = fixed_scalar[:-3]
                    else:
                        fixed_scalar = str(scalar)
                    assert result == fixed_scalar.encode("utf-8")
                # A date can be cast to a datetime.
                case date() if t is datetime:
                    assert result == datetime(scalar.year, scalar.month, scalar.day)
                # And a datetime can be cast to a date.
                case datetime() if t is date:
                    assert result == scalar.date()
                case MyIntEnum() if t is int:
                    assert result == scalar
                case MyStrEnum() if t is str:
                    assert result == scalar
                # Tuple/list/set objects wrapped in `Json`
                # are represented as JSON list strings.
                case tuple() | list() | set() if t is str:
                    assert result == json.dumps(list(scalar), separators=(",", ":"))
                # And said string can be cast to bytes as well.
                case tuple() | list() | set() if t is bytes:
                    assert result == json.dumps(
                        list(scalar), separators=(",", ":")
                    ).encode("utf-8")
                # When wrapping tuple/list/set objects in `Json`,
                #  they can all be cast to each other.
                case tuple() | list() | set() if t in {tuple, list, set}:
                    assert result == t(scalar)
                case dict() if t is str:
                    assert result == json.dumps(
                        scalar, separators=(",", ":")
                    )  # <await>
                case dict() if t is bytes:
                    assert result == json.dumps(scalar, separators=(",", ":")).encode(
                        "utf-8"
                    )
                # A dictionary mapped to a JSON string can be cast to
                # any "model"-type.
                case dict() if t in {
                    ScalarDataclass,
                    ScalarPydanticDataclass,
                    ScalarPydanticModel,
                }:
                    assert result == t(**scalar)
                case (
                    ScalarDataclass()
                    | ScalarPydanticDataclass()
                    | ScalarPydanticModel()
                ) if t is str:
                    assert result == json.dumps(scalar.__dict__, separators=(",", ":"))
                case (
                    ScalarDataclass()
                    | ScalarPydanticDataclass()
                    | ScalarPydanticModel()
                ) if t is bytes:
                    assert result == json.dumps(
                        scalar.__dict__, separators=(",", ":")
                    ).encode("utf-8")
                case (
                    ScalarDataclass()
                    | ScalarPydanticDataclass()
                    | ScalarPydanticModel()
                ) if t is dict:
                    assert result == scalar.__dict__
                case (
                    ScalarDataclass()
                    | ScalarPydanticDataclass()
                    | ScalarPydanticModel()
                ) if t in {
                    ScalarDataclass,
                    ScalarPydanticDataclass,
                    ScalarPydanticModel,
                }:
                    assert result == t(**scalar.__dict__)
                case _:
                    assert isinstance(result, Exception)
                    with pytest.raises(TypeError):
                        raise result

    @pytest.mark.parametrize(
        "scalar, scalar_type",
        [
            ((1, 2, 3), tuple[int, int, int]),
            ([1, 2, 3], list[int]),
            ({1, 2, 3}, set[int]),
            ({"col0": 1, "col1": 2}, dict[str, int]),
        ],
    )
    def test_query_on_parametrized_types_for_scalar(  # <async>
        self, db: Database, scalar: Any, scalar_type: type
    ) -> None:
        """
        Tests whether the query scalar result is mapped to the
        appropriate type during parametrization.
        """
        assert (
            db.fetch_one(  # <await>
                scalar_type, SQL.SELECT_SINGLE_SCALAR(db.driver), Json(scalar)
            )
            == scalar
        )

    @pytest.mark.parametrize(
        "row, row_type",
        [
            ((0, 1, 2), tuple[int, int, int]),
            ([0, 1, 2], list[int]),
            ({0, 1, 2}, set[int]),
            ({"c0": 1, "c1": 1, "c2": 2}, dict[str, int]),
        ],
    )
    def test_query_on_parametrized_types_for_row(  # <async>
        self, db: Database, row: Any, row_type: type
    ) -> None:
        """
        Tests whether the query row result is mapped to the
        appropriate type during parametrization.
        """
        assert (
            db.fetch_one(  # <await>
                row_type,
                SQL.SELECT_SINGLE_ROW_WITH_INT_COL_NAMES(db.driver, len(row)),
                *get_row_values(row),
            )
            == row
        )

    @pytest.mark.parametrize(
        "row, row_type",
        [
            ((0, 1, 2), tuple[str, str, str]),
            ((0, 1, 2), tuple[int, int]),
            ([0, 1, 2], list[str]),
            ({0, 1, 2}, set[str]),
            ({"c0": 1, "c1": 1, "c2": 2}, dict[str, str]),
        ],
    )
    def test_query_on_type_error_for_parametrized_types(  # <async>
        self, db: Database, row: Any, row_type: type
    ) -> None:
        """
        Tests whether the query raises a `TypeError` when the parametrized
        type is invalid.
        """
        with pytest.raises(TypeError):
            db.fetch_one(  # <await>
                row_type,
                SQL.SELECT_SINGLE_ROW_WITH_INT_COL_NAMES(db.driver, len(row)),
                *get_row_values(row),
            )

    def test_query_on_invalid_result_type(self, db: Database) -> None:  # <async>
        """
        Tests whether a `TypeError` is raised when a non-type
        is provided in place of a type.
        """
        with pytest.raises(TypeError):
            db.fetch_one(  # <await>
                1, SQL.SELECT_NONE
            )  # type: ignore

    def test_query_on_both_params_and_kwparams(self, db: Database) -> None:  # <async>
        """
        Tests whether a `ValueError` is raised when both positional
        and keyword arguments are provided.
        """
        with pytest.raises(ValueError):
            db.exec(  # <await>
                SQL.SELECT_NONE, 1, param=1
            )  # type: ignore

    def test_query_on_result_type_type_error(self, db: Database) -> None:  # <async>
        """
        Tests whether a `TypeError` is raised when types do not
        match and type casting is not possible.
        """
        with pytest.raises(TypeError):
            query = SQL.SELECT_SINGLE_SCALAR(db.driver)
            db.fetch_one(int, query, "a")  # <await>

    def test_query_on_complex_bulk_with_sequence_args(  # <async>
        self, db: Database
    ) -> None:
        """
        Tests whether arguments are properly handled when wrapped
        within a `Bulk` argument containing sequence arguments.
        """

        if db.driver in {Driver.SQL_SERVER, Driver.ORACLE_DB}:
            pytest.skip(
                reason="Temporary tables not supported or need different syntax."
            )

        tmp_table = "tmp_table"
        c1, c2 = "c1", "c2"

        db.exec(  # <await>
            f"""
            CREATE TEMPORARY TABLE {tmp_table} (
                {c1} VARCHAR(100),
                {c2} VARCHAR(100)
            )
            """
        )

        class PydanticModel(BaseModel):
            """
            A simple pydantic model class.
            """

            n: int

        params = [[Json([i]), PydanticModel(n=i)] for i in range(5)]

        plchld_1 = SQL.placeholder(db.driver, n=1)
        plchld_2 = SQL.placeholder(db.driver, n=2)

        # Asserts no exception is raised.
        db.exec(  # <await>
            f"INSERT INTO {tmp_table}({c1}, {c2}) VALUES({plchld_1}, {plchld_2})",
            Bulk(params),
        )

    def test_query_on_complex_bulk_with_mapping_args(  # <async>
        self, db: Database
    ) -> None:
        """
        Tests whether arguments are properly handled when wrapped
        within a `Bulk` argument containing mapping arguments.
        """

        if db.driver in {Driver.SQL_SERVER, Driver.ORACLE_DB}:
            pytest.skip(
                reason="Temporary tables not supported or need different syntax."
            )

        tmp_table = "tmp_table"
        c1, c2 = "c1", "c2"

        db.exec(  # <await>
            f"""
            CREATE TEMPORARY TABLE {tmp_table} (
                {c1} VARCHAR(100),
                {c2} VARCHAR(100)
            )
            """
        )

        class PydanticModel(BaseModel):
            """
            A simple pydantic model class.
            """

            n: int

        params = [
            {"scalar1": Json([i]), "scalar2": PydanticModel(n=i)} for i in range(5)
        ]

        plchld_1 = SQL.kw_placeholder(db.driver, n=1)
        plchld_2 = SQL.kw_placeholder(db.driver, n=2)

        # Asserts no exception is raised.
        db.exec(  # <await>
            f"INSERT INTO {tmp_table}({c1}, {c2}) VALUES({plchld_1}, {plchld_2})",
            Bulk(params),
        )

    @pytest.mark.parametrize("method", ["fetch_one", "fetch_one_or_none", "fetch_many"])
    def test_query_on_bulk_param(self, db: Database, method: str) -> None:  # <async>
        """
        Tests whether a `ValueError` is raised when a `Bulk` parameter
        is provided for a query that is not `exec`.
        """
        with pytest.raises(ValueError):
            bulk = Bulk([[1]])
            getattr(db, method)(..., "SELECT 1", bulk)  # <await>

    def test_query_on_additional_param_with_bulk_param(  # <async>
        self, db: Database
    ) -> None:
        """
        Tests whether a `ValueError` when an additional positional
        parameter along with a `Bulk` parameter.
        """
        with pytest.raises(ValueError):
            bulk = Bulk([[1]])
            db.exec("SELECT 1", bulk, 1)  # <await>

    def test_query_on_additional_kwparam_with_bulk_param(  # <async>
        self, db: Database
    ) -> None:
        """
        Tests whether a `ValueError` when an additional keyword
        parameter along with a `Bulk` parameter.
        """
        with pytest.raises(ValueError):
            bulk = Bulk([[1]])
            # fmt: off
            db.exec( # <await>
                "SELECT 1", bulk, param=1
            )  #  type: ignore

    def test_query_on_complex_model_type(self, db: Database) -> None:  # <async>
        """
        Tests querying on a rather complex pydatic model.
        """

        class ComplexModel(BaseModel):
            """
            A compex pydantic model.
            """

            class NestedModel(BaseModel):
                """
                A nested pydantic model.
                """

                id: UUID
                created_at: datetime
                data: list[str]

            id: int
            label: str
            type_a: Literal[1, 2, 3]
            type_b: int | None
            type_c: Optional[str]
            type_d: Union[int, bool]
            metadata: NestedModel

        model = ComplexModel(
            id=0,
            label="label",
            type_a=2,
            type_b=None,
            type_c="c",
            type_d=2,
            metadata=ComplexModel.NestedModel(
                id=uuid4(),
                created_at=datetime(1970, 1, 1, 1, 1, 1, 1),
                data=["Hello", "World!"],
            ),
        )

        assert (
            db.fetch_one(  # <await>
                ComplexModel,
                f"""SELECT
                    {model.id} AS id,
                    '{model.label}' AS label,
                    {model.type_a} AS type_a,
                    NULL AS type_b,
                    '{model.type_c}' AS type_c,
                    {model.type_d} AS type_d,
                    '{model.metadata.model_dump_json()}' AS metadata
                """,
            )
            == model
        )
