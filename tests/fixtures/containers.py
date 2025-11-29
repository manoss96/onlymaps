# Copyright (c) 2025 Manos Stoumpos
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

"""
This module contains all fixtures that are used to set up
a database via a Docker container.
"""

import gc
import os
import re
import sqlite3
from time import sleep
from typing import Iterator
from uuid import uuid4

import pytest
from pytest import FixtureRequest
from testcontainers.mssql import SqlServerContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.oracle import OracleDbContainer
from testcontainers.postgres import PostgresContainer

from onlymaps._drivers import Driver
from tests.utils import (
    SQL,
    DbContainer,
    MariaDbContainer,
    SqliteContainer,
    get_request_param,
)


@pytest.fixture(scope="session")
def pg_container() -> Iterator[PostgresContainer]:
    """
    A fixture used to set up a Postgres Docker container.
    """

    with PostgresContainer(image="postgres:16", driver="psycopg") as pg:
        pg.exec(f"psql -U {pg.username} -c '{SQL.CREATE_TEST_TABLE}'")
        yield pg


@pytest.fixture(scope="session")
def mysql_container() -> Iterator[MySqlContainer]:
    """
    A fixture used to set up a MySQL Docker container.
    """

    with MySqlContainer(image="mysql:8.0.36") as mysql:
        mysql.exec(
            "mysql "
            f"-u {mysql.username} "
            f"-p{mysql.password} "
            f"-D {mysql.dbname} "
            f"-e '{SQL.CREATE_TEST_TABLE}'"
        )
        yield mysql


@pytest.fixture(scope="session")
def mariadb_container() -> Iterator[MariaDbContainer]:
    """
    A fixture used to set up a MariaDb Docker container.
    """
    with MariaDbContainer(
        image="mariadb:lts-ubi",
        env={
            # "MYSQL_ROOT_HOST": "localhost",
            "MARIADB_USER": "test-user",
            "MARIADB_PASSWORD": "test-password",
            "MARIADB_DATABASE": "testdb",
        },
    ) as mariadb:
        # NOTE: Wait a bit for the container to initialize else
        #       we get the following error when trying to connect:
        #
        #       mariadb.InterfaceError:
        #           Lost connection to server at 'handshake:
        #           reading initial communication packet',
        #           system error: 0
        sleep(10)
        mariadb.exec(
            "mariadb "
            f"--user={mariadb.username} "
            f"--password={mariadb.password} "
            f"--database={mariadb.dbname} "
            f"-e '{SQL.CREATE_TEST_TABLE}'"
        )
        yield mariadb


@pytest.fixture(scope="session")
def sql_server_container() -> Iterator[SqlServerContainer]:
    """
    A fixture used to set up an MS SQL Server Docker container.
    """

    with SqlServerContainer(
        image="mcr.microsoft.com/mssql/server:2022-CU12-ubuntu-22.04"
    ) as sql_server:
        sql_server.exec(
            "/opt/mssql-tools/bin/sqlcmd "
            f"-U {sql_server.username} "
            f"-P '{sql_server.password}' "
            f"-d {sql_server.dbname} "
            f"-Q '{SQL.CREATE_TEST_TABLE}'"
        )
        yield sql_server


@pytest.fixture(scope="session")
def oracledb_container() -> Iterator[OracleDbContainer]:
    """
    A fixture used to set up an OracleDB Docker container.
    """

    with OracleDbContainer(image="gvenzl/oracle-free:slim") as oracledb:

        conn_str = oracledb.get_connection_url()

        m = re.fullmatch(
            r"oracle\+oracledb://(.+):(.+)@.+/\?service_name=(.+)", conn_str
        )
        assert m is not None

        username, password, dbname = m.groups()

        # NOTE: Increase the number of processes and restart the database.
        #       Do this due to an error that sometimes occurs during opening
        #       a new connection:
        # 
        #       oracledb.exceptions.OperationalError:
        #           DPY-6005: cannot connect to database (CONNECTION_ID=3uxZaUf0IUknsi7pokzdtg==).
        #           DPY-6000: Listener refused connection. (Similar to ORA-12516)
        oracledb.exec(
            [
                "bash",
                "-c",
                "echo 'ALTER SYSTEM SET processes=500 SCOPE=spfile;' | sqlplus / as sysdba",
            ]
        )
        oracledb.exec(["bash", "-c", "echo 'SHUTDOWN IMMEDIATE' | sqlplus / as sysdba"])
        oracledb.exec(["bash", "-c", f"echo 'STARTUP' | sqlplus / as sysdba"])

        # Create any necessary tables.
        oracledb.exec(
            [
                "bash",
                "-c",
                (
                    f"echo '{SQL.CREATE_TEST_TABLE};' | "
                    "sqlplus -s "
                    f"{username}/{password}@{dbname}"
                ),
            ]
        )

        oracledb.username = username
        oracledb.password = password
        oracledb.dbname = dbname

        yield oracledb


@pytest.fixture(scope="session")
def sqlite_container() -> Iterator[SqliteContainer]:
    """
    A fixture used to set up a pseudo SQLite container.
    """

    db = f"testdb-{uuid4()}.sqlite"

    with sqlite3.connect(database=db, isolation_level="DEFERRED") as conn:
        conn.execute(SQL.CREATE_TEST_TABLE)
        yield SqliteContainer(dbname=db)

    # NOTE: Explicitly delete `conn` and invoke garbage collector
    #       else a `PermissionError` exception is thrown while trying
    #       to delete the sqlite database.
    del conn
    gc.collect()

    os.remove(db)


@pytest.fixture(scope="function")
def db_container(request: FixtureRequest) -> DbContainer:
    """
    A database Docker container fixture parametrized
    via a `Driver` argument.
    """

    param = get_request_param(request)

    match param:
        # NOTE: Use postgres to test the unknown driver case as well.
        case Driver.POSTGRES | Driver.UNKNOWN:
            container = "pg_container"
        case Driver.MY_SQL:
            container = "mysql_container"
        case Driver.SQL_SERVER:
            container = "sql_server_container"
        case Driver.MARIA_DB:
            container = "mariadb_container"
        case Driver.ORACLE_DB:
            container = "oracledb_container"
        case Driver.SQL_LITE:
            container = "sqlite_container"
        case _:  # pragma: no cover
            raise ValueError(f"Invalid driver: `{param}`")

    return request.getfixturevalue(container)
