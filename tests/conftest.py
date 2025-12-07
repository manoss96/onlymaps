# Copyright (c) 2025 Manos Stoumpos
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

"""
This module imports all fixtures that are to be available globally.
"""

from tests.fixtures.connections import connection, connection_B, pool
from tests.fixtures.containers import (
    db_container,
    duckdb_container,
    mariadb_container,
    mysql_container,
    oracledb_container,
    pg_container,
    snowflake_container,
    sql_server_container,
    sqlite_container,
)
from tests.fixtures.executors import thread_executor as executor
from tests.fixtures.uid import uid, uid_factory
