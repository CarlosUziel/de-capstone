from typing import Any, Iterable

from sql_queries import (
    STAGING_TABLES,
    STAR_TABLES,
    STAR_TABLES_CONSTRAINTS,
    STAR_TABLES_DISTSTYLES,
)


def get_drop_table_query(table_name: str) -> str:
    """Generate a DROP TABLE query given a table name.

    Args:
        table_name: table name.
    """
    return f"DROP TABLE IF EXISTS {table_name} CASCADE"


def get_create_table_query(table_name: str, table_args: Iterable[str]) -> str:
    """Generate a CREATE TABLE query given a table name and a list of arguments.

    Args:
        table_name: table name.
        table_args: An iterable of strings including column names, data types and any
            other modifiers.
    """
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(table_args)})"


def drop_tables(cur: Any, conn: Any):
    """Drop staging, fact and dimension tables"""
    for table_name in [*STAR_TABLES.keys(), *STAGING_TABLES.keys()]:
        cur.execute(get_drop_table_query(table_name))
        conn.commit()


def create_tables(cur: Any, conn: Any):
    """Create staging, fact and dimension tables"""
    for table_name, table_args in [*STAR_TABLES.items(), *STAGING_TABLES.items()]:
        cur.execute(
            get_create_table_query(
                table_name, [*table_args, *STAR_TABLES_CONSTRAINTS.get(table_name, [])]
            )
            + f" {STAR_TABLES_DISTSTYLES.get(table_name, '')}"
        )
        conn.commit()
