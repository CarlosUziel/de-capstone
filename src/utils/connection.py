from configparser import ConfigParser

import psycopg2


def get_db_connection(dwh_config: ConfigParser):
    """Get database connection and cursor objects"""
    conn = psycopg2.connect(
        f"host={dwh_config.get('DWH', 'DWH_ENDPOINT')} "
        f"dbname={dwh_config.get('DWH', 'DWH_DB')} "
        f"user={dwh_config.get('DWH', 'DWH_DB_USER')} "
        f"password={dwh_config.get('DWH', 'DWH_DB_PASSWORD')} "
        f"port={dwh_config.get('DWH', 'DWH_DB_PORT')} "
    )
    cur = conn.cursor()
    return conn, cur
