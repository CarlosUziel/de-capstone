from configparser import ConfigParser

import psycopg2


def get_db_connection(dl_config: ConfigParser):
    """Get database connection and cursor objects"""
    conn = psycopg2.connect(
        f"host={dl_config.get('DWH', 'DWH_ENDPOINT')} "
        f"dbname={dl_config.get('DWH', 'DWH_DB')} "
        f"user={dl_config.get('DWH', 'DWH_DB_USER')} "
        f"password={dl_config.get('DWH', 'DWH_DB_PASSWORD')} "
        f"port={dl_config.get('DWH', 'DWH_DB_PORT')} "
    )
    cur = conn.cursor()
    return conn, cur
