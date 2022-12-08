import logging

from airflow import settings
from airflow.models import Connection


def register_connection(**kwargs):
    """Register Airflow connection"""
    conn = Connection(**kwargs)
    session = settings.Session()
    try:
        session.add(conn)
        session.commit()
    except Exception as e:
        logging.error(e)
        session.rollback()
