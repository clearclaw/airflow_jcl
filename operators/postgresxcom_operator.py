#! /usr/bin/env python

import logging, logtool, psycopg2.extras
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import PostgresOperator
from airflow.plugins_manager import AirflowPlugin

LOG = logging.getLogger (__name__)

class PostgresXComOperator (PostgresOperator):
  ui_color = "#deb887" # BurlyWood

  @logtool.log_call
  def execute (self, context): # pylint: disable=unused-argument
    LOG.info ("Executing: %s", str (self.sql))
    db = PostgresHook (postgres_conn_id = self.postgres_conn_id).get_conn ()
    rc = []
    with db.cursor (cursor_factory = psycopg2.extras.DictCursor) as cur:
      LOG.info ("Executing SQL: %s", self.sql)
      cur.execute (self.sql)
      for rec in cur:
        rc.append (dict (rec))
    return rc

class PostgresXComOperatorPlugin (AirflowPlugin):
  name = "PostgresXComOperator"
  operators = [PostgresXComOperator]
