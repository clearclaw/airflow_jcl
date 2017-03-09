#! /usr/bin/env python

import logging, logtool, psycopg2.extras
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import PostgresOperator

LOG = logging.getLogger (__name__)

class PostgresXComOperator (PostgresOperator):
  ui_color = "#deb887" # BurlyWood

  @logtool.log_call
  def execute (self, context): # pylint: disable=unused-argument
    logging.info ("Executing: " + str (self.sql))
    db = PostgresHook (postgres_conn_id = self.postgres_conn_id).get_conn ()
    cur = db.cursor (cursor_factory = psycopg2.extras.NamedTupleCursor)
    LOG.info ("Executing SQL: %s", self.sql)
    cur.execute (self.sql)
    rc = []
    for rec in cur:
      rc.append (rec)
    cur.close ()
    return rc
