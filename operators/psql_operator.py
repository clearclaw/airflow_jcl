#! /usr/bin/env python

import logging, logtool, os, signal, subprocess
from tempfile import NamedTemporaryFile
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

LOG = logging.getLogger (__name__)

class PsqlOperatorException (AirflowException):
  pass

class PsqlOperator (BaseOperator):
  template_fields =  ("sql", "env", "db_url")
  template_ext = (".sql",)
  ui_color = "#f0e68c" # Khaki

  @logtool.log_call
  @apply_defaults
  def __init__ (
      self,
      sql = None,
      db_url = None,
      env = None,
      output_encoding = "utf-8",
      *args, **kwargs):
    self.proc = None
    super (PsqlOperator, self).__init__ (*args, **kwargs)
    self.sql = sql
    self.db_url = db_url
    self.env = env
    self.output_encoding = output_encoding
    if not self.sql:
      raise PsqlOperatorException ("No SQL provided.")
    if not db_url:
      raise PsqlOperatorException ("No db_url provided.")

  @logtool.log_call
  def execute (self, context):
    output = []
    with NamedTemporaryFile (prefix = self.task_id) as f:
      f.write (self.sql.encode ("utf_8", "replace"))
      f.flush ()
      LOG.debug ("SQL stored in %s", f.name)
      LOG.info ("SQL contents: \n%s", self.sql)
      cmd = ["psql", self.db_url, "-f", f.name,]
      self.proc = subprocess.Popen (
        cmd,
        stdout = subprocess.PIPE, stderr = subprocess.STDOUT,
        preexec_fn = os.setsid,
        shell = False, env = self.env)
      LOG.info ("Output:")
      for line in iter (self.proc.stdout.readline, b""):
        line = line.decode (self.output_encoding).strip ()
        # LOG.info (line)
        output.append (line)
      self.proc.wait ()
      LOG.info ("psql RC: %s", self.proc.returncode)
      if self.proc.returncode:
        raise PsqlOperatorException ("psql command failed: %s",
                                     self.proc.returncode)
    return "\n".join (output)

  @logtool.log_call
  def on_kill (self):
    LOG.info ("Sending SIGTERM signal to bash process group")
    os.killpg (os.getpgid (self.proc.pid), signal.SIGTERM)

class PsqlOperatorPlugin (AirflowPlugin):
  name = "PsqlOperator"
  operators = [PsqlOperator]
