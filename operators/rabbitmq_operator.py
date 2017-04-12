#! /usr/bin/env python

import json, logging, logtool, pika
from airflow import configuration
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException, AirflowConfigException
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

LOG = logging.getLogger (__name__)
try:
  Q_URL = configuration.get ("celery", "BROKER_URL")
except AirflowConfigException:
  Q_URL = None

class RabbitmqOperatorException (AirflowException):
  pass

class RabbitmqEnqueueOperator (BaseOperator):
  ui_color = "#afeeee   " # PaleTurquoise

  @logtool.log_call
  @apply_defaults
  def __init__ (
      self,
      q_url = Q_URL,
      q_names = None,
      # prefetch = 1,
      socket_timeout = 10,
      connection_attempts = 10,
      backpressure_detection = True,
      source_fn = None,
      **kwargs):
    super (RabbitmqEnqueueOperator, self).__init__ (**kwargs)
    if q_url is None:
      raise RabbitmqOperatorException ("No queue connection URL specified")
    if not isinstance (q_names, list) or isinstance (q_names, tuple):
      raise RabbitmqOperatorException ("Queue names not specified")
    if backpressure_detection not in (True, False):
      raise RabbitmqOperatorException (
        "Illegal backpressure_detection setting")
    if source_fn is None:
      raise RabbitmqOperatorException ("No data source function specified")
    self.q_url = q_url
    self.q_names = q_names
    # self.prefetch = prefetch
    self.socket_timeout = socket_timeout
    self.connection_attempts = connection_attempts
    self.backpressure_detection = backpressure_detection
    self.source_fn = source_fn
    self.q_params = pika.URLParameters (
      self.q_url
      + "&socket_timeout=%d" % self.socket_timeout
      #      + "&connection_attempts=%d" % self.connection_attempts
      + "&backpressure_detection=%s" % (
        "t" if self.backpressure_detection else "f")
    )

  @logtool.log_call
  def _q_setup (self, channel, q_names, prefetch = None):
    for q in q_names:
      channel.exchange_declare (exchange = q, type = "direct")
      channel.queue_declare (queue = q, durable = True)
      channel.queue_bind (exchange = q, queue = q,
                          routing_key = q)
    if prefetch is not None:
      channel.basic_qos (prefetch_count = prefetch)

  @logtool.log_call
  def execute (self, context):
    ndx = 0
    with pika.BlockingConnection (parameters = self.q_params) as conn:
      LOG.info ("Priming queues and exchanges...")
      with conn.channel () as channel:
        self._q_setup (channel, self.q_names)
        LOG.info ("Starting send...")
        rc = {}
        for ndx, d in enumerate (self.source_fn ()):
          q_name, msg = d
          if conn.channel.publish (
              body = json.dumps (msg, sort_keys = True),
              exchange = q_name,
              routing_key = q_name,
              mandatory = True,
              properties = pika.BasicProperties (
                content_type = "application/json",
                content_encoding = "utf-8",
                delivery_mode = 2,
              )):
            raise RabbitmqOperatorException (
              "Failed to send message: %s" % msg)
          rc.setdefault (q_name, 0)
          rc[q_name] += 1
          LOG.debug ("Sent to %s: %s", q_name, msg)
        conn.close ()
        LOG.info ("Done sending...")
      return {"message_count": ndx,
              "breakdown": rc}

class RabbitmqDequeueOperator (BaseOperator):
  ui_color = "#7fffd4" # Aquamarine

  @logtool.log_call
  @apply_defaults
  def __init__ (
      self,
      q_url = Q_URL,
      q_name = None,
      q_timeout = 10,
      prefetch = 1,
      socket_timeout = 10,
      connection_attempts = 10,
      backpressure_detection = True,
      process_fn = None,
      **kwargs):
    super (RabbitmqDequeueOperator, self).__init__ (**kwargs)
    if q_url is None:
      raise RabbitmqOperatorException ("No queue connection URL specified")
    if not isinstance (q_name, basestring):
      raise RabbitmqOperatorException ("Queue name is bad")
    if backpressure_detection not in (True, False):
      raise RabbitmqOperatorException (
        "Illegal backpressure_detection setting")
    if process_fn is None:
      raise RabbitmqOperatorException ("No data process function specified")
    self.q_url = q_url
    self.q_name = q_name
    self.q_timeout = q_timeout
    self.prefetch = prefetch
    self.socket_timeout = socket_timeout
    self.connection_attempts = connection_attempts
    self.backpressure_detection = backpressure_detection
    self.process_fn = process_fn
    self.q_params = pika.URLParameters (
      self.q_url
      + "&socket_timeout=%d" % self.socket_timeout
      #      + "&connection_attempts=%d" % self.connection_attempts
      + "&backpressure_detection=%s" % (
        "t" if self.backpressure_detection else "f")
    )

  @logtool.log_call
  def execute (self, context):
    with pika.BlockingConnection (parameters = self.q_params) as conn:
      LOG.info ("Priming queues and exchanges...")
      with conn.channel () as channel:
        self._q_setup (channel, [self.q_name,], prefetch = self.prefetch)
        LOG.info ("Starting send...")
        for ndx, msg in enumerate (channel.consume (
            self.q_name,
            exclusive = False,
            inactivity_timeout = self.q_timeout)):
          if not msg:
            return {"message_count": ndx}
          method, _, body = msg
          self.process_fn (json.loads (body))
          channel.basic_ack (method.delivery_tag)

class RabbitmqOperatorPlugin (AirflowPlugin):
  name = "RabbitmqOperator"
  operators = [RabbitmqEnqueueOperator, RabbitmqDequeueOperator]
