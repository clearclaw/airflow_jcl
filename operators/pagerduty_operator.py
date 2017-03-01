#! /usr/bin/env python

import json, logging, logtool, requests, retryp, socket
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

LOG = logging.getLogger (__name__)
EVENT_URL = "https://events.pagerduty.com/generic/2010-04-15/create_event.json"
RETRY_COUNT = 5

class PagerdutyOperatorException (AirflowException):
  pass

class PagerdutyOperator (BaseOperator):
  template_fields = ("description",)
  ui_color = "#ff7f50" # Coral

  @logtool.log_call
  @apply_defaults
  def __init__ (
      self,
      api_key = None, # Required
      description = None, # Required
      service_key = None, # Required
      details = None,
      hostname = socket.gethostname () if hasattr (
        socket, 'gethostname') else "-missing-",
      client_url = None,
      contexts = None,
      *args, **kwargs):
    super (PagerdutyOperator, self).__init__ (*args, **kwargs)
    self.api_key = api_key
    self.description = description if description is not None else ""
    self.details = details
    self.hostname = hostname
    self.client_url = client_url
    self.contexts = contexts if contexts is not None else []
    self.service_key = service_key
    if not api_key:
      raise PagerdutyOperatorException ("Missing api_key.")
    if not description:
      raise PagerdutyOperatorException ("Missing description.")
    if not service_key:
      raise PagerdutyOperatorException ("Missing description.")
    if contexts and not isinstance (contexts, list):
      raise PagerdutyOperatorException ("contexts is not a list.")

  @retryp.retryp (count = RETRY_COUNT, expose_last_exc = True)
  @logtool.log_call
  def execute (self, context):
    headers = {
      "Authorization": "Token token=" + self.api_key,
      "Accept": "application/vnd.pagerduty+json;version=2"
    }
    event = {
      "service_key": self.service_key,
      "event_type": "trigger",
      "description": self.description,
      "client": "Airflow: <%s>" % self.hostname,
    }
    if self.details is not None:
      event["details"] = json.dumps (self.details)
    if self.client_url is not None:
      event["client_url"] = self.client_url
    if self.contexts is not None:
      event["contexts"] = self.contexts
    r = requests.post (EVENT_URL, headers = headers, data = json.dumps (event))
    LOG.info ("Pagerduty (%s) response: %s", r.status_code, r.text)
    if r.status_code != 200:
      raise PagerdutyOperatorException ("Pagerduty API call returned: %s"
                                        % r.status_code)
    return json.loads (r.text)
