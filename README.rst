Plugins for Airflow:
--------------------

	https://airflow.incubator.apache.org/

PagerdutyOperator

  Sends events to Pagerduty.  Requires having a Pagerduty account:

  	https://www.pagerduty.com/

PostgresXComOperator

  Much like PostgresOperator, but returns the results set as a dict to
  XCom.

PsqlOperator

  Much like PostgresOperator, but uses `psql` instead so as to allow
  taking advantage of psql-isms (eg timings).


RabbitmqEnqueueOperator

  Untested!  Enqueue takes a function which when called returns an
  iterable (or is a generator) of tuples of queue names and JSON-able
  objects, and serialises those onto a queue of those names.

RabbitmqDequeueOperator

  Untested!  Dequeue takes a queue name and a process function which
  it then applied to each message on the queue, returning when the
  queue is empty.

Note: These two Rabbit* operations require RabbitMQ and default to the
queue connection used by Airflow if present.  It whould be possible to
run multiple dequeue operators in parallel, scaling out horizontally
as appropriate to your load.  (This code is based on very similar
inline code I have in a few production DAGs)

Installation
------------

  Just copy the relevant .py file to your Airflow plugins directory:

	  https://airflow.incubator.apache.org/plugins.html

  Then:

  	pip install -r requirements.txt
