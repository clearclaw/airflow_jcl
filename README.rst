Plugins for Airflow:
--------------------

	https://airflow.incubator.apache.org/
  
PagerdutyOperator

  Sends events to Pagerduty.  Requires having a Pagerduty account:
  
  	https://www.pagerduty.com/
    
PsqlOperator

  Much like PostgresOperator, but uses `psql` instead soto allow
  taking advantage of psql-isms (eg timings).
  
  
INSTALLATION
------------

  Just copy the relevant .py file to your Airflow plugins directory:
  
	  https://airflow.incubator.apache.org/plugins.html
    
  Then:
  
  	pip install -r requirements.txt