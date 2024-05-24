from datetime import timedelta
from airflow import DAG
#from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='f1-etl-2',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=15),
    tags=['ingest', 'transform'],
    params={"example_key": "example_value"},

) as dag:

	inicializa_proceso = EmptyOperator(
		task_id='inicializa_proceso',
	)

	finaliza_proceso = EmptyOperator(
		task_id='finaliza_proceso',
	)

	transform = BashOperator(
	        task_id='transform',
	        bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/practica_airflow/f1-transformation2.py ',
	)

#	alert = SlackWebhookOperator(
#		task_id='slack_message',
#		http_conn_id='edvai_slack',
#		message='hola :p',
#		channel='#bootcamp-de-data-engineering',
#		webhook_token='T074SF0VCMB/B074VENS94J/ZWJIHmsWhG07JILc0pNKZKex',
#	        dag=dag
#		)

	with TaskGroup("ingest") as ingest:

		inicializa_ingest = EmptyOperator(
			task_id='inicializa_ingest',
		)

		finaliza_ingest = EmptyOperator(
			task_id='finaliza_ingest',
		)

		with TaskGroup("ingest_actors") as ingest_actors:

			ingestDrivers = BashOperator(
	        		task_id='ingest-drivers',
		        	bash_command='/usr/bin/sh /home/hadoop/scripts/practica_airflow/clase8/ingest-drivers.sh ',
			)

			ingestConstructors = BashOperator(
			        task_id='ingest-constructors',
		        	bash_command='/usr/bin/sh /home/hadoop/scripts/practica_airflow/clase8/ingest-constructors.sh ',
			)

		with TaskGroup("ingest_metrics") as ingest_metrics:

			ingestResults = BashOperator(
        			task_id='ingest-results',
			        bash_command='/usr/bin/sh /home/hadoop/scripts/practica_airflow/clase8/ingest-results.sh ',
			)

			ingestRaces = BashOperator(
			        task_id='ingest-races',
	        		bash_command='/usr/bin/sh /home/hadoop/scripts/practica_airflow/clase8/ingest-races.sh ',
			)

		inicializa_ingest >> [ingest_actors, ingest_metrics] >> finaliza_ingest

#	inicializa_proceso >> alert >>  ingest >> transform >> finaliza_proceso
	inicializa_proceso >> ingest >> transform >> finaliza_proceso


if __name__ == "__main__":
    dag.cli()
