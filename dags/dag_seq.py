from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import seq
import pg_load
from datetime import datetime

default_args = {
	"start_date":datetime(2020,1,1),
	"owner":"airflow"
}

with DAG(dag_id="seq_dag",schedule_interval="@daily",default_args=default_args) as dag:
	t_generate_seq_data = PythonOperator(task_id="tid_gen_seq_data",python_callable=seq.main,op_args=("10","150","5"))
	t_pg_load = PythonOperator(task_id="tid_pg_load",python_callable=pg_load.main,op_kwargs={"v_optype":"seq"})
	t_generate_seq_data >> t_pg_load

