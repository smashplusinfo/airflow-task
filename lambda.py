from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
# Importing airflow hook
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook
import boto3, json

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'shamsudeen',
    'depends_on_past': False,
    'email': ['shamsuvs@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


# Function arn:aws:lambda:us-east-2:171166186201:function:AddObjectToS3:1

def lambda1(ds,**kwargs):

        
        hook = AwsLambdaHook('airflowLamdaToS3', region_name='us-east-2', log_type='None',qualifier='2',invocation_type='RequestResponse',config=None,aws_conn_id='aws_default')
        response_1 = hook.invoke_lambda(payload='null')
        print ('Response--->' , response_1)




dag = DAG(dag_id='lambdaflw', default_args=default_args, start_date=days_ago(1),schedule_interval=timedelta(days=1),)

# t1, t2, t3 and t4 are examples of tasks created using operators

t1 = PythonOperator(
        task_id="lambda1",
        python_callable=lambda1,
        provide_context=True
)



# Templated command with macros
bash_command="""
        {% for task in dag.task_ids %}
            echo "{{ task }}"
            echo "{{ ti.xcom_pull(task) }}"
        {% endfor %}
    """
# Show result
show_data = BashOperator(
    task_id='show_result',
    bash_command=bash_command,
    dag=dag
)

t1 >> show_data


