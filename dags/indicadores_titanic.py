from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime
import boto3

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')

client = boto3.client(
    'emr', region_name='us-east-1',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

default_args = {
    'owner': 'Ney',
    'start_date': datetime(2022, 4, 2)
}

@dag(default_args=default_args, schedule_interval="@once", description="Executa um job Spark no EMR", catchup=False, tags=['Spark','EMR'])
def indicadores_titanic():

    @task
    def inicio():
        return True
    
    @task
    def emr_process_titanic(success_before: bool):
        if success_before:
            newstep = client.add_job_flow_steps(
                JobFlowId="j-1037T3DHAN9EW",
                Steps=[
                    {
                        'Name': 'Processa indicadores Titanic',
                        'ActionOnFailure': "CONTINUE",
                        'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                    '--master', 'yarn',
                                    '--deploy-mode', 'cluster',
                                    '--packages', 'io.delta:delta-core_2.12:2.1.0',
                                    's3://emr-code-539445819060/ney/pyspark/titanic_example_delta.py'
                                    ]
                        }
                    }
                ]
            )
            return newstep['StepIds'][0]

    @task
    def wait_emr_job(stepId: str):
        waiter = client.get_waiter('step_complete')

        waiter.wait(
            ClusterId="j-1037T3DHAN9EW",
            StepId=stepId,
            WaiterConfig={
                'Delay': 10,
                'MaxAttempts': 600
            }
        )
        return True

    fim = DummyOperator(task_id="fim")

    # Orquestração
    start = inicio()
    indicadores = emr_process_titanic(start)
    wait_step = wait_emr_job(indicadores)
    wait_step >> fim
    #---------------

execucao = indicadores_titanic()
