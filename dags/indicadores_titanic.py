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

    inicio = DummyOperator(task_id='inicio')

    @task
    def emr_create_cluster():
        cluster_id = client.run_job_flow(
            Name='Automated_EMR_Ney',
            ServiceRole='EMR_DefaultRole',
            JobFlowRole='EMR_EC2_DefaultRole',
            VisibleToAllUsers=True,
            LogUri='s3://aws-logs-539445819060-us-east-1/elasticmapreduce/',
            ReleaseLabel='emr-6.8.0',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'Master nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Worker nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    }
                ],
                'Ec2KeyName': 'ney-pucminas-testes',
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-064aa7623d526f5e5'
            },

            Applications=[{'Name': 'Spark'}],

            # Configurations=[
            #     {
            #         "Classification": "spark-env",
            #         "Properties": {},
            #         "Configurations": [{
            #             "Classification": "export",
            #             "Properties": {
            #                 "PYSPARK_PYTHON": "/usr/bin/python3",
            #                 "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
            #             }
            #         }]
            #     },
            #     {
            #         "Classification": "spark-hive-site",
            #         "Properties": {
            #             "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            #         }
            #     },
            #     {
            #         "Classification": "spark-defaults",
            #         "Properties": {
            #             "spark.submit.deployMode": "cluster",
            #             "spark.speculation": "false",
            #             "spark.sql.adaptive.enabled": "true",
            #             "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
            #         }
            #     },
            #     {
            #         "Classification": "spark",
            #         "Properties": {
            #             "maximizeResourceAllocation": "true"
            #         }
            #     }
            # ],
        )
        return cluster_id["JobFlowId"]


    @task
    def wait_emr_cluster(cid: str):
        waiter = client.get_waiter('cluster_running')

        waiter.wait(
            ClusterId=cid,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 120
            }
        )
        return True


    
    @task
    def emr_process_titanic(cid: str):
        newstep = client.add_job_flow_steps(
            JobFlowId=cid,
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
    cluster = emr_create_cluster()
    inicio >> cluster

    esperacluster = wait_emr_cluster(cluster)

    indicadores = emr_process_titanic(cluster)
    esperacluster >> indicadores

    wait_step = wait_emr_job(indicadores)
    wait_step >> fim
    #---------------

execucao = indicadores_titanic()
