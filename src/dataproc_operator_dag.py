# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: josuegen@google.com

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    ClusterGenerator
)



default_args = {
    'owner': 'CVS',
    'depends_on_past': False,
    'email': ['josuegen@google.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    dag_id='dataproc_jobs',
    start_date=None,
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["dataproc", "pbm"]
)


"""
     Option A: Use the cluster generator to create the cluster config
"""

# https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/dataproc.html#howto-operator-dataproccreateclusteroperator
# https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/dataproc/index.html#airflow.providers.google.cloud.operators.dataproc.ClusterGenerator
cluster_config = ClusterGenerator(
    cluster_name='edp-dev-pbmedh-orch-dataproc-00',
    project_id='josuegen-gsd',  # value of --project in the bash command
    region='us-central1',  # value of --region in the bash command
    num_masters=1,
    master_machine_type="n1-standard-4",
    master_disk_size=1000,
    worker_machine_type="n1-standard-4",
    worker_disk_size=1000,
    num_workers=2,
    # Install Python packages in Dataproc using the DataprocCreateClusterOperator in Airflow
    properties={
        "dataproc:conda.packages": "googlemaps==4.10.0,google-cloud-secret-manager==2.20.0",
        "dataproc:pip.packages": "googlemaps==4.10.0,google-cloud-secret-manager==2.20.0"
    },
    internal_ip_only=True,
    image_version='2.0.113-debian10',
    subnetwork_uri='projects/josuegen-gsd/regions/us-central1/subnetworks/default',
    tags=['allow-iap', 'dataproc-edp-nonprod-pbmshared-compute'],
    service_account='447100982147-compute@developer.gserviceaccount.com',
    service_account_scopes=['https://www.googleapis.com/auth/cloud-platform']
).make()

"""
     Option B: Manually create the cluster config using a dictionary
"""
# Replaced by the ClusterGenerator
cluster_dict = {
    'config_bucket': 'cvs-poc-config-bucket',  # value of --bucket in the bash command
    'gce_cluster_config': {
        'subnetwork_uri': 'projects/josuegen-gsd/regions/us-central1/subnetworks/default',  # value of --subnet in the bash command
        'service_account': '447100982147-compute@developer.gserviceaccount.com',  # value of --service-account in the bash command
        'service_account_scopes': ['https://www.googleapis.com/auth/cloud-platform'],
        'tags': ['allow-iap', 'dataproc-edp-nonprod-pbmshared-copmpute'],
        'internal_ip_only': True,
    },
    'worker_config': {
        'num_instances': 2,
        'machine_type_uri': 'n1-standard-4',
        'disk_config': {
            'boot_disk_size_gb': 1000
        }
    },
    'master_config': {
        'machine_type_uri': 'n1-standard-4',
        'disk_config': {
            'boot_disk_size_gb': 1000
        }
    },
    'software_config': {
        'image_version': '2.0.113-debian10'
    }
}


create_ephemeral_cluster = DataprocCreateClusterOperator(
    dag=dag,
    task_id='create_dataproc_cluster',
    cluster_name='edp-dev-pbmedh-orch-dataproc-00',
    project_id='josuegen-gsd',  # value of --project in the bash command
    use_if_exists=True,
    delete_on_error=True,
    cluster_config=cluster_config,
    region='us-central1',  # value of --region in the bash command
    labels={
        'environment': 'dev',
        'team': 'edp-pbmedh-data-operations'
    },
    polling_interval_seconds=60,
    deferrable=True
)


job_dict = {
    'reference': {
        'project_id': 'josuegen-gsd',
        'job_id': 'composerjob1230'
    },
    'placement': {
        'cluster_name': 'edp-dev-pbmedh-orch-dataproc-00'
    },
    'pyspark_job': {
        'main_python_file_uri': 'gs://cvs_poc_source_bucket/spark_demo.py',
        'args': ['--one_argument', 'test1', '--other_argument', 'test2']
    },
    'labels': {
        'key': 'value'
    }
}

submit_dataproc_job = DataprocSubmitJobOperator(
    dag=dag,
    task_id='submit_job_to_cluster',
    project_id='josuegen-gsd',
    asynchronous=False,
    job=job_dict,
    region='us-central1'
)


delete_cluster = DataprocDeleteClusterOperator(
    dag=dag,
    task_id='delete_cluster',
    project_id='josuegen-gsd',
    region='us-central1',
    cluster_name='edp-dev-pbmedh-orch-dataproc-00',
    deferrable=True,
    polling_interval_seconds=30,
    trigger_rule='all_done',
)



create_ephemeral_cluster >> submit_dataproc_job >> delete_cluster
