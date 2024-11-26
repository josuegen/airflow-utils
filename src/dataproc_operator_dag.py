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


"""
This DAG demonstrates how to:
    1. Provision a Dataproc cluster
    2. Submit a Pyspark Job to the Dataproc cluster
    3. Delete the Datapŕoc cluster upon completion of the Job

Author: josuegen@google.com
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator
)


default_args = {
    'owner': 'defualt',
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
    tags=["dataproc", "pyspark"]
)


cluster_dict = {
    'config_bucket': '',  # TODO: Developer set config bucket
    'gce_cluster_config': {
        'subnetwork_uri': 'projects/<project>/regions/<region>/subnetworks/<subnet>',  # TODO: Developer set project, region and subnet
        'service_account': '',  # TODO: Developer set service account
        'service_account_scopes': ['https://www.googleapis.com/auth/cloud-platform'],
        'tags': ['', ''], # TODO: Developer set network tags
        'internal_ip_only': True, # TODO: Developer set only internal or not traffic
    },
    'initialization_actions': [
        {'executable_file': 'gs://<bucket>/<prefix>.sh'}, # TODO: Developer set bucket and prefix
        {'executable_file': 'gs://<bucket>/<prefix>.sh'} # TODO: Developer set bucket and prefix
    ],
    'worker_config': {
        'num_instances': 2, # TODO: Developer adjust worker count
        'machine_type_uri': 'n1-standard-4', # TODO: Developer adjust worker machine type
        'disk_config': {
            'boot_disk_size_gb': 1000 # TODO: Developer adjust worker disk size
        }
    },
    'master_config': {
        'machine_type_uri': 'n1-standard-4', # TODO: Developer adjust master machine type
        'disk_config': {
            'boot_disk_size_gb': 1000 # TODO: Developer adjust master disk size
        }
    },
    'software_config': {
        'image_version': '2.0.113-debian10' # TODO: Developer adjust Dataproc image
    },
    'encryption_config': {
        'gce_pd_kms_key_name': 'projects/cvs-key-vault-nonprod/locations/us-east4/keyRings/gkr-nonprod-us-eats4/cryptoKeys/gk-edp-dev-pbmedh-orch-us-east4' # TODO: Developer adjust KMS key (if any, only for CMEK)
    },
    'autoscaling_config': {
        'policy_uri': 'projects/<project_id>/locations/<dataproc_region>/autoscalingPolicies/<policy_id>' # TODO: Developer adjust autoscaling policy (if any)
    }
}


create_ephemeral_cluster = DataprocCreateClusterOperator(
    dag=dag,
    task_id='create_dataproc_cluster',
    cluster_name='dataproc-00', # TODO: Developer adjust cluster name
    project_id='josuegen-gsd',  # TODO: Developer adjust cluster project name
    use_if_exists=True,
    delete_on_error=True,
    cluster_config=cluster_dict,
    region='us-central1',   # TODO: Developer adjust cluster region
    labels={ # TODO: Developer adjust cluster labels
        'environment': 'dev',
        'team': 'my-team'
    },
    polling_interval_seconds=60,
    deferrable=True
)

job_dict = {
    'reference': {
        'project_id': 'josuegen-gsd', # TODO: Developer adjust project ID for the job
        'job_id': 'composerjob1230' # TODO: Developer set Job ID to the PySpark job
    },
    'placement': {
        'cluster_name': 'dataproc-00' # TODO: Developer set the cluster used to place the Job (line 93)
    },
    'pyspark_job': {
        'main_python_file_uri': 'gs://poc_source_bucket/spark_demo.py', # TODO: Developer set PySpark main file URI in GCS
        'args': ['--one_argument', 'test1', '--other_argument', 'test2'] #  TODO: Developer set the arguments to be passed to the main PySpark job
    },
    'labels': {
        'key': 'value'
    }
}

submit_dataproc_job = DataprocSubmitJobOperator(
    dag=dag,
    task_id='submit_job_to_cluster',
    project_id='josuegen-gsd', # TODO: Developer to adjust the Spark job project
    asynchronous=False,
    job=job_dict,
    region='us-central1' # TODO: Developer to adjust the Spark job region
)

delete_cluster = DataprocDeleteClusterOperator(
    dag=dag,
    task_id='delete_cluster',
    project_id='josuegen-gsd', # TODO: Developer to adjust the Dataproc cluster project
    region='us-central1', # TODO: Developer to adjust the Dataproc cluster region
    cluster_name='dataproc-00', # TODO: Developer to adjust the Dataproc cluster ID
    deferrable=True,
    polling_interval_seconds=30,
    trigger_rule='all_done',
)


create_ephemeral_cluster >> submit_dataproc_job >> delete_cluster
