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
    DataprocDeleteClusterOperator
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
cluster_dict = {
    'config_bucket': 'cvs-poc-config-bucket',  # value of --bucket in the bash command
    'gce_cluster_config': {
        'subnetwork_uri': 'projects/josuegen-gsd/regions/us-central1/subnetworks/default',  # value of --subnet in the bash command
        'service_account': '447100982147-compute@developer.gserviceaccount.com',  # value of --service-account in the bash command
        'service_account_scopes': ['https://www.googleapis.com/auth/cloud-platform'],
        'tags': ['allow-iap', 'dataproc-edp-nonprod-pbmshared-copmpute'],
        'internal_ip_only': True, # equivalent to --no-address
    },
    'initialization_actions': [
        {'executable_file': 'gs://usm-edp-dev-voltage-lob/init-scripts/voltage-init.sh'},
        {'executable_file': 'gs://goog-dataproc-initialization-actions-us-east4/cloud-sql-proxy/cloud-sql-proxy.sh'}
    ],
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
    },
    'encryption_config': {
        'gce_pd_kms_key_name': 'projects/cvs-key-vault-nonprod/locations/us-east4/keyRings/gkr-nonprod-us-eats4/cryptoKeys/gk-edp-dev-pbmedh-orch-us-east4'
    },
    'autoscaling_config': {
        'policy_uri': 'projects/[project_id]/locations/[dataproc_region]/autoscalingPolicies/us-f4zou1-policy'
    }
}
"""

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
    cluster_config=cluster_dict,
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
