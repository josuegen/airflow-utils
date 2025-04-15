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
from airflow.sensors.python import PythonSensor

from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceRunJobOperator
)
from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import (
    CloudDataTransferServiceHook
)
from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import (
    GcpTransferOperationStatus
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
    dag_id='sts_jobs',
    start_date=None,
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["sts", "pbm"]
)


def poke_transfer_op_status(operation_name: str):
    sts_hook = CloudDataTransferServiceHook(
        # The developer can provide impersonation_chain and gcp_conn_id parameters
    )

    operation = sts_hook.get_transfer_operation(
        operation_name=operation_name
    )

    status = operation.get("metadata").get("status")

    if status == GcpTransferOperationStatus.SUCCESS:
        return True
    elif status == GcpTransferOperationStatus.IN_PROGRESS
        return False
    else:
        raise Exception(
            "The Job Operation is neither in SUCCESS or IN_PROGRESS status. "
            "Check the console for details"
        )
    

# STS Operators

# https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/cloud_storage_transfer_service/index.html#airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceCreateJobOperator


# https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/cloud_storage_transfer_service/index.html#airflow.providers.google.cloud.operators.cloud_storage_transfer_service.CloudDataTransferServiceRunJobOperator
trigger_sts_job = CloudDataTransferServiceRunJobOperator(
    task_id='trigger_sts_job',
    dag=dag,
    job_name="transferJobs/OPI13130116506069809007" # Name of the job already created in STS
    # Other parameters such as impersonation_chain and gcp_conn_id
)

sensor_sts_job = PythonSensor(
    task_id='wait_for_sts_job',
    dag=dag,
    python_callable=poke_transfer_op_status,
    op_args=[
        "{{ ti.xcom_pull(key='return_value', task_ids='trigger_sts_job')['name'] }}"
    ],
    mode="reschedule",
    poke_interval=10
)

trigger_sts_job >> sensor_sts_job
