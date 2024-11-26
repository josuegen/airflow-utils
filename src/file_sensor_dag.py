from __future__ import annotations
from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from typing import Any
import google.auth
from google.auth.transport.requests import AuthorizedSession
import requests

default_args = {
    'owner': 'CVS',
    'depends_on_past': True,
    'email': ['josuegen@google.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'sla': timedelta(minutes=1)
}

dag = DAG(
    dag_id='file_sensor_dag',
    start_date=datetime(2024, 10, 7, 0, 0),
    schedule="0 14 * * 1-5",
    default_args=default_args,
    catchup=False,
    tags=["gcs", "sensor"]
)

check_gcs_data_object_existence = GCSObjectExistenceSensor(
    task_id='check_gcs_data_object_existence',
    bucket='cvs_poc_source_bucket',
    object='data_file_*.txt',
    use_glob=True,
    mode='reschedule',  # Options are: { poke | reschedule }, default is poke. 
    soft_fail=False,  # Set to true to mark the task as SKIPPED on failure
    poke_interval=10,
    timeout=180,
    sla=timedelta(seconds=60),
    dag=dag
)

check_gcs_validation_object_existence = GCSObjectExistenceSensor(
    task_id='check_gcs_validation_object_existence',
    bucket='cvs_poc_source_bucket',
    object='validation_file_*.txt',
    use_glob=True,
    mode='reschedule',  # Options are: { poke | reschedule }, default is poke. 
    soft_fail=False,  # Set to true to mark the task as SKIPPED on failure
    poke_interval=10,
    timeout=180,
    dag=dag
)


def response_check(response, task_instance):
    correct_result = 'request_id' in response.json()
    return correct_result


call_api = HttpOperator(
    task_id='call_api',
    http_conn_id='API_test_start',
    endpoint=None,
    method='POST',
    data=json.dumps({'id': 'test_call_cvs'}),
    headers={'Content-Type': 'application/json'},
    response_check=response_check,
    log_response=True,
    dag=dag,
)


def response_check_poke(response, task_instance):
    print(response.text)
    correct_result = response.json()["status"] == "completed"
    return correct_result


wait_for_api_response = HttpSensor(
    task_id='wait_for_api_response',
    http_conn_id='API_test_poke',
    endpoint=None,
    method='GET',
    headers={"Content-Type": "application/json"},
    # request_params="{{ ti.xcom_pull(task_ids='call_api') }}",
    request_params={'request_id': 'test_call_cvs'},
    response_check=response_check_poke,
    poke_interval=10,
    mode='reschedule',
    dag=dag
)

trigger_internal_dag = TriggerDagRunOperator(
    # https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/trigger_dagrun/index.html
    task_id='trigger_internal_dag',
    trigger_dag_id='internally_triggered_dag',
    wait_for_completion=True,
    conf={'request_id': '123abc', 'foo': 'bar'},
    poke_interval=15,
    dag=dag
)

AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])


def make_composer2_web_server_request(
    url: str, method: str = "POST", **kwargs: Any
) -> google.auth.transport.Response:
    """
    Make a request to Cloud Composer 2 environment's web server.
    Args:
      url: The URL to fetch.
      method: The request method to use ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT',
        'PATCH', 'DELETE')
      **kwargs: Any of the parameters defined for the request function:
                https://github.com/requests/requests/blob/master/requests/api.py
                  If no timeout is provided, it is set to 90 by default.
    """

    authed_session = AuthorizedSession(CREDENTIALS)

    # Set the default timeout, if missing
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90

    return authed_session.request(method=method, url=url, **kwargs)


def trigger_dag(web_server_url: str, dag_id: str, data: dict) -> str:
    """
    Make a request to trigger a dag using the stable Airflow 2 REST API.
    https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html

    Args:
      web_server_url: The URL of the Airflow 2 web server.
      dag_id: The DAG ID.
      data: Additional configuration parameters for the DAG run (json).
    """

    endpoint = f"api/v1/dags/{dag_id}/dagRuns"
    request_url = f"{web_server_url}/{endpoint}"
    json_data = {"conf": data}

    response = make_composer2_web_server_request(
        url=request_url, method="POST", json=json_data
    )

    if response.status_code == 403:
        raise requests.HTTPError(
            "You do not have a permission to perform this operation. "
            "Check Airflow RBAC roles for your account."
            f"{response.headers} / {response.text}"
        )
    elif response.status_code != 200:
        response.raise_for_status()
    else:
        return response.text


@task(task_id="trigger_external_dag", dag=dag)
def trigger_external_dag(**kwargs):
    dag_id = "externally_triggered_dag"
    dag_config = {'request_id': '123abc', 'foo': 'bar'}
    web_server_url = (
        "https://b25d19212dae4ea4b732515a16f79c06-dot-us-central1.composer.googleusercontent.com"
    )

    response_text = trigger_dag(
        web_server_url=web_server_url, dag_id=dag_id, data=dag_config
    )

    print(response_text)
    

downstream_task = EmptyOperator(
    task_id='downstream_workflow'
)

[check_gcs_data_object_existence, check_gcs_validation_object_existence] >> \
    call_api >> \
    wait_for_api_response >> \
    trigger_internal_dag >> \
    trigger_external_dag() >> \
    downstream_task
