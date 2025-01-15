# Airflow Utils
Utilities for Apache Airflow

## Index

| File          | Description |
| -----------       | ----------- |
|KPOFactory.py      | Shows how to extend an Operator and set default configurations. Used the GKEStartOperator in this example |
|dag_triggerer.py   | Executes a different DAG both in the same Airflow Composer instance (internally_triggered_dag.py), and in a different Airflow Composer (exterally_triggered_dag.py) instance through the Airflow REST API|
|externally_triggered_dag.py | A sample DAG that is being triggered from another DAG in a different Airflow Composer instance (dag_triggerer.py) |
|internally_triggered_dag.py | A sample DAG that is being triggered from another DAG in a the same Airflow Composer instance (dag_triggerer.py) |
|dataproc_operator_dag.py |   Shows the woprflow to create a Dataproc cluster, Submit a job to the cvreated Dataproc cluster and Delete the created cluster after job completion |

