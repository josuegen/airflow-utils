"""
Kubernetes Pod Operator (KPO) Factory

Overrides a GKEStartPodOperator to set default values for compliance
"""

import os
import json
import logging
from typing import TYPE_CHECKING
from typing import Any
from typing import Sequence
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator,
)

logging.basicConfig(level=logging.INFO)

# read from configuration file
ENV_CONFIG = "" # TODO: Dveveloper to set where to pull the config from: module, file, hard-coding, etc
ENV = ENV_CONFIG["env"]
CONFIG_PATH = ENV_CONFIG["CONFIG_PATH"]

K8S_CLUSTER_CONFIG_FILE_NM = ENV_CONFIG["k8s_cluster_config"]
K8S_NODE_CONFIG_FILE_NM = ENV_CONFIG["k8s_node_config"]
K8S_NAMESPACE = ENV_CONFIG["k8s_namespace"]

K8S_NODE_CONFIG_FILE = os.path.join(
    CONFIG_PATH, "k8s_node_config", K8S_NODE_CONFIG_FILE_NM
)

K8S_CLUSTER_CONFIG_PATH = os.path.join(CONFIG_PATH, "k8s_cluster_config")


class KPOFactory(GKEStartPodOperator):
    template_fields = GKEStartPodOperator.template_fields + ("k8s_cluster_config",)

    default_kwargs = {
        "location": None,
        "cluster_name": None,
        "project_id": None,
        "deferrable": False,
        "namespace": K8S_NAMESPACE,
        "is_delete_operator_pod": False,
        "startup_timeout_seconds": None,
        "pod_template_file": K8S_NODE_CONFIG_FILE,
    }

    def __init__(
        self, *, k8s_cluster_config=K8S_CLUSTER_CONFIG_FILE_NM, **dag_kwargs
    ) -> None:
        kwargs = self.default_kwargs.copy()
        kwargs.update(dag_kwargs)

        super().__init__(**kwargs)
        self.k8s_cluster_config = k8s_cluster_config
        if k8s_cluster_config:
            with open(
                os.path.join(K8S_CLUSTER_CONFIG_PATH, self.k8s_cluster_config), "r"
            ) as fk8s:
                K8S_CONFIG = json.load(fk8s)

                self.location = K8S_CONFIG["gke_location"]
                self.cluster_name = K8S_CONFIG["gke_cluster_name"]
                self.project_id = K8S_CONFIG["gke_project_id"]
                self.startup_timeout_seconds = K8S_CONFIG["gke_startup_timeout_seconds"]
