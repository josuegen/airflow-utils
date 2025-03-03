#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""This module contains operator to move data from HTTP endpoint to GCS."""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.http.hooks.http import HttpHook

if TYPE_CHECKING:
    from collections.abc import Sequence

    from requests.auth import AuthBase

    from airflow.utils.context import Context


class HttpToGCSOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action and store the result in GCS.

    :param http_conn_id: The :ref:`http connection<howto/connection:http>` to run
        the operator against
    :param endpoint: The relative part of the full url. (templated)
    :param method: The HTTP method to use, default = "POST"
    :param data: The data to pass. POST-data in POST/PUT and params
        in the URL for a GET request. (templated)
    :param headers: The HTTP headers to be added to the GET request
    :param response_check: A check against the 'requests' response object.
        The callable takes the response object as the first positional argument
        and optionally any number of keyword arguments available in the context dictionary.
        It should return True for 'pass' and False otherwise.
    :param response_filter: A function allowing you to manipulate the response
        text. e.g response_filter=lambda response: json.loads(response.text).
        The callable takes the response object as the first positional argument
        and optionally any number of keyword arguments available in the context dictionary.
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :param log_response: Log the response (default: False)
    :param auth_type: The auth type for the service
    :param tcp_keep_alive: Enable TCP Keep Alive for the connection.
    :param tcp_keep_alive_idle: The TCP Keep Alive Idle parameter (corresponds to ``socket.TCP_KEEPIDLE``).
    :param tcp_keep_alive_count: The TCP Keep Alive count parameter (corresponds to ``socket.TCP_KEEPCNT``)
    :param tcp_keep_alive_interval: The TCP Keep Alive interval parameter (corresponds to
        ``socket.TCP_KEEPINTVL``)
    :param bucket_name: Name of the GCS bucket where to save the object. (templated)
    :param object_name: The object name to set when uploading the file. (templated)
        It can have ther full prefix for the file.
    :param gcp_conn_id: Connection id of the S3 connection to use
    """

    template_fields: Sequence[str] = (
        "http_conn_id", "endpoint", "data",
        "headers", "bucket_name", "object_name"
    )
    template_fields_renderers = {"headers": "json", "data": "py"}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        method: str = "GET",
        data: Any = None,
        headers: dict[str, str] | None = None,
        extra_options: dict[str, Any] | None = None,
        http_conn_id: str = "http_default",
        log_response: bool = False,
        auth_type: type[AuthBase] | None = None,
        tcp_keep_alive: bool = True,
        tcp_keep_alive_idle: int = 120,
        tcp_keep_alive_count: int = 20,
        tcp_keep_alive_interval: int = 30,
        bucket_name: str | None = None,
        object_name: str,
        replace: bool = False,
        encrypt: bool = False,
        acl_policy: str | None = None,
        aws_conn_id: str | None = "aws_default",
        verify: str | bool | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoint = endpoint
        self.headers = headers or {}
        self.data = data or {}
        self.extra_options = extra_options or {}
        self.log_response = log_response
        self.auth_type = auth_type
        self.tcp_keep_alive = tcp_keep_alive
        self.tcp_keep_alive_idle = tcp_keep_alive_idle
        self.tcp_keep_alive_count = tcp_keep_alive_count
        self.tcp_keep_alive_interval = tcp_keep_alive_interval
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.replace = replace
        self.encrypt = encrypt
        self.acl_policy = acl_policy
        self.aws_conn_id = aws_conn_id
        self.verify = verify

    @cached_property
    def http_hook(self) -> HttpHook:
        """Create and return an HttpHook."""
        return HttpHook(
            self.method,
            http_conn_id=self.http_conn_id,
            auth_type=self.auth_type,
            tcp_keep_alive=self.tcp_keep_alive,
            tcp_keep_alive_idle=self.tcp_keep_alive_idle,
            tcp_keep_alive_count=self.tcp_keep_alive_count,
            tcp_keep_alive_interval=self.tcp_keep_alive_interval,
        )

    @cached_property
    def gcs_hook(self) -> GCSHook:
        """Create and return an GCSHook."""
        return GCSHook(
            gcp_conn_id=self.aws_conn_id,
            verify=self.verify,
        )

    def execute(self, context: Context):
        self.log.info("Calling HTTP method")
        response = self.http_hook.run(
            self.endpoint,
            self.data,
            self.headers,
            self.extra_options
        )

        self.gcs_hook.upload(
            data=response.content,
            bucket_name=self.bucket_name,
            object_name=self.object_name
        )
