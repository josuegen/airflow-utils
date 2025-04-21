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
"""
    This module contains operator to (pre/post) adjust files
    in Teradata to BigQuery translation.

    author: josuegen@google.com (github.com/josuegen)
"""

from __future__ import annotations

from io import BytesIO
from tempfile import NamedTemporaryFile
from functools import cached_property
from typing import TYPE_CHECKING

from airflow.models import BaseOperator

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.common.links.storage import StorageLink

from airflow.providers.ftp.hooks.ftp import FTPHook

if TYPE_CHECKING:
    from collections.abc import Sequence
    from airflow.utils.context import Context


class FTPtoGCSOperator(BaseOperator):

    template_fields: Sequence[str] = (
        "source_path",
        "destination_path",
        "destination_bucket",
        "impersonation_chain",
    )

    operator_extra_links = (StorageLink(),)
    ui_color = "#b0fc83"

    def __init__(
        self,
        *,
        source_path: str,
        destination_bucket: str,
        destination_path: str | None = None,
        ftp_conn_id: str,
        block_size: int = 8192,
        gcp_conn_id: str = "google_cloud_default",
        mime_type: str = "application/octet-stream",
        gzip: bool = False,
        move_object: bool = False,
        impersonation_chain: str | Sequence[str] | None = None,
        use_stream: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.source_path = source_path
        self.destination_path = destination_path
        self.destination_bucket = destination_bucket
        self.gcp_conn_id = gcp_conn_id
        self.mime_type = mime_type
        self.gzip = gzip
        self.ftp_conn_id = ftp_conn_id
        self.block_size = block_size
        self.move_object = move_object
        self.impersonation_chain = impersonation_chain
        self.use_stream = use_stream

    @cached_property
    def ftp_hook(self) -> FTPHook:
        """Create and return a FTPHook."""
        return FTPHook(
            ftp_conn_id=self.ftp_conn_id
        )

    def execute(self, context: Context):
        self.destination_path = self._set_destination_path(
            path=self.destination_path
        )
        self.destination_bucket = self._set_bucket_name(
            name=self.destination_bucket
        )
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        StorageLink.persist(
            context=context,
            task_instance=self,
            uri=self.destination_bucket,
            project_id=gcs_hook.project_id,
        )

        destination_object = (
            self.destination_path if self.destination_path else self.source_path.rsplit("/", 1)[1]
        )

        self.log.info(
            "Executing copy of %s to gs://%s/%s",
            self.source_path,
            self.destination_bucket,
            destination_object,
        )

        if self.use_stream:
            dest_bucket = gcs_hook.get_bucket(self.destination_bucket)
            dest_blob = dest_bucket.blob(destination_object)

            with dest_blob.open("wb") as write_stream:
                self.ftp_hook.retrieve_file(
                    remote_full_path=self.source_path,
                    local_full_path_or_buffer=write_stream,
                    block_size=self.block_size
                )

        else:
            with NamedTemporaryFile("w") as tmp:
                self.ftp_hook.retrieve_file(
                    remote_full_path=self.source_path,
                    local_full_path_or_buffer=tmp.name,
                    block_size=self.block_size
                )

                gcs_hook.upload(
                    bucket_name=self.destination_bucket,
                    object_name=destination_object,
                    filename=tmp.name,
                    mime_type=self.mime_type,
                    gzip=self.gzip,
                )

        if self.move_object:
            self.log.info("Executing delete of %s", self.source_path)
            self.ftp_hook.delete_file(path=self.source_path)

    @staticmethod
    def _set_destination_path(path: str | None) -> str:
        if path is not None:
            return path.lstrip("/") if path.startswith("/") else path
        return ""

    @staticmethod
    def _set_bucket_name(name: str) -> str:
        bucket = name if not name.startswith("gs://") else name[5:]
        return bucket.strip("/")
