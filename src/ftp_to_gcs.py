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
    This module contains operator to move files from a FTP(S) server
    to a Google Cloud Storage destination.
"""

from __future__ import annotations

from io import BytesIO
from tempfile import NamedTemporaryFile
from functools import cached_property
from typing import TYPE_CHECKING

from airflow.models import BaseOperator

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.common.links.storage import StorageLink

from airflow.providers.ftp.hooks.ftp import FTPHook, FTPSHook

if TYPE_CHECKING:
    from collections.abc import Sequence
    from airflow.utils.context import Context


class FTPtoGCSOperator(BaseOperator):
    """
    Transfer files to Google Cloud Storage from FTP server.

    :param source_path: The sftp remote path. This is the specified file path
        for downloading the single file or multiple files from the FTP server.
    :param destination_bucket: The bucket to upload to.
    :param destination_path: The destination name of the object in the
        destination Google Cloud Storage bucket.
        If destination_path is not provided file/files will be placed in the
        main bucket path.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param ftp_conn_id: The sftp connection id. The name or identifier for
        establishing a connection to the FTP server.
    :param mime_type: The mime-type string
    :param gzip: Allows for file to be compressed and uploaded as gzip
    :param move_object: When move object is True, the object is moved instead
        of copied to the new location. This is the equivalent of a mv command
        as opposed to a cp command.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param block_size: File is transferred in chunks of default size 8192 or as set by user
    :param use_stream: Determines the transfer method from SFTP to GCS.
        When ``False`` (default), the file downloads locally
        then uploads (may require significant disk space).
        When ``True``, the file streams directly without using local disk.
        Defaults to ``False``.
    :param use_ftps: Wether to use FTPS over FTP. Defaults to ``False``
    """

    template_fields: Sequence[str] = (
        "source_path",
        "destination_path",
        "destination_bucket",
        "impersonation_chain",
    )

    operator_extra_links = (StorageLink(),)
    ui_color = "#a2f5b8"

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
        use_ftps: bool = False,
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
        self.use_ftps = use_ftps

    @cached_property
    def ftp_hook(self) -> FTPHook | FTPSHook:
        """Create and return a FTPHook or FTPSHook."""
        if self.use_ftps:
            return FTPSHook(
                ftp_conn_id=self.ftp_conn_id
            )
        
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
