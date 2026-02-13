#!/usr/bin/env python3

import os
import random
import tempfile

from config.node_config import GENESIS_ACCOUNT
from utility.utils import (
    wait_until,
)
from client_test_framework.test_framework import ClientTestFramework


class FileEncryptedUploadDownloadTest(ClientTestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 1
        self.num_nodes = 4
        self.zgs_node_configs[0] = {
            "db_max_num_sectors": 2**30,
            "shard_position": "0/4",
        }
        self.zgs_node_configs[1] = {
            "db_max_num_sectors": 2**30,
            "shard_position": "1/4",
        }
        self.zgs_node_configs[2] = {
            "db_max_num_sectors": 2**30,
            "shard_position": "2/4",
        }
        self.zgs_node_configs[3] = {
            "db_max_num_sectors": 2**30,
            "shard_position": "3/4",
        }

    def run_test(self):
        data_sizes = [
            2,
            255,
            256,
            257,
            1023,
            1024,
            1025,
            256 * 960,
            256 * 1023,
            256 * 1024,
            256 * 1025,
            256 * 2048,
            256 * 16385,
            256 * 1024 * 64,
            256 * 480,
            256 * 1024 * 10,
            1000,
            256 * 960,
            256 * 100,
            256 * 960,
        ]

        for i, v in enumerate(data_sizes):
            self.__test_encrypted_upload_download(v, i + 1)

    def __test_encrypted_upload_download(self, size, submission_index):
        self.log.info("encrypted file size: %d", size)

        encryption_key = "0x" + random.randbytes(32).hex()

        file_to_upload = tempfile.NamedTemporaryFile(dir=self.root_dir, delete=False)
        data = random.randbytes(size)
        file_to_upload.write(data)
        file_to_upload.close()

        node_urls = ",".join([x.rpc_url for x in self.nodes])

        root = self._upload_file_use_cli(
            self.blockchain_nodes[0].rpc_url,
            GENESIS_ACCOUNT.key,
            node_urls,
            None,
            file_to_upload,
            skip_tx=False,
            encryption_key=encryption_key,
        )

        self.log.info("root: %s", root)
        wait_until(lambda: self.contract.num_submissions() == submission_index)

        for node_idx in range(4):
            client = self.nodes[node_idx]
            wait_until(lambda: client.zgs_get_file_info(root) is not None)
            wait_until(lambda: client.zgs_get_file_info(root)["finalized"])

        # Download with proof and verify decrypted content
        file_to_download = os.path.join(
            self.root_dir, "download_enc_proof_%s" % root[:16]
        )
        self._download_file_use_cli(
            node_urls,
            None,
            root,
            file_to_download=file_to_download,
            with_proof=True,
            remove=False,
            encryption_key=encryption_key,
        )

        with open(file_to_download, "rb") as f:
            downloaded_data = f.read()
        os.remove(file_to_download)

        assert downloaded_data == data, (
            "Decrypted file mismatch (with proof): expected %d bytes, got %d bytes"
            % (len(data), len(downloaded_data))
        )

        # Download without proof and verify decrypted content
        file_to_download = os.path.join(
            self.root_dir, "download_enc_noproof_%s" % root[:16]
        )
        self._download_file_use_cli(
            node_urls,
            None,
            root,
            file_to_download=file_to_download,
            with_proof=False,
            remove=False,
            encryption_key=encryption_key,
        )

        with open(file_to_download, "rb") as f:
            downloaded_data = f.read()
        os.remove(file_to_download)

        assert downloaded_data == data, (
            "Decrypted file mismatch (without proof): expected %d bytes, got %d bytes"
            % (len(data), len(downloaded_data))
        )

        self.log.info("encrypted upload/download passed for size %d", size)


if __name__ == "__main__":
    FileEncryptedUploadDownloadTest().main()
