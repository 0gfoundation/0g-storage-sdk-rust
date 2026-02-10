#!/usr/bin/env python3

import math
import os
import random
import tempfile

from config.node_config import GENESIS_ACCOUNT
from utility.utils import (
    wait_until,
)
from utility.spec import ENTRY_SIZE, PORA_CHUNK_SIZE
from client_test_framework.test_framework import ClientTestFramework

SEGMENT_SIZE = ENTRY_SIZE * PORA_CHUNK_SIZE  # 256 * 1024 = 262144 bytes
ENCRYPTION_HEADER_SIZE = 17


class DownloadSegmentTest(ClientTestFramework):
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
            256 * 1024,  # exactly 1 segment
            256 * 1025,  # just over 1 segment (2 segments)
            256 * 2048,  # exactly 2 segments
            256 * 100,  # less than 1 segment
        ]

        submission_index = 0
        for size in data_sizes:
            submission_index += 1
            self.__test_download_segments(size, submission_index)

        # Encrypted upload/download tests
        encrypted_sizes = [
            256 * 100,  # less than 1 segment
            256 * 1024,  # exactly 1 segment (becomes 2 encrypted segments due to header)
            256 * 1025,  # just over 1 segment
        ]

        for size in encrypted_sizes:
            submission_index += 1
            self.__test_encrypted_download(size, submission_index)

    def __test_download_segments(self, size, submission_index):
        self.log.info("Testing download_segment with file size: %d", size)

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
        )

        self.log.info("root: %s", root)
        wait_until(lambda: self.contract.num_submissions() == submission_index)

        for node_idx in range(4):
            client = self.nodes[node_idx]
            wait_until(lambda: client.zgs_get_file_info(root) is not None)
            wait_until(lambda: client.zgs_get_file_info(root)["finalized"])

        # Calculate expected number of segments
        num_chunks = math.ceil(size / ENTRY_SIZE)
        num_segments = math.ceil(num_chunks / PORA_CHUNK_SIZE)
        self.log.info(
            "File size: %d, num_chunks: %d, num_segments: %d",
            size,
            num_chunks,
            num_segments,
        )

        # Download each segment individually and concatenate
        reconstructed = b""
        for seg_idx in range(num_segments):
            segment_data = self._download_segment_use_cli(
                node_urls,
                None,
                root=root,
                segment_index=seg_idx,
                with_proof=True,
            )
            self.log.info("Segment %d: %d bytes", seg_idx, len(segment_data))
            reconstructed += segment_data

        # Verify reconstructed data matches original
        assert reconstructed == data, "Reconstructed data mismatch: expected %d bytes, got %d bytes" % (
            len(data),
            len(reconstructed),
        )
        self.log.info(
            "download_segment test passed for file size %d (%d segments)",
            size,
            num_segments,
        )

    def __test_encrypted_download(self, size, submission_index):
        self.log.info("Testing encrypted upload/download with file size: %d", size)

        # Generate random AES-256 key
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

        self.log.info("encrypted root: %s", root)
        wait_until(lambda: self.contract.num_submissions() == submission_index)

        for node_idx in range(4):
            client = self.nodes[node_idx]
            wait_until(lambda: client.zgs_get_file_info(root) is not None)
            wait_until(lambda: client.zgs_get_file_info(root)["finalized"])

        # Test 1: Full file download with decryption
        file_to_download = os.path.join(self.root_dir, "download_enc_%s" % root[:16])
        self._download_file_use_cli(
            node_urls,
            None,
            root=root,
            file_to_download=file_to_download,
            with_proof=True,
            remove=False,
            encryption_key=encryption_key,
        )

        with open(file_to_download, "rb") as f:
            downloaded_data = f.read()
        os.remove(file_to_download)

        assert downloaded_data == data, "Decrypted full file mismatch: expected %d bytes, got %d bytes" % (
            len(data),
            len(downloaded_data),
        )
        self.log.info("Encrypted full file download test passed for size %d", size)

        # Test 2: Segment-by-segment download with decryption
        encrypted_size = size + ENCRYPTION_HEADER_SIZE
        num_chunks = math.ceil(encrypted_size / ENTRY_SIZE)
        num_segments = math.ceil(num_chunks / PORA_CHUNK_SIZE)
        self.log.info(
            "Encrypted file size: %d, num_chunks: %d, num_segments: %d",
            encrypted_size,
            num_chunks,
            num_segments,
        )

        reconstructed = b""
        for seg_idx in range(num_segments):
            segment_data = self._download_segment_use_cli(
                node_urls,
                None,
                root=root,
                segment_index=seg_idx,
                with_proof=True,
                encryption_key=encryption_key,
            )
            self.log.info("Encrypted segment %d: %d bytes", seg_idx, len(segment_data))
            reconstructed += segment_data

        assert reconstructed == data, "Encrypted segment reconstruction mismatch: expected %d bytes, got %d bytes" % (
            len(data),
            len(reconstructed),
        )
        self.log.info(
            "Encrypted download_segment test passed for file size %d (%d segments)",
            size,
            num_segments,
        )


if __name__ == "__main__":
    DownloadSegmentTest().main()
