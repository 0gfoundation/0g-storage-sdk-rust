import os

from client_test_framework.test_framework import ClientTestFramework
from config.node_config import GENESIS_PRIV_KEY
from client_utility.run_rust_test import run_rust_test

class IndexerTest(ClientTestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 1
        self.num_nodes = 2

    def run_test(self):
        ports = ",".join([x.rpc_url.split(":")[-1] for x in self.nodes])
        self.setup_indexer(self.nodes[0].rpc_url, self.nodes[0].rpc_url, ports)

        # Use pre-compiled binary if available, otherwise compile on-demand
        test_dir = os.path.dirname(__file__)
        binary_path = os.path.join(test_dir, "rust_tests", "indexer_test", "target", "release", "indexer_test")

        if os.path.exists(binary_path):
            test_args = [
                binary_path,
                GENESIS_PRIV_KEY,
                self.blockchain_nodes[0].rpc_url,
                ",".join([x.rpc_url for x in self.nodes]),
                self.indexer_rpc_url
            ]
        else:
            test_args = [
                "cargo",
                "run",
                "--release",
                "--manifest-path",
                os.path.join(test_dir, "rust_tests", "indexer_test", "Cargo.toml"),
                "--",
                GENESIS_PRIV_KEY,
                self.blockchain_nodes[0].rpc_url,
                ",".join([x.rpc_url for x in self.nodes]),
                self.indexer_rpc_url
            ]
        run_rust_test(self.root_dir, test_args)

if __name__ == "__main__":
    IndexerTest().main()
