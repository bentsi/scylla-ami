import base64
import json
from textwrap import dedent
from unittest import TestCase
from pathlib import Path
import shutil
import tempfile
import yaml
import logging
from log import setup_logging
from scylla_configure import ScyllaAmiConfigurator


setup_logging(log_level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)


class TestScyllaConfigurator(TestCase):

    def setUp(self):
        LOGGER.info("Setting up test dir")
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_dir_path = Path(self.temp_dir.name)
        LOGGER.info("Test dir: %s", self.temp_dir_path)
        shutil.copyfile("tests-data/scylla.yaml", str(self.temp_dir_path / "scylla.yaml"))
        self.raw_user_data = ""
        self.private_ip = "172.16.16.1"
        self.configurator = ScyllaAmiConfigurator(scylla_yaml_path=str(self.temp_dir_path / "scylla.yaml"))
        self.configurator.get_instance_metadata = self.get_instance_metadata
        self.test_cluster_name = "test-cluster"

    def tearDown(self):
        self.temp_dir.cleanup()

    def get_instance_metadata(self, path):
        if path == "user-data":
            return self.raw_user_data
        elif path == "/meta-data/local-ipv4":
            return self.private_ip

    def check_yaml_files_exist(self):
        assert self.configurator.scylla_yaml_example_path.exists(), "scylla.yaml example file not created"
        assert self.configurator.scylla_yaml_path.exists(), "scylla.yaml file not created"

    def test_empty_user_data(self):
        self.configurator.configure_scylla_yaml()
        self.check_yaml_files_exist()
        with self.configurator.scylla_yaml_path.open() as scylla_yaml_file:
            scyll_yaml = yaml.load(scylla_yaml_file, Loader=yaml.SafeLoader)
            assert scyll_yaml["listen_address"] == self.private_ip
            assert scyll_yaml["broadcast_rpc_address"] == self.private_ip
            assert "scylladb-cluster-" in scyll_yaml["cluster_name"]

    def test_user_data_params_are_set(self):
        ip_to_set = "172.16.16.84"
        self.raw_user_data = json.dumps(
            dict(
                scylla_yaml=dict(
                    cluster_name=self.test_cluster_name,
                    listen_address=ip_to_set,
                    broadcast_rpc_address=ip_to_set,
                    seed_provider=[{
                        "class_name": "org.apache.cassandra.locator.SimpleSeedProvider",
                        "parameters": [{"seeds": ip_to_set}]}],
                )
            )
        )
        self.configurator.configure_scylla_yaml()
        self.check_yaml_files_exist()
        with self.configurator.scylla_yaml_path.open() as scylla_yaml_file:
            scylla_yaml = yaml.load(scylla_yaml_file, Loader=yaml.SafeLoader)
            assert scylla_yaml["cluster_name"] == self.test_cluster_name
            assert scylla_yaml["listen_address"] == ip_to_set
            assert scylla_yaml["broadcast_rpc_address"] == ip_to_set
            assert scylla_yaml["seed_provider"][0]["parameters"][0]["seeds"] == ip_to_set
            # check defaults
            assert scylla_yaml["experimental"] is False
            assert scylla_yaml["auto_bootstrap"] is False

    def test_postconfig_script(self):
        test_file = "scylla_configure_test"
        script = dedent("""
            touch {0.temp_dir_path}/{1}
        """.format(self, test_file))
        self.raw_user_data = json.dumps(
            dict(
                post_configuration_script=base64.b64encode(bytes(script, "utf-8")).decode("utf-8")
            )
        )
        self.configurator.configure_scylla_yaml()
        self.configurator.run_post_configuration_script()
        assert (self.temp_dir_path / test_file).exists(), "Post configuration script didn't run"

    def test_postconfig_script_with_timeout(self):
        test_file = "scylla_configure_test"
        script_timeout = 5
        script = dedent("""
            sleep {0}
            touch {1.temp_dir_path}/{2}
        """.format(script_timeout, self, test_file))
        self.raw_user_data = json.dumps(
            dict(
                post_configuration_script=base64.b64encode(bytes(script, "utf-8")).decode("utf-8"),
                post_configuration_script_timeout=script_timeout - 2,
            )
        )
        self.configurator.configure_scylla_yaml()
        with self.assertRaises(expected_exception=SystemExit):
            self.configurator.run_post_configuration_script()
        assert not (self.temp_dir_path / test_file).exists(), "Post configuration script didn't fail with timeout"

    def test_postconfig_script_with_bad_exit_code(self):
        script = dedent("""
            exit 84
        """)
        self.raw_user_data = json.dumps(
            dict(
                post_configuration_script=base64.b64encode(bytes(script, "utf-8")).decode("utf-8"),
            )
        )
        self.configurator.configure_scylla_yaml()
        with self.assertRaises(expected_exception=SystemExit):
            self.configurator.run_post_configuration_script()


