#!/usr/bin/env python3
import base64
import json
import subprocess

import yaml
import time
import logging
from pathlib import Path
from urllib.request import urlopen

LOGGER = logging.getLogger(__name__)


class ScyllaAmiConfigurator:
    AMI_CONF_DEFAULTS = {
        'scylla_yaml': {
            'cluster_name': "scylladb-cluster-%s" % int(time.time()),
            'experimental': False,
            'auto_bootstrap': False,
            'listen_address': "",  # will be configured as a private IP when instance meta data is read
            'broadcast_rpc_address': "",  # will be configured as a private IP when instance meta data is read
        },
        'scylla_startup_args': [],  # Example ["--smp 1"]
        'developer_mode': False,  # run('/usr/sbin/scylla_dev_mode_setup', '--developer-mode', '1')
        'post_configuration_script': '',  # base64()/url(urllib.parse.urlparse)
        'post_configuration_script_timeout': 600,  # seconds
        'start_scylla_after_config': False,  # Scylla is stopped by default when creating AMI
    }

    def __init__(self, scylla_yaml_path="/etc/scylla/scylla.yaml"):
        self.scylla_yaml_path = Path(scylla_yaml_path)
        self.scylla_yaml_example_path = Path(scylla_yaml_path + ".example")
        self._scylla_yaml = {}
        self._instance_user_data = {}

    @property
    def scylla_yaml(self):
        if not self._scylla_yaml:
            with self.scylla_yaml_path.open() as scylla_yaml_file:
                self._scylla_yaml = yaml.load(scylla_yaml_file, Loader=yaml.SafeLoader)
        return self._scylla_yaml

    def save_scylla_yaml(self):
        LOGGER.info("Saving %s", self.scylla_yaml_path)
        with self.scylla_yaml_path.open("w") as scylla_yaml_file:
            return yaml.dump(data=self.scylla_yaml, stream=scylla_yaml_file)

    @staticmethod
    def get_instance_metadata(path):
        LOGGER.info("Getting '%s'...", path)
        with urlopen('http://169.254.169.254/latest/%s/' % path) as url:
            try:
                meta_data = url.read().decode("utf-8")
                return meta_data
            except Exception as error:
                LOGGER.exception("Unable to get '{path}': {error}".format(**locals()))

    @property
    def instance_user_data(self):
        if not self._instance_user_data:
            try:
                raw_user_data = self.get_instance_metadata("user-data")
                LOGGER.debug("Got user-data: %s", raw_user_data)

                self._instance_user_data = json.loads(raw_user_data)
                LOGGER.debug(self._instance_user_data)
            except Exception as e:
                LOGGER.warning("Error parsing user data: %s. Will use defaults!", e)
        return self._instance_user_data

    def updated_ami_conf_defaults(self):
        private_ip = self.get_instance_metadata("/meta-data/local-ipv4")
        self.AMI_CONF_DEFAULTS["scylla_yaml"]["listen_address"] = private_ip
        self.AMI_CONF_DEFAULTS["scylla_yaml"]["broadcast_rpc_address"] = private_ip

    def configure_scylla_yaml(self):
        self.updated_ami_conf_defaults()
        LOGGER.info("Creating scylla.yaml...")
        new_scylla_yaml_config = self.instance_user_data.get("scylla_yaml", {})
        if new_scylla_yaml_config:
            LOGGER.info("Setting params from user-data...")
            for param in new_scylla_yaml_config:
                param_value = new_scylla_yaml_config[param]
                LOGGER.info("Setting {param}={param_value}".format(**locals()))
                self.scylla_yaml[param] = param_value
        LOGGER.info("Setting AMI default params...")
        for param in self.AMI_CONF_DEFAULTS["scylla_yaml"]:
            if param not in new_scylla_yaml_config:
                default_param_value = self.AMI_CONF_DEFAULTS["scylla_yaml"][param]
                LOGGER.info("Setting {param}={default_param_value}".format(**locals()))
                self.scylla_yaml[param] = default_param_value
        self.scylla_yaml_path.rename(str(self.scylla_yaml_example_path))
        self.save_scylla_yaml()

    def configure_scylla_startup_args(self):
        pass

    def set_developer_mode(self):
        pass

    def run_post_configuration_script(self):
        post_configuration_script = self.instance_user_data.get("post_configuration_script")
        if post_configuration_script:
            try:
                default_timeout = self.AMI_CONF_DEFAULTS["post_configuration_script_timeout"]
                script_timeout = self.instance_user_data.get("post_configuration_script_timeout", default_timeout)
                decoded_script = base64.b64decode(post_configuration_script)
                LOGGER.info("Running post configuration script:\n%s", decoded_script)
                subprocess.run(decoded_script, check=True, shell=True, timeout=int(script_timeout))
            except Exception as e:
                LOGGER.error("Post configuration script failed: %s", e)

    def run_scylla_after_config(self):
        pass

    def configure(self):
        self.configure_scylla_yaml()
        self.configure_scylla_startup_args()
        self.set_developer_mode()
        self.run_post_configuration_script()
        self.run_scylla_after_config()


if __name__ == "__main__":
    sac = ScyllaAmiConfigurator()
    sac.configure()
