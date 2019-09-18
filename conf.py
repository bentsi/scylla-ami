#!/usr/bin/env python3
### Script provided by DataStax.

import configparser

configfile = '/etc/scylla/ami.conf'

config = configparser.RawConfigParser()
config.read(configfile)
try:
    config.add_section('AMI')
    config.add_section('Cassandra')
    config.add_section('OpsCenter')
except:
    pass


def set_config(section, variable, value):
    config.set(section, variable, value)
    with open(configfile, 'wb') as configtext:
        config.write(configtext)

def get_config(section, variable):
    try:
        config.read(configfile)
        return config.get(section, variable.lower())
    except:
        return False
