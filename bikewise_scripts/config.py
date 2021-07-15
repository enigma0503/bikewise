import yaml


def get_config(config_loc, env):
    with open(config_loc, 'r') as stream:
        config = yaml.safe_load(stream)
        return config[env]
