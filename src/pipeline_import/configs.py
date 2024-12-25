#! /usr/bin/env python3

import configparser


def _load_cfg():
    cfg = configparser.ConfigParser()
    cfg.read('/io/config.toml')
    return cfg


def get_cfg(key: str):
    return _load_cfg()[key]
