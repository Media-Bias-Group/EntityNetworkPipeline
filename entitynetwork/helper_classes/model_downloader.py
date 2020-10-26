#!/usr/bin/env python

import os
import configparser
import appdirs
from tqdm import tqdm
from urllib.request import urlretrieve


class TqdmUpTo(tqdm):
    """Provides `update_to(n)` which uses `tqdm.update(delta_n)`."""

    def update_to(self, b=1, bsize=1, tsize=None):
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] remains unchanged.
        """
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)  # will also set self.n = b * bsize


class ModelDownloader:

    def __init__(self):
        """Constructor."""
        config_path = os.path.join(appdirs.user_config_dir('EntityNetwork'), "config")
        config = configparser.ConfigParser()
        config.read(config_path)
        self.default_model_url = config['DEFAULT_MODEL']['URL']
        self.default_model_name = config['DEFAULT_MODEL']['NAME']
        self.model_data_path = config['DEFAULT_MODEL']['PATH']

    def download(self, url=None, model_name=None, model_data_path=None):
        """Sets up the model directory and downloads the default model, overwrites"""

        # initialize defaults
        if not model_data_path:
            model_data_path = self.model_data_path
        if not model_name:
            model_name = self.default_model_name
        if not url:
            url = self.default_model_url

        # prepare directories
        os.makedirs(model_data_path, exist_ok=True)

        # set the savepath
        save_path = os.path.join(model_data_path, model_name)

        # download the model
        with TqdmUpTo(unit='M', unit_scale=True, miniters=1) as t:
            urlretrieve(url, save_path, reporthook=t.update_to)

        return save_path

    def model_exists(self, model_name=None, model_data_path=None):
        """Tests, whether the default model exists locally"""

        # initialize defaults
        if not model_data_path:
            model_data_path = self.model_data_path
        if not model_name:
            model_name = self.default_model_name

        # set the savepath
        save_path = os.path.join(model_data_path, model_name)

        return os.path.isfile(save_path)
