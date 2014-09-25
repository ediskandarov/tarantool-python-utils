# -*- coding: utf-8 -*-
import os
from setuptools.command.test import ScanningLoader

class TestLoader(ScanningLoader):
    def __init__(self):
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.test_settings")
        super(TestLoader, self).__init__()
