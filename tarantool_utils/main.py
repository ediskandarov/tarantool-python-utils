# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function
import os


def main():
    DIR_NAME = os.path.dirname(__file__)
    print('Copy python_utils.lua to tarantool script dir with name init.lua')
    print('\t', os.path.abspath(os.path.join(DIR_NAME, 'python_utils.lua')))
