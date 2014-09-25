# -*- coding: utf-8 -*-
import codecs
import os
import re
from pip.req import parse_requirements
from setuptools import setup, find_packages


def read(*parts):
    filename = os.path.join(os.path.dirname(__file__), *parts)
    with codecs.open(filename, encoding='utf-8') as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


requirements_file = os.path.join(os.path.dirname(__file__), 'requirements.txt')
install_requires = [str(r.req) for r in parse_requirements(requirements_file)]

setup(
    name = 'tarantool-utils',
    version = find_version('tarantool_utils', '__init__.py'),
    description='Tarantool utils for Python',
    long_description=read('README.rst'),
    packages = find_packages(exclude=['tests']),
    include_package_data=True,

    install_requires = install_requires,
    entry_points = {
        'console_scripts': [
            'tarantool_python_utils = tarantool_utils.main:main',
        ],
    },
    test_suite = 'tests',
    test_loader = 'tests.loader:TestLoader',

    author = 'Iskandarov Eduard',
    author_email = 'e.iskandarov@corp.mail.ru',
    license = 'BSD',
    url = 'https://github.com/toidi/tarantool-python-utils',
)
