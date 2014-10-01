# -*- coding: utf-8 -*-
from __future__ import absolute_import
__version__ = '0.1.4'

try:
    from kombu.transport import TRANSPORT_ALIASES
    from celery.backends import BACKEND_ALIASES
except ImportError:
    pass
else:
    TRANSPORT_ALIASES['tarantool'] = 'python_utils.celery:TarantoolTransport'
    BACKEND_ALIASES['tarantool'] = 'python_utils.celery:TarantoolBackend'
