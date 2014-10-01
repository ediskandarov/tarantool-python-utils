# -*- coding: utf-8 -*-
from __future__ import absolute_import
__version__ = '0.1.5'

try:
    from kombu.transport import TRANSPORT_ALIASES
    from celery.backends import BACKEND_ALIASES
except ImportError:
    pass
else:
    TRANSPORT_ALIASES['tarantool'] = 'tarantool_utils.celery:TarantoolTransport'
    BACKEND_ALIASES['tarantool'] = 'tarantool_utils.celery:TarantoolBackend'
