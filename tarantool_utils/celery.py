# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json
try:
   import cPickle as pickle
except:
   import pickle

from celery.backends.base import KeyValueStoreBackend
from kombu.utils.url import _parse_url

import tarantool


class TarantoolBackend(KeyValueStoreBackend):
    default_timeout = 60 * 60 * 24 * 30

    def __init__(self, url=None, *args, **kwargs):
        super(TarantoolBackend, self).__init__(*args, **kwargs)

        if url is not None:
            _, self._host, self._port, _, _, _, _ = _parse_url(url)

    def _extract_value(self, value):
        return pickle.loads(value)

    def extract_value(self, response):
        row = response[0]
        value = row[0]
        return self._extract_value(value)

    def get_backend_timeout(self, timeout=None):
        if timeout is None:
            timeout = self.default_timeout
        elif int(timeout) == 0:
            # Other cache backends treat 0 as set-and-expire. To achieve this
            # in memcache backends, a negative timeout must be passed.
            timeout = -1

        return str(timeout)

    def make_value(self, value):
        return pickle.dumps(value)

    def set(self, key, value):
        call_args = (key, self.make_value(value),
                     self.get_backend_timeout())

        self._tnt.call('box.celery_backend.set', call_args)

    def mget(self, keys):
        new_keys = []
        key_map = {}
        for key in keys:
            new_key = key
            key_map[new_key] = key
            new_keys.append(new_key)

        call_args = (json.dumps(new_keys),)
        response = self._tnt.call('box.celery_backend.mget', call_args)

        new_response = {}
        for key, value in response:
            new_response[key_map[key]] = self._extract_value(value)
        return new_response

    def get(self, key):
        call_args = (key,)
        response = self._tnt.call('box.celery_backend.get', call_args)
        if len(response) == 0:
            return None
        return self.extract_value(response)

    def delete(self, key):
        call_args = (key,)
        self._tnt.call('box.celery_backend.delete', call_args)

    def expire(self, key, value):
        call_args = (key, str(value))
        self._tnt.call('box.celery_backend.expire', call_args)

    @property
    def _tnt(self):
        if not hasattr(self, '_client'):
            self._client = tarantool.connect(self._host, int(self._port))
        return self._client
