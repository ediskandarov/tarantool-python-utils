# -*- coding: utf-8 -*-
from __future__ import absolute_import
import json

from django.utils.encoding import force_str
from django.core.cache.backends.base import BaseCache, DEFAULT_TIMEOUT
from django.utils.encoding import force_str
from django.utils.functional import cached_property

import tarantool


class TarantoolCache(BaseCache):
    def __init__(self, servers, params):
        super(TarantoolCache, self).__init__(params)
        self._servers = servers

    def extract_value(self, response):
        return response[0][0]

    def get_backend_timeout(self, timeout=DEFAULT_TIMEOUT):
        """
        Memcached deals with long (> 30 days) timeouts in a special
        way. Call this function to obtain a safe value for your timeout.
        """
        if timeout == DEFAULT_TIMEOUT:
            timeout = self.default_timeout

        if timeout is None:
            # Using 0 in memcache sets a non-expiring timeout.
            return '0'
        elif int(timeout) == 0:
            # Other cache backends treat 0 as set-and-expire. To achieve this
            # in memcache backends, a negative timeout must be passed.
            timeout = -1

        if timeout > 2592000:  # 60*60*24*30, 30 days
            # See http://code.google.com/p/memcached/wiki/FAQ
            # "You can set expire times up to 30 days in the future. After that
            # memcached interprets it as a date, and will expire the item after
            # said date. This is a simple (but obscure) mechanic."
            #
            # This means that we have to switch to absolute timestamps.
            timeout += int(time.time())
        return str(timeout)

    def make_key(self, key, version=None):
        # Python 2 memcache requires the key to be a byte string.
        return force_str(super(TarantoolCache, self).make_key(key, version))

    def add(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        call_args = (self.make_key(key, version), value,
                     self.get_backend_timeout(timeout))

        try:
            self._tnt.call('box.django_cache.add', call_args)
        except tarantool.error.DatabaseError:
            return False
        return True

    def get(self, key, default=None, version=None):
        call_args = (self.make_key(key, version),)
        response = self._tnt.call('box.django_cache.get', call_args)
        if len(response) == 0:
            return default
        return self.extract_value(response)

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        call_args = (self.make_key(key, version), value,
                     self.get_backend_timeout(timeout))

        self._tnt.call('box.django_cache.set', call_args)

    def delete(self, key, version=None):
        call_args = (self.make_key(key, version),)
        self._tnt.call('box.django_cache.delete', call_args)

    def get_many(self, keys, version=None):
        new_keys = []
        key_map = {}
        for key in keys:
            new_key = self.make_key(key, version)
            key_map[new_key] = key
            new_keys.append(new_key)

        call_args = (json.dumps(new_keys),)
        response = self._tnt.call('box.django_cache.get_many', call_args)

        new_response = {}
        for key, value in response:
            new_response[key_map[key]] = value
        return new_response

    def has_key(self, key, version=None):
        call_args = (self.make_key(key, version),)
        response = self._tnt.call('box.django_cache.has_key', call_args)

        if len(response) == 0:
            return False
        return True

    def incr(self, key, delta=1, version=None):
        call_args = (self.make_key(key, version), str(delta))
        response = self._tnt.call('box.django_cache.incr', call_args)

        if len(response) == 0:
            raise ValueError("Key '%s' not found" % (key,))

        return int(self.extract_value(response))

    def decr(self, key, delta=1, version=None):
        call_args = (self.make_key(key, version), str(delta))
        response = self._tnt.call('box.django_cache.decr', call_args)

        if len(response) == 0:
            raise ValueError("Key '%s' not found" % (key,))

        return int(self.extract_value(response))

    def set_many(self, data, timeout=DEFAULT_TIMEOUT, version=None):
        safe_data = {}
        for key, value in data.iteritems():
            key = self.make_key(key, version)
            safe_data[key] = value
        call_args = (json.dumps(safe_data), self.get_backend_timeout(timeout))
        self._tnt.call('box.django_cache.set_many', call_args)


    def delete_many(self, keys, version=None):
        keys_json = json.dumps([self.make_key(key, version) for key in keys])
        call_args = (keys_json,)
        response = self._tnt.call('box.django_cache.delete_many', call_args)

    def clear(self):
        call_args = tuple()
        response = self._tnt.call('box.django_cache.clear', call_args)

    def close(self, **kwargs):
        self._tnt.close()


class Tarantool15Cache(TarantoolCache):
    def __init__(self, server, params):
        super(Tarantool15Cache, self).__init__(server, params)

    @cached_property
    def _tnt(self):
        host, _, port = self._servers.rpartition(':')
        client = tarantool.connect(host, int(port))
        return client
