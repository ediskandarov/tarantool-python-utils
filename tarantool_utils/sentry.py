# -*- coding: utf-8 -*-
from __future__ import absolute_import

from django.conf import settings
from django.db import models
from django.utils.encoding import smart_str
from django.utils.functional import cached_property

from hashlib import md5
from sentry.buffer import Buffer
from sentry.utils.compat import pickle

import tarantool


class TarantoolBuffer(Buffer):
    key_expire = 60 * 60  # 1 hour

    def __init__(self, **options):
        if not options:
            options = settings.SENTRY_TARANTOOL_OPTIONS

        super(TarantoolBuffer, self).__init__(**options)
        options.setdefault('hosts', {
            0: {},
        })
        self.hosts = options['hosts']

    def _coerce_val(self, value):
        if isinstance(value, models.Model):
            value = value.pk
        return smart_str(value)

    def _make_key(self, model, filters, column):
        return '%s:%s:%s' % (
            model._meta,
            md5(smart_str('&'.join('%s=%s' % (k, self._coerce_val(v))
                for k, v in sorted(filters.iteritems())))).hexdigest(),
            column,
        )

    def _make_extra_key(self, model, filters):
        return '%s:extra:%s' % (
            model._meta,
            md5(smart_str('&'.join('%s=%s' % (k, self._coerce_val(v))
                for k, v in sorted(filters.iteritems())))).hexdigest(),
        )

    def _make_lock_key(self, model, filters):
        return '%s:lock:%s' % (
            model._meta,
            md5(smart_str('&'.join('%s=%s' % (k, self._coerce_val(v))
                for k, v in sorted(filters.iteritems())))).hexdigest(),
        )

    def incr(self, model, columns, filters, extra=None):
        for column, amount in columns.iteritems():
            key = self._make_key(model, filters, column)
            call_args = (key, str(amount), str(self.key_expire))
            self._tnt.call('box.sentry_buffer.incr', call_args)

        if extra:
            key = self._make_extra_key(model, filters)
            for column, value in extra.iteritems():
                call_args = (key, column, pickle.dumps(value),
                             str(self.key_expire))
                self._tnt.call('box.sentry_buffer.hset', call_args)
        super(TarantoolBuffer, self).incr(model, columns, filters, extra)

    def process(self, model, columns, filters, extra=None):
        lock_key = self._make_lock_key(model, filters)
        call_args = (lock_key, '1', self.delay)
        if not self._tnt.call('box.sentry_buffer.setnx', call_args):
            return

        results = {}
        for column, amount in columns.iteritems():
            key = self._make_key(model, filters, column)
            call_args = (key, '0', str(self.key_expire))
            response = self._tnt.call('box.sentry_buffer.getset', call_args)
            if len(response) == 0:
                continue
            value = int(response[0][0])
            results[column] = value

        hash_key = self._make_extra_key(model, filters)
        call_args = (hash_key,)
        extra_results = self._tnt.call('box.sentry_buffer.hgetalldelete',
                                       call_args)

        if len(extra_results):
            if not extra:
                extra = {}
            for key, value in extra_results:
                if not value:
                    continue
                extra[key] = pickle.loads(str(value))

        # Filter out empty or zero'd results to avoid a potentially unnecessary update
        results = dict((k, int(v)) for k, v in results.iteritems() if int(v or 0) > 0)
        if not results:
            return
        super(TarantoolBuffer, self).process(model, results, filters, extra)


class Tarantool15Buffer(TarantoolBuffer):
    def __init__(self, **options):
        super(Tarantool15Buffer, self).__init__(**options)

    @cached_property
    def _tnt(self):
        first_host = self.hosts[0]['host']
        host, _, port = first_host.rpartition(':')
        client = tarantool.connect(host, int(port))
        return client
