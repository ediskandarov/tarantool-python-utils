# -*- coding: utf-8 -*-
from datetime import timedelta
import os
import mock
from unittest import TestCase

from django.conf import settings
from django.utils import timezone
from sentry.models import Project, Group
from sentry.utils.imports import import_string
from sentry.utils.compat import pickle

import tarantool


class TarantoolSentryBuffetTestCase(TestCase):
    def __init__(self, *args, **kwargs):
        super(TarantoolSentryBuffetTestCase, self).__init__(*args, **kwargs)
        sentry_space = int(os.environ.get('SENTRY_BUFFER_SPACE', '2'))
        sentry_extra_space = int(os.environ.get('SENTRY_BUFFER_EXTRA_SPACE',
                                                '3'))
        self.sentry_space = sentry_space
        self.sentry_extra_space = sentry_extra_space

    def setUp(self):
        host_cfg = settings.SENTRY_BUFFER_OPTIONS['hosts'][0]['host']
        host, _, port = host_cfg.rpartition(':')
        self.client = tarantool.connect(host, int(port))
        self.buffer = self.get_instance(
            'tarantool_utils.sentry.Tarantool15Buffer',
            settings.SENTRY_BUFFER_OPTIONS)
        self.clean_tnt()

    def clean_tnt(self):
        lua_code = 'box.space[%s]:truncate()'
        self.buffer._tnt.call('box.dostring', lua_code % self.sentry_space)
        self.buffer._tnt.call('box.dostring', lua_code % self.sentry_extra_space)

    def tearDown(self):
        self.client.close()

    def test_coerce_val_handles_foreignkeys(self):
        assert self.buffer._coerce_val(Project(id=1)) == '1'

    def test_coerce_val_handles_unicode(self):
        assert self.buffer._coerce_val(u'\u201d') == '”'

    def test_make_key_response(self):
        column = 'times_seen'
        filters = {'pk': 1}
        self.assertEquals(self.buffer._make_key(Group, filters, column), 'sentry.group:88b48b31b5f100719c64316596b10b0f:times_seen')

    def test_make_extra_key_response(self):
        filters = {'pk': 1}
        self.assertEquals(self.buffer._make_extra_key(Group, filters), 'sentry.group:extra:88b48b31b5f100719c64316596b10b0f')

    @mock.patch('tarantool_utils.sentry.TarantoolBuffer._make_extra_key', mock.Mock(return_value='extra'))
    @mock.patch('tarantool_utils.sentry.TarantoolBuffer._make_key', mock.Mock(return_value='foo'))
    @mock.patch('sentry.buffer.base.process_incr')
    def test_incr_delays_task(self, process_incr):
        model = mock.Mock()
        columns = {'times_seen': 1}
        filters = {'pk': 1}
        self.buffer.incr(model, columns, filters)
        kwargs = dict(model=model, columns=columns, filters=filters, extra=None)
        process_incr.apply_async.assert_called_once_with(
            kwargs=kwargs, countdown=5)

    @mock.patch('tarantool_utils.sentry.TarantoolBuffer._make_extra_key', mock.Mock(return_value='extra'))
    @mock.patch('tarantool_utils.sentry.TarantoolBuffer._make_key', mock.Mock(return_value='foo'))
    @mock.patch('sentry.buffer.base.process_incr', mock.Mock())
    def test_incr_does_buffer_to_conn(self):
        model = mock.Mock()
        columns = {'times_seen': 1}
        filters = {'pk': 1}
        self.buffer.incr(model, columns, filters)
        response = self.buffer._tnt.select(self.sentry_space, 'foo')
        self.assertEquals(int(response[0][1]), 1)

        self.buffer.incr(model, columns, filters)
        response = self.buffer._tnt.select(self.sentry_space, 'foo')
        self.assertEquals(int(response[0][1]), 2)

    @mock.patch('tarantool_utils.sentry.TarantoolBuffer._make_extra_key', mock.Mock(return_value='extra'))
    @mock.patch('tarantool_utils.sentry.TarantoolBuffer._make_key', mock.Mock(return_value='foo'))
    @mock.patch('sentry.buffer.base.Buffer.process')
    def test_process_does_not_save_empty_results(self, process):
        group = Group(project=Project(id=1))
        columns = {'times_seen': 1}
        filters = {'pk': group.pk}
        self.buffer.process(Group, columns, filters)
        self.assertFalse(process.called)

    @mock.patch('tarantool_utils.sentry.TarantoolBuffer._make_extra_key', mock.Mock(return_value='extra'))
    @mock.patch('tarantool_utils.sentry.TarantoolBuffer._make_key', mock.Mock(return_value='foo'))
    @mock.patch('sentry.buffer.base.Buffer.process')
    def test_process_does_save_call_with_results(self, process):
        group = Group(project=Project(id=1))
        columns = {'times_seen': 1}
        filters = {'pk': group.pk}
        self.buffer._tnt.insert(self.sentry_space, ('foo', 2, 0L))
        self.buffer.process(Group, columns, filters)
        process.assert_called_once_with(Group, {'times_seen': 2}, filters, None)

    @mock.patch('tarantool_utils.sentry.TarantoolBuffer._make_extra_key', mock.Mock(return_value='extra'))
    @mock.patch('tarantool_utils.sentry.TarantoolBuffer._make_key', mock.Mock(return_value='foo'))
    @mock.patch('sentry.buffer.base.Buffer.process')
    def test_process_does_clear_buffer(self, process):
        group = Group(project=Project(id=1))
        columns = {'times_seen': 1}
        filters = {'pk': group.pk}
        self.buffer._tnt.insert(self.sentry_space, ('foo', 2, 0L))
        self.buffer.process(Group, columns, filters)
        response = self.buffer._tnt.select(self.sentry_space, ['foo'])
        self.assertEquals(int(response[0][1]), 0)

    @mock.patch('tarantool_utils.sentry.TarantoolBuffer._make_extra_key', mock.Mock(return_value='extra'))
    @mock.patch('tarantool_utils.sentry.TarantoolBuffer._make_key', mock.Mock(return_value='foo'))
    @mock.patch('sentry.buffer.base.process_incr', mock.Mock())
    def test_incr_does_buffer_extra_to_conn(self):
        model = mock.Mock()
        columns = {'times_seen': 1}
        filters = {'pk': 1}
        self.buffer.incr(model, columns, filters, extra={'foo': 'bar'})
        response = self.buffer._tnt.select(self.sentry_extra_space, [('extra', 'foo')])
        self.assertEquals(response[0][2], pickle.dumps('bar'))

    @mock.patch('tarantool_utils.sentry.TarantoolBuffer._make_key', mock.Mock(return_value='foo'))
    @mock.patch('tarantool_utils.sentry.TarantoolBuffer._make_extra_key', mock.Mock(return_value='extra'))
    @mock.patch('sentry.buffer.base.Buffer.process')
    def test_process_saves_extra(self, process):
        group = Group(project=Project(id=1))
        columns = {'times_seen': 1}
        filters = {'pk': group.pk}
        the_date = (timezone.now() + timedelta(days=5)).replace(microsecond=0)
        self.buffer._tnt.insert(self.sentry_space, ('foo', 1, 0L))
        self.buffer._tnt.insert(self.sentry_extra_space, ('extra', 'last_seen', pickle.dumps(the_date), 0L))
        self.buffer.process(Group, columns, filters)
        process.assert_called_once_with(Group, columns, filters, {'last_seen': the_date})

        lua_code = 'return box.space[%s]:len()' % (self.sentry_extra_space,)
        response = self.buffer._tnt.call('box.dostring', lua_code)
        self.assertEqual(0, int(response[0][0]))

    @mock.patch('tarantool_utils.sentry.TarantoolBuffer._make_extra_key', mock.Mock(return_value='extra'))
    @mock.patch('tarantool_utils.sentry.TarantoolBuffer._make_key', mock.Mock(return_value='foo'))
    @mock.patch('sentry.buffer.base.Buffer.process')
    def test_process_lock_key(self, process):
        group = Group(project=Project(id=1))
        columns = {'times_seen': 1}
        filters = {'pk': group.pk}
        self.buffer._tnt.insert(self.sentry_space, ('foo', 2, 0L))
        self.buffer.process(Group, columns, filters)
        self.buffer.process(Group, columns, filters)
        self.buffer.process(Group, columns, filters)

        process.assert_called_once_with(Group, {'times_seen': 2}, filters, None)

    @staticmethod
    def get_instance(path, options):
        cls = import_string(path)
        return cls(**options)
