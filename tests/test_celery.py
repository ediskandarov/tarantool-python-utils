# -*- coding: utf-8 -*-
import os
try:
   import cPickle as pickle
except:
   import pickle
from unittest import TestCase

from django.conf import settings
from tarantool_utils.celery import TarantoolBackend


class TarantoolCeleryBackendTestCase(TestCase):
    def __init__(self, *args, **kwargs):
        super(TarantoolCeleryBackendTestCase, self).__init__(*args, **kwargs)
        backend_space = int(os.environ.get('CELERY_TARANTOOL_BACKEND_SPACE', '4'))
        self.backend_space = backend_space
        self.backend = TarantoolBackend(url=settings.CELERY_RESULT_BACKEND)

    def setUp(self):
        self.clean_tnt()

    def clean_tnt(self):
        lua_code = 'box.space[%s]:truncate()' % (self.backend_space,)
        self.backend._tnt.call('box.dostring', lua_code)

    def test_make_value(self):
        self.assertEqual(pickle.dumps('ololo'), self.backend.make_value('ololo'))

    def test_get(self):
        reference_value = 'test get value'
        pickled_value = pickle.dumps(reference_value)
        self.backend._tnt.insert(self.backend_space, ('test_get', pickled_value,
                           100500L))
        value = self.backend.get('test_get')
        self.assertEqual(reference_value, value)

        self.assertIsNone(self.backend.get('test_get_1'))

    def test_set(self):
        reference_value = 'test set value'
        self.backend._tnt.insert(self.backend_space, ('test_set', 'ololo value',
                           100500L))
        is_ok = self.backend.set('test_set', reference_value)
        self.assertEqual(reference_value, self.backend.get('test_set'))

    def test_delete(self):
        key = 'test_delete_1'
        self.backend.set(key, 'ololo')
        self.backend.delete(key)
        self.assertIsNone(self.backend.get(key))

        self.backend.delete('ololo delete key')

    def test_mget(self):
        self.backend.set('get many key1', 'val1')
        self.backend.set('get many key2', 'val2')
        data = self.backend.mget(['get many key1', 'get many key2'])
        self.assertEqual({'get many key1': 'val1', 'get many key2': 'val2'},
                         data)

    def test_expire(self):
        self.backend.set('expire key', 'value')
        response1 = self.backend._tnt.select(self.backend_space, 'expire key')
        timeout1 = int(response1[0][2])
        self.backend.expire('expire key', 5)
        response2 = self.backend._tnt.select(self.backend_space, 'expire key')
        timeout2 = int(response2[0][2])
        self.assertNotEqual(timeout1, timeout2)
