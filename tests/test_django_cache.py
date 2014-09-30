# -*- coding: utf-8 -*-
import os
import pickle
from unittest import TestCase

from django.core.cache import cache
from django.conf import settings


class TarantoolCacheTestCase(TestCase):
    def __init__(self, *args, **kwargs):
        super(TarantoolCacheTestCase, self).__init__(*args, **kwargs)
        cache_space = int(os.environ.get('DJANGO_TARANTOOL_CACHE_SPACE', '1'))
        self.cache_space = cache_space

    def setUp(self):
        self.clean_tnt()

    def clean_tnt(self):
        lua_code = 'box.space[%s]:truncate()' % (self.cache_space,)
        cache._tnt.call('box.dostring', lua_code)

    def test_make_key(self):
        key1 = cache.make_key('ololo')
        self.assertEqual(':1:ololo', key1)

        key2 = cache.make_key(123123)
        self.assertEqual(':1:123123', key2)

    def test_make_value(self):
        self.assertEqual(1, cache.make_value(1))
        self.assertEqual(' ' * 8 + pickle.dumps('ololo'), cache.make_value('ololo'))

    def test_add(self):
        reference_value = 'test add value'
        pickled_value = ' ' * 8 + pickle.dumps(reference_value)
        cache._tnt.insert(self.cache_space, (':1:test_add', pickled_value,
                           100500L))
        is_ok = cache.add('test_add', 'lololo value')
        self.assertFalse(is_ok)
        self.assertEqual(reference_value, cache.get('test_add'))

        is_ok_new = cache.add('test_add_new', 'new lololo value')
        self.assertTrue(is_ok_new)
        self.assertEqual('new lololo value', cache.get('test_add_new'))

    def test_get(self):
        reference_value = 'test get value'
        pickled_value = ' ' * 8 + pickle.dumps(reference_value)
        cache._tnt.insert(self.cache_space, (':1:test_get', pickled_value,
                           100500L))
        value = cache.get('test_get')
        self.assertEqual(reference_value, value)

        self.assertIsNone(cache.get('test_get_1'))
        self.assertEquals(1, cache.get('test_get_1', 1))

    def test_set(self):
        reference_value = 'test set value'
        cache._tnt.insert(self.cache_space, (':1:test_set', 'ololo value',
                           100500L))
        is_ok = cache.set('test_set', reference_value)
        self.assertEqual(reference_value, cache.get('test_set'))

    def test_delete(self):
        key = 'test_delete_1'
        cache.add(key, 'ololo')
        cache.delete(key)
        self.assertIsNone(cache.get(key))

        cache.delete('ololo delete key')

    def test_get_many(self):
        cache.set('get many key1', 'val1')
        cache.set('get many key2', 'val2')
        data = cache.get_many(['get many key1', 'get many key2'])
        self.assertEqual({'get many key1': 'val1', 'get many key2': 'val2'},
                         data)

    def test_has_key(self):
        key = 'test_has_key'
        self.assertFalse(cache.has_key(key))
        cache.set(key, 'ololo')
        self.assertTrue(cache.has_key(key))

    def test_incr(self):
        key = 'test incr'
        cache.set(key, 100)
        self.assertEqual(101, cache.incr(key))
        self.assertEqual(151, cache.incr(key, 50))
        self.assertEqual(100, cache.incr(key, -51))

    def test_decr(self):
        key = 'test decr'
        cache.set(key, 101)
        self.assertEqual(100, cache.decr(key))
        self.assertEqual(50, cache.decr(key, 50))
        self.assertEqual(100, cache.decr(key, -50))

    def test_set_many(self):
        cache.set_many({'key set many 1': 'val1', 'key set many 2': 'val2'})
        self.assertEqual('val1', cache.get('key set many 1'))
        self.assertEqual('val2', cache.get('key set many 2'))

    def test_delete_many(self):
        cache.set('key', 'ololo')
        cache.set('key2', 'ololo2')
        cache.set('key3', 'ololo3')
        cache.delete_many(['key', 'key3'])

        self.assertIsNone(cache.get('key'))
        self.assertEqual('ololo2', cache.get('key2'))

    def test_clear(self):
        cache.set('key', 'ololo')
        cache.set('key2', 'ololo2')
        cache.set('key3', 'ololo3')
        cache.clear()

        lua_code = 'return box.space[%s]:len()' % (self.cache_space,)
        response = cache._tnt.call('box.dostring', lua_code)
        self.assertEqual(0, int(response[0][0]))
