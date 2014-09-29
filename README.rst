======================
tarantool-python-utils
======================

* tarantool_utils.django.TarantoolCache
* tarantool_utils.celery.TarantoolBackend
* tarantool_utils.sentry.TarantoolBuffer

Example tarantool.conf
----------------------

::
    # Django cache
    space[0].enabled = 1
    space[0].index[0].type = "HASH"
    space[0].index[0].unique = 1
    space[0].index[0].key_field[0].fieldno = 0
    space[0].index[0].key_field[0].type = "STR" # key
    space[0].index[1].type = "TREE"
    space[0].index[1].unique = 0
    space[0].index[1].key_field[0].fieldno = 2
    space[0].index[1].key_field[0].type = "NUM64" # timeout
    
    # Sentry buffer
    space[1].enabled = 1
    space[1].index[0].type = "HASH"
    space[1].index[0].unique = 1
    space[1].index[0].key_field[0].fieldno = 0
    space[1].index[0].key_field[0].type = "STR" # key
    space[1].index[1].type = "TREE"
    space[1].index[1].unique = 0
    space[1].index[1].key_field[0].fieldno = 2
    space[1].index[1].key_field[0].type = "NUM64" # timeout
    
    space[2].enabled = 1
    space[2].index[0].type = "HASH"
    space[2].index[0].unique = 1
    space[2].index[0].key_field[0].fieldno = 0
    space[2].index[0].key_field[0].type = "STR" # key
    space[2].index[0].key_field[1].fieldno = 1
    space[2].index[0].key_field[1].type = "STR" # column
    space[2].index[1].type = "TREE"
    space[2].index[1].unique = 0
    space[2].index[1].key_field[0].fieldno = 0
    space[2].index[1].key_field[0].type = "STR" # key
    space[2].index[2].type = "TREE"
    space[2].index[2].unique = 0
    space[2].index[2].key_field[0].fieldno = 3
    space[2].index[2].key_field[0].type = "NUM64" # timeout
