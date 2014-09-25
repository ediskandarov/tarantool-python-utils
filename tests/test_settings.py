SECRET_KEY = 'ololo'

CACHES = {
    'default': {
        'BACKEND': 'tarantool_utils.django.Tarantool15Cache',
        'LOCATION': 'win91.dev.mail.ru:33033',
    }
}
