SECRET_KEY = 'ololo'

CACHES = {
    'default': {
        'BACKEND': 'tarantool_utils.django.Tarantool15Cache',
        'LOCATION': 'win91.dev.mail.ru:33033',
    }
}

SENTRY_QUOTAS = 'sentry.quotas.Quota'
SENTRY_QUOTA_OPTIONS = {}
SENTRY_BUFFER = 'tarantool_utils.sentry.Tarantool15Buffer'
SENTRY_BUFFER_OPTIONS = {
    'hosts': {
        0: {
            'host': 'win91.dev.mail.ru:33033'
        }
    },
}
