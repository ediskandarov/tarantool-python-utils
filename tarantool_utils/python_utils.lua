-- Settings
local celery_backend_space = 3
local django_cache_space = 0
local sentry_buffer_space = 1
local sentry_buffer_extra_space = 2


-- Init expirationd
dofile('expirationd.lua')

local function is_expired(args, tuple)
    if tuple == nil or #tuple <= args.field_no then
        return nil
    end

    local field = tuple[args.field_no]
    local current_time = tonumber64(box.time())
    local tuple_expire_time = box.unpack("l", field)

    return current_time >= tuple_expire_time
end

local function delete_expired_tuple(space_no, args, tuple)
    local key_fields = {}
    for i = 0, #box.space[space_no].index[0].key_field do
        table.insert(key_fields, box.space[space_no].index[0].key_field[i].fieldno)
    end

    local key = {}
    for i = 1, #key_fields do
        table.insert(key, tuple[key_fields[i]])
    end

    box.delete(space_no, unpack(key))
end

expirationd.run_task("django-cache", django_cache_space, is_expired, delete_expired_tuple, {field_no = 2})
expirationd.run_task("celery-backend", celery_backend_space, is_expired, delete_expired_tuple, {field_no = 2})
expirationd.run_task("sentry-buffer", sentry_buffer_space, is_expired, delete_expired_tuple, {field_no = 2})
expirationd.run_task("sentry-buffer-extra", sentry_buffer_extra_space, is_expired, delete_expired_tuple, {field_no = 3})

-- Celery backend
box.celery_backend = {
    get = function(key)
        local tuple = box.select(celery_backend_space, 0, key)
        if tuple ~= nil then
            return tuple[1]
        end
    end,

    set = function(key, value, timeout)
        timeout = math.floor(box.time() + 0.5) + tonumber64(timeout)
        box.replace(celery_backend_space, key, value, timeout)
    end,

    delete = function(key)
        box.delete(celery_backend_space, key)
    end,

    mget = function(keys_json)
        local keys = box.cjson.decode(keys_json)
        local response = {}
        for i, key in ipairs(keys) do
           local tuple = box.select(celery_backend_space, 0, key)
           if tuple ~= nil then
               table.insert(response, {tuple[0], tuple[1]})
           end
        end
        return response
    end,

    expire = function(key, timeout)
        timeout = math.floor(box.time() + 0.5) + tonumber64(timeout)
        box.update(celery_backend_space, key, '=p', 2, timeout)
    end,
}


-- Django cache
-- 1 month
local django_cache_default_timeout = 60 * 60 * 24 * 30

box.django_cache = {
    add = function(key, value, timeout)
        timeout = math.floor(box.time() + 0.5) + tonumber64(timeout)
        box.insert(django_cache_space, key, value, timeout)
    end,

    get = function(key)
        local tuple = box.select(django_cache_space, 0, key)
        if tuple ~= nil then
            return tuple[1]
        end
    end,

    set = function(key, value, timeout)
        timeout = math.floor(box.time() + 0.5) + tonumber64(timeout)
        box.replace(django_cache_space, key, value, timeout)
    end,

    delete = function(key)
        box.delete(django_cache_space, key)
    end,

    get_many = function(keys_json)
        local keys = box.cjson.decode(keys_json)
        local response = {}
        for i, key in ipairs(keys) do
           local tuple = box.select(django_cache_space, 0, key)
           if tuple ~= nil then
               table.insert(response, {tuple[0], tuple[1]})
           end
        end
        return response
    end,

    has_key = function(key)
        local tuple = box.select(django_cache_space, 0, key)
        if tuple ~= nil then
            return true
        end
    end,

    incr = function(key, delta)
        delta = tonumber(delta)
        local tuple = box.update(django_cache_space, key, '+p', 1, delta)
        if tuple ~= nil then
            return tuple[1]
        end
    end,

    decr = function(key, delta)
        delta = tonumber(delta)
        local tuple = box.update(django_cache_space, key, '-p', 1, delta)
        if tuple ~= nil then
            return tuple[1]
        end
    end,

    set_many = function(data_json, timeout)
        local data = box.cjson.decode(data_json)
        timeout = math.floor(box.time() + 0.5) + tonumber64(timeout)
        for key, value in pairs(data) do
            box.insert(django_cache_space, key, value, timeout)
        end
    end,

    delete_many = function(keys_json)
        local keys = box.cjson.decode(keys_json)
        for i = 1, #keys do
            box.delete(django_cache_space, keys[i])
        end
    end,

    clear = function()
        box.space[django_cache_space]:truncate()
    end,
}


-- Sentry buffer
box.sentry_buffer = {
    incr = function(key, amount, timeout)
        amount = tonumber(amount)
        timeout = math.floor(box.time() + 0.5) + tonumber64(timeout)
        local tuple = box.update(sentry_buffer_space, key, '+p=p', 1, amount, 2, timeout)
        if tuple == nil then
            tuple = box.insert(sentry_buffer_space, key, amount, timeout)
        end
        return tuple[1]
    end,

    hset = function(key, column, value, timeout)
        timeout = math.floor(box.time() + 0.5) + tonumber64(timeout)
        box.replace(sentry_buffer_extra_space, key, column, value, timeout)
    end,

    setnx = function(lock_key, timeout)
        timeout = math.floor(box.time() + 0.5) + tonumber64(timeout)
        local tuple = box.select(sentry_buffer_space, 0, lock_key)
        if tuple == nil then
            box.insert(sentry_buffer_space, lock_key, '1', timeout)
            return true
        end
    end,

    getset = function(key, value, timeout)
        timeout = math.floor(box.time() + 0.5) + tonumber64(timeout)
        local tuple = box.select(sentry_buffer_space, 0, key)
        box.update(sentry_buffer_space, key, '=p=p', 1, 0, 2, timeout)
        if tuple ~= nil then
            return tuple[1]
        end
    end,

    hgetalldelete = function(hash_key)
        local response = {}
        local delete_keys = {}
        for k, v in box.space[sentry_buffer_extra_space].index[1]:iterator(box.index.EQ, hash_key) do
            table.insert(response, {k[1], k[2]})
            table.insert(delete_keys, {k[0], k[1]})
        end

        for i = 1, #delete_keys do
            box.delete(sentry_buffer_extra_space, unpack(delete_keys[i]))
        end
        if #response ~= 0 then
            return response
        end
    end,
}
