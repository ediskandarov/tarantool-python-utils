local django_cache_space = 1
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
