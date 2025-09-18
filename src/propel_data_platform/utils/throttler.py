import time
import uuid

import redis


class Throttler:

    def __init__(self, redis_config, retries=10):
        self._redis = None
        self._key = None
        self.redis_config = redis_config
        self.retries = retries

    @property
    def redis(self):
        attempts = 0
        while True:
            try:
                if self._redis is not None:
                    self._redis.ping()
                    return self._redis
                else:
                    redis_kwargs = {
                        "host": self.redis_config.get("host", "localhost"),
                        "port": self.redis_config.get("port", 6379),
                        "db": self.redis_config.get("db", 0),
                        "password": self.redis_config.get("password", None),
                        "ssl": self.redis_config.get("ssl", True),
                        "socket_keepalive": self.redis_config.get(
                            "socket_keepalive", True
                        ),
                        "health_check_interval": self.redis_config.get(
                            "health_check_interval", 10
                        ),
                        "retry_on_timeout": self.redis_config.get(
                            "retry_on_timeout", True
                        ),
                    }
                    self._redis = redis.Redis(**redis_kwargs)
                    self._redis.ping()
                    return self._redis
            except Exception as e:
                attempts += 1
                if attempts >= self.retries:
                    raise Exception("Redis retry limit exceeded")
                time.sleep(0.1)

    def configure(self, key, limit, window):
        self._key = key
        if isinstance(limit, int) and isinstance(window, int):
            self.limit = [limit]
            self.window = [window]
        elif isinstance(limit, list) and isinstance(window, list):
            self.limit = limit
            self.window = window
            assert len(self.limit) == len(
                self.window
            ), "Limit and window must have the same length"
        else:
            raise ValueError(
                "Limit and window must be either both int or both list of ints of equal length"
            )
        return self

    @property
    def keys(self):
        return [f"throttle:{self._key}_l{i}" for i in range(len(self.limit))]

    def _check_limit(self):
        lua_script = """
            local now = tonumber(ARGV[1])
            local expired_keys = {}

            for i = 1, #KEYS do
                local key = KEYS[i]
                local limit = tonumber(ARGV[(i - 1) * 3 + 2])
                local window = tonumber(ARGV[(i - 1) * 3 + 3])

                redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
                local count = redis.call('ZCARD', key)

                if count >= limit then
                    table.insert(expired_keys, key)
                end
            end

            if #expired_keys > 0 then
                return {false, expired_keys}
            else
                for i = 1, #KEYS do
                    local key = KEYS[i]
                    local window = tonumber(ARGV[(i - 1) * 3 + 3])
                    local uuid = ARGV[(i - 1) * 3 + 4]

                    redis.call('ZADD', key, now, uuid)
                    redis.call('EXPIRE', key, window)
                end
                return {true, {}}
            end
        """
        uuids = [str(uuid.uuid4()) for _ in range(len(self.keys))]
        argv = [time.time()]
        for limit, window, uuid_ in zip(self.limit, self.window, uuids):
            argv.extend([limit, window, uuid_])

        attempts = 0
        while True:
            try:
                status, expired_keys = self.redis.eval(
                    lua_script, len(self.keys), *self.keys, *argv
                )
                return bool(status), [ek.decode("utf-8") for ek in expired_keys]
            except Exception as e:
                attempts += 1
                if attempts >= self.retries:
                    raise Exception("Redis retry limit exceeded")
                time.sleep(0.1)

    def _get_sleep_time(self, expired_keys):
        expired_windows = [
            self.window[i]
            for i in range(len(self.window))
            if self.keys[i] in expired_keys
        ]
        attempts = 0
        while True:
            try:
                now = time.time()
                pipeline = self.redis.pipeline()
                for key in expired_keys:
                    pipeline.zrange(key, 0, 0, withscores=True)
                sleep_time = 0.1
                for score, window in zip(pipeline.execute(), expired_windows):
                    _sleep_time = max(0.1, window - (now - float(score[0][1])))
                    sleep_time = max(sleep_time, _sleep_time)
                return sleep_time
            except Exception as e:
                attempts += 1
                if attempts >= self.retries:
                    raise Exception("Redis retry limit exceeded")
                time.sleep(0.1)

    def throttle(self, func):
        def wrapper(*args, **kwargs):
            while True:
                status, expired_keys = self._check_limit()
                if status:
                    return func(*args, **kwargs)
                else:
                    sleep_time = self._get_sleep_time(expired_keys)
                    time.sleep(sleep_time)

        return wrapper
