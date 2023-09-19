-- Get the current connection state, adding addr to the set and unlocking the joinlock if needed.
--
-- key args:
-- KEYS[1]: addr
-- KEYS[2]: roundctr
-- KEYS[3]: addr-members set key
-- KEYS[4]: joinlock
-- 
-- value args:
-- ARGV[1]: client name
-- ARGV[2]: client expiry time
-- ARGV[3]: client insert time
--
-- returns 3-tuple:
-- nonce: Vec<u8>
-- round_ctr: usize
-- conn_count: usize

redis.call('zremrangebyscore', KEYS[3], 0, ARGV[2])
if (redis.call('get', KEYS[4]) == ARGV[1])
then
    redis.call('del', KEYS[4])
end

local nonce = redis.call('get', KEYS[1])
local round_ctr = redis.call('get', KEYS[2])

redis.call('zadd', KEYS[3], ARGV[3], ARGV[1])
local conn_count = redis.call('zcard', KEYS[3])
return { nonce, round_ctr, conn_count }
