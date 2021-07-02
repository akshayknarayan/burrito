-- Initialization of a client's semantics on the key corresponding to its address.
--
-- Check if the addr key is already set. If it is not, zadd ourselves to -members.
-- If the addr key was already set but our semantics are compatible, also zadd.
-- Otherwise, prune old clients etc, but don't zadd.
--
-- key args:
-- KEYS[1]: addr
-- KEYS[2]: addr-round ctr key
-- KEYS[3]: addr-members set key
-- 
-- value args:
-- ARGV[1]: proposed nonce
-- ARGV[2]: client name
-- ARGV[3]: client expiry time
-- ARGV[4]: client insert time
--
-- returns 4-tuple:
-- joined: bool. true if zadd happened, false otherwise.
-- round_ctr: usize. the current round number (0 if key freshly created)
-- conn_count: usize. the current number of participants (including ourselves if joined == true)
-- semantics: the value at addr after this script is over. safe to ignore if joined = true.

local was_set = redis.call('setnx', KEYS[1], ARGV[1])
redis.call('zremrangebyscore', KEYS[3], 0, ARGV[3])

-- 1 means success
if (was_set == 1) 
then
    if (redis.call('setnx', KEYS[2], 0) ~= 1) then
        return redis.error_reply("KV store polluted: roundctr key already present")
    end
    -- join
    redis.call('zadd', KEYS[3], ARGV[4], ARGV[2])
    local conn_count = redis.call('zcard', KEYS[3])
    return { true, 0, conn_count, ARGV[1] }
else -- the addr key was already set.
    local existing_semantics = redis.call('get', KEYS[1])
    local round_ctr = redis.call('get', KEYS[2])
    -- if existing_semantics == proposed semantics, then join, otherwise don't.
    if (existing_semantics == ARGV[1]) then
        -- join
        redis.call('zadd', KEYS[3], ARGV[4], ARGV[2])
        local conn_count = redis.call('zcard', KEYS[3])
        return { true, round_ctr, conn_count, ARGV[1] }
    else
        local conn_count = redis.call('zcard', KEYS[3])
        return { false, round_ctr, conn_count, existing_semantics }
    end
end
