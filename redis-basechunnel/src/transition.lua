-- Get the round_ctr value, and increment it if it is even (taking the lock)
--
-- key args:
-- KEYS[1]: round_ctr
-- KEYS[2]: joinlock
-- 
-- value args:
-- ARGV[1]: client name

if (redis.call('get', KEYS[2]) == ARGV[1])
then
    redis.call('del', KEYS[2])
end

local round_ctr = redis.call('get', KEYS[1])
if (round_ctr % 2 == 0)
then
    return { true, redis.call('incr', KEYS[1]) }
else
    return { false,  round_ctr }
end
