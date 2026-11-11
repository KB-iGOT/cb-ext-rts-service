package com.igot.cb;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * <pre>
 * rts:pass:{id}        string  join pass (userId, name, code, role); expires after rts.auth.pass-ttl; GETDEL on
 *                              use, so it works once (the LLD's K2 used-pass marker, for an opaque pass)
 * rts:{code}:roster    hash    coordinators = "u1,u2", participants = "u3,u4" or "*" (anyone)
 * </pre>
 */
@Component
public class RedisAuthStore implements AuthStore {

    /** Demo roster: created only if the code has none, so the first PC claim wins. */
    private static final RedisScript<Long> CLAIM = RedisScript.of("""
            if redis.call('EXISTS', KEYS[1]) == 0 then
              redis.call('HSET', KEYS[1], 'coordinators', ARGV[1], 'participants', '*')
              redis.call('PEXPIRE', KEYS[1], ARGV[2])
              return 1
            end
            return 0
            """, Long.class);

    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final Duration ttl;
    private final Scheduler worker = Schedulers.parallel();

    public RedisAuthStore(ReactiveStringRedisTemplate redis, ObjectMapper mapper, RtsSettings settings) {
        this.redis = redis;
        this.mapper = mapper;
        this.ttl = settings.room().ttl();
    }

    private static String passKey(String id) {
        return "rts:pass:" + id;
    }

    private static String rosterKey(String code) {
        return "rts:{" + code + "}:roster";
    }

    @Override
    public Mono<Void> putPass(String id, Pass pass, Duration ttl) {
        try {
            return redis.opsForValue().set(passKey(id), mapper.writeValueAsString(pass), ttl).publishOn(worker)
                    .then();
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    @Override
    public Mono<Pass> takePass(String id) {
        return redis.opsForValue().getAndDelete(passKey(id)).publishOn(worker).map(json -> {
            try {
                return mapper.readValue(json, Pass.class);
            } catch (Exception e) {
                throw new IllegalStateException("Bad pass", e);
            }
        });
    }

    @Override
    public Mono<Roster> roster(String code) {
        return redis.<String, String>opsForHash().entries(rosterKey(code))
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .filter(h -> !h.isEmpty())
                .map(h -> new Roster(ids(h.get("coordinators")), ids(h.get("participants")),
                        "*".equals(h.get("participants"))))
                .publishOn(worker);
    }

    @Override
    public Mono<Void> putRoster(String code, Roster roster) {
        String key = rosterKey(code);
        Map<String, String> h = Map.of("coordinators", String.join(",", roster.coordinators()),
                "participants", roster.anyLearner() ? "*" : String.join(",", roster.participants()));
        return redis.delete(key).then(redis.opsForHash().putAll(key, h)).then(redis.expire(key, ttl))
                .publishOn(worker).then();
    }

    @Override
    public Mono<Roster> claimRoster(String code, String userId) {
        return redis.execute(CLAIM, List.of(rosterKey(code)), List.of(userId, Long.toString(ttl.toMillis())))
                .next().then(roster(code));
    }

    private static Set<String> ids(String csv) {
        return csv == null || csv.isBlank() || "*".equals(csv) ? Set.of()
                : Arrays.stream(csv.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
    }
}
