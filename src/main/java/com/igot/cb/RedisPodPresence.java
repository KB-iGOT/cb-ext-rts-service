package com.igot.cb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * Keys (not per survey, so outside the {code} hash tags):
 * <pre>
 * rts:pods                    zset  pod instance id -> last heartbeat (ms)
 * rts:pod:{podId}:conns       hash  connId -> {code, userId, name, coordinator} of each socket on that pod
 * rts:paused                  zset  survey code -> pausedAt (ms), while the survey is PAUSED (PC lost)
 * </pre>
 * The pod instance id is the pod name plus a random suffix: a restarted container gets a new id, so the sockets
 * of the old process are cleaned up even when the pod name stays the same.
 */
@Component
public class RedisPodPresence implements PodPresence {

    private static final Logger log = LoggerFactory.getLogger(RedisPodPresence.class);
    private static final String PODS = "rts:pods";
    private static final String PAUSED = "rts:paused";

    /** Removes a paused entry only if it is still the same pause (a newer pause of the survey is kept). */
    private static final RedisScript<Long> TAKE_PAUSED = RedisScript.of("""
            local s = redis.call('ZSCORE', KEYS[1], ARGV[1])
            if s and tonumber(s) == tonumber(ARGV[2]) then return redis.call('ZREM', KEYS[1], ARGV[1]) end
            return 0
            """, Long.class);

    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final String podId;
    private final Duration ttl;
    private final Scheduler worker = Schedulers.parallel();
    private volatile boolean registered;

    public RedisPodPresence(ReactiveStringRedisTemplate redis, ObjectMapper mapper, RtsSettings settings,
            @Value("${pod.name}") String pod) {
        this.redis = redis;
        this.mapper = mapper;
        this.podId = pod + "-" + UUID.randomUUID().toString().substring(0, 8);
        this.ttl = settings.room().ttl();
        log.info("Pod instance {}", podId);
    }

    /** Clean shutdown: this pod's sockets have run their own leave, so there is nothing left for a sweeper. */
    @PreDestroy
    void stop() {
        try {
            redis.opsForZSet().remove(PODS, podId).block(Duration.ofSeconds(2));
        } catch (Exception e) {
            log.debug("Pod deregistration: {}", e.toString());
        }
    }

    private static String conns(String podId) {
        return "rts:pod:{" + podId + "}:conns";
    }

    @Override
    public Mono<Void> add(Conn c) {
        String v;
        try {
            v = mapper.writeValueAsString(Map.of("code", c.code, "userId", c.userId, "name", c.name,
                    "coordinator", c.coordinator));
        } catch (Exception e) {
            return Mono.error(e);
        }
        return redis.opsForHash().put(conns(podId), c.connId, v).publishOn(worker).then();
    }

    @Override
    public Mono<Boolean> remove(Conn c) {
        return redis.opsForHash().remove(conns(podId), c.connId).publishOn(worker).map(n -> n > 0);
    }

    @Override
    public Mono<Boolean> heartbeat() {
        return redis.opsForZSet().add(PODS, podId, System.currentTimeMillis())
                .flatMap(added -> redis.expire(conns(podId), ttl).thenReturn(added))
                .publishOn(worker)
                .map(added -> {
                    boolean reaped = added && registered;
                    registered = true;
                    return reaped;
                });
    }

    @Override
    public Flux<Orphan> claimDeadPods(long cutoff) {
        return redis.opsForZSet().rangeByScore(PODS, Range.closed(0d, (double) cutoff))
                .filter(id -> !id.equals(podId))
                // ZREM claims the dead pod: only one sweeping pod gets 1
                .concatMap(id -> redis.opsForZSet().remove(PODS, id).filter(n -> n > 0).map(n -> id))
                .doOnNext(id -> log.warn("Pod {} has no heartbeat: cleaning up its sockets", id))
                .concatMap(id -> redis.<String, String>opsForHash().entries(conns(id))
                        // HDEL claims each socket: its own pod (if it was only slow) may be removing it too
                        .concatMap(e -> redis.opsForHash().remove(conns(id), e.getKey()).filter(n -> n > 0)
                                .map(n -> orphan(e.getKey(), e.getValue()))))
                .publishOn(worker);
    }

    private Orphan orphan(String connId, String json) {
        try {
            JsonNode n = mapper.readTree(json);
            return new Orphan(connId, n.path("code").asText(), n.path("userId").asText(), n.path("name").asText(),
                    n.path("coordinator").asBoolean());
        } catch (Exception e) {
            throw new IllegalStateException("Bad socket record " + connId, e);
        }
    }

    @Override
    public Mono<Void> paused(String code, long pausedAt) {
        return redis.opsForZSet().add(PAUSED, code, pausedAt).publishOn(worker).then();
    }

    @Override
    public Flux<Tuple2<String, Long>> takePausedBefore(long cutoff) {
        return redis.opsForZSet().rangeByScoreWithScores(PAUSED, Range.closed(0d, (double) cutoff))
                .concatMap(t -> {
                    long at = t.getScore().longValue();
                    return redis.execute(TAKE_PAUSED, List.of(PAUSED), List.of(t.getValue(), Long.toString(at)))
                            .next().filter(n -> n > 0).map(n -> Tuples.of(t.getValue(), at));
                })
                .publishOn(worker);
    }
}
