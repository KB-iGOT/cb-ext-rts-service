package com.igot.cb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.SessionView.Delivery;
import com.igot.cb.SessionView.PushResult;
import com.igot.cb.SessionView.PushView;
import com.igot.cb.SessionView.Question;
import com.igot.cb.SessionView.StartResult;
import com.igot.cb.SessionView.Status;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Survey state in Redis, shared by all pods (LLD §4). Keys carry the hash tag {code} so one survey stays on one
 * Redis Cluster slot:
 * <pre>
 * rts:{code}:room              hash  roomId status coordinatorId pausedAt pushCount pushId q open endsAt shown revealed
 * rts:{code}:part              hash  learner userId -> open sockets (the 200 cap counts fields)
 * rts:{code}:pcs               set   PC socket ids (pause only when the last PC socket is gone)
 * rts:{code}:{pushId}:ans      set   learners who answered
 * rts:{code}:{pushId}:cnt      hash  total, c0..cN
 * rts:{code}:{pushId}:words    zset  word -> count
 * rts:{code}:{pushId}:dlv      hash  ok = sockets the question was written to (summed over pods)
 * rts:{code}:{pushId}:dlvf     set   learners it could not be written to
 * </pre>
 * Every check-then-change is one Lua script, so pods never overwrite each other.
 */
@Component
@ConditionalOnProperty(name = "rts.store", havingValue = "redis", matchIfMissing = true)
public class RedisSessionStore implements SessionStore {

    private static final RedisScript<String> START = RedisScript.of("""
            local s = redis.call('HGET', KEYS[1], 'status')
            if s == 'LIVE' or s == 'PAUSED' then return '0' end
            redis.call('HDEL', KEYS[1], 'pushId', 'q', 'open', 'endsAt', 'shown', 'revealed')
            redis.call('HSET', KEYS[1], 'roomId', ARGV[1], 'status', 'LIVE', 'coordinatorId', ARGV[2],
                'pausedAt', '0', 'pushCount', '0')
            redis.call('PEXPIRE', KEYS[1], ARGV[3])
            return '1'
            """, String.class);

    private static final RedisScript<String> TRANSITION = RedisScript.of("""
            if redis.call('HGET', KEYS[1], 'status') ~= ARGV[1] then return '0' end
            redis.call('HSET', KEYS[1], 'status', ARGV[2], 'pausedAt', ARGV[3])
            return '1'
            """, String.class);

    private static final RedisScript<String> END = RedisScript.of("""
            local s = redis.call('HGET', KEYS[1], 'status')
            if s ~= 'LIVE' and s ~= 'PAUSED' then return '0' end
            redis.call('HSET', KEYS[1], 'status', 'ENDED', 'open', '0')
            redis.call('PEXPIRE', KEYS[1], ARGV[1])
            return '1'
            """, String.class);

    private static final RedisScript<String> PUSH = RedisScript.of("""
            if redis.call('HGET', KEYS[1], 'status') ~= 'LIVE' then return 'NOT_LIVE' end
            local prev = redis.call('HGET', KEYS[1], 'pushId')
            local prevOpen = redis.call('HGET', KEYS[1], 'open')
            local n = redis.call('HINCRBY', KEYS[1], 'pushCount', 1)
            local pid = 'P-' .. n
            redis.call('HSET', KEYS[1], 'pushId', pid, 'q', ARGV[1], 'open', '1', 'endsAt', ARGV[2],
                'shown', '0', 'revealed', '0')
            local closed = ''
            if prev and prevOpen == '1' then closed = prev end
            return pid .. '|' .. closed .. '|' .. redis.call('HGET', KEYS[1], 'roomId')
            """, String.class);

    private static final RedisScript<String> SET_OPEN = RedisScript.of("""
            if redis.call('HGET', KEYS[1], 'pushId') ~= ARGV[1] then return '0' end
            if redis.call('HGET', KEYS[1], 'status') == 'ENDED' then return '0' end
            redis.call('HSET', KEYS[1], 'open', ARGV[2], 'endsAt', ARGV[3])
            return '1'
            """, String.class);

    private static final RedisScript<String> EXPIRE = RedisScript.of("""
            if redis.call('HGET', KEYS[1], 'pushId') ~= ARGV[1] then return '0' end
            if redis.call('HGET', KEYS[1], 'open') ~= '1' then return '0' end
            if redis.call('HGET', KEYS[1], 'endsAt') ~= ARGV[2] then return '0' end
            redis.call('HSET', KEYS[1], 'open', '0')
            return '1'
            """, String.class);

    private static final RedisScript<String> MARK = RedisScript.of("""
            if redis.call('HGET', KEYS[1], 'pushId') ~= ARGV[1] then return '0' end
            redis.call('HSET', KEYS[1], ARGV[2], '1')
            return '1'
            """, String.class);

    private static final RedisScript<String> SUBMIT = RedisScript.of("""
            local cur = redis.call('HGET', KEYS[1], 'pushId')
            if not cur then return 'NO_ACTIVE_QUESTION' end
            if cur ~= ARGV[1] then return 'NOT_CURRENT_QUESTION' end
            if redis.call('HGET', KEYS[1], 'open') ~= '1' then return 'ANSWERS_STOPPED' end
            if redis.call('HGET', KEYS[1], 'status') ~= 'LIVE' then return 'ANSWERS_STOPPED' end
            local e = redis.call('HGET', KEYS[1], 'endsAt')
            if e and e ~= '' and tonumber(ARGV[3]) > tonumber(e) then return 'ANSWERS_STOPPED' end
            if redis.call('SADD', KEYS[2], ARGV[2]) == 0 then return 'ALREADY_ANSWERED' end
            redis.call('HINCRBY', KEYS[3], 'total', 1)
            if ARGV[4] ~= '' then
              for c in string.gmatch(ARGV[4], '[^,]+') do redis.call('HINCRBY', KEYS[3], 'c' .. c, 1) end
            end
            if ARGV[5] ~= '' then
              for w in string.gmatch(ARGV[5], '[^\\31]+') do redis.call('ZINCRBY', KEYS[4], 1, w) end
              redis.call('PEXPIRE', KEYS[4], ARGV[6])
            end
            redis.call('PEXPIRE', KEYS[2], ARGV[6])
            redis.call('PEXPIRE', KEYS[3], ARGV[6])
            return 'RECORDED'
            """, String.class);

    /** Learner socket joins: distinct learners capped atomically; a second tab of the same learner is not new. */
    private static final RedisScript<Long> JOIN = RedisScript.of("""
            if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
              redis.call('HINCRBY', KEYS[1], ARGV[1], 1)
            else
              if redis.call('HLEN', KEYS[1]) >= tonumber(ARGV[2]) then return -1 end
              redis.call('HSET', KEYS[1], ARGV[1], 1)
            end
            redis.call('PEXPIRE', KEYS[1], ARGV[3])
            return redis.call('HLEN', KEYS[1])
            """, Long.class);

    private static final RedisScript<Long> LEAVE = RedisScript.of("""
            if redis.call('HINCRBY', KEYS[1], ARGV[1], -1) <= 0 then redis.call('HDEL', KEYS[1], ARGV[1]) end
            return redis.call('HLEN', KEYS[1])
            """, Long.class);

    private static final String WORD_SEPARATOR = "\u001f";

    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final Duration ttl;
    private final Duration ttlAfterEnd;
    /**
     * Work after a Redis reply continues here, not on the Redis client's I/O thread: that one thread carries all
     * of this pod's Redis traffic, and building frames / writing sockets on it delays every other reply.
     */
    private final Scheduler worker = Schedulers.parallel();

    public RedisSessionStore(ReactiveStringRedisTemplate redis, ObjectMapper mapper, RtsSettings settings) {
        this.redis = redis;
        this.mapper = mapper;
        this.ttl = settings.room().ttl();
        this.ttlAfterEnd = settings.room().ttlAfterEnd();
    }

    private static String room(String code) {
        return "rts:{" + code + "}:room";
    }

    private static String part(String code) {
        return "rts:{" + code + "}:part";
    }

    private static String pcs(String code) {
        return "rts:{" + code + "}:pcs";
    }

    private static String pushKey(String code, String pushId, String suffix) {
        return "rts:{" + code + "}:" + pushId + ":" + suffix;
    }

    private <T> Mono<T> run(RedisScript<T> script, List<String> keys, Object... args) {
        List<String> argv = new ArrayList<>(args.length);
        for (Object a : args) {
            argv.add(a == null ? "" : a.toString());
        }
        return off(redis.execute(script, keys, argv).next());
    }

    private <T> Mono<T> off(Mono<T> m) {
        return m.publishOn(worker);
    }

    // ---------------------------------------------------------------- read

    @Override
    public Mono<SessionView> view(String code) {
        return off(redis.<String, String>opsForHash().entries(room(code))
                .collectMap(Map.Entry::getKey, Map.Entry::getValue))
                .map(h -> toView(code, h));
    }

    private SessionView toView(String code, Map<String, String> h) {
        if (h.get("status") == null) {
            return SessionView.waiting(code);
        }
        PushView push = null;
        if (h.get("pushId") != null && h.get("q") != null) {
            push = new PushView(h.get("pushId"), readQuestion(h.get("q")), "1".equals(h.get("open")),
                    parseLong(h.get("endsAt")), "1".equals(h.get("shown")), "1".equals(h.get("revealed")));
        }
        return new SessionView(code, h.get("roomId"), Status.valueOf(h.get("status")), h.get("coordinatorId"),
                Long.parseLong(h.getOrDefault("pausedAt", "0")), Integer.parseInt(h.getOrDefault("pushCount", "0")),
                push);
    }

    private static Long parseLong(String s) {
        return s == null || s.isEmpty() ? null : Long.valueOf(s);
    }

    private Question readQuestion(String json) {
        try {
            return mapper.readValue(json, Question.class);
        } catch (Exception e) {
            throw new IllegalStateException("Corrupt question snapshot", e);
        }
    }

    private String writeQuestion(Question q) {
        try {
            return mapper.writeValueAsString(q);
        } catch (Exception e) {
            throw new IllegalArgumentException("Question not serialisable", e);
        }
    }

    // ---------------------------------------------------------------- lifecycle

    @Override
    public Mono<StartResult> start(String code, String coordinatorId) {
        String roomId = "R-" + UUID.randomUUID().toString().substring(0, 8);
        return run(START, List.of(room(code)), roomId, coordinatorId, ttl.toMillis())
                .flatMap(created -> view(code).map(v -> new StartResult(v, "1".equals(created))));
    }

    @Override
    public Mono<SessionView> pause(String code, long now) {
        return run(TRANSITION, List.of(room(code)), "LIVE", "PAUSED", now).filter("1"::equals).flatMap(ok -> view(code));
    }

    @Override
    public Mono<SessionView> resume(String code) {
        return run(TRANSITION, List.of(room(code)), "PAUSED", "LIVE", 0).filter("1"::equals).flatMap(ok -> view(code));
    }

    @Override
    public Mono<SessionView> end(String code) {
        return view(code).flatMap(before -> run(END, List.of(room(code)), ttlAfterEnd.toMillis())
                .filter("1"::equals).map(ok -> before));
    }

    // ---------------------------------------------------------------- questions

    @Override
    public Mono<PushResult> push(String code, Question question, Long endsAt) {
        return run(PUSH, List.of(room(code)), writeQuestion(question), endsAt).flatMap(r -> {
            if ("NOT_LIVE".equals(r)) {
                return Mono.error(new RtsError("NOT_LIVE", "Survey is not live"));
            }
            String[] p = r.split("\\|", -1);
            return Mono.just(new PushResult(p[2], new PushView(p[0], question, true, endsAt, false, false),
                    p[1].isEmpty() ? null : p[1]));
        });
    }

    @Override
    public Mono<PushView> setOpen(String code, String pushId, boolean open, Long endsAt) {
        return run(SET_OPEN, List.of(room(code)), pushId, open ? "1" : "0", endsAt)
                .filter("1"::equals).flatMap(ok -> view(code)).mapNotNull(SessionView::current);
    }

    @Override
    public Mono<Boolean> expire(String code, String pushId, long endsAt) {
        return run(EXPIRE, List.of(room(code)), pushId, endsAt).map("1"::equals);
    }

    @Override
    public Mono<PushView> mark(String code, String pushId, String field) {
        return run(MARK, List.of(room(code)), pushId, field).filter("1"::equals)
                .flatMap(ok -> view(code)).mapNotNull(SessionView::current).filter(p -> p.pushId().equals(pushId));
    }

    @Override
    public Mono<String> submit(String code, String pushId, String userId, List<Integer> choice, List<String> words,
            long now) {
        String choices = choice == null ? "" : choice.stream().map(String::valueOf).collect(Collectors.joining(","));
        String w = words == null ? "" : String.join(WORD_SEPARATOR, words);
        return run(SUBMIT, List.of(room(code), pushKey(code, pushId, "ans"), pushKey(code, pushId, "cnt"),
                pushKey(code, pushId, "words")), pushId, userId, now, choices, w, ttl.toMillis());
    }

    @Override
    public Mono<Boolean> hasAnswered(String code, String pushId, String userId) {
        return off(redis.opsForSet().isMember(pushKey(code, pushId, "ans"), userId));
    }

    @Override
    public Mono<Long> answered(String code, String pushId) {
        return off(redis.<String, String>opsForHash().get(pushKey(code, pushId, "cnt"), "total")
                .map(Long::valueOf).defaultIfEmpty(0L));
    }

    @Override
    public Mono<Map<String, Object>> results(String code, PushView push) {
        String pushId = push.pushId();
        Question q = push.question();
        Mono<Map<String, String>> counts = off(redis.<String, String>opsForHash().entries(pushKey(code, pushId, "cnt"))
                .collectMap(Map.Entry::getKey, Map.Entry::getValue));
        if (q.choice()) {
            return counts.map(h -> {
                long[] c = new long[q.optionCount()];
                for (int i = 0; i < c.length; i++) {
                    c[i] = Long.parseLong(h.getOrDefault("c" + i, "0"));
                }
                return Results.choice(pushId, Long.parseLong(h.getOrDefault("total", "0")), c);
            });
        }
        if ("WORD_CLOUD".equals(q.type())) {
            return counts.zipWith(redis.opsForZSet()
                    .reverseRangeWithScores(pushKey(code, pushId, "words"), Range.closed(0L, (long) Results.TOP_WORDS - 1))
                    .map(t -> List.<Object>of(t.getValue(), t.getScore().longValue()))
                    .collectList().publishOn(worker))
                    .map(t -> Results.words(pushId, Long.parseLong(t.getT1().getOrDefault("total", "0")), t.getT2()));
        }
        return counts.map(h -> Results.text(pushId, Long.parseLong(h.getOrDefault("total", "0"))));
    }

    // ---------------------------------------------------------------- who is connected

    @Override
    public Mono<Long> join(String code, String userId, int max) {
        return run(JOIN, List.of(part(code)), userId, max, ttl.toMillis());
    }

    @Override
    public Mono<Long> leave(String code, String userId) {
        return run(LEAVE, List.of(part(code)), userId);
    }

    @Override
    public Mono<Long> connected(String code) {
        return off(redis.opsForHash().size(part(code)));
    }

    @Override
    public Mono<Long> pcJoin(String code, String connId) {
        return off(redis.opsForSet().add(pcs(code), connId).then(redis.expire(pcs(code), ttl))
                .then(redis.opsForSet().size(pcs(code))));
    }

    @Override
    public Mono<Long> pcLeave(String code, String connId) {
        return off(redis.opsForSet().remove(pcs(code), connId).then(redis.opsForSet().size(pcs(code))));
    }

    @Override
    public Mono<Long> pcCount(String code) {
        return off(redis.opsForSet().size(pcs(code)));
    }

    // ---------------------------------------------------------------- delivery

    @Override
    public Mono<Void> addDelivery(String code, String pushId, long delivered, Collection<String> failed) {
        String dlv = pushKey(code, pushId, "dlv");
        Mono<Boolean> ok = redis.opsForHash().increment(dlv, "ok", delivered).then(redis.expire(dlv, ttl));
        if (failed.isEmpty()) {
            return off(ok.then());
        }
        String dlvf = pushKey(code, pushId, "dlvf");
        return off(ok.then(redis.opsForSet().add(dlvf, failed.toArray(String[]::new))).then(redis.expire(dlvf, ttl))
                .then());
    }

    @Override
    public Mono<Delivery> delivery(String code, String pushId) {
        Mono<Long> ok = redis.<String, String>opsForHash().get(pushKey(code, pushId, "dlv"), "ok")
                .map(Long::valueOf).defaultIfEmpty(0L);
        Mono<List<String>> failed = redis.opsForSet().members(pushKey(code, pushId, "dlvf")).take(500).collectList();
        return off(Mono.zip(ok, failed)).map(t -> new Delivery(t.getT1(), t.getT2()));
    }
}
