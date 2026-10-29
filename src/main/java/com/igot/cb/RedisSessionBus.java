package com.igot.cb;

import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Redis pub/sub, one channel per survey: {@code rts:room:{code}}. A pod subscribes while it holds at least one
 * socket of the survey (reference-counted) and hears its own messages too, so delivery has one path.
 * Message: header {@code event TAB target TAB pushId TAB closeAfter}, newline, then the frame JSON.
 * Pub/sub keeps nothing: a pod that misses a message is corrected by the state in Redis (clients get
 * SESSION_STATE on reconnect, and every event carries the current pushId).
 */
@Component
@ConditionalOnProperty(name = "rts.store", havingValue = "redis", matchIfMissing = true)
public class RedisSessionBus implements SessionBus {

    private static final Logger log = LoggerFactory.getLogger(RedisSessionBus.class);

    private final ReactiveStringRedisTemplate redis;
    private final ReactiveRedisMessageListenerContainer listenerContainer;
    private volatile Consumer<Event> listener = e -> { };
    private final Map<String, Sub> subs = new ConcurrentHashMap<>();

    /** One subscription per survey on this pod; ready completes once Redis confirmed it. */
    private static final class Sub {
        int refs;
        Mono<Void> ready;
        volatile Disposable disposable;
    }

    public RedisSessionBus(ReactiveStringRedisTemplate redis, ReactiveRedisConnectionFactory factory) {
        this.redis = redis;
        this.listenerContainer = new ReactiveRedisMessageListenerContainer(factory);
    }

    @PreDestroy
    void stop() {
        subs.values().forEach(s -> {
            if (s.disposable != null) {
                s.disposable.dispose();
            }
        });
        try {
            listenerContainer.destroyLater().block(Duration.ofSeconds(2));
        } catch (Exception e) {
            log.debug("Listener shutdown: {}", e.toString());
        }
    }

    private static String channel(String code) {
        return "rts:room:{" + code + "}";
    }

    @Override
    public void onEvent(Consumer<Event> listener) {
        this.listener = listener;
    }

    @Override
    public synchronized Mono<Void> subscribe(String code) {
        Sub s = subs.computeIfAbsent(code, k -> new Sub());
        s.refs++;
        if (s.ready == null) {
            s.ready = listenerContainer.receiveLater(ChannelTopic.of(channel(code)))
                    // deliver on a worker thread, not the Redis pub/sub I/O thread; order per survey is kept
                    .doOnNext(flux -> s.disposable = flux.publishOn(Schedulers.parallel()).subscribe(m -> {
                        try {
                            listener.accept(decode(code, m.getMessage()));
                        } catch (Exception e) {
                            log.error("Bad event on {}", channel(code), e);
                        }
                    }, e -> log.warn("Subscription to {} ended: {}", channel(code), e.toString())))
                    .then()
                    .cache();
        }
        return s.ready;
    }

    @Override
    public synchronized void unsubscribe(String code) {
        Sub s = subs.get(code);
        if (s == null || --s.refs > 0) {
            return;
        }
        subs.remove(code);
        if (s.ready != null) {
            // dispose once the subscription exists (it may still be confirming)
            s.ready.subscribe(null, e -> { }, () -> {
                if (s.disposable != null) {
                    s.disposable.dispose();
                }
            });
        }
    }

    @Override
    public Mono<Void> publish(Event e) {
        String header = e.event() + "\t" + e.target() + "\t" + (e.pushId() == null ? "" : e.pushId()) + "\t"
                + (e.closeAfter() ? "1" : "0");
        return redis.convertAndSend(channel(e.code()), header + "\n" + new String(e.frame(), StandardCharsets.UTF_8))
                .publishOn(Schedulers.parallel()).then();
    }

    private static Event decode(String code, String msg) {
        int nl = msg.indexOf('\n');
        String[] h = msg.substring(0, nl).split("\t", -1);
        return new Event(code, h[0], h[1], h[2].isEmpty() ? null : h[2], "1".equals(h[3]),
                msg.substring(nl + 1).getBytes(StandardCharsets.UTF_8));
    }
}
