package com.igot.cb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.http.client.HttpClient;

/** Tiny WebSocket test client: collects frames, sends envelopes. */
final class WsClient {

    private WsClient() {
    }

    static final class Client {
        final BlockingQueue<JsonNode> in = new LinkedBlockingQueue<>();
        final Sinks.Many<String> out = Sinks.many().unicast().onBackpressureBuffer();
        final Latch closed = new Latch();
        final AtomicInteger seq = new AtomicInteger();
        final Disposable conn;

        private final ObjectMapper mapper;

        Client(String url, ObjectMapper mapper) {
            this.mapper = mapper;
            conn = HttpClient.create().websocket().uri(url).handle((i, o) -> Mono.firstWithSignal(
                    i.receive().asString().doOnNext(t -> {
                        try {
                            in.add(mapper.readTree(t));
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    }).then(), o.sendString(out.asFlux()).then()))
                    .doFinally(s -> closed.done()).subscribe();
        }

        void send(String event, Object data) throws Exception {
            sendRaw(mapper.writeValueAsString(Map.of("event", event, "requestId", "r" + seq.incrementAndGet(),
                    "data", data)));
        }

        void sendRaw(String text) {
            out.tryEmitNext(text);
        }

        JsonNode next(String event) throws InterruptedException {
            return nextWhere("event", event);
        }

        JsonNode nextWhere(String field, String value) throws InterruptedException {
            long until = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < until) {
                JsonNode f = in.poll(200, TimeUnit.MILLISECONDS);
                if (f != null && value.equals(f.path(field).asText())) {
                    return f;
                }
            }
            throw new AssertionError("No " + field + "=" + value + " within 10 s");
        }

        void close() {
            conn.dispose();
        }
    }

    static final class Latch {
        private final java.util.concurrent.CountDownLatch l = new java.util.concurrent.CountDownLatch(1);

        void done() {
            l.countDown();
        }

        boolean await() throws InterruptedException {
            return l.await(10, TimeUnit.SECONDS);
        }
    }
}
