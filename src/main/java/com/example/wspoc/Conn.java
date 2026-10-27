package com.example.wspoc;

import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

/**
 * One WebSocket. Frames go through a bounded per-socket queue; when it is full the client is reading too slowly
 * and is dropped (1008), so one slow phone cannot make the pod buffer without limit.
 */
public final class Conn {

    public static final CloseStatus TOO_SLOW = new CloseStatus(1008, "TOO_SLOW");
    public static final CloseStatus CONNECTION_LOST = new CloseStatus(1001, "CONNECTION_LOST");

    final WebSocketSession ws;
    final String code;
    final String userId;
    final String name;
    final boolean coordinator;
    /** Old room=/clientId= client (scripts/poc_test.py, pod_load_test.py): gets the plain QUESTION relay. */
    final boolean legacy;
    private final Stats stats;
    private final Sinks.Many<WebSocketMessage> out;

    volatile long lastSeen = System.currentTimeMillis();
    /** Counted in the session (false when refused, e.g. participant limit). */
    volatile boolean joined;
    /** LEAVE or END_SESSION sent: the close that follows is not a network loss. */
    volatile boolean explicitLeave;
    /** Survey ended: the close that follows is expected. */
    volatile boolean ended;
    /** Closed by the server's dead-connection sweep or as too slow: reported as CONNECTION_LOST. */
    volatile boolean lostByServer;
    /** No new frames accepted. */
    private boolean done;
    private boolean completed;

    Conn(WebSocketSession ws, String code, String userId, String name, boolean coordinator, boolean legacy,
            int queueSize, Stats stats) {
        this.ws = ws;
        this.code = code;
        this.userId = userId;
        this.name = name;
        this.coordinator = coordinator;
        this.legacy = legacy;
        this.stats = stats;
        this.out = Sinks.many().unicast().onBackpressureBuffer(Queues.<WebSocketMessage>get(queueSize).get());
    }

    Flux<WebSocketMessage> outbound() {
        return out.asFlux();
    }

    void touch() {
        lastSeen = System.currentTimeMillis();
    }

    /** Queues one text frame. Returns false if the socket is gone or too slow (then it is closed). */
    boolean send(byte[] frame) {
        Sinks.EmitResult r;
        synchronized (this) {
            if (done) {
                return false;
            }
            r = out.tryEmitNext(new WebSocketMessage(WebSocketMessage.Type.TEXT, ws.bufferFactory().wrap(frame)));
        }
        if (r.isSuccess()) {
            stats.frameOut(frame.length);
            return true;
        }
        stats.emitFailed();
        if (r == Sinks.EmitResult.FAIL_OVERFLOW) {
            stats.slowDropped();
            close(TOO_SLOW);
        }
        return false;
    }

    void ping() {
        synchronized (this) {
            if (!done) {
                out.tryEmitNext(ws.pingMessage(f -> f.wrap(new byte[0])));
            }
        }
    }

    /** No more frames: the handler flushes what is queued and then closes the socket normally. */
    void finish() {
        synchronized (this) {
            done = true;
            if (!completed) {
                completed = true;
                out.tryEmitComplete();
            }
        }
    }

    void close(CloseStatus status) {
        if (status != CloseStatus.NORMAL) {
            lostByServer = true;
        }
        synchronized (this) {
            done = true;
        }
        ws.close(status).subscribe(null, e -> { });
    }

    boolean isOpen() {
        return ws.isOpen();
    }
}
