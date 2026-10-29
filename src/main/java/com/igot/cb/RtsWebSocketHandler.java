package com.igot.cb;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;

/**
 * One call per socket: read who it is from the URL, join the survey, process frames in order, clean up.
 * Any pod can serve any socket: the survey state is in the {@link SessionStore}.
 *
 * <p>URL (dev identity, no auth — this service measures capacity):
 * {@code /ws?rtsCode=482913&userId=asha&role=PARTICIPANT|COORDINATOR&name=Asha}.
 * The old {@code ?room=&clientId=&role=trainer|learner} form still works for scripts/poc_test.py and
 * scripts/pod_load_test.py. In production the identity comes from a signed join pass instead (LLD §3).
 */
@Component
public class RtsWebSocketHandler implements WebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(RtsWebSocketHandler.class);
    static final CloseStatus LIMIT = new CloseStatus(4429, "PARTICIPANT_LIMIT_REACHED");
    private static final CloseStatus TRY_AGAIN = new CloseStatus(1011, "TRY_AGAIN");

    private final SessionManager manager;
    private final Stats stats;
    private final Frames frames;
    private final RtsSettings.Ws ws;
    private final String pod;

    public RtsWebSocketHandler(SessionManager manager, Stats stats, Frames frames, RtsSettings settings,
            @Value("${pod.name}") String pod) {
        this.manager = manager;
        this.stats = stats;
        this.frames = frames;
        this.ws = settings.ws();
        this.pod = pod;
    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        stats.handshakes.increment();
        // decoded like a form: "Ravi+Kumar" and "Ravi%20Kumar" both become "Ravi Kumar" (browsers send "+")
        Map<String, String> q = new java.util.HashMap<>();
        UriComponentsBuilder.fromUri(session.getHandshakeInfo().getUri()).build().getQueryParams().toSingleValueMap()
                .forEach((k, v) -> q.put(k, v == null ? null : java.net.URLDecoder.decode(v, StandardCharsets.UTF_8)));
        boolean legacy = !q.containsKey("rtsCode") && q.containsKey("room");
        String code = legacy ? q.get("room") : q.get("rtsCode");
        String userId = q.getOrDefault("userId", q.getOrDefault("clientId", "unknown"));
        String role = q.getOrDefault("role", "PARTICIPANT");
        boolean coordinator = role.equalsIgnoreCase("COORDINATOR") || role.equalsIgnoreCase("trainer");
        String name = q.getOrDefault("name", userId);
        if (code == null || code.isBlank()) {
            stats.rejected.increment();
            return session.send(Mono.just(session.textMessage(
                    new String(frames.error(null, "BAD_MESSAGE", "rtsCode is required")))))
                    .then(session.close(CloseStatus.POLICY_VIOLATION));
        }

        Conn c = new Conn(session, code, userId, name, coordinator, legacy, ws.sendQueue(), stats);
        return manager.join(c)
                .onErrorResume(e -> {
                    log.warn("Join of {} to {} failed", userId, code, e);
                    return Mono.just(Boolean.FALSE).doOnNext(x -> c.lostByServer = true);
                })
                .flatMap(ok -> {
                    if (ok) {
                        return lifecycle(session, c, role);
                    }
                    stats.rejected.increment();
                    boolean full = !c.lostByServer;
                    c.send(full ? manager.limitError()
                            : frames.error(null, "TRY_AGAIN", "Service busy, please reconnect"));
                    c.finish();
                    return session.send(c.outbound()).then(session.close(full ? LIMIT : TRY_AGAIN));
                });
    }

    private Mono<Void> lifecycle(WebSocketSession session, Conn c, String role) {
        stats.connected(c.coordinator);
        boolean logIt = c.legacy || ws.logConnections();
        if (logIt) {
            log.info("CONNECT pod={} client={} name={} role={} room={} sessionId={}", pod, c.userId, c.name, role,
                    c.code, session.getId());
        }

        AtomicReference<CloseStatus> closeStatus = new AtomicReference<>();
        session.closeStatus().subscribe(closeStatus::set, e -> { });

        // frames of one socket are handled strictly in order (a PC's PUSH can't overtake its own STOP)
        Mono<Void> inbound = session.receive()
                .doOnNext(m -> c.touch())
                .filter(m -> m.getType() == WebSocketMessage.Type.TEXT)
                .map(WebSocketMessage::getPayloadAsText)
                .concatMap(text -> manager.onFrame(c, text))
                .onErrorResume(e -> Mono.empty())
                // peer gone: stop the outbound side too
                .doFinally(sig -> c.finish())
                .then();
        // outbound completes when the survey ends or the socket closes; then close normally
        Mono<Void> outbound = session.send(c.outbound())
                .onErrorResume(e -> Mono.empty())
                .then(Mono.defer(() -> session.isOpen() ? session.close(CloseStatus.NORMAL) : Mono.empty()));

        Mono<Void> joined = manager.onJoined(c).onErrorResume(e -> {
            log.warn("Session state for {} in {} failed: {}", c.userId, c.code, e.toString());
            c.send(frames.error(null, "TRY_AGAIN", "Temporary problem, please reconnect"));
            c.lostByServer = true;
            c.finish();
            return Mono.empty();
        });
        return Mono.when(joined.then(inbound), outbound).doFinally(sig -> {
            stats.disconnected(c.coordinator);
            CloseStatus st = closeStatus.get();
            boolean clean = c.explicitLeave || c.ended || (!c.lostByServer && st != null
                    && (st.getCode() == 1000 || st.getCode() == 1001));
            String reason = clean ? "LEFT" : "CONNECTION_LOST";
            if (!clean) {
                stats.unexpectedCloses.increment();
            }
            if (logIt) {
                log.info("DISCONNECT pod={} client={} room={} reason={} close={}", pod, c.userId, c.code, reason, st);
            }
            manager.onLeft(c, reason)
                    .subscribe(null, e -> log.warn("Leave of {} from {} failed: {}", c.userId, c.code, e.toString()));
        });
    }
}
