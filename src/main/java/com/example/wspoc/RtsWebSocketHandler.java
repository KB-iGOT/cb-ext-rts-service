package com.example.wspoc;

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
 *
 * <p>URL (dev identity, no auth — this service measures capacity):
 * {@code /ws?rtsCode=482913&userId=asha&role=PARTICIPANT|COORDINATOR&name=Asha}.
 * The old {@code ?room=&clientId=&role=trainer|learner} form still works for scripts/poc_test.py and
 * scripts/pod_load_test.py. In production the identity comes from a signed join pass instead (LLD §3).
 */
@Component
public class RtsWebSocketHandler implements WebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(RtsWebSocketHandler.class);
    private static final CloseStatus LIMIT = new CloseStatus(4429, "PARTICIPANT_LIMIT_REACHED");

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
        Map<String, String> q = UriComponentsBuilder.fromUri(session.getHandshakeInfo().getUri()).build()
                .getQueryParams().toSingleValueMap();
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
        if (!manager.join(c)) {
            stats.rejected.increment();
            c.send(frames.error(null, "PARTICIPANT_LIMIT_REACHED", "This survey already has the maximum participants"));
            c.finish();
            return session.send(c.outbound()).then(session.close(LIMIT));
        }
        stats.connected(coordinator);
        boolean logIt = legacy || ws.logConnections();
        if (logIt) {
            log.info("CONNECT pod={} client={} role={} room={} sessionId={}", pod, userId, role, code, session.getId());
        }

        AtomicReference<CloseStatus> closeStatus = new AtomicReference<>();
        session.closeStatus().subscribe(closeStatus::set, e -> { });

        Mono<Void> inbound = session.receive()
                .doOnNext(m -> {
                    c.touch();
                    if (m.getType() == WebSocketMessage.Type.TEXT) {
                        manager.onFrame(c, m.getPayloadAsText());
                    }
                })
                .onErrorResume(e -> Mono.empty())
                // peer gone: stop the outbound side too
                .doFinally(sig -> c.finish())
                .then();
        // outbound completes when the survey ends or the socket closes; then close normally
        Mono<Void> outbound = session.send(c.outbound())
                .onErrorResume(e -> Mono.empty())
                .then(Mono.defer(() -> session.isOpen() ? session.close(CloseStatus.NORMAL) : Mono.empty()));

        manager.onJoined(c);
        return Mono.when(inbound, outbound).doFinally(sig -> {
            stats.disconnected(coordinator);
            CloseStatus st = closeStatus.get();
            boolean clean = c.explicitLeave || c.ended || (!c.lostByServer && st != null
                    && (st.getCode() == 1000 || st.getCode() == 1001));
            String reason = clean ? "LEFT" : "CONNECTION_LOST";
            if (!clean) {
                stats.unexpectedCloses.increment();
            }
            if (logIt) {
                log.info("DISCONNECT pod={} client={} room={} reason={} close={}", pod, userId, code, reason,
                        closeStatus.get());
            }
            manager.onLeft(c, reason);
        });
    }
}
