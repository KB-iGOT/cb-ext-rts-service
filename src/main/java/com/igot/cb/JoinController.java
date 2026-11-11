package com.igot.cb;

import static com.igot.cb.Frames.map;

import com.igot.cb.AuthStore.Roster;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * POST /rts/v1/join (LLD §3.1): the client sends only the survey code, with its access token. userId, name and role
 * come from the token; the survey's roster checks the role for this survey; the reply is a single-use join pass for
 * {@code /ws?pass=...}.
 * PUT /admin/sessions/{code}: sets the roster (stands in for the service that owns RTS sessions).
 */
@RestController
public class JoinController {

    /** Only the survey code: who the user is, their name and role come from the token. */
    public record JoinRequest(String rtsCode) {
    }

    public record RosterRequest(List<String> coordinators, List<String> participants, boolean anyLearner) {
    }

    private final JoinPasses passes;
    private final IdentityVerifier identity;
    private final AuthStore store;
    private final RtsSettings settings;

    public JoinController(JoinPasses passes, IdentityVerifier identity, AuthStore store, RtsSettings settings) {
        this.passes = passes;
        this.identity = identity;
        this.store = store;
        this.settings = settings;
    }

    /**
     * For the demo page: whether to use passes, which kind of token to send and, for a demo token, where to put the
     * roles and the coordinator role's name.
     */
    @GetMapping("/auth-config")
    public Map<String, Object> config() {
        RtsSettings.Jwt jwt = settings.auth().jwt();
        boolean demo = "demo".equals(settings.auth().identity());
        List<String> pc = jwt == null || jwt.coordinatorRoles() == null ? List.of() : jwt.coordinatorRoles();
        return map("mode", passes.enabled() ? "pass" : "dev", "identity", settings.auth().identity(),
                "roleClaim", demo && jwt != null ? jwt.roleClaim() : null,
                "coordinatorRole", demo && !pc.isEmpty() ? pc.get(0).trim() : null);
    }

    @PostMapping("/rts/v1/join")
    public Mono<ResponseEntity<Map<String, Object>>> join(
            @RequestHeader(name = "Authorization", required = false) String authorization,
            @RequestBody JoinRequest req) {
        if (!passes.enabled()) {
            return Mono.just(error(new RtsError("NOT_ENABLED", "rts.auth.mode=dev: no join passes")));
        }
        return identity.verify(authorization)
                .flatMap(who -> passes.authorize(who, req.rtsCode()))
                .flatMap(p -> passes.issue(p).map(pass -> ResponseEntity.ok(map("pass", pass,
                        "path", settings.ws().path(),
                        "expiresAt", System.currentTimeMillis() + passes.passTtl().toMillis(),
                        "userId", p.userId(), "name", p.name(), "rtsCode", p.code(), "role", p.role()))))
                .onErrorResume(RtsError.class, e -> Mono.just(error(e)));
    }

    @PutMapping("/admin/sessions/{code}")
    public Mono<ResponseEntity<Map<String, Object>>> roster(@PathVariable String code,
            @RequestHeader(name = "X-Admin-Key", required = false) String key, @RequestBody RosterRequest req) {
        String expected = settings.auth().adminKey();
        if (expected == null || expected.isBlank()) {
            return Mono.just(error(new RtsError("NOT_ENABLED", "Set rts.auth.admin-key to use this endpoint")));
        }
        if (key == null || !MessageDigest.isEqual(key.getBytes(StandardCharsets.UTF_8),
                expected.getBytes(StandardCharsets.UTF_8))) {
            return Mono.just(error(new RtsError("FORBIDDEN", "Wrong X-Admin-Key")));
        }
        if (req.coordinators() == null || req.coordinators().isEmpty()) {
            return Mono.just(error(new RtsError("BAD_REQUEST", "At least one coordinator")));
        }
        Roster r = new Roster(Set.copyOf(req.coordinators()),
                req.participants() == null ? Set.of() : Set.copyOf(req.participants()), req.anyLearner());
        return store.putRoster(code, r).thenReturn(ResponseEntity.ok(map("rtsCode", code,
                "coordinators", r.coordinators(), "participants", r.participants(), "anyLearner", r.anyLearner())));
    }

    private static ResponseEntity<Map<String, Object>> error(RtsError e) {
        HttpStatus status = switch (e.code) {
            case "UNAUTHENTICATED" -> HttpStatus.UNAUTHORIZED;
            case "FORBIDDEN", "NOT_ENROLLED" -> HttpStatus.FORBIDDEN;
            case "RTS_NOT_ENABLED", "NOT_ENABLED" -> HttpStatus.NOT_FOUND;
            default -> HttpStatus.BAD_REQUEST;
        };
        return ResponseEntity.status(status).body(map("error", map("code", e.code, "message", e.getMessage())));
    }
}
