package com.igot.cb;

import com.igot.cb.AuthStore.Pass;
import com.igot.cb.AuthStore.Roster;
import com.igot.cb.IdentityVerifier.Identity;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Base64;
import java.util.Set;
import java.util.regex.Pattern;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * Join passes (LLD §3): who a socket is (user, name, survey, role), decided by the server. A pass lives
 * rts.auth.pass-ttl and is removed when used. Every connection, the first one or a reconnect to the same live
 * survey, opens with a new pass from POST /rts/v1/join.
 */
@Component
public class JoinPasses {

    static final String COORDINATOR = "COORDINATOR";
    static final String PARTICIPANT = "PARTICIPANT";
    private static final Pattern CODE = Pattern.compile("[A-Za-z0-9_-]{1,64}");

    private final AuthStore store;
    private final RtsSettings.Auth auth;
    private final SecureRandom random = new SecureRandom();

    public JoinPasses(AuthStore store, RtsSettings settings) {
        this.store = store;
        this.auth = settings.auth();
    }

    /**
     * The pass for this user in this survey. Who they are comes from their token. Role: COORDINATOR when the token
     * carries a coordinator role and the roster names them PC of this survey; otherwise PARTICIPANT, if the roster
     * lets them join as a learner (so a PC can attend another PC's survey as a learner). Errors (LLD §3.1):
     * BAD_REQUEST, RTS_NOT_ENABLED (no such survey), NOT_ENROLLED (neither its PC nor one of its learners).
     */
    Mono<Pass> authorize(Identity who, String code) {
        if (code == null || !CODE.matcher(code).matches()) {
            return Mono.error(new RtsError("BAD_REQUEST", "rtsCode is required (1-64 letters, digits, - or _)"));
        }
        boolean wantsPc = who.coordinator();
        Mono<Roster> roster = store.roster(code);
        if (auth.devClaimCoordinator()) {
            // demo: an unknown code is open to learners, and its first PC claim makes the roster
            roster = roster.switchIfEmpty(Mono.defer(() -> wantsPc ? store.claimRoster(code, who.userId())
                    : Mono.just(new Roster(Set.of(), Set.of(), true))));
        }
        return roster.switchIfEmpty(Mono.error(new RtsError("RTS_NOT_ENABLED", "No such survey")))
                .flatMap(r -> {
                    if (wantsPc && r.mayCoordinate(who.userId())) {
                        return Mono.just(new Pass(who.userId(), who.name(), code, COORDINATOR));
                    }
                    // a learner, or a PC of another survey attending this one as a learner
                    if (!r.mayParticipate(who.userId())) {
                        return Mono.error(new RtsError("NOT_ENROLLED", "Not a participant of this survey"));
                    }
                    return Mono.just(new Pass(who.userId(), who.name(), code, PARTICIPANT));
                });
    }

    /** A new single-use pass; returns its value. */
    Mono<String> issue(Pass p) {
        byte[] b = new byte[18];
        random.nextBytes(b);
        String id = Base64.getUrlEncoder().withoutPadding().encodeToString(b);
        return store.putPass(id, p, auth.passTtl()).thenReturn(id);
    }

    /** The pass, used up; empty if unknown, used or expired. */
    Mono<Pass> take(String id) {
        return id == null || id.isBlank() || id.length() > 64 ? Mono.empty() : store.takePass(id);
    }

    Duration passTtl() {
        return auth.passTtl();
    }

    boolean enabled() {
        return auth.passes();
    }
}
