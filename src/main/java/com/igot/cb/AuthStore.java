package com.igot.cb;

import java.time.Duration;
import java.util.Set;
import reactor.core.publisher.Mono;

/**
 * Join passes and session rosters, in Redis, so a pass issued by one pod opens a socket on any pod.
 */
public interface AuthStore {

    /** Who a socket is, decided by the server when the pass was issued (LLD §3.1). role: COORDINATOR or PARTICIPANT. */
    record Pass(String userId, String name, String code, String role) {

        boolean coordinator() {
            return "COORDINATOR".equals(role);
        }
    }

    /**
     * Who may join a session: the PCs, and the learners (anyLearner: anyone with a valid identity). Stands in for
     * the join API's checks in the LLD (code exists and RTS is on; enrolled in the batch, or PC of the batch).
     */
    record Roster(Set<String> coordinators, Set<String> participants, boolean anyLearner) {

        boolean mayCoordinate(String userId) {
            return coordinators.contains(userId);
        }

        boolean mayParticipate(String userId) {
            return anyLearner || participants.contains(userId);
        }
    }

    Mono<Void> putPass(String id, Pass pass, Duration ttl);

    /** The pass, removed in the same step (single use); empty if unknown, used or expired. */
    Mono<Pass> takePass(String id);

    /** Empty if the session has no roster. */
    Mono<Roster> roster(String code);

    Mono<Void> putRoster(String code, Roster roster);

    /** Demo: creates the roster with userId as PC and anyone as learner, if the code has none. Returns the roster. */
    Mono<Roster> claimRoster(String code, String userId);
}
