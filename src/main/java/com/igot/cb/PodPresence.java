package com.igot.cb;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

/**
 * What another pod needs to clean up after this pod if it crashes (LLD "to build": pod heartbeat, PC presence).
 * Every pod heartbeats and records its open sockets; a pod whose heartbeat stops is declared dead by any other
 * pod, which then runs the "left" step for each of its sockets (PARTICIPANT_LEFT, or pausing the survey when it
 * held the last PC socket). Paused surveys are listed too, so any pod can end one when the PC wait runs out,
 * not only the pod that paused it.
 *
 * <p>Each socket record is removed exactly once, by its own pod (socket closed) or by a sweeping pod (pod dead):
 * whoever removes it runs the leave, so a socket is never counted out twice.
 */
public interface PodPresence {

    /** A socket of a dead pod, claimed by this pod to run its leave. */
    record Orphan(String connId, String code, String userId, String name, boolean coordinator) {
    }

    /** Records a socket of this pod (before it is counted in the survey). */
    Mono<Void> add(Conn c);

    /** True when this pod still held the record, so it runs the leave; false when a sweeping pod already did. */
    Mono<Boolean> remove(Conn c);

    /** Refreshes this pod's heartbeat. True when another pod had declared this pod dead (its sockets are cleaned up). */
    Mono<Boolean> heartbeat();

    /** Pods with no heartbeat since cutoff, each claimed by at most one caller, then their sockets (claimed one by one). */
    Flux<Orphan> claimDeadPods(long cutoff);

    /** The survey paused at pausedAt (PC lost). */
    Mono<Void> paused(String code, long pausedAt);

    /** Paused surveys whose pause began before cutoff: code and pausedAt. Listed entries are removed. */
    Flux<Tuple2<String, Long>> takePausedBefore(long cutoff);
}
