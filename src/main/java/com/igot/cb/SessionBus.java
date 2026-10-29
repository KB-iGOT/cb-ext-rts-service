package com.igot.cb;

import java.util.function.Consumer;
import reactor.core.publisher.Mono;

/**
 * Carries one survey event to every pod that holds sockets of that survey; each pod then writes it to its own
 * sockets. rts.store=redis: Redis pub/sub, one channel per survey (rts:room:{code}). memory: delivered in-process.
 * One message per event, not per person.
 */
public interface SessionBus {

    /**
     * target: ALL, PARTICIPANTS, COORDINATORS. pushId: set on QUESTION_PUSHED (pods count delivery).
     * closeAfter: sockets are closed after this frame (SESSION_ENDED).
     */
    record Event(String code, String event, String target, String pushId, boolean closeAfter, byte[] frame) {
    }

    /** Called with every event of a survey this pod is subscribed to. Set once by SessionManager. */
    void onEvent(Consumer<Event> listener);

    /** This pod now has a socket of the survey: completes when its events will reach this pod. */
    Mono<Void> subscribe(String code);

    /** The last socket of the survey on this pod has gone. */
    void unsubscribe(String code);

    Mono<Void> publish(Event event);
}
