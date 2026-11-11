package com.igot.cb;

import com.igot.cb.SessionView.Delivery;
import com.igot.cb.SessionView.PushResult;
import com.igot.cb.SessionView.PushView;
import com.igot.cb.SessionView.Question;
import com.igot.cb.SessionView.StartResult;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import reactor.core.publisher.Mono;

/**
 * Live state of every survey, keyed by RTS code, in Redis: shared by all pods (any pod serves anyone, state
 * survives a pod restart).
 * Every change is atomic per survey. Mono.empty() = "condition not met, nothing changed".
 */
public interface SessionStore {

    Mono<SessionView> view(String code);

    /** WAITING/ENDED -> LIVE with a new roomId. created=false if it was already running. */
    Mono<StartResult> start(String code, String coordinatorId);

    /** LIVE -> PAUSED (PC network lost). */
    Mono<SessionView> pause(String code, long now);

    /** PAUSED -> LIVE (PC back). */
    Mono<SessionView> resume(String code);

    /** LIVE/PAUSED -> ENDED; returns the state just before. */
    Mono<SessionView> end(String code);

    /** Errors with {@link RtsError} NOT_LIVE unless LIVE. Closes answers on the previous question. */
    Mono<PushResult> push(String code, Question question, Long endsAt);

    /** Opens or closes answers on the current question. Empty if pushId is not current or the survey ended. */
    Mono<PushView> setOpen(String code, String pushId, boolean open, Long endsAt);

    /** Question timer: closes answers only if still open with the same deadline. */
    Mono<Boolean> expire(String code, String pushId, long endsAt);

    /** Sets "shown" or "revealed" on the current question. Empty if pushId is not current. */
    Mono<PushView> mark(String code, String pushId, String field);

    /**
     * RECORDED, ALREADY_ANSWERED, ANSWERS_STOPPED, NOT_CURRENT_QUESTION or NO_ACTIVE_QUESTION. text (free-text
     * questions) is kept with the learner's name so a PC who reconnects sees the answers again.
     */
    Mono<String> submit(String code, String pushId, String userId, String name, List<Integer> choice,
            List<String> words, String text, long now);

    /** Free-text answers to pushId, oldest first. */
    Mono<List<SessionView.TextAnswer>> textAnswers(String code, String pushId);

    Mono<Boolean> hasAnswered(String code, String pushId, String userId);

    Mono<Long> answered(String code, String pushId);

    /** choice: total + counts[]; word cloud: total + top words; text: total. */
    Mono<Map<String, Object>> results(String code, PushView push);

    /** Adds a learner socket and remembers the learner's name. Returns distinct learners connected, or -1 when full. */
    Mono<Long> join(String code, String userId, String name, int max);

    /** Every learner who joined the survey, by name, with whether they are connected now (the PC's list). */
    Mono<List<SessionView.Participant>> participants(String code);

    /** Removes a learner socket. stillConnected: the learner has another socket (second tab, or reconnected). */
    Mono<SessionView.Left> leave(String code, String userId);

    Mono<Long> connected(String code);

    /** A PC socket connected / disconnected (any pod). Return: PC sockets connected now. */
    Mono<Long> pcJoin(String code, String connId);

    Mono<Long> pcLeave(String code, String connId);

    Mono<Long> pcCount(String code);

    /** Each pod adds what it wrote for a push; the PC's pod reads the total. */
    Mono<Void> addDelivery(String code, String pushId, long delivered, Collection<String> failedUserIds);

    Mono<Delivery> delivery(String code, String pushId);
}
