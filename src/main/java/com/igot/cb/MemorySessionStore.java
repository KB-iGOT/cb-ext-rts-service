package com.igot.cb;

import com.igot.cb.SessionView.Delivery;
import com.igot.cb.SessionView.PushResult;
import com.igot.cb.SessionView.PushView;
import com.igot.cb.SessionView.Question;
import com.igot.cb.SessionView.StartResult;
import com.igot.cb.SessionView.Status;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * Survey state in this pod's memory (rts.store=memory): one pod, no Redis. Used to measure what one
 * pod can carry. Same rules as {@link RedisSessionStore}; every change runs under the survey's lock. Only the
 * current question's answers are kept.
 */
@Component
@ConditionalOnProperty(name = "rts.store", havingValue = "memory")
public class MemorySessionStore implements SessionStore {

    private final Map<String, State> states = new ConcurrentHashMap<>();

    private static final class State {
        Status status = Status.WAITING;
        String roomId;
        String coordinatorId;
        long pausedAt;
        int pushCount;
        String pushId;
        Question q;
        boolean open;
        Long endsAt;
        boolean shown;
        boolean revealed;
        final Set<String> answered = new HashSet<>();
        long total;
        long[] counts = new long[0];
        final Map<String, Long> words = new HashMap<>();
        long delivered;
        final Set<String> failed = new LinkedHashSet<>();
        final Map<String, Integer> participants = new HashMap<>();
        final Set<String> pcs = new HashSet<>();

        boolean running() {
            return status == Status.LIVE || status == Status.PAUSED;
        }

        SessionView view(String code) {
            PushView p = pushId == null ? null : new PushView(pushId, q, open, endsAt, shown, revealed);
            return new SessionView(code, roomId, status, coordinatorId, pausedAt, pushCount, p);
        }
    }

    private <T> Mono<T> with(String code, Function<State, T> f) {
        return Mono.fromCallable(() -> {
            State s = states.computeIfAbsent(code, k -> new State());
            synchronized (s) {
                return f.apply(s);
            }
        });
    }

    @Override
    public Mono<SessionView> view(String code) {
        return with(code, s -> s.view(code));
    }

    @Override
    public Mono<StartResult> start(String code, String coordinatorId) {
        return with(code, s -> {
            boolean created = !s.running();
            if (created) {
                s.status = Status.LIVE;
                s.roomId = "R-" + UUID.randomUUID().toString().substring(0, 8);
                s.coordinatorId = coordinatorId;
                s.pausedAt = 0;
                s.pushCount = 0;
                s.pushId = null;
                s.q = null;
            }
            return new StartResult(s.view(code), created);
        });
    }

    @Override
    public Mono<SessionView> pause(String code, long now) {
        return with(code, s -> {
            if (s.status != Status.LIVE) {
                return null;
            }
            s.status = Status.PAUSED;
            s.pausedAt = now;
            return s.view(code);
        });
    }

    @Override
    public Mono<SessionView> resume(String code) {
        return with(code, s -> {
            if (s.status != Status.PAUSED) {
                return null;
            }
            s.status = Status.LIVE;
            s.pausedAt = 0;
            return s.view(code);
        });
    }

    @Override
    public Mono<SessionView> end(String code) {
        return with(code, s -> {
            if (!s.running()) {
                return null;
            }
            SessionView before = s.view(code);
            s.status = Status.ENDED;
            s.open = false;
            return before;
        });
    }

    @Override
    public Mono<PushResult> push(String code, Question q, Long endsAt) {
        return with(code, s -> {
            if (s.status != Status.LIVE) {
                throw new RtsError("NOT_LIVE", "Survey is not live");
            }
            String closed = s.pushId != null && s.open ? s.pushId : null;
            s.pushCount++;
            s.pushId = "P-" + s.pushCount;
            s.q = q;
            s.open = true;
            s.endsAt = endsAt;
            s.shown = false;
            s.revealed = false;
            s.answered.clear();
            s.total = 0;
            s.counts = new long[q.optionCount()];
            s.words.clear();
            s.delivered = 0;
            s.failed.clear();
            return new PushResult(s.roomId, new PushView(s.pushId, q, true, endsAt, false, false), closed);
        });
    }

    @Override
    public Mono<PushView> setOpen(String code, String pushId, boolean open, Long endsAt) {
        return with(code, s -> {
            if (!pushId.equals(s.pushId) || s.status == Status.ENDED) {
                return null;
            }
            s.open = open;
            s.endsAt = endsAt;
            return s.view(code).current();
        });
    }

    @Override
    public Mono<Boolean> expire(String code, String pushId, long endsAt) {
        return with(code, s -> {
            if (!pushId.equals(s.pushId) || !s.open || s.endsAt == null || s.endsAt != endsAt) {
                return false;
            }
            s.open = false;
            return true;
        });
    }

    @Override
    public Mono<PushView> mark(String code, String pushId, String field) {
        return with(code, s -> {
            if (!pushId.equals(s.pushId)) {
                return null;
            }
            if ("revealed".equals(field)) {
                s.revealed = true;
            } else {
                s.shown = true;
            }
            return s.view(code).current();
        });
    }

    @Override
    public Mono<String> submit(String code, String pushId, String userId, List<Integer> choice, List<String> words,
            long now) {
        return with(code, s -> {
            if (s.pushId == null) {
                return "NO_ACTIVE_QUESTION";
            }
            if (!s.pushId.equals(pushId)) {
                return "NOT_CURRENT_QUESTION";
            }
            if (!s.open || s.status != Status.LIVE || (s.endsAt != null && now > s.endsAt)) {
                return "ANSWERS_STOPPED";
            }
            if (!s.answered.add(userId)) {
                return "ALREADY_ANSWERED";
            }
            s.total++;
            if (choice != null) {
                choice.forEach(i -> s.counts[i]++);
            }
            if (words != null) {
                words.forEach(w -> s.words.merge(w, 1L, Long::sum));
            }
            return "RECORDED";
        });
    }

    @Override
    public Mono<Boolean> hasAnswered(String code, String pushId, String userId) {
        return with(code, s -> pushId.equals(s.pushId) && s.answered.contains(userId));
    }

    @Override
    public Mono<Long> answered(String code, String pushId) {
        return with(code, s -> pushId.equals(s.pushId) ? s.total : 0L);
    }

    @Override
    public Mono<Map<String, Object>> results(String code, PushView push) {
        return with(code, s -> {
            if (!push.pushId().equals(s.pushId)) {
                return Results.text(push.pushId(), 0);
            }
            if (s.q.choice()) {
                return Results.choice(s.pushId, s.total, s.counts.clone());
            }
            if ("WORD_CLOUD".equals(s.q.type())) {
                List<List<Object>> top = s.words.entrySet().stream()
                        .sorted(Map.Entry.<String, Long>comparingByValue(Comparator.reverseOrder()))
                        .limit(Results.TOP_WORDS).map(e -> List.<Object>of(e.getKey(), e.getValue())).toList();
                return Results.words(s.pushId, s.total, top);
            }
            return Results.text(s.pushId, s.total);
        });
    }

    @Override
    public Mono<Long> join(String code, String userId, int max) {
        return with(code, s -> {
            if (!s.participants.containsKey(userId) && s.participants.size() >= max) {
                return -1L;
            }
            s.participants.merge(userId, 1, Integer::sum);
            return (long) s.participants.size();
        });
    }

    @Override
    public Mono<Long> leave(String code, String userId) {
        return with(code, s -> {
            s.participants.computeIfPresent(userId, (k, n) -> n > 1 ? n - 1 : null);
            return (long) s.participants.size();
        });
    }

    @Override
    public Mono<Long> connected(String code) {
        return with(code, s -> (long) s.participants.size());
    }

    @Override
    public Mono<Long> pcJoin(String code, String connId) {
        return with(code, s -> {
            s.pcs.add(connId);
            return (long) s.pcs.size();
        });
    }

    @Override
    public Mono<Long> pcLeave(String code, String connId) {
        return with(code, s -> {
            s.pcs.remove(connId);
            return (long) s.pcs.size();
        });
    }

    @Override
    public Mono<Long> pcCount(String code) {
        return with(code, s -> (long) s.pcs.size());
    }

    @Override
    public Mono<Void> addDelivery(String code, String pushId, long delivered, Collection<String> failed) {
        return with(code, s -> {
            if (pushId.equals(s.pushId)) {
                s.delivered += delivered;
                s.failed.addAll(failed);
            }
            return Boolean.TRUE;
        }).then();
    }

    @Override
    public Mono<Delivery> delivery(String code, String pushId) {
        return with(code, s -> pushId.equals(s.pushId) ? new Delivery(s.delivered, new ArrayList<>(s.failed))
                : new Delivery(0, List.of()));
    }

    /** Drops a survey that nobody is connected to and that is not running (a paused one waits for its PC). */
    @Override
    public void forget(String code) {
        states.computeIfPresent(code, (k, s) -> {
            synchronized (s) {
                return s.participants.isEmpty() && s.pcs.isEmpty() && !s.running() ? null : s;
            }
        });
    }

    int size() {
        return states.size();
    }
}
