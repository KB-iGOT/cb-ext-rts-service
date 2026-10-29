package com.igot.cb;

import static com.igot.cb.Frames.map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.SessionBus.Event;
import com.igot.cb.SessionView.PushView;
import com.igot.cb.SessionView.Question;
import com.igot.cb.SessionView.Status;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.CloseStatus;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * The survey protocol (HLD §9). The survey state is in the {@link SessionStore} (Redis: shared by all pods);
 * this pod only holds its own sockets. A change is made in the store first, then announced on the
 * {@link SessionBus}; every pod with sockets of that survey writes the event to them. So any pod can serve
 * any PC or learner, and a pod restart does not lose a survey.
 */
@Component
public class SessionManager {

    private static final Logger log = LoggerFactory.getLogger(SessionManager.class);
    private static final Set<String> TYPES = Set.of("SINGLE", "MULTI", "TEXT", "WORD_CLOUD");
    static final String ALL = "ALL";
    static final String PARTICIPANTS = "PARTICIPANTS";
    static final String COORDINATORS = "COORDINATORS";

    private final SessionStore store;
    private final SessionBus bus;
    private final Frames frames;
    private final ObjectMapper mapper;
    private final Stats stats;
    private final RtsSettings.Room room;
    private final RtsSettings.Ws ws;
    /** This pod's name: every socket held here is on this pod (shown to clients and in logs). */
    private final String pod;
    private final Scheduler timers = Schedulers.newParallel("rts-timer", 2, true);
    private final List<Disposable> loops = new ArrayList<>();

    /** This pod's sockets by RTS code. */
    private final Map<String, Set<Conn>> local = new ConcurrentHashMap<>();
    /** Current question per survey, for answer validation without a store read (from QUESTION_PUSHED). */
    private final Map<String, CachedPush> pushCache = new ConcurrentHashMap<>();
    /** Last known status per survey with sockets here (for /stats only). */
    private final Map<String, Status> statusCache = new ConcurrentHashMap<>();

    private record CachedPush(String pushId, Question question) {
    }

    public SessionManager(SessionStore store, SessionBus bus, Frames frames, ObjectMapper mapper, Stats stats,
            RtsSettings settings, @Value("${pod.name}") String pod) {
        this.store = store;
        this.bus = bus;
        this.frames = frames;
        this.mapper = mapper;
        this.stats = stats;
        this.room = settings.room();
        this.ws = settings.ws();
        this.pod = pod;
        bus.onEvent(this::deliver);
        stats.sessionSummary(this::summary);
        long live = room.liveStatsInterval().toMillis();
        loops.add(timers.schedulePeriodically(this::liveStatsTick, live, live, TimeUnit.MILLISECONDS));
        long ping = ws.pingInterval().toMillis();
        loops.add(timers.schedulePeriodically(this::pingSweep, ping, ping, TimeUnit.MILLISECONDS));
        log.info("Session store: {} (pod {})", store.getClass().getSimpleName(), pod);
    }

    @PreDestroy
    void stop() {
        loops.forEach(Disposable::dispose);
        timers.dispose();
    }

    /** Pod shutting down: close every socket with 1001 so clients reconnect to another pod (state is kept). */
    void closeAll() {
        int n = 0;
        for (Set<Conn> set : local.values()) {
            for (Conn c : set) {
                c.ended = true;
                c.close(CloseStatus.GOING_AWAY);
                n++;
            }
        }
        log.info("Shutdown: closed {} sockets", n);
    }

    byte[] limitError() {
        return frames.error(null, "PARTICIPANT_LIMIT_REACHED", "This survey already has the maximum participants");
    }

    // ================================================================ connect / disconnect

    /** Counts the socket in the survey (false = refused, participant limit) and subscribes this pod to it. */
    Mono<Boolean> join(Conn c) {
        Mono<Boolean> admit = c.legacy ? Mono.just(true)
                : c.coordinator ? store.pcJoin(c.code, c.connId).thenReturn(true)
                : store.join(c.code, c.userId, room.maxParticipants()).map(n -> n >= 0);
        return admit.flatMap(ok -> {
            if (!ok) {
                return Mono.just(false);
            }
            c.joined = true;
            local.computeIfAbsent(c.code, k -> ConcurrentHashMap.newKeySet()).add(c);
            return (c.legacy ? Mono.<Void>empty() : bus.subscribe(c.code)).thenReturn(true);
        });
    }

    /** SESSION_STATE to the new socket. A PC joining starts the survey or resumes a paused one. */
    Mono<Void> onJoined(Conn c) {
        if (c.legacy) {
            c.send(frames.raw(map("type", "CONNECTED", "pod", pod)));
            return Mono.empty();
        }
        String code = c.code;
        if (c.coordinator) {
            return store.view(code).flatMap(v -> {
                if (v.status() == Status.PAUSED) {
                    return store.resume(code).map(r -> new Step(r, "COORDINATOR_BACK")).defaultIfEmpty(new Step(v, null));
                }
                if (room.startOnPcJoin() && !v.running()) {
                    return store.start(code, c.userId)
                            .map(s -> new Step(s.view(), s.created() ? "SESSION_STARTED" : null));
                }
                return Mono.just(new Step(v, null));
            }).flatMap(step -> store.connected(code).flatMap(n -> {
                SessionView v = step.view();
                statusCache.put(code, v.status());
                c.send(state(c, v, n, null));
                if ("SESSION_STARTED".equals(step.event())) {
                    log.info("Survey {} started as room {} when PC {} joined", code, v.roomId(), c.userId);
                    return publish(code, "SESSION_STARTED", PARTICIPANTS, v.roomId(), map("roomId", v.roomId()));
                }
                if ("COORDINATOR_BACK".equals(step.event())) {
                    log.info("Coordinator {} back in {}", c.userId, code);
                    return publish(code, "COORDINATOR_BACK", PARTICIPANTS, v.roomId(), map());
                }
                return Mono.<Void>empty();
            }));
        }
        return Mono.zip(store.view(code), store.connected(code)).flatMap(t -> {
            SessionView v = t.getT1();
            long n = t.getT2();
            statusCache.put(code, v.status());
            PushView p = v.running() ? v.current() : null;
            Mono<Boolean> answered = p == null ? Mono.just(false) : store.hasAnswered(code, p.pushId(), c.userId);
            return answered.flatMap(a -> {
                c.send(state(c, v, n, a ? "RECORDED" : null));
                return v.running() ? publish(code, "PARTICIPANT_JOINED", COORDINATORS, v.roomId(),
                        map("userId", c.userId, "name", c.name, "pod", pod, "connected", n)) : Mono.<Void>empty();
            });
        });
    }

    private record Step(SessionView view, String event) {
    }

    private byte[] state(Conn c, SessionView v, long connected, String myAnswer) {
        PushView p = v.running() ? v.current() : null;
        return frames.event("SESSION_STATE", v.roomId(), map("status", v.status(),
                "role", c.coordinator ? "COORDINATOR" : "PARTICIPANT", "roomId", v.roomId(), "pod", pod,
                "connected", connected,
                "currentPush", p == null ? null : c.coordinator ? p.forCoordinator() : p.forLearners(),
                "myAnswer", myAnswer == null ? null : map("status", myAnswer)));
    }

    /** The socket closed. A PC that drops (no LEAVE/END) pauses the survey when no other PC socket is left. */
    Mono<Void> onLeft(Conn c, String reason) {
        boolean lastHere = removeLocal(c);
        if (!c.joined || c.legacy) {
            return Mono.empty();
        }
        String code = c.code;
        bus.unsubscribe(code);
        Mono<Void> work;
        if (c.coordinator) {
            work = store.pcLeave(code, c.connId).flatMap(remaining -> {
                if (remaining > 0 || c.explicitLeave || c.ended) {
                    return Mono.<Void>empty();
                }
                long now = System.currentTimeMillis();
                return store.pause(code, now).flatMap(v -> {
                    log.info("Coordinator {} lost in {}; survey paused", c.userId, code);
                    statusCache.put(code, Status.PAUSED);
                    schedulePcTimeout(code, now);
                    long waitSec = room.pcWaitTimeout().toSeconds();
                    return publish(code, "COORDINATOR_AWAY", PARTICIPANTS, v.roomId(),
                            map("waitSec", waitSec > 0 ? waitSec : null));
                });
            });
        } else {
            work = store.leave(code, c.userId).flatMap(n -> c.ended ? Mono.<Void>empty()
                    : store.view(code).filter(SessionView::running).flatMap(v -> publish(code, "PARTICIPANT_LEFT",
                            COORDINATORS, v.roomId(), map("userId", c.userId, "name", c.name, "reason", reason,
                                    "connected", n))));
        }
        return work.then(Mono.fromRunnable(() -> {
            if (lastHere) {
                store.forget(code);
            }
        }));
    }

    /** True when it was this pod's last socket of the survey. */
    private boolean removeLocal(Conn c) {
        boolean[] last = {false};
        local.computeIfPresent(c.code, (k, set) -> {
            set.remove(c);
            last[0] = set.isEmpty();
            return last[0] ? null : set;
        });
        if (last[0]) {
            pushCache.remove(c.code);
            statusCache.remove(c.code);
        }
        return last[0];
    }

    // ================================================================ inbound frames

    /** One frame from a socket. Frames of one socket are handled in order (the caller waits for this Mono). */
    Mono<Void> onFrame(Conn c, String text) {
        stats.framesIn.increment();
        JsonNode msg;
        try {
            msg = mapper.readTree(text);
        } catch (Exception e) {
            c.send(frames.error(null, "BAD_MESSAGE", "Not valid JSON"));
            return Mono.empty();
        }
        if (!msg.has("event") && msg.has("type")) {
            legacy(c, msg, text);
            return Mono.empty();
        }
        String event = msg.path("event").asText("");
        String requestId = msg.hasNonNull("requestId") ? msg.get("requestId").asText() : null;
        JsonNode data = msg.path("data");
        Mono<Void> work = switch (event) {
            case "SUBMIT_ANSWER" -> c.coordinator ? forbidden(c, event, requestId, "Participants only")
                    : submit(c, requestId, data);
            case "LEAVE" -> leave(c, requestId);
            case "START_SESSION" -> pcOnly(c, event, requestId, () -> start(c, requestId));
            case "PUSH_QUESTION" -> pcOnly(c, event, requestId, () -> push(c, requestId, data));
            case "STOP_ANSWERS" -> pcOnly(c, event, requestId, () -> setOpen(c, event, requestId, data, false));
            case "RESUME_ANSWERS" -> pcOnly(c, event, requestId, () -> setOpen(c, event, requestId, data, true));
            case "SHOW_RESULTS" -> pcOnly(c, event, requestId, () -> showResults(c, requestId, data));
            case "REVEAL_ANSWER" -> pcOnly(c, event, requestId, () -> reveal(c, requestId, data));
            case "END_SESSION" -> pcOnly(c, event, requestId, () -> endByCoordinator(c, requestId));
            default -> {
                c.send(frames.error(requestId, "BAD_MESSAGE", "Unknown event '" + event + "'"));
                yield Mono.empty();
            }
        };
        return work.onErrorResume(RtsError.class, e -> {
            c.send(frames.fail(event, requestId, null, e.code, e.getMessage()));
            return Mono.empty();
        }).onErrorResume(e -> {
            log.warn("{} from {} in {} failed: {}", event, c.userId, c.code, e.toString());
            c.send(frames.fail(event, requestId, null, "TRY_AGAIN", "Temporary problem, please retry"));
            return Mono.empty();
        });
    }

    private Mono<Void> pcOnly(Conn c, String event, String requestId, java.util.function.Supplier<Mono<Void>> action) {
        return c.coordinator ? Mono.defer(action) : forbidden(c, event, requestId, "Coordinator only");
    }

    private Mono<Void> forbidden(Conn c, String event, String requestId, String message) {
        c.send(frames.fail(event, requestId, null, "FORBIDDEN", message));
        return Mono.empty();
    }

    /** Old protocol: a trainer's {"type":"QUESTION",...} is relayed as-is to this pod's legacy sockets of the room. */
    private void legacy(Conn c, JsonNode msg, String text) {
        String type = msg.path("type").asText();
        if (!c.coordinator || !"QUESTION".equals(type)) {
            log.info("IGNORED pod={} client={} type={}", pod, c.userId, type);
            return;
        }
        Set<Conn> set = local.getOrDefault(c.code, Set.of());
        log.info("PUBLISH pod={} trainer={} room={} eventId={} localRoomSize={}", pod, c.userId, c.code,
                msg.path("eventId").asText(), set.size());
        byte[] frame = text.getBytes(StandardCharsets.UTF_8);
        long t0 = System.nanoTime();
        for (Conn o : set) {
            if (o.legacy) {
                o.send(frame);
            }
        }
        stats.fanout(System.nanoTime() - t0);
    }

    // ================================================================ coordinator

    private Mono<Void> start(Conn c, String requestId) {
        return store.start(c.code, c.userId).flatMap(r -> {
            SessionView v = r.view();
            statusCache.put(c.code, v.status());
            c.send(frames.ok("START_SESSION", requestId, v.roomId(), map("roomId", v.roomId(), "status", v.status(),
                    "created", r.created())));
            if (!r.created()) {
                return Mono.empty();
            }
            log.info("Survey {} started as room {} by {}", c.code, v.roomId(), c.userId);
            return publish(c.code, "SESSION_STARTED", PARTICIPANTS, v.roomId(), map("roomId", v.roomId()));
        });
    }

    private Mono<Void> push(Conn c, String requestId, JsonNode data) {
        JsonNode qn = data.path("question");
        String invalid = validateQuestion(qn);
        if (invalid != null) {
            return Mono.error(new RtsError("BAD_MESSAGE", invalid));
        }
        Question q0;
        try {
            q0 = mapper.treeToValue(qn, Question.class);
        } catch (Exception e) {
            return Mono.error(new RtsError("BAD_MESSAGE", "Malformed question"));
        }
        int maxChars = Math.min(q0.maxChars() == null ? room.maxTextChars() : q0.maxChars(), room.maxTextChars());
        String questionId = data.hasNonNull("questionId") ? data.get("questionId").asText() : q0.questionId();
        Question q = new Question(questionId, q0.type(), q0.text(), q0.options(), q0.correct(), maxChars);
        int timerSec = data.path("timerSec").asInt(0);
        Long endsAt = timerSec > 0 ? System.currentTimeMillis() + timerSec * 1000L : null;
        return store.push(c.code, q, endsAt).flatMap(r -> {
            PushView p = r.push();
            stats.pushes.increment();
            c.send(frames.ok("PUSH_QUESTION", requestId, r.roomId(), map("pushId", p.pushId(),
                    "questionId", questionId, "endsAt", endsAt)));
            Mono<Void> closePrev = r.closedPushId() == null ? Mono.empty()
                    : publish(c.code, "ANSWERS_STOPPED", ALL, r.roomId(),
                            map("pushId", r.closedPushId(), "reason", "NEXT_QUESTION"));
            Event pushed = new Event(c.code, "QUESTION_PUSHED", PARTICIPANTS, p.pushId(), false,
                    frames.event("QUESTION_PUSHED", r.roomId(), p.forLearners()));
            return closePrev.then(bus.publish(pushed)).then(Mono.fromRunnable(() -> {
                timers.schedule(() -> deliveryReport(c, r.roomId(), p.pushId()),
                        room.deliveryReportDelay().toMillis(), TimeUnit.MILLISECONDS);
                if (endsAt != null) {
                    scheduleQuestionTimer(c.code, r.roomId(), p.pushId(), endsAt);
                }
            }));
        });
    }

    private void deliveryReport(Conn pc, String roomId, String pushId) {
        if (!pc.isOpen()) {
            return;
        }
        store.delivery(pc.code, pushId).subscribe(d -> pc.send(frames.event("PUSH_DELIVERY", roomId,
                map("pushId", pushId, "delivered", d.delivered(),
                        "failed", d.failedUserIds().stream().map(u -> map("userId", u)).toList()))),
                e -> log.warn("Delivery report for {} {} failed: {}", pc.code, pushId, e.toString()));
    }

    private Mono<Void> setOpen(Conn c, String event, String requestId, JsonNode data, boolean open) {
        String pushId = data.path("pushId").asText("");
        int timerSec = data.path("timerSec").asInt(0);
        Long endsAt = open && timerSec > 0 ? System.currentTimeMillis() + timerSec * 1000L : null;
        return store.setOpen(c.code, pushId, open, endsAt)
                .switchIfEmpty(Mono.defer(() -> notCurrent(c.code)))
                .flatMap(p -> store.view(c.code).flatMap(v -> {
                    if (open) {
                        c.send(frames.ok(event, requestId, v.roomId(), map("pushId", pushId, "endsAt", endsAt)));
                        if (endsAt != null) {
                            scheduleQuestionTimer(c.code, v.roomId(), pushId, endsAt);
                        }
                        return publish(c.code, "ANSWERS_RESUMED", ALL, v.roomId(),
                                map("pushId", pushId, "endsAt", endsAt));
                    }
                    return store.answered(c.code, pushId).flatMap(n -> {
                        c.send(frames.ok(event, requestId, v.roomId(), map("pushId", pushId, "answered", n)));
                        return publish(c.code, "ANSWERS_STOPPED", ALL, v.roomId(),
                                map("pushId", pushId, "reason", "BY_COORDINATOR"));
                    });
                }));
    }

    private Mono<Void> showResults(Conn c, String requestId, JsonNode data) {
        String pushId = data.path("pushId").asText("");
        return store.mark(c.code, pushId, "shown").switchIfEmpty(Mono.defer(() -> notCurrent(c.code)))
                .flatMap(p -> Mono.zip(store.view(c.code), store.results(c.code, p)).flatMap(t -> {
                    c.send(frames.ok("SHOW_RESULTS", requestId, t.getT1().roomId(), map("pushId", pushId)));
                    return publish(c.code, "RESULTS_SHOWN", ALL, t.getT1().roomId(), t.getT2());
                }));
    }

    private Mono<Void> reveal(Conn c, String requestId, JsonNode data) {
        String pushId = data.path("pushId").asText("");
        return store.view(c.code).flatMap(v -> {
            if (!v.running()) {
                return Mono.error(new RtsError("NOT_LIVE", "Survey is not live"));
            }
            if (v.current() == null || !v.current().pushId().equals(pushId)) {
                return Mono.error(new RtsError("NOT_CURRENT_QUESTION", "pushId is not the current question"));
            }
            if (!v.current().question().graded()) {
                return Mono.error(new RtsError("NOT_GRADED", "This question has no correct answer"));
            }
            return store.mark(c.code, pushId, "revealed").switchIfEmpty(Mono.defer(() -> notCurrent(c.code)))
                    .flatMap(p -> {
                        c.send(frames.ok("REVEAL_ANSWER", requestId, v.roomId(), map("pushId", pushId)));
                        return publish(c.code, "ANSWER_REVEALED", ALL, v.roomId(),
                                map("pushId", pushId, "correct", p.question().correct()));
                    });
        });
    }

    /** NOT_LIVE if the survey is not running, else NOT_CURRENT_QUESTION. */
    private Mono<PushView> notCurrent(String code) {
        return store.view(code).flatMap(v -> Mono.error(v.running()
                ? new RtsError("NOT_CURRENT_QUESTION", "pushId is not the current question")
                : new RtsError("NOT_LIVE", "Survey is not live")));
    }

    private Mono<Void> endByCoordinator(Conn c, String requestId) {
        c.explicitLeave = true;
        return Mono.zip(store.view(c.code), store.connected(c.code)).flatMap(t -> {
            SessionView v = t.getT1();
            c.send(frames.ok("END_SESSION", requestId, v.roomId(), map("roomId", v.roomId(),
                    "pushed", v.pushCount(), "participants", t.getT2())));
            return end(c.code, "ENDED_BY_COORDINATOR");
        });
    }

    private Mono<Void> leave(Conn c, String requestId) {
        c.explicitLeave = true;
        c.send(frames.ok("LEAVE", requestId, null, map()));
        Mono<Void> work = c.coordinator
                ? store.pcCount(c.code).flatMap(n -> n <= 1 ? end(c.code, "COORDINATOR_LEFT") : Mono.<Void>empty())
                : Mono.empty();
        return work.then(Mono.fromRunnable(c::finish));
    }

    /** LIVE/PAUSED -> ENDED; every pod sends SESSION_ENDED to its sockets of the survey and closes them. */
    private Mono<Void> end(String code, String reason) {
        return store.end(code).flatMap(before -> {
            log.info("Survey {} room {} ended: {}", code, before.roomId(), reason);
            statusCache.put(code, Status.ENDED);
            return bus.publish(new Event(code, "SESSION_ENDED", ALL, null, true,
                    frames.event("SESSION_ENDED", before.roomId(), map("reason", reason))));
        });
    }

    // ================================================================ learner

    private Mono<Void> submit(Conn c, String requestId, JsonNode data) {
        String pushId = data.path("pushId").asText(null);
        if (pushId == null) {
            c.send(frames.submitResult(requestId, null, null, "INVALID_ANSWER", "pushId is required"));
            return Mono.empty();
        }
        return currentQuestion(c.code, pushId).flatMap(q -> {
            List<Integer> choice = new ArrayList<>();
            List<String> words = new ArrayList<>();
            if (!validAnswer(q, data, choice, words)) {
                return reply(c, requestId, pushId, "INVALID_ANSWER");
            }
            return store.submit(c.code, pushId, c.userId, choice, words, System.currentTimeMillis())
                    .flatMap(outcome -> {
                        Mono<Void> r = reply(c, requestId, pushId, outcome);
                        if ("RECORDED".equals(outcome) && "TEXT".equals(q.type())) {
                            return r.then(publish(c.code, "LIVE_TEXT_ANSWERS", COORDINATORS, null,
                                    map("pushId", pushId, "items",
                                            List.of(map("name", c.name, "text", data.path("text").asText())))));
                        }
                        return r;
                    });
        }).onErrorResume(RtsError.class, e -> reply(c, requestId, pushId, e.code));
    }

    private Mono<Void> reply(Conn c, String requestId, String pushId, String outcome) {
        boolean recorded = "RECORDED".equals(outcome);
        (recorded ? stats.answersRecorded : stats.answersNotRecorded).increment();
        c.send(frames.submitResult(requestId, null, pushId, recorded ? null : outcome, recorded ? null : outcome));
        return Mono.empty();
    }

    /** The question of pushId, from this pod's cache or the store; errors with the reason it cannot be answered. */
    private Mono<Question> currentQuestion(String code, String pushId) {
        CachedPush cached = pushCache.get(code);
        if (cached != null && cached.pushId().equals(pushId)) {
            return Mono.just(cached.question());
        }
        return store.view(code).flatMap(v -> {
            PushView p = v.current();
            if (p == null) {
                return Mono.error(new RtsError("NO_ACTIVE_QUESTION", "No question"));
            }
            pushCache.put(code, new CachedPush(p.pushId(), p.question()));
            return p.pushId().equals(pushId) ? Mono.just(p.question())
                    : Mono.error(new RtsError("NOT_CURRENT_QUESTION", "Not the current question"));
        });
    }

    private static boolean validAnswer(Question q, JsonNode a, List<Integer> choice, List<String> words) {
        switch (q.type()) {
            case "SINGLE", "MULTI" -> {
                a.path("choice").forEach(n -> choice.add(n.asInt(-1)));
                return !choice.isEmpty() && !("SINGLE".equals(q.type()) && choice.size() != 1)
                        && new HashSet<>(choice).size() == choice.size()
                        && choice.stream().allMatch(i -> i >= 0 && i < q.optionCount());
            }
            case "TEXT" -> {
                String t = a.path("text").asText("");
                int max = q.maxChars() == null ? Integer.MAX_VALUE : q.maxChars();
                return !t.isBlank() && t.length() <= max;
            }
            default -> {
                a.path("words").forEach(n -> words.add(n.asText("").trim().toLowerCase()));
                return !words.isEmpty() && words.size() <= 3
                        && words.stream().noneMatch(w -> w.isEmpty() || w.length() > 30);
            }
        }
    }

    private static String validateQuestion(JsonNode q) {
        String type = q.path("type").asText("");
        if (!TYPES.contains(type) || q.path("text").asText("").isBlank()) {
            return "Question type (SINGLE, MULTI, TEXT, WORD_CLOUD) and text are required";
        }
        if (type.equals("SINGLE") || type.equals("MULTI")) {
            int n = q.path("options").size();
            if (n < 2 || n > 10) {
                return "Choice questions need 2-10 options";
            }
            for (JsonNode c : q.path("correct")) {
                if (c.asInt(-1) < 0 || c.asInt() >= n) {
                    return "Correct option out of range";
                }
            }
            if (type.equals("SINGLE") && q.path("correct").size() > 1) {
                return "Single choice has one correct option";
            }
        }
        return null;
    }

    // ================================================================ delivery to this pod's sockets

    private Mono<Void> publish(String code, String event, String target, String roomId, Object data) {
        return bus.publish(new Event(code, event, target, null, false, frames.event(event, roomId, data)));
    }

    /** An event of a survey this pod has sockets of: write it to the matching sockets. */
    private void deliver(Event e) {
        noteEvent(e);
        Set<Conn> set = local.get(e.code());
        if (set == null) {
            return;
        }
        long t0 = System.nanoTime();
        int ok = 0;
        List<String> failed = new ArrayList<>();
        List<Conn> matched = new ArrayList<>();
        for (Conn c : set) {
            if (c.legacy || !matches(e.target(), c)) {
                continue;
            }
            matched.add(c);
            if (c.send(e.frame())) {
                ok++;
            } else {
                failed.add(c.userId);
            }
        }
        stats.fanout(System.nanoTime() - t0);
        if (e.closeAfter()) {
            matched.forEach(c -> {
                c.ended = true;
                c.finish();
            });
        }
        if (e.pushId() != null && !matched.isEmpty()) {
            store.addDelivery(e.code(), e.pushId(), ok, failed)
                    .subscribe(null, err -> log.warn("Delivery count for {} failed: {}", e.code(), err.toString()));
        }
    }

    private static boolean matches(String target, Conn c) {
        return switch (target) {
            case PARTICIPANTS -> !c.coordinator;
            case COORDINATORS -> c.coordinator;
            default -> true;
        };
    }

    /** Keeps this pod's question cache and status (for /stats) in step with the survey's events. */
    private void noteEvent(Event e) {
        switch (e.event()) {
            case "SESSION_STARTED", "COORDINATOR_BACK" -> statusCache.put(e.code(), Status.LIVE);
            case "COORDINATOR_AWAY" -> statusCache.put(e.code(), Status.PAUSED);
            case "SESSION_ENDED" -> statusCache.put(e.code(), Status.ENDED);
            case "QUESTION_PUSHED" -> {
                try {
                    JsonNode d = mapper.readTree(e.frame()).path("data");
                    pushCache.put(e.code(), new CachedPush(e.pushId(), mapper.treeToValue(d, Question.class)));
                } catch (Exception ex) {
                    pushCache.remove(e.code());
                }
            }
            default -> { }
        }
    }

    // ================================================================ timers and loops

    private void scheduleQuestionTimer(String code, String roomId, String pushId, long endsAt) {
        timers.schedule(() -> store.expire(code, pushId, endsAt).filter(Boolean::booleanValue)
                .flatMap(b -> publish(code, "ANSWERS_STOPPED", ALL, roomId, map("pushId", pushId, "reason", "TIMER")))
                .subscribe(null, e -> log.warn("Question timer for {} failed: {}", code, e.toString())),
                Math.max(0, endsAt - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
    }

    /** PC lost: end the survey if it is still paused from the same drop when the wait runs out. */
    private void schedulePcTimeout(String code, long pausedAt) {
        long wait = room.pcWaitTimeout().toMillis();
        if (wait <= 0) {
            return;
        }
        timers.schedule(() -> store.view(code)
                .filter(v -> v.status() == Status.PAUSED && v.pausedAt() == pausedAt)
                .flatMap(v -> end(code, "COORDINATOR_TIMEOUT"))
                .subscribe(null, e -> log.warn("PC timeout for {} failed: {}", code, e.toString())),
                wait, TimeUnit.MILLISECONDS);
    }

    /** LIVE_STATS (+ LIVE_RESULTS while answers are open) to each PC on this pod, only when the numbers change. */
    private void liveStatsTick() {
        for (Set<Conn> set : local.values()) {
            for (Conn pc : set) {
                if (!pc.coordinator || pc.legacy || !pc.isOpen()) {
                    continue;
                }
                store.view(pc.code).filter(v -> v.status() == Status.LIVE && v.current() != null)
                        .flatMap(v -> {
                            PushView p = v.current();
                            return Mono.zip(store.connected(pc.code), store.answered(pc.code, p.pushId()),
                                    store.delivery(pc.code, p.pushId()), store.results(pc.code, p))
                                    .doOnNext(t -> {
                                        Map<String, Object> m = map("pushId", p.pushId(), "connected", t.getT1(),
                                                "delivered", t.getT3().delivered(),
                                                "failed", t.getT3().failedUserIds().size(), "answered", t.getT2());
                                        String key = m.toString();
                                        if (!key.equals(pc.lastLiveStats)) {
                                            pc.lastLiveStats = key;
                                            pc.send(frames.event("LIVE_STATS", v.roomId(), m));
                                            if (p.open()) {
                                                pc.send(frames.event("LIVE_RESULTS", v.roomId(), t.getT4()));
                                            }
                                        }
                                    });
                        })
                        .subscribe(null, e -> log.debug("Live stats for {} failed: {}", pc.code, e.toString()));
            }
        }
    }

    /** Ping every socket on this pod; close the ones silent for longer than idle-timeout. */
    private void pingSweep() {
        long cutoff = System.currentTimeMillis() - ws.idleTimeout().toMillis();
        for (Set<Conn> set : local.values()) {
            for (Conn c : set) {
                if (c.lastSeen < cutoff) {
                    stats.idleClosed.increment();
                    c.close(Conn.CONNECTION_LOST);
                } else {
                    c.ping();
                }
            }
        }
    }

    /** Surveys with sockets on this pod, by last known status (for /stats). */
    Map<String, Object> summary() {
        Map<String, Integer> by = new LinkedHashMap<>();
        int maxSize = 0;
        for (Map.Entry<String, Set<Conn>> e : local.entrySet()) {
            by.merge(statusCache.getOrDefault(e.getKey(), Status.WAITING).name().toLowerCase(), 1, Integer::sum);
            maxSize = Math.max(maxSize, e.getValue().size());
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("total", local.size());
        for (Status st : Status.values()) {
            m.put(st.name().toLowerCase(), by.getOrDefault(st.name().toLowerCase(), 0));
        }
        m.put("largestSockets", maxSize);
        return m;
    }
}
