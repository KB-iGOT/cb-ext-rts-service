package com.igot.cb;

import static com.igot.cb.Frames.map;

import com.igot.cb.Session.Push;
import com.igot.cb.Session.Status;
import com.igot.cb.Session.Target;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * The survey protocol (HLD §9) on in-memory state. State changes happen under the session's lock; frames are
 * sent after the lock is released.
 */
@Component
public class SessionManager {

    private static final Logger log = LoggerFactory.getLogger(SessionManager.class);
    private static final Set<String> TYPES = Set.of("SINGLE", "MULTI", "TEXT", "WORD_CLOUD");

    private final ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();
    private final Frames frames;
    private final ObjectMapper mapper;
    private final Stats stats;
    private final RtsSettings.Room room;
    private final RtsSettings.Ws ws;
    private final String pod;
    private final Scheduler timers = Schedulers.newParallel("rts-timer", 2, true);
    private final List<Disposable> loops = new ArrayList<>();

    public SessionManager(Frames frames, ObjectMapper mapper, Stats stats, RtsSettings settings,
            @Value("${pod.name}") String pod) {
        this.frames = frames;
        this.mapper = mapper;
        this.stats = stats;
        this.room = settings.room();
        this.ws = settings.ws();
        this.pod = pod;
        stats.sessionSummary(this::summary);
        long live = room.liveStatsInterval().toMillis();
        loops.add(timers.schedulePeriodically(this::liveStatsTick, live, live, TimeUnit.MILLISECONDS));
        long ping = ws.pingInterval().toMillis();
        loops.add(timers.schedulePeriodically(this::pingSweep, ping, ping, TimeUnit.MILLISECONDS));
    }

    @PreDestroy
    void stop() {
        loops.forEach(Disposable::dispose);
        timers.dispose();
    }

    /** Pod shutting down: close every socket with 1001 so clients reconnect elsewhere. */
    void closeAll() {
        int n = 0;
        for (Session s : sessions.values()) {
            for (Conn c : s.conns) {
                c.ended = true;
                c.close(org.springframework.web.reactive.socket.CloseStatus.GOING_AWAY);
                n++;
            }
        }
        log.info("Shutdown: closed {} sockets", n);
    }

    // ================================================================ connect / disconnect

    /** Adds the socket to its survey. False = refused (participant limit). */
    boolean join(Conn c) {
        boolean[] ok = {true};
        sessions.compute(c.code, (k, s) -> {
            Session session = s == null ? new Session(k) : s;
            synchronized (session) {
                if (!c.coordinator && !c.legacy && !session.hasParticipant(c.userId)
                        && session.participantCount() >= room.maxParticipants()) {
                    ok[0] = false;
                    return session;
                }
                if (!c.coordinator && !c.legacy) {
                    session.addParticipant(c.userId);
                }
                session.conns.add(c);
                c.joined = true;
            }
            return session;
        });
        if (!ok[0]) {
            removeIfIdle(c.code);
        }
        return ok[0];
    }

    /** SESSION_STATE to the new socket; a PC reconnecting to a PAUSED survey resumes it. */
    void onJoined(Conn c) {
        Session s = sessions.get(c.code);
        if (s == null) {
            return;
        }
        if (c.legacy) {
            c.send(frames.raw(map("type", "CONNECTED", "pod", pod)));
            return;
        }
        boolean back = false;
        boolean created = false;
        byte[] state;
        byte[] joined = null;
        synchronized (s) {
            if (c.coordinator && s.status == Status.PAUSED) {
                s.status = Status.LIVE;
                s.pausedAt = 0;
                back = true;
            } else if (c.coordinator && room.startOnPcJoin() && !s.running()) {
                created = createRoom(s, c);
            }
            Push p = s.running() ? s.current : null;
            String myAnswer = p != null && !c.coordinator && p.answered.contains(c.userId) ? "RECORDED" : null;
            state = frames.event("SESSION_STATE", s.roomId, map("status", s.status,
                    "role", c.coordinator ? "COORDINATOR" : "PARTICIPANT", "roomId", s.roomId, "pod", pod,
                    "connected", s.participantCount(),
                    "currentPush", p == null ? null : c.coordinator ? p.forCoordinator() : p.forLearners(),
                    "myAnswer", myAnswer == null ? null : map("status", myAnswer)));
            if (!c.coordinator && s.running()) {
                joined = frames.event("PARTICIPANT_JOINED", s.roomId, map("userId", c.userId, "name", c.name,
                        "pod", pod, "connected", s.participantCount()));
            }
        }
        c.send(state);
        if (joined != null) {
            broadcast(s, Target.COORDINATORS, joined);
        }
        if (created) {
            log.info("Survey {} started as room {} when PC {} joined", s.code, s.roomId, c.userId);
            broadcast(s, Target.PARTICIPANTS, frames.event("SESSION_STARTED", s.roomId, map("roomId", s.roomId)));
        }
        if (back) {
            log.info("Coordinator {} back in {}", c.userId, c.code);
            broadcast(s, Target.PARTICIPANTS, frames.event("COORDINATOR_BACK", s.roomId, map()));
        }
    }

    void onLeft(Conn c, String reason) {
        Session s = sessions.get(c.code);
        if (s == null || !c.joined) {
            return;
        }
        byte[] notice = null;
        Target target = null;
        long pausedAt = 0;
        synchronized (s) {
            s.conns.remove(c);
            if (!c.coordinator && !c.legacy) {
                s.removeParticipant(c.userId);
            }
            if (c.coordinator && !c.legacy) {
                if (!c.explicitLeave && !c.ended && s.status == Status.LIVE && !s.hasCoordinatorOtherThan(c)) {
                    s.status = Status.PAUSED;
                    pausedAt = System.currentTimeMillis();
                    s.pausedAt = pausedAt;
                    target = Target.PARTICIPANTS;
                    long waitSec = room.pcWaitTimeout().toSeconds();
                    notice = frames.event("COORDINATOR_AWAY", s.roomId, map("waitSec", waitSec > 0 ? waitSec : null));
                }
            } else if (!c.legacy && s.running() && !c.ended) {
                target = Target.COORDINATORS;
                notice = frames.event("PARTICIPANT_LEFT", s.roomId, map("userId", c.userId, "name", c.name,
                        "reason", reason, "connected", s.participantCount()));
            }
        }
        if (notice != null) {
            broadcast(s, target, notice);
        }
        if (pausedAt > 0) {
            log.info("Coordinator {} lost in {}; survey paused", c.userId, c.code);
            schedulePcTimeout(s, pausedAt);
        }
        removeIfIdle(c.code);
    }

    /** Drops a survey nobody is connected to, unless it is running (a PAUSED survey waits for its PC). */
    private void removeIfIdle(String code) {
        sessions.computeIfPresent(code, (k, s) -> {
            synchronized (s) {
                return s.conns.isEmpty() && !s.running() ? null : s;
            }
        });
    }

    // ================================================================ inbound frames

    void onFrame(Conn c, String text) {
        stats.framesIn.increment();
        JsonNode msg;
        try {
            msg = mapper.readTree(text);
        } catch (Exception e) {
            c.send(frames.error(null, "BAD_MESSAGE", "Not valid JSON"));
            return;
        }
        if (!msg.has("event") && msg.has("type")) {
            legacy(c, msg, text);
            return;
        }
        String event = msg.path("event").asText("");
        String requestId = msg.hasNonNull("requestId") ? msg.get("requestId").asText() : null;
        JsonNode data = msg.path("data");
        Session s = sessions.get(c.code);
        if (s == null) {
            return;
        }
        switch (event) {
            case "SUBMIT_ANSWER" -> {
                if (participant(c, event, requestId)) {
                    submit(s, c, requestId, data);
                }
            }
            case "LEAVE" -> leave(s, c, requestId);
            case "START_SESSION" -> {
                if (coordinator(c, event, requestId)) {
                    start(s, c, requestId);
                }
            }
            case "PUSH_QUESTION" -> {
                if (coordinator(c, event, requestId)) {
                    push(s, c, requestId, data);
                }
            }
            case "STOP_ANSWERS", "RESUME_ANSWERS" -> {
                if (coordinator(c, event, requestId)) {
                    setOpen(s, c, event, requestId, data);
                }
            }
            case "SHOW_RESULTS", "REVEAL_ANSWER" -> {
                if (coordinator(c, event, requestId)) {
                    showOrReveal(s, c, event, requestId, data);
                }
            }
            case "END_SESSION" -> {
                if (coordinator(c, event, requestId)) {
                    endByCoordinator(s, c, requestId);
                }
            }
            default -> c.send(frames.error(requestId, "BAD_MESSAGE", "Unknown event '" + event + "'"));
        }
    }

    private boolean coordinator(Conn c, String event, String requestId) {
        if (!c.coordinator) {
            c.send(frames.fail(event, requestId, null, "FORBIDDEN", "Coordinator only"));
        }
        return c.coordinator;
    }

    private boolean participant(Conn c, String event, String requestId) {
        if (c.coordinator) {
            c.send(frames.fail(event, requestId, null, "FORBIDDEN", "Participants only"));
        }
        return !c.coordinator;
    }

    /** Old protocol: a trainer's {"type":"QUESTION",...} is relayed as-is to every socket of the room. */
    private void legacy(Conn c, JsonNode msg, String text) {
        String type = msg.path("type").asText();
        if (!c.coordinator || !"QUESTION".equals(type)) {
            log.info("IGNORED pod={} client={} type={}", pod, c.userId, type);
            return;
        }
        Session s = sessions.get(c.code);
        if (s == null) {
            return;
        }
        log.info("PUBLISH pod={} trainer={} room={} eventId={} localRoomSize={}", pod, c.userId, c.code,
                msg.path("eventId").asText(), s.conns.size());
        byte[] frame = text.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        long t0 = System.nanoTime();
        for (Conn o : new ArrayList<>(s.conns)) {
            if (o.legacy) {
                o.send(frame);
            }
        }
        stats.fanout(System.nanoTime() - t0);
    }

    // ================================================================ coordinator

    private void start(Session s, Conn c, String requestId) {
        boolean created = false;
        String roomId;
        Status status;
        synchronized (s) {
            created = createRoom(s, c);
            roomId = s.roomId;
            status = s.status;
        }
        c.send(frames.ok("START_SESSION", requestId, roomId, map("roomId", roomId, "status", status,
                "created", created)));
        if (created) {
            log.info("Survey {} started as room {} by {}", s.code, roomId, c.userId);
            broadcast(s, Target.PARTICIPANTS, frames.event("SESSION_STARTED", roomId, map("roomId", roomId)));
        }
    }

    /** WAITING/ENDED -> LIVE with a new roomId (caller holds the lock). False if already running. */
    private static boolean createRoom(Session s, Conn c) {
        if (s.running()) {
            return false;
        }
        s.status = Status.LIVE;
        s.roomId = "R-" + UUID.randomUUID().toString().substring(0, 8);
        s.coordinatorId = c.userId;
        s.pausedAt = 0;
        s.pushCount = 0;
        s.current = null;
        return true;
    }

    private void push(Session s, Conn c, String requestId, JsonNode data) {
        JsonNode q = data.path("question");
        String invalid = validateQuestion(q);
        if (invalid != null) {
            c.send(frames.fail("PUSH_QUESTION", requestId, s.roomId, "BAD_MESSAGE", invalid));
            return;
        }
        int timerSec = data.path("timerSec").asInt(0);
        Long endsAt = timerSec > 0 ? System.currentTimeMillis() + timerSec * 1000L : null;
        String type = q.path("type").asText();
        List<Integer> correct = new ArrayList<>();
        q.path("correct").forEach(n -> correct.add(n.asInt()));
        int maxChars = Math.min(q.path("maxChars").asInt(room.maxTextChars()), room.maxTextChars());
        String questionId = data.hasNonNull("questionId") ? data.get("questionId").asText()
                : q.path("questionId").asText(null);
        Push p;
        String closedPrev = null;
        String roomId;
        synchronized (s) {
            if (s.status != Status.LIVE) {
                c.send(frames.fail("PUSH_QUESTION", requestId, s.roomId, "NOT_LIVE", "Survey is not live"));
                return;
            }
            if (s.current != null && s.current.open) {
                s.current.open = false;
                closedPrev = s.current.pushId;
            }
            s.pushCount++;
            p = new Push("P-" + s.pushCount, questionId, type, q, q.path("options").size(), correct, maxChars,
                    endsAt);
            s.current = p;
            roomId = s.roomId;
        }
        stats.pushes.increment();
        c.send(frames.ok("PUSH_QUESTION", requestId, roomId, map("pushId", p.pushId, "questionId", questionId,
                "endsAt", endsAt)));
        if (closedPrev != null) {
            broadcast(s, Target.ALL, frames.event("ANSWERS_STOPPED", roomId,
                    map("pushId", closedPrev, "reason", "NEXT_QUESTION")));
        }
        // The question frame: counted per socket so the PC gets "delivered N / failed M".
        byte[] frame = frames.event("QUESTION_PUSHED", roomId, p.forLearners());
        long t0 = System.nanoTime();
        List<Conn> learners;
        synchronized (s) {
            learners = s.targets(Target.PARTICIPANTS);
        }
        List<Map<String, Object>> failed = new ArrayList<>();
        for (Conn l : learners) {
            if (l.send(frame)) {
                p.delivered.incrementAndGet();
            } else {
                failed.add(map("userId", l.userId, "name", l.name));
            }
        }
        stats.fanout(System.nanoTime() - t0);
        synchronized (s) {
            p.failed.addAll(failed);
        }
        timers.schedule(() -> deliveryReport(s, c, p, roomId), room.deliveryReportDelay().toMillis(),
                TimeUnit.MILLISECONDS);
        if (endsAt != null) {
            scheduleQuestionTimer(s, p.pushId, endsAt);
        }
    }

    private void deliveryReport(Session s, Conn pc, Push p, String roomId) {
        if (!pc.isOpen()) {
            return;
        }
        List<Map<String, Object>> failed;
        synchronized (s) {
            failed = new ArrayList<>(p.failed);
        }
        pc.send(frames.event("PUSH_DELIVERY", roomId, map("pushId", p.pushId, "delivered", p.delivered.get(),
                "failed", failed)));
    }

    private void setOpen(Session s, Conn c, String event, String requestId, JsonNode data) {
        boolean open = "RESUME_ANSWERS".equals(event);
        String pushId = data.path("pushId").asText(null);
        int timerSec = data.path("timerSec").asInt(0);
        Long endsAt = open && timerSec > 0 ? System.currentTimeMillis() + timerSec * 1000L : null;
        String roomId;
        int answered;
        synchronized (s) {
            roomId = s.roomId;
            String err = currentCheck(s, pushId);
            if (err != null) {
                c.send(frames.fail(event, requestId, roomId, err, err.equals("NOT_LIVE") ? "Survey is not live"
                        : "pushId is not the current question"));
                return;
            }
            s.current.open = open;
            s.current.endsAt = endsAt;
            answered = s.current.answered.size();
        }
        if (open) {
            c.send(frames.ok(event, requestId, roomId, map("pushId", pushId, "endsAt", endsAt)));
            broadcast(s, Target.ALL, frames.event("ANSWERS_RESUMED", roomId, map("pushId", pushId, "endsAt", endsAt)));
            if (endsAt != null) {
                scheduleQuestionTimer(s, pushId, endsAt);
            }
        } else {
            c.send(frames.ok(event, requestId, roomId, map("pushId", pushId, "answered", answered)));
            broadcast(s, Target.ALL, frames.event("ANSWERS_STOPPED", roomId,
                    map("pushId", pushId, "reason", "BY_COORDINATOR")));
        }
    }

    private void showOrReveal(Session s, Conn c, String event, String requestId, JsonNode data) {
        boolean reveal = "REVEAL_ANSWER".equals(event);
        String pushId = data.path("pushId").asText(null);
        String roomId;
        byte[] out;
        synchronized (s) {
            roomId = s.roomId;
            String err = currentCheck(s, pushId);
            if (err == null && reveal && s.current.correct.isEmpty()) {
                err = "NOT_GRADED";
            }
            if (err != null) {
                c.send(frames.fail(event, requestId, roomId, err, "Cannot " + event.toLowerCase() + ": " + err));
                return;
            }
            if (reveal) {
                s.current.revealed = true;
                out = frames.event("ANSWER_REVEALED", roomId, map("pushId", pushId, "correct", s.current.correct));
            } else {
                s.current.shown = true;
                out = frames.event("RESULTS_SHOWN", roomId, s.current.results());
            }
        }
        c.send(frames.ok(event, requestId, roomId, map("pushId", pushId)));
        broadcast(s, Target.ALL, out);
    }

    /** null if pushId is the current question of a running survey, else the error code. */
    private static String currentCheck(Session s, String pushId) {
        if (!s.running()) {
            return "NOT_LIVE";
        }
        if (s.current == null || !s.current.pushId.equals(pushId)) {
            return "NOT_CURRENT_QUESTION";
        }
        return null;
    }

    private void endByCoordinator(Session s, Conn c, String requestId) {
        c.explicitLeave = true;
        int pushed;
        int participants;
        synchronized (s) {
            pushed = s.pushCount;
            participants = s.participantCount();
        }
        c.send(frames.ok("END_SESSION", requestId, s.roomId, map("roomId", s.roomId, "pushed", pushed,
                "participants", participants)));
        end(s, "ENDED_BY_COORDINATOR");
    }

    private void leave(Session s, Conn c, String requestId) {
        c.explicitLeave = true;
        c.send(frames.ok("LEAVE", requestId, null, map()));
        boolean last;
        synchronized (s) {
            last = c.coordinator && !s.hasCoordinatorOtherThan(c);
        }
        if (last) {
            end(s, "COORDINATOR_LEFT");
        }
        c.finish();
    }

    /** LIVE/PAUSED -> ENDED: everyone gets SESSION_ENDED, then each socket is closed after it is flushed. */
    private void end(Session s, String reason) {
        List<Conn> all;
        String roomId;
        synchronized (s) {
            if (!s.running()) {
                return;
            }
            s.status = Status.ENDED;
            if (s.current != null) {
                s.current.open = false;
            }
            roomId = s.roomId;
            all = s.targets(Target.ALL);
            all.forEach(c -> c.ended = true);
        }
        log.info("Survey {} room {} ended: {}", s.code, roomId, reason);
        byte[] frame = frames.event("SESSION_ENDED", roomId, map("reason", reason));
        for (Conn c : all) {
            c.send(frame);
            c.finish();
        }
    }

    // ================================================================ learner

    private void submit(Session s, Conn c, String requestId, JsonNode data) {
        String pushId = data.path("pushId").asText(null);
        String code;
        String text = null;
        String roomId;
        synchronized (s) {
            roomId = s.roomId;
            Push p = s.current;
            if (p == null) {
                code = "NO_ACTIVE_QUESTION";
            } else if (!p.pushId.equals(pushId)) {
                code = "NOT_CURRENT_QUESTION";
            } else if (s.status != Status.LIVE || !p.open
                    || (p.endsAt != null && System.currentTimeMillis() > p.endsAt)) {
                code = "ANSWERS_STOPPED";
            } else {
                code = record(p, c.userId, data);
                if (code == null && "TEXT".equals(p.type)) {
                    text = data.path("text").asText();
                }
            }
        }
        if (code == null) {
            stats.answersRecorded.increment();
            c.send(frames.submitResult(requestId, roomId, pushId, null, null));
            if (text != null) {
                broadcast(s, Target.COORDINATORS, frames.event("LIVE_TEXT_ANSWERS", roomId,
                        map("pushId", pushId, "items", List.of(map("name", c.name, "text", text)))));
            }
        } else {
            stats.answersNotRecorded.increment();
            c.send(frames.submitResult(requestId, roomId, pushId, code, code));
        }
    }

    /** Validates and counts one answer (caller holds the session lock). null = recorded, else the reason. */
    private String record(Push p, String userId, JsonNode a) {
        List<Integer> choice = new ArrayList<>();
        List<String> words = new ArrayList<>();
        switch (p.type) {
            case "SINGLE", "MULTI" -> {
                a.path("choice").forEach(n -> choice.add(n.asInt(-1)));
                if (choice.isEmpty() || ("SINGLE".equals(p.type) && choice.size() != 1)
                        || new HashSet<>(choice).size() != choice.size()
                        || choice.stream().anyMatch(i -> i < 0 || i >= p.options)) {
                    return "INVALID_ANSWER";
                }
            }
            case "TEXT" -> {
                String t = a.path("text").asText("");
                if (t.isBlank() || t.length() > p.maxChars) {
                    return "INVALID_ANSWER";
                }
            }
            default -> {
                a.path("words").forEach(n -> words.add(n.asText("").trim().toLowerCase()));
                if (words.isEmpty() || words.size() > 3 || words.stream().anyMatch(w -> w.isEmpty() || w.length() > 30)) {
                    return "INVALID_ANSWER";
                }
            }
        }
        if (!p.answered.add(userId)) {
            return "ALREADY_ANSWERED";
        }
        choice.forEach(i -> p.counts[i]++);
        words.forEach(w -> p.words.merge(w, 1, Integer::sum));
        return null;
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

    // ================================================================ timers and loops

    private void scheduleQuestionTimer(Session s, String pushId, long endsAt) {
        timers.schedule(() -> {
            String roomId;
            synchronized (s) {
                Push p = s.current;
                if (p == null || !p.pushId.equals(pushId) || !p.open || !Objects.equals(p.endsAt, endsAt)) {
                    return;
                }
                p.open = false;
                roomId = s.roomId;
            }
            broadcast(s, Target.ALL, frames.event("ANSWERS_STOPPED", roomId, map("pushId", pushId, "reason", "TIMER")));
        }, Math.max(0, endsAt - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
    }

    private void schedulePcTimeout(Session s, long pausedAt) {
        long wait = room.pcWaitTimeout().toMillis();
        if (wait <= 0) {
            return;
        }
        timers.schedule(() -> {
            boolean stillPaused;
            synchronized (s) {
                stillPaused = s.status == Status.PAUSED && s.pausedAt == pausedAt;
            }
            if (stillPaused) {
                end(s, "COORDINATOR_TIMEOUT");
                removeIfIdle(s.code);
            }
        }, wait, TimeUnit.MILLISECONDS);
    }

    /** LIVE_STATS (+ LIVE_RESULTS while answers are open) to the PC, only when the numbers changed. */
    private void liveStatsTick() {
        try {
            for (Session s : sessions.values()) {
                byte[] stats1 = null;
                byte[] results = null;
                List<Conn> pcs;
                synchronized (s) {
                    Push p = s.current;
                    if (s.status != Status.LIVE || p == null) {
                        continue;
                    }
                    Map<String, Object> m = map("pushId", p.pushId, "connected", s.participantCount(),
                            "delivered", p.delivered.get(), "failed", p.failed.size(), "answered", p.answered.size());
                    String key = m.toString();
                    if (key.equals(s.lastLiveStats)) {
                        continue;
                    }
                    s.lastLiveStats = key;
                    stats1 = frames.event("LIVE_STATS", s.roomId, m);
                    if (p.open) {
                        results = frames.event("LIVE_RESULTS", s.roomId, p.results());
                    }
                    pcs = s.targets(Target.COORDINATORS);
                }
                for (Conn pc : pcs) {
                    pc.send(stats1);
                    if (results != null) {
                        pc.send(results);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Live stats tick failed: {}", e.toString());
        }
    }

    /** Ping every socket; close the ones silent for longer than idle-timeout. */
    private void pingSweep() {
        long cutoff = System.currentTimeMillis() - ws.idleTimeout().toMillis();
        for (Session s : sessions.values()) {
            for (Conn c : s.conns) {
                if (c.lastSeen < cutoff) {
                    stats.idleClosed.increment();
                    c.close(Conn.CONNECTION_LOST);
                } else {
                    c.ping();
                }
            }
        }
    }

    // ================================================================ helpers

    private void broadcast(Session s, Target t, byte[] frame) {
        List<Conn> list;
        synchronized (s) {
            list = s.targets(t);
        }
        long t0 = System.nanoTime();
        for (Conn c : list) {
            c.send(frame);
        }
        stats.fanout(System.nanoTime() - t0);
    }

    Map<String, Object> summary() {
        Map<Status, Integer> by = new EnumMap<>(Status.class);
        int maxSize = 0;
        for (Session s : sessions.values()) {
            by.merge(s.status, 1, Integer::sum);
            maxSize = Math.max(maxSize, s.conns.size());
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("total", sessions.size());
        for (Status st : Status.values()) {
            m.put(st.name().toLowerCase(), by.getOrDefault(st, 0));
        }
        m.put("largestSockets", maxSize);
        return m;
    }
}
