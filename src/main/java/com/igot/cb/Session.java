package com.igot.cb;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Live state of one survey, keyed by RTS code. Everything is in this pod's memory (no Redis): this service
 * measures what one pod can hold. All fields are changed under {@code synchronized (session)}.
 */
final class Session {

    enum Status { WAITING, LIVE, PAUSED, ENDED }

    final String code;
    final Set<Conn> conns = ConcurrentHashMap.newKeySet();
    /** userId -> open sockets (a learner may have two tabs); the 200 cap counts distinct users. */
    private final Map<String, Integer> participants = new HashMap<>();

    Status status = Status.WAITING;
    String roomId;
    String coordinatorId;
    long pausedAt;
    int pushCount;
    /** Only the current question is kept; in the real design earlier answers are already in Kafka. */
    Push current;
    String lastLiveStats = "";

    Session(String code) {
        this.code = code;
    }

    boolean running() {
        return status == Status.LIVE || status == Status.PAUSED;
    }

    int participantCount() {
        return participants.size();
    }

    boolean hasParticipant(String userId) {
        return participants.containsKey(userId);
    }

    void addParticipant(String userId) {
        participants.merge(userId, 1, Integer::sum);
    }

    void removeParticipant(String userId) {
        participants.computeIfPresent(userId, (k, n) -> n > 1 ? n - 1 : null);
    }

    boolean hasCoordinatorOtherThan(Conn c) {
        for (Conn o : conns) {
            if (o != c && o.coordinator) {
                return true;
            }
        }
        return false;
    }

    List<Conn> targets(Target t) {
        List<Conn> list = new ArrayList<>(conns.size());
        for (Conn c : conns) {
            if (!c.legacy && (t == Target.ALL || (t == Target.COORDINATORS) == c.coordinator)) {
                list.add(c);
            }
        }
        return list;
    }

    enum Target { ALL, PARTICIPANTS, COORDINATORS }

    /** One pushed question and its answers. */
    static final class Push {
        final String pushId;
        final String questionId;
        final String type;
        final JsonNode question;
        final int options;
        final List<Integer> correct;
        final int maxChars;
        boolean open = true;
        Long endsAt;
        boolean shown;
        boolean revealed;
        final Set<String> answered = new HashSet<>();
        final long[] counts;
        final Map<String, Integer> words = new HashMap<>();
        final AtomicInteger delivered = new AtomicInteger();
        final List<Map<String, Object>> failed = new ArrayList<>();

        Push(String pushId, String questionId, String type, JsonNode question, int options, List<Integer> correct,
                int maxChars, Long endsAt) {
            this.pushId = pushId;
            this.questionId = questionId;
            this.type = type;
            this.question = question;
            this.options = options;
            this.correct = correct;
            this.maxChars = maxChars;
            this.endsAt = endsAt;
            this.counts = new long[options];
        }

        boolean choice() {
            return "SINGLE".equals(type) || "MULTI".equals(type);
        }

        /** What learners see: never the correct answer. */
        Map<String, Object> forLearners() {
            return Frames.map("pushId", pushId, "questionId", questionId, "type", type,
                    "text", question.path("text").asText(),
                    "options", choice() ? question.get("options") : null,
                    "maxChars", "TEXT".equals(type) ? maxChars : null,
                    "answersOpen", open, "endsAt", endsAt);
        }

        Map<String, Object> forCoordinator() {
            Map<String, Object> m = forLearners();
            m.put("correct", correct);
            m.put("resultsShown", shown);
            m.put("answerRevealed", revealed);
            return m;
        }

        Map<String, Object> results() {
            if (choice()) {
                return Frames.map("pushId", pushId, "total", answered.size(), "counts", counts);
            }
            if ("WORD_CLOUD".equals(type)) {
                List<Object[]> top = words.entrySet().stream()
                        .sorted(Map.Entry.<String, Integer>comparingByValue(Comparator.reverseOrder()))
                        .limit(30).map(e -> new Object[] {e.getKey(), e.getValue()}).toList();
                return Frames.map("pushId", pushId, "total", answered.size(), "top", top);
            }
            return Frames.map("pushId", pushId, "total", answered.size());
        }
    }
}
