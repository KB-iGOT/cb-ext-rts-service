package com.igot.cb;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;

/**
 * State of one survey as read from the {@link SessionStore} (memory or Redis), keyed by RTS code. Immutable
 * snapshots: the store is the truth; pods only hold sockets.
 */
public record SessionView(String code, String roomId, Status status, String coordinatorId, long pausedAt,
        int pushCount, PushView current) {

    public enum Status { WAITING, LIVE, PAUSED, ENDED }

    static SessionView waiting(String code) {
        return new SessionView(code, null, Status.WAITING, null, 0, 0, null);
    }

    boolean running() {
        return status == Status.LIVE || status == Status.PAUSED;
    }

    /** A question as pushed by the PC (snapshot kept with the push). */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Question(String questionId, String type, String text, List<String> options, List<Integer> correct,
            Integer maxChars) {

        boolean choice() {
            return "SINGLE".equals(type) || "MULTI".equals(type);
        }

        int optionCount() {
            return options == null ? 0 : options.size();
        }

        boolean graded() {
            return correct != null && !correct.isEmpty();
        }
    }

    /** The current question of a survey and its controls. */
    public record PushView(String pushId, Question question, boolean open, Long endsAt, boolean shown,
            boolean revealed) {

        /** What learners see: never the correct answer. */
        Map<String, Object> forLearners() {
            Question q = question;
            return Frames.map("pushId", pushId, "questionId", q.questionId(), "type", q.type(), "text", q.text(),
                    "options", q.choice() ? q.options() : null,
                    "maxChars", "TEXT".equals(q.type()) ? q.maxChars() : null,
                    "answersOpen", open, "endsAt", endsAt);
        }

        Map<String, Object> forCoordinator() {
            Map<String, Object> m = forLearners();
            m.put("correct", question.correct() == null ? List.of() : question.correct());
            m.put("resultsShown", shown);
            m.put("answerRevealed", revealed);
            return m;
        }
    }

    public record StartResult(SessionView view, boolean created) {
    }

    /** closedPushId: the previous question whose answers this push closed (null if none was open). */
    public record PushResult(String roomId, PushView push, String closedPushId) {
    }

    public record Delivery(long delivered, List<String> failedUserIds) {
    }
}
