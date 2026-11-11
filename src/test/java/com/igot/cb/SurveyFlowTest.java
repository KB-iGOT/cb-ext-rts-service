package com.igot.cb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import com.igot.cb.WsClient.Client;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

/** The whole survey over real sockets: wait, start, push, answer, stop, results, reveal, PC drop, end. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"rts.auth.mode=dev", "rts.room.pc-wait-timeout=2s", "rts.room.max-participants=2", "rts.room.start-on-pc-join=false"})
class SurveyFlowTest {

    @BeforeAll
    static void needsRedis() {
        Assumptions.assumeTrue(RedisAvailable.up(), "Redis not running on localhost:6379");
    }

    /** Unique per test run: the Redis store keeps keys between runs. */
    final String run = "T" + java.util.UUID.randomUUID().toString().substring(0, 6) + "-";

    @LocalServerPort
    int port;

    @Autowired
    ObjectMapper mapper;

    @Test
    void fullSurvey() throws Exception {
        Client asha = connect(run + "S1", "asha", "PARTICIPANT");
        assertEquals("WAITING", asha.next("SESSION_STATE").at("/data/status").asText());

        Client pc = connect(run + "S1", "meena", "COORDINATOR");
        pc.next("SESSION_STATE");
        pc.send("START_SESSION", Map.of());
        assertTrue(pc.next("START_SESSION_RESULT").at("/data/created").asBoolean());
        asha.next("SESSION_STARTED");

        Client ravi = connect(run + "S1", "ravi", "PARTICIPANT");
        assertEquals("LIVE", ravi.next("SESSION_STATE").at("/data/status").asText());
        JsonNode joined = pc.next("PARTICIPANT_JOINED");
        assertEquals("ravi", joined.at("/data/userId").asText());
        assertEquals("ravi", joined.at("/data/name").asText());
        assertTrue(joined.at("/data/pod").isTextual(), "PC is told which pod the learner joined");
        assertEquals(2, joined.at("/data/connected").asInt());
        Client third = connect(run + "S1", "third", "PARTICIPANT");
        assertEquals("PARTICIPANT_LIMIT_REACHED", third.next("ERROR").at("/error/code").asText());

        pc.send("PUSH_QUESTION", Map.of("question", Map.of("questionId", "Q1", "type", "SINGLE", "text", "Pick",
                "options", new String[] {"A", "B", "C"}, "correct", new int[] {1})));
        String pushId = pc.next("PUSH_QUESTION_RESULT").at("/data/pushId").asText();
        JsonNode q = asha.next("QUESTION_PUSHED");
        assertEquals(pushId, q.at("/data/pushId").asText());
        assertTrue(q.at("/data/correct").isMissingNode(), "learners must not see the correct answer");
        ravi.next("QUESTION_PUSHED");

        asha.send("SUBMIT_ANSWER", Map.of("pushId", pushId, "choice", new int[] {1}));
        assertEquals("RECORDED", asha.next("SUBMIT_ANSWER_RESULT").path("status").asText());
        asha.send("SUBMIT_ANSWER", Map.of("pushId", pushId, "choice", new int[] {0}));
        assertEquals("ALREADY_ANSWERED", asha.next("SUBMIT_ANSWER_RESULT").at("/error/code").asText());
        ravi.send("SUBMIT_ANSWER", Map.of("pushId", pushId, "choice", new int[] {7}));
        assertEquals("INVALID_ANSWER", ravi.next("SUBMIT_ANSWER_RESULT").at("/error/code").asText());
        asha.send("START_SESSION", Map.of());
        assertEquals("FORBIDDEN", asha.next("START_SESSION_RESULT").at("/error/code").asText());

        JsonNode delivery = pc.next("PUSH_DELIVERY");
        assertEquals(2, delivery.at("/data/delivered").asInt());

        pc.send("STOP_ANSWERS", Map.of("pushId", pushId));
        assertEquals(1, pc.next("STOP_ANSWERS_RESULT").at("/data/answered").asInt());
        assertEquals("BY_COORDINATOR", ravi.next("ANSWERS_STOPPED").at("/data/reason").asText());
        ravi.send("SUBMIT_ANSWER", Map.of("pushId", pushId, "choice", new int[] {2}));
        assertEquals("ANSWERS_STOPPED", ravi.next("SUBMIT_ANSWER_RESULT").at("/error/code").asText());

        pc.send("SHOW_RESULTS", Map.of("pushId", pushId));
        JsonNode results = asha.next("RESULTS_SHOWN");
        assertEquals(1, results.at("/data/counts/1").asInt());
        pc.send("REVEAL_ANSWER", Map.of("pushId", pushId));
        assertEquals(1, asha.next("ANSWER_REVEALED").at("/data/correct/0").asInt());

        // a reconnecting learner sees the question and that they already answered
        asha.close();
        pc.next("PARTICIPANT_LEFT");
        Client asha2 = connect(run + "S1", "asha", "PARTICIPANT");
        JsonNode state = asha2.next("SESSION_STATE");
        assertEquals("RECORDED", state.at("/data/myAnswer/status").asText());

        // PC network loss pauses; reconnect resumes
        pc.close();
        asha2.next("COORDINATOR_AWAY");
        Client pc2 = connect(run + "S1", "meena", "COORDINATOR");
        assertEquals("LIVE", pc2.next("SESSION_STATE").at("/data/status").asText());
        asha2.next("COORDINATOR_BACK");

        pc2.send("END_SESSION", Map.of());
        pc2.next("END_SESSION_RESULT");
        assertEquals("ENDED_BY_COORDINATOR", asha2.next("SESSION_ENDED").at("/data/reason").asText());
        assertTrue(asha2.closed.await(), "learner socket closed after SESSION_ENDED");
    }

    @Test
    void pausedSurveyEndsWhenPcDoesNotReturn() throws Exception {
        Client l = connect(run + "S2", "l1", "PARTICIPANT");
        Client pc = connect(run + "S2", "pc", "COORDINATOR");
        pc.send("START_SESSION", Map.of());
        l.next("SESSION_STARTED");
        pc.close();
        l.next("COORDINATOR_AWAY");
        assertEquals("COORDINATOR_TIMEOUT", l.next("SESSION_ENDED").at("/data/reason").asText());
    }

    @Test
    void learnerBackAfterDropSeesResultsAndAnswerShownWhileAway() throws Exception {
        Client asha = connect(run + "S3", "asha", "PARTICIPANT");
        Client pc = connect(run + "S3", "pc", "COORDINATOR");
        pc.send("START_SESSION", Map.of());
        asha.next("SESSION_STARTED");
        pc.send("PUSH_QUESTION", Map.of("question", Map.of("type", "SINGLE", "text", "Pick",
                "options", new String[] {"A", "B"}, "correct", new int[] {1})));
        String pushId = pc.next("PUSH_QUESTION_RESULT").at("/data/pushId").asText();
        asha.next("QUESTION_PUSHED");
        asha.send("SUBMIT_ANSWER", Map.of("pushId", pushId, "choice", new int[] {1}));
        asha.next("SUBMIT_ANSWER_RESULT");

        // asha drops; meanwhile the PC shows the results and reveals the answer
        asha.close();
        pc.next("PARTICIPANT_LEFT");
        pc.send("SHOW_RESULTS", Map.of("pushId", pushId));
        pc.next("SHOW_RESULTS_RESULT");
        pc.send("REVEAL_ANSWER", Map.of("pushId", pushId));
        pc.next("REVEAL_ANSWER_RESULT");

        JsonNode cp = connect(run + "S3", "asha", "PARTICIPANT").next("SESSION_STATE").at("/data/currentPush");
        assertEquals(pushId, cp.path("pushId").asText());
        assertTrue(cp.path("resultsShown").asBoolean());
        assertEquals(1, cp.at("/results/total").asInt());
        assertEquals(1, cp.at("/results/counts/1").asInt());
        assertTrue(cp.path("answerRevealed").asBoolean());
        assertEquals(1, cp.at("/correct/0").asInt());

        // a PC that reconnects gets the shown results too
        pc.close();
        JsonNode pcp = connect(run + "S3", "pc", "COORDINATOR").next("SESSION_STATE").at("/data/currentPush");
        assertEquals(1, pcp.at("/results/counts/1").asInt());
    }

    @Test
    void pcBackAfterDropSeesLearnersAndTextAnswersOfTheCurrentQuestion() throws Exception {
        Client asha = connect(run + "S6", "asha", "PARTICIPANT");
        Client ravi = connect(run + "S6", "ravi", "PARTICIPANT");
        Client pc = connect(run + "S6", "pc", "COORDINATOR");
        pc.send("START_SESSION", Map.of());
        asha.next("SESSION_STARTED");
        pc.send("PUSH_QUESTION", Map.of("question", Map.of("type", "TEXT", "text", "One thing to improve?")));
        String pushId = pc.next("PUSH_QUESTION_RESULT").at("/data/pushId").asText();
        asha.next("QUESTION_PUSHED");
        asha.send("SUBMIT_ANSWER", Map.of("pushId", pushId, "text", "More examples"));
        asha.next("SUBMIT_ANSWER_RESULT");
        pc.next("LIVE_TEXT_ANSWERS");

        // the PC drops; meanwhile ravi leaves
        pc.close();
        asha.next("COORDINATOR_AWAY");
        ravi.close();
        assertTrue(ravi.closed.await());
        Thread.sleep(300);

        JsonNode d = connect(run + "S6", "pc", "COORDINATOR").next("SESSION_STATE").path("data");
        assertEquals(2, d.path("participants").size());
        assertEquals("asha", d.at("/participants/0/userId").asText());
        assertTrue(d.at("/participants/0/connected").asBoolean());
        assertEquals("ravi", d.at("/participants/1/userId").asText());
        assertFalse(d.at("/participants/1/connected").asBoolean(), "ravi left while the PC was away");
        assertEquals(1, d.at("/currentPush/textAnswers").size());
        assertEquals("asha", d.at("/currentPush/textAnswers/0/name").asText());
        assertEquals("More examples", d.at("/currentPush/textAnswers/0/text").asText());

        // learners get neither the list nor other people's answers
        asha.close();
        JsonNode l = connect(run + "S6", "asha", "PARTICIPANT").next("SESSION_STATE").path("data");
        assertTrue(l.path("participants").isMissingNode());
        assertTrue(l.at("/currentPush/textAnswers").isMissingNode());
    }

    @Test
    void learnerNotShownBeforeResultsOrReveal() throws Exception {
        Client asha = connect(run + "S4", "asha", "PARTICIPANT");
        Client pc = connect(run + "S4", "pc", "COORDINATOR");
        pc.send("START_SESSION", Map.of());
        asha.next("SESSION_STARTED");
        pc.send("PUSH_QUESTION", Map.of("question", Map.of("type", "SINGLE", "text", "Pick",
                "options", new String[] {"A", "B"}, "correct", new int[] {1})));
        pc.next("PUSH_QUESTION_RESULT");
        JsonNode cp = connect(run + "S4", "ravi", "PARTICIPANT").next("SESSION_STATE").at("/data/currentPush");
        assertFalse(cp.path("resultsShown").asBoolean());
        assertTrue(cp.path("results").isMissingNode(), "no results before the PC shows them");
        assertTrue(cp.path("correct").isMissingNode(), "learners must not see the correct answer before reveal");
    }

    @Test
    void closingOneOfTwoTabsIsNotALeave() throws Exception {
        Client pc = connect(run + "S5", "pc", "COORDINATOR");
        pc.send("START_SESSION", Map.of());
        pc.next("START_SESSION_RESULT");
        Client tab1 = connect(run + "S5", "asha", "PARTICIPANT");
        tab1.next("SESSION_STATE");
        Client tab2 = connect(run + "S5", "asha", "PARTICIPANT");
        tab2.next("SESSION_STATE");

        tab1.close();
        assertTrue(tab1.closed.await());
        Thread.sleep(500);
        assertTrue(pc.in.stream().noneMatch(f -> "PARTICIPANT_LEFT".equals(f.path("event").asText())),
                "asha still has a tab open: not reported as left");

        tab2.close();
        JsonNode left = pc.next("PARTICIPANT_LEFT");
        assertEquals("asha", left.at("/data/userId").asText());
        assertEquals(0, left.at("/data/connected").asInt());
    }

    @Test
    void legacyQuestionRelay() throws Exception {
        Client trainer = connectRaw("/ws?room=" + run + "LEG&clientId=trainer&role=trainer");
        Client learner = connectRaw("/ws?room=" + run + "LEG&clientId=l1&role=learner");
        assertNotNull(learner.nextWhere("type", "CONNECTED"));
        trainer.nextWhere("type", "CONNECTED");
        trainer.sendRaw("{\"type\":\"QUESTION\",\"eventId\":\"E1\",\"payload\":\"hi\"}");
        assertEquals("E1", learner.nextWhere("type", "QUESTION").path("eventId").asText());
    }

    // ---------------------------------------------------------------- tiny client

    private Client connect(String code, String user, String role) {
        return connectRaw("/ws?rtsCode=" + code + "&userId=" + user + "&role=" + role + "&name=" + user);
    }

    private Client connectRaw(String path) {
        return new Client("ws://localhost:" + port + path, mapper);
    }

}
