package com.example.wspoc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Map;
import com.example.wspoc.WsClient.Client;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.http.client.HttpClient;

/** The whole survey over real sockets: wait, start, push, answer, stop, results, reveal, PC drop, end. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"rts.room.pc-wait-timeout=2s", "rts.room.max-participants=2", "rts.room.start-on-pc-join=false"})
class SurveyFlowTest {

    @LocalServerPort
    int port;

    @Autowired
    ObjectMapper mapper;

    @Test
    void fullSurvey() throws Exception {
        Client asha = connect("S1", "asha", "PARTICIPANT");
        assertEquals("WAITING", asha.next("SESSION_STATE").at("/data/status").asText());

        Client pc = connect("S1", "meena", "COORDINATOR");
        pc.next("SESSION_STATE");
        pc.send("START_SESSION", Map.of());
        assertTrue(pc.next("START_SESSION_RESULT").at("/data/created").asBoolean());
        asha.next("SESSION_STARTED");

        Client ravi = connect("S1", "ravi", "PARTICIPANT");
        assertEquals("LIVE", ravi.next("SESSION_STATE").at("/data/status").asText());
        Client third = connect("S1", "third", "PARTICIPANT");
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
        Client asha2 = connect("S1", "asha", "PARTICIPANT");
        JsonNode state = asha2.next("SESSION_STATE");
        assertEquals("RECORDED", state.at("/data/myAnswer/status").asText());

        // PC network loss pauses; reconnect resumes
        pc.close();
        asha2.next("COORDINATOR_AWAY");
        Client pc2 = connect("S1", "meena", "COORDINATOR");
        assertEquals("LIVE", pc2.next("SESSION_STATE").at("/data/status").asText());
        asha2.next("COORDINATOR_BACK");

        pc2.send("END_SESSION", Map.of());
        pc2.next("END_SESSION_RESULT");
        assertEquals("ENDED_BY_COORDINATOR", asha2.next("SESSION_ENDED").at("/data/reason").asText());
        assertTrue(asha2.closed.await(), "learner socket closed after SESSION_ENDED");
    }

    @Test
    void pausedSurveyEndsWhenPcDoesNotReturn() throws Exception {
        Client l = connect("S2", "l1", "PARTICIPANT");
        Client pc = connect("S2", "pc", "COORDINATOR");
        pc.send("START_SESSION", Map.of());
        l.next("SESSION_STARTED");
        pc.close();
        l.next("COORDINATOR_AWAY");
        assertEquals("COORDINATOR_TIMEOUT", l.next("SESSION_ENDED").at("/data/reason").asText());
    }

    @Test
    void legacyQuestionRelay() throws Exception {
        Client trainer = connectRaw("/ws?room=LEG&clientId=trainer&role=trainer");
        Client learner = connectRaw("/ws?room=LEG&clientId=l1&role=learner");
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
