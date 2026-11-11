package com.igot.cb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.WsClient.Client;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.reactive.context.ReactiveWebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Two pods, state in one Redis: a session whose PC and learners are on both pods, then one pod goes away
 * mid-survey and its people reconnect to the other pod and continue where they were.
 */
class RedisTwoPodsTest {

    static ConfigurableApplicationContext podA;
    static ConfigurableApplicationContext podB;
    static int portA;
    static int portB;
    final ObjectMapper mapper = new ObjectMapper();

    @BeforeAll
    static void startPods() {
        Assumptions.assumeTrue(RedisAvailable.up(), "Redis not running on localhost:6379");
        podA = pod("pod-A");
        podB = pod("pod-B");
        portA = ((ReactiveWebServerApplicationContext) podA).getWebServer().getPort();
        portB = ((ReactiveWebServerApplicationContext) podB).getWebServer().getPort();
    }

    @AfterAll
    static void stopPods() {
        if (podA != null && podA.isActive()) {
            podA.close();
        }
        if (podB != null) {
            podB.close();
        }
    }

    @Test
    void sessionAcrossPodsSurvivesLosingAPod() throws Exception {
        String code = "R2-" + UUID.randomUUID().toString().substring(0, 6);

        // a learner first, on pod B; then the PC on pod A starts the survey for everyone
        Client l0 = connect(portB, code, "L0", "PARTICIPANT");
        assertEquals("WAITING", l0.next("SESSION_STATE").at("/data/status").asText());
        Client pc = connect(portA, code, "pc", "COORDINATOR");
        JsonNode pcState = pc.next("SESSION_STATE");
        assertEquals("LIVE", pcState.at("/data/status").asText());
        assertEquals(1, pcState.at("/data/connected").asInt(), "L0 on pod B is counted");
        l0.next("SESSION_STARTED");

        // learners on both pods; the PC hears about each, with the pod each is on
        Client l1 = connect(portA, code, "L1", "PARTICIPANT");
        assertEquals("LIVE", l1.next("SESSION_STATE").at("/data/status").asText());
        assertJoined(pc.next("PARTICIPANT_JOINED"), "L1", "pod-A", 2);
        Client l2 = connect(portB, code, "L2", "PARTICIPANT");
        assertEquals("LIVE", l2.next("SESSION_STATE").at("/data/status").asText());
        assertJoined(pc.next("PARTICIPANT_JOINED"), "L2", "pod-B", 3);

        // the cap (3 here) counts learners on all pods: a 4th on pod A is refused
        Client extra = connect(portA, code, "L3", "PARTICIPANT");
        assertEquals("PARTICIPANT_LIMIT_REACHED", extra.next("ERROR").at("/error/code").asText());

        // a question reaches every learner on both pods; delivery is summed over pods
        pc.send("PUSH_QUESTION", Map.of("question", Map.of("type", "SINGLE", "text", "Pick",
                "options", new String[] {"A", "B"}, "correct", new int[] {0})));
        String pushId = pc.next("PUSH_QUESTION_RESULT").at("/data/pushId").asText();
        for (Client l : new Client[] {l0, l1, l2}) {
            assertEquals(pushId, l.next("QUESTION_PUSHED").at("/data/pushId").asText());
        }
        assertEquals(3, pc.next("PUSH_DELIVERY").at("/data/delivered").asInt());

        // answers from both pods counted once, in Redis
        l1.send("SUBMIT_ANSWER", Map.of("pushId", pushId, "choice", new int[] {0}));
        assertEquals("RECORDED", l1.next("SUBMIT_ANSWER_RESULT").path("status").asText());
        l2.send("SUBMIT_ANSWER", Map.of("pushId", pushId, "choice", new int[] {1}));
        assertEquals("RECORDED", l2.next("SUBMIT_ANSWER_RESULT").path("status").asText());
        l2.send("SUBMIT_ANSWER", Map.of("pushId", pushId, "choice", new int[] {0}));
        assertEquals("ALREADY_ANSWERED", l2.next("SUBMIT_ANSWER_RESULT").at("/error/code").asText());

        // pod A goes away (PC and L1 were on it): what a stopping pod does, close its sockets with 1001.
        // (Not podA.close(): in one JVM both pods share Reactor Netty's global resources, so closing one
        // context would stop the other's server too. A real crash is checked with two processes.)
        podA.getBean(SessionManager.class).closeAll();
        assertTrue(pc.closed.await(), "PC socket closed when its pod stopped");
        assertTrue(l1.closed.await(), "L1 socket closed when its pod stopped");

        // they reconnect to pod B and find the survey exactly where it was
        Client pc2 = connect(portB, code, "pc", "COORDINATOR");
        JsonNode s = pc2.next("SESSION_STATE");
        assertEquals("LIVE", s.at("/data/status").asText());
        assertEquals(pushId, s.at("/data/currentPush/pushId").asText(), "current question kept");
        assertEquals("pod-B", s.at("/data/pod").asText());
        Client l1b = connect(portB, code, "L1", "PARTICIPANT");
        JsonNode s1 = l1b.next("SESSION_STATE");
        assertEquals(pushId, s1.at("/data/currentPush/pushId").asText());
        assertEquals("RECORDED", s1.at("/data/myAnswer/status").asText(), "L1's answer from pod A is kept");

        // results include the answers given on the pod that is gone
        pc2.send("SHOW_RESULTS", Map.of("pushId", pushId));
        JsonNode results = l0.next("RESULTS_SHOWN");
        assertEquals(2, results.at("/data/total").asInt());
        assertEquals(1, results.at("/data/counts/0").asInt());
        assertEquals(1, results.at("/data/counts/1").asInt());

        // end: everyone left (all on pod B now) gets SESSION_ENDED and is disconnected
        pc2.send("END_SESSION", Map.of());
        pc2.next("END_SESSION_RESULT");
        for (Client l : new Client[] {l0, l2, l1b}) {
            assertEquals("ENDED_BY_COORDINATOR", l.next("SESSION_ENDED").at("/data/reason").asText());
            assertTrue(l.closed.await());
        }
    }

    private static void assertJoined(JsonNode joined, String userId, String pod, int connected) {
        assertEquals(userId, joined.at("/data/userId").asText());
        assertEquals(pod, joined.at("/data/pod").asText());
        assertEquals(connected, joined.at("/data/connected").asInt());
    }

    private Client connect(int port, String code, String user, String role) {
        return new Client("ws://localhost:" + port + "/ws?rtsCode=" + code + "&userId=" + user + "&role=" + role,
                mapper);
    }

    private static ConfigurableApplicationContext pod(String name) {
        // command-line args win over application.properties
        return new SpringApplicationBuilder(WebSocketPocApplication.class).run(
                "--server.port=0", "--rts.auth.mode=dev", "--pod.name=" + name, "--rts.room.max-participants=3",
                "--spring.data.redis.host=localhost", "--spring.data.redis.port=6379");
    }
}
