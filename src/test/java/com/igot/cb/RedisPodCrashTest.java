package com.igot.cb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.WsClient.Client;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * A pod that crashes runs no "left" step for its sockets. Here a crashed pod is left behind in Redis exactly as a
 * real one would be (a stale heartbeat and its socket records), and this pod must clean up after it.
 * A real crash (docker kill) was checked by hand with docker compose.
 *
 * <p>dead-after stays above the default 5 s heartbeat: the other test contexts share this Redis and keep running,
 * and with a shorter dead-after this pod would take their (live) pods for dead.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"rts.auth.mode=dev", "rts.pod.heartbeat=300ms", "rts.pod.dead-after=15s",
                "rts.room.pc-wait-timeout=3s"})
class RedisPodCrashTest {

    @LocalServerPort
    int port;

    @Autowired
    ObjectMapper mapper;

    @Autowired
    ReactiveStringRedisTemplate redis;

    @Autowired
    SessionStore store;

    final String run = "C" + UUID.randomUUID().toString().substring(0, 6) + "-";

    @BeforeAll
    static void needsRedis() {
        Assumptions.assumeTrue(RedisAvailable.up(), "Redis not running on localhost:6379");
    }

    @Test
    void learnersOfACrashedPodAreReportedLost() throws Exception {
        String code = run + "L";
        Client pc = connect(code, "pc", "COORDINATOR");
        pc.next("SESSION_STATE");

        // ravi was on a pod that has just crashed: counted in the survey, never counted out
        store.join(code, "ravi", "ravi", 200).block();
        String ghost = crashedPod(code, "c1", "ravi", false);

        JsonNode left = pc.next("PARTICIPANT_LEFT");
        assertEquals("ravi", left.at("/data/userId").asText());
        assertEquals("CONNECTION_LOST", left.at("/data/reason").asText());
        assertEquals(0, left.at("/data/connected").asInt());
        assertNull(redis.opsForZSet().score("rts:pods", ghost).block(), "dead pod claimed and removed");
    }

    @Test
    void learnerAlreadyBackOnAnotherPodIsNotReportedLost() throws Exception {
        String code = run + "B";
        Client pc = connect(code, "pc", "COORDINATOR");
        pc.next("SESSION_STATE");
        // asha's old socket is on the crashed pod, and she has already reconnected here
        store.join(code, "asha", "asha", 200).block();
        Client asha = connect(code, "asha", "PARTICIPANT");
        asha.next("SESSION_STATE");
        pc.next("PARTICIPANT_JOINED");
        String ghost = crashedPod(code, "c2", "asha", false);

        long until = System.currentTimeMillis() + 5000;
        while (redis.opsForZSet().score("rts:pods", ghost).block() != null && System.currentTimeMillis() < until) {
            Thread.sleep(100);
        }
        Thread.sleep(300);
        assertEquals(0, pc.in.stream().filter(f -> "PARTICIPANT_LEFT".equals(f.path("event").asText())).count());
        assertEquals(1L, store.connected(code).block(), "asha counted once");
    }

    @Test
    void pcOnACrashedPodPausesTheSurveyThenItEnds() throws Exception {
        String code = run + "P";
        store.start(code, "pc").block();
        Client l = connect(code, "l1", "PARTICIPANT");
        assertEquals("LIVE", l.next("SESSION_STATE").at("/data/status").asText());

        // the PC's only socket was on the crashed pod
        store.pcJoin(code, "c3").block();
        crashedPod(code, "c3", "pc", true);

        l.next("COORDINATOR_AWAY");
        assertEquals("COORDINATOR_TIMEOUT", l.next("SESSION_ENDED").at("/data/reason").asText());
    }

    @Test
    void pausedSurveyEndsEvenWhenThePodHoldingItsTimerIsGone() throws Exception {
        String code = run + "T";
        store.start(code, "pc").block();
        Client l = connect(code, "l1", "PARTICIPANT");
        l.next("SESSION_STATE");
        // paused 10 s ago by a pod that is gone (its timer with it): only the shared list can end it
        long pausedAt = System.currentTimeMillis() - 10_000;
        store.pause(code, pausedAt).block();
        redis.opsForZSet().add("rts:paused", code, pausedAt).block();

        assertEquals("COORDINATOR_TIMEOUT", l.next("SESSION_ENDED").at("/data/reason").asText());
    }

    /** Leaves behind what a crashed pod leaves: one socket record and a heartbeat a minute old. */
    private String crashedPod(String code, String connId, String userId, boolean coordinator) throws Exception {
        String ghost = "crashed-" + UUID.randomUUID().toString().substring(0, 6);
        redis.opsForHash().put("rts:pod:{" + ghost + "}:conns", connId, mapper.writeValueAsString(
                Map.of("code", code, "userId", userId, "name", userId, "coordinator", coordinator))).block();
        redis.opsForZSet().add("rts:pods", ghost, System.currentTimeMillis() - 60_000).block();
        return ghost;
    }

    private Client connect(String code, String user, String role) {
        return new Client("ws://localhost:" + port + "/ws?rtsCode=" + code + "&userId=" + user + "&role=" + role
                + "&name=" + user, mapper);
    }
}
