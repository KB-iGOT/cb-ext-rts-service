package com.igot.cb;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.WsClient.Client;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/** LLD §4.4: after END every key of the survey expires within rts.room.ttl-after-end (1 h), not 12 h. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"rts.auth.mode=dev"})
class RedisKeyLifetimeTest {

    @LocalServerPort
    int port;

    @Autowired
    ObjectMapper mapper;

    @Autowired
    ReactiveStringRedisTemplate redis;

    @BeforeAll
    static void needsRedis() {
        Assumptions.assumeTrue(RedisAvailable.up(), "Redis not running on localhost:6379");
    }

    @Test
    void everyKeyOfAnEndedSurveyGoesWithinAnHour() throws Exception {
        String code = "E" + UUID.randomUUID().toString().substring(0, 6);
        Client asha = connect(code, "asha", "PARTICIPANT");
        Client pc = connect(code, "pc", "COORDINATOR");
        pc.next("SESSION_STATE");
        asha.next("SESSION_STARTED");
        pc.send("PUSH_QUESTION", Map.of("question", Map.of("type", "TEXT", "text", "Why?")));
        String pushId = pc.next("PUSH_QUESTION_RESULT").at("/data/pushId").asText();
        asha.next("QUESTION_PUSHED");
        asha.send("SUBMIT_ANSWER", Map.of("pushId", pushId, "text", "Because"));
        asha.next("SUBMIT_ANSWER_RESULT");
        pc.next("PUSH_DELIVERY");

        pc.send("END_SESSION", Map.of());
        asha.next("SESSION_ENDED");
        Thread.sleep(300);

        var keys = redis.keys("rts:{" + code + "}:*").collectList().block();
        assertTrue(keys.size() >= 6, "room, part, names, pcs, ans, cnt, texts, dlv: " + keys);
        for (String k : keys) {
            Duration ttl = redis.getExpire(k).block();
            assertTrue(ttl != null && !ttl.isNegative() && ttl.compareTo(Duration.ofHours(1)) <= 0,
                    k + " expires in " + ttl);
        }
    }

    private Client connect(String code, String user, String role) {
        return new Client("ws://localhost:" + port + "/ws?rtsCode=" + code + "&userId=" + user + "&role=" + role,
                mapper);
    }
}
