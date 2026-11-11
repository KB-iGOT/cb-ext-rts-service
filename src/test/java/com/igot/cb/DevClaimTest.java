package com.igot.cb;

import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Demo setting: a code with no roster is open to learners and its first PC claim makes the claimer its only PC; any
 * other PC joins it as a learner.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"rts.auth.identity=demo", "rts.auth.dev-claim-coordinator=true"})
class DevClaimTest {

    @BeforeAll
    static void needsRedis() {
        Assumptions.assumeTrue(RedisAvailable.up(), "Redis not running on localhost:6379");
    }

    @LocalServerPort
    int port;

    @Test
    void firstPcClaimWinsAndLearnersMayJoin() {
        WebTestClient http = WebTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
        String code = "D" + UUID.randomUUID().toString().substring(0, 6);
        join(http, code, "early-learner", "PARTICIPANT").expectStatus().isOk();
        role(join(http, code, "meena", "COORDINATOR"), "COORDINATOR");
        role(join(http, code, "meena", "COORDINATOR"), "COORDINATOR");
        // another PC asking for the same code: Meena already claimed it, so a learner here
        role(join(http, code, "asha", "COORDINATOR"), "PARTICIPANT");
        join(http, code, "asha", "PARTICIPANT").expectStatus().isOk();
    }

    private static void role(WebTestClient.ResponseSpec r, String role) {
        r.expectStatus().isOk().expectBody().jsonPath("$.role").isEqualTo(role);
    }

    private static WebTestClient.ResponseSpec join(WebTestClient http, String code, String user, String role) {
        String token = DemoTokens.token(user, user, "COORDINATOR".equals(role));
        return http.post().uri("/rts/v1/join").header("Authorization", "Bearer " + token)
                .bodyValue(Map.of("rtsCode", code)).exchange();
    }
}
