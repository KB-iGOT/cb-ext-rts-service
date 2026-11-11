package com.igot.cb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.WsClient.Client;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Join passes (LLD §3): identity and role come only from a single-use pass that POST /rts/v1/join issued after
 * reading userId, name and role from the caller's token and checking the role against the survey's roster. Demo
 * (unsigned) tokens here; the signature check is {@link JwtIdentityVerifierTest}.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"rts.auth.mode=pass", "rts.auth.identity=demo",
                "rts.auth.admin-key=test-admin", "rts.room.start-on-pc-join=true"})
class JoinPassTest {

    @BeforeAll
    static void needsRedis() {
        Assumptions.assumeTrue(RedisAvailable.up(), "Redis not running on localhost:6379");
    }

    @LocalServerPort
    int port;

    @Autowired
    ObjectMapper mapper;

    WebTestClient http;
    final String code = "A" + UUID.randomUUID().toString().substring(0, 6);

    @BeforeEach
    void roster() {
        http = WebTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
        http.put().uri("/admin/sessions/" + code).header("X-Admin-Key", "test-admin")
                .bodyValue(Map.of("coordinators", List.of("meena"), "participants", List.of("asha", "ravi")))
                .exchange().expectStatus().isOk();
    }

    @Test
    void noPassNoSession() throws Exception {
        // the URL identity of dev mode, a made-up pass, or no pass: ERROR UNAUTHORIZED, then close 4401
        for (String query : List.of("rtsCode=" + code + "&userId=meena&role=COORDINATOR", "pass=made-up", "")) {
            Client c = new Client("ws://localhost:" + port + "/ws?" + query, mapper);
            assertEquals("UNAUTHORIZED", c.next("ERROR").at("/error/code").asText(), query);
            assertTrue(c.closed.await());
            assertEquals(4401, c.closeCode, query);
        }
    }

    @Test
    void rosterDecidesWhoGetsAPass() {
        JsonNode ok = status(join("meena", "COORDINATOR"), 200);
        assertEquals("/ws", ok.path("path").asText());
        assertEquals("COORDINATOR", ok.path("role").asText());
        assertTrue(ok.path("expiresAt").asLong() > System.currentTimeMillis());
        status(join("asha", "PARTICIPANT"), 200);
        // a coordinator role in the token, but not the PC of this survey: joins as a learner if allowed to
        assertEquals("PARTICIPANT", status(join("asha", "COORDINATOR"), 200).path("role").asText());
        assertEquals("NOT_ENROLLED", status(join("other-pc", "COORDINATOR"), 403).at("/error/code").asText());
        assertEquals("NOT_ENROLLED", status(join("intruder", "PARTICIPANT"), 403).at("/error/code").asText());
        assertEquals("RTS_NOT_ENABLED", status(http.post().uri("/rts/v1/join")
                .header("Authorization", "Bearer " + DemoTokens.token("asha", "Asha", false))
                .bodyValue(Map.of("rtsCode", "NOPE" + code)).exchange(), 404).at("/error/code").asText());
        assertEquals("UNAUTHENTICATED", status(http.post().uri("/rts/v1/join").bodyValue(Map.of("rtsCode", code))
                .exchange(), 401).at("/error/code").asText());
        http.put().uri("/admin/sessions/" + code).header("X-Admin-Key", "wrong")
                .bodyValue(Map.of("coordinators", List.of("asha"))).exchange().expectStatus().isForbidden();
    }

    @Test
    void userNameAndRoleComeFromTheTokenNotTheRequest() {
        // asha's learner token, with a body claiming to be meena, the PC, under another name: all ignored
        JsonNode p = status(http.post().uri("/rts/v1/join")
                .header("Authorization", "Bearer " + DemoTokens.token("asha", "Asha Rao", false))
                .bodyValue(Map.of("rtsCode", code, "userId", "meena", "role", "COORDINATOR", "name", "Fake"))
                .exchange(), 200);
        assertEquals("asha", p.path("userId").asText(), "userId from the token's sub");
        assertEquals("Asha Rao", p.path("name").asText(), "name from the token");
        assertEquals("PARTICIPANT", p.path("role").asText(), "role from the token's roles");
        // the old "demo:<id>" form is not a token
        status(http.post().uri("/rts/v1/join").header("Authorization", "Bearer demo:meena")
                .bodyValue(Map.of("rtsCode", code)).exchange(), 401);
    }

    @Test
    void identityComesOnlyFromThePassWhichIsSingleUse() throws Exception {
        Client pc = connect(pass(join("meena", "COORDINATOR")));
        assertEquals("COORDINATOR", pc.next("SESSION_STATE").at("/data/role").asText());

        // URL parameters claiming another identity are ignored
        String p = pass(join("asha", "PARTICIPANT"));
        Client asha = new Client("ws://localhost:" + port + "/ws?pass=" + p + "&userId=meena&role=COORDINATOR", mapper);
        assertEquals("PARTICIPANT", asha.next("SESSION_STATE").at("/data/role").asText());
        assertEquals("asha", pc.next("PARTICIPANT_JOINED").at("/data/userId").asText());

        // the same pass again: refused
        Client again = connect(p);
        assertEquals("UNAUTHORIZED", again.next("ERROR").at("/error/code").asText());
        assertTrue(again.closed.await());
        assertEquals(4401, again.closeCode);
    }

    @Test
    void rejoinTheSameLiveSurveyWithANewPass() throws Exception {
        Client pc = connect(pass(join("meena", "COORDINATOR")));
        String roomId = pc.next("SESSION_STATE").at("/data/roomId").asText();
        Client asha = connect(pass(join("asha", "PARTICIPANT")));
        asha.next("SESSION_STATE");
        pc.next("PARTICIPANT_JOINED");

        pc.send("PUSH_QUESTION", Map.of("question", Map.of("type", "SINGLE", "text", "Pick",
                "options", new String[] {"A", "B"})));
        String pushId = pc.next("PUSH_QUESTION_RESULT").at("/data/pushId").asText();
        asha.next("QUESTION_PUSHED");
        asha.send("SUBMIT_ANSWER", Map.of("pushId", pushId, "choice", new int[] {0}));
        asha.next("SUBMIT_ANSWER_RESULT");

        // network drop: a new socket with a new pass; same person, same room, same answer
        asha.close();
        pc.next("PARTICIPANT_LEFT");
        JsonNode s = connect(pass(join("asha", "PARTICIPANT"))).next("SESSION_STATE");
        assertEquals(roomId, s.at("/data/roomId").asText(), "no new session");
        assertEquals("RECORDED", s.at("/data/myAnswer/status").asText());
        assertEquals(pushId, s.at("/data/currentPush/pushId").asText());
        assertTrue(s.at("/data/reconnectTicket").isMissingNode(), "no reconnect ticket any more");
        JsonNode back = pc.next("PARTICIPANT_JOINED");
        assertEquals("asha", back.at("/data/userId").asText());
        assertEquals(1, back.at("/data/connected").asInt(), "counted once");
    }

    // ---------------------------------------------------------------- helpers

    /** Only the code in the body; the role is in the (demo) token. */
    private WebTestClient.ResponseSpec join(String userId, String role) {
        String token = DemoTokens.token(userId, userId, "COORDINATOR".equals(role));
        return http.post().uri("/rts/v1/join").header("Authorization", "Bearer " + token)
                .bodyValue(Map.of("rtsCode", code)).exchange();
    }

    private JsonNode status(WebTestClient.ResponseSpec r, int status) {
        byte[] body = r.expectStatus().isEqualTo(status).expectBody().returnResult().getResponseBody();
        try {
            return mapper.readTree(body);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private String pass(WebTestClient.ResponseSpec r) {
        return status(r, 200).path("pass").asText();
    }

    private Client connect(String pass) {
        return new Client("ws://localhost:" + port + "/ws?pass=" + pass, mapper);
    }
}
