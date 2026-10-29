package com.igot.cb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.igot.cb.WsClient.Client;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

/** Default settings: the PC joining creates the session; 200 participants per session. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "rts.store=memory")
class PcJoinTest {

    /** Unique per test run: the Redis store keeps keys between runs. */
    final String run = "T" + java.util.UUID.randomUUID().toString().substring(0, 6) + "-";

    @LocalServerPort
    int port;

    @Autowired
    ObjectMapper mapper;

    @Autowired
    Stats stats;

    @Test
    void pcJoinCreatesSessionAndCapIs200() throws Exception {
        Client early = connect(run + "J1", "early", "PARTICIPANT");
        assertEquals("WAITING", early.next("SESSION_STATE").at("/data/status").asText());

        Client pc = connect(run + "J1", "pc", "COORDINATOR");
        JsonNode state = pc.next("SESSION_STATE");
        assertEquals("LIVE", state.at("/data/status").asText(), "PC joining starts the session");
        String roomId = state.at("/data/roomId").asText();
        assertEquals(roomId, early.next("SESSION_STARTED").at("/data/roomId").asText());

        // START_SESSION still accepted, but does not create a second room
        pc.send("START_SESSION", Map.of());
        JsonNode start = pc.next("START_SESSION_RESULT");
        assertFalse(start.at("/data/created").asBoolean());
        assertEquals(roomId, start.at("/data/roomId").asText());

        // 199 more learners = 200 participants; the 201st is refused
        List<Client> learners = new ArrayList<>();
        for (int i = 1; i <= 199; i++) {
            learners.add(connect(run + "J1", "u" + i, "PARTICIPANT"));
        }
        for (Client l : learners) {
            assertEquals("LIVE", l.next("SESSION_STATE").at("/data/status").asText());
        }
        Client extra = connect(run + "J1", "u200", "PARTICIPANT");
        assertEquals("PARTICIPANT_LIMIT_REACHED", extra.next("ERROR").at("/error/code").asText());
        assertTrue(extra.closed.await(), "refused socket is closed");

        // the same user on a second tab is not a new participant
        Client secondTab = connect(run + "J1", "u1", "PARTICIPANT");
        assertEquals("LIVE", secondTab.next("SESSION_STATE").at("/data/status").asText());

        // a different session on the same pod has its own 200
        Client pc2 = connect(run + "J2", "pc2", "COORDINATOR");
        assertEquals("LIVE", pc2.next("SESSION_STATE").at("/data/status").asText());
        Client other = connect(run + "J2", "x1", "PARTICIPANT");
        assertEquals("LIVE", other.next("SESSION_STATE").at("/data/status").asText());

        assertEquals(2, stats.snapshot().get("sessions") instanceof Map<?, ?> m ? m.get("live") : null);
    }

    private Client connect(String code, String user, String role) {
        return new Client("ws://localhost:" + port + "/ws?rtsCode=" + code + "&userId=" + user + "&role=" + role,
                mapper);
    }
}
