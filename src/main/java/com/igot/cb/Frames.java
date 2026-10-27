package com.igot.cb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Builds the JSON envelope used in both directions (HLD §9.1) and serialises it once to bytes, so a broadcast
 * to 200 sockets encodes the frame one time.
 */
@Component
public class Frames {

    private final ObjectMapper mapper;

    public Frames(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /** Server event (broadcast or unsolicited). ts = server time, used by clients for timers and latency. */
    public byte[] event(String event, String roomId, Object data) {
        ObjectNode n = mapper.createObjectNode();
        n.put("event", event);
        if (roomId != null) {
            n.put("roomId", roomId);
        }
        n.put("ts", System.currentTimeMillis());
        n.set("data", mapper.valueToTree(data == null ? Map.of() : data));
        return bytes(n);
    }

    /** Positive response to a request: "<EVENT>_RESULT", status OK. */
    public byte[] ok(String request, String requestId, String roomId, Object data) {
        return result(request, requestId, roomId, "OK", data, null, null);
    }

    /** Negative response to a request: "<EVENT>_RESULT", status ERROR. */
    public byte[] fail(String request, String requestId, String roomId, String code, String message) {
        return result(request, requestId, roomId, "ERROR", null, code, message);
    }

    /** SUBMIT_ANSWER_RESULT: status RECORDED, or NOT_RECORDED with the reason. */
    public byte[] submitResult(String requestId, String roomId, String pushId, String code, String message) {
        return result("SUBMIT_ANSWER", requestId, roomId, code == null ? "RECORDED" : "NOT_RECORDED",
                map("pushId", pushId), code, message);
    }

    /** ERROR event for frames that are not a valid request. */
    public byte[] error(String requestId, String code, String message) {
        ObjectNode n = mapper.createObjectNode();
        n.put("event", "ERROR");
        if (requestId != null) {
            n.put("requestId", requestId);
        }
        n.put("ts", System.currentTimeMillis());
        n.putObject("error").put("code", code).put("message", message);
        return bytes(n);
    }

    public byte[] raw(Object value) {
        return bytes(mapper.valueToTree(value));
    }

    private byte[] result(String request, String requestId, String roomId, String status, Object data, String code,
            String message) {
        ObjectNode n = mapper.createObjectNode();
        n.put("event", request + "_RESULT");
        if (requestId != null) {
            n.put("requestId", requestId);
        }
        if (roomId != null) {
            n.put("roomId", roomId);
        }
        n.put("ts", System.currentTimeMillis());
        n.put("status", status);
        if (data != null) {
            n.set("data", mapper.valueToTree(data));
        }
        if (code != null) {
            n.putObject("error").put("code", code).put("message", message);
        }
        return bytes(n);
    }

    private byte[] bytes(Object node) {
        try {
            return mapper.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot serialise frame", e);
        }
    }

    /** Ordered map that skips null values, for message bodies. */
    public static Map<String, Object> map(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            if (kv[i + 1] != null) {
                m.put((String) kv[i], kv[i + 1]);
            }
        }
        return m;
    }
}
