package com.igot.cb;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** Unsigned demo tokens with the claims an iGOT token has (default role claim: realm_access.roles). */
final class DemoTokens {

    private DemoTokens() {
    }

    static String token(String userId, String name, boolean coordinator) {
        String roles = coordinator ? "[\"PROGRAM_COORDINATOR\"]" : "[]";
        String claims = "{\"sub\":\"f:kc-1:" + userId + "\",\"name\":\"" + name + "\",\"realm_access\":{\"roles\":"
                + roles + "}}";
        return b64("{\"alg\":\"none\",\"typ\":\"JWT\"}") + "." + b64(claims) + ".";
    }

    private static String b64(String s) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }
}
