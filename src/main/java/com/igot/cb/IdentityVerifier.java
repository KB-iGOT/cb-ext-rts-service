package com.igot.cb;

import com.nimbusds.jwt.JWTClaimsSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import reactor.core.publisher.Mono;

/**
 * Who is calling POST /rts/v1/join: everything comes from the token in the Authorization header, nothing from the
 * request body. Errors with {@link RtsError} UNAUTHENTICATED.
 */
public interface IdentityVerifier {

    /** coordinator: the token carries one of rts.auth.jwt.coordinator-roles. */
    record Identity(String userId, String name, boolean coordinator) {
    }

    Mono<Identity> verify(String authorization);

    static RtsError unauthenticated(String message) {
        return new RtsError("UNAUTHENTICATED", message);
    }

    /** "Bearer xyz" -> "xyz", or null. */
    static String bearer(String authorization) {
        if (authorization == null || !authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
            return null;
        }
        String t = authorization.substring(7).trim();
        return t.isEmpty() ? null : t;
    }

    /**
     * The same reading for every kind of token. userId: "sub" after the last ':' (iGOT/Sunbird sub is
     * "f:&lt;keycloak id&gt;:&lt;userId&gt;"; a plain sub is used as is). name: "name", else "preferred_username", else
     * the userId. coordinator: the roles found at roleClaim (a dot path; a list, or one string of names separated by
     * commas or spaces) include one of coordinatorRoles.
     */
    static Identity fromClaims(JWTClaimsSet c, RtsSettings.Jwt cfg) {
        String sub = c.getSubject();
        if (sub == null || sub.isBlank()) {
            throw unauthenticated("Token has no subject");
        }
        String userId = sub.substring(sub.lastIndexOf(':') + 1);
        String name = text(c.getClaim("name"));
        if (name == null) {
            name = text(c.getClaim("preferred_username"));
        }
        List<String> roles = roles(c.getClaims(), cfg == null ? null : cfg.roleClaim());
        List<String> pcRoles = cfg == null || cfg.coordinatorRoles() == null ? List.of() : cfg.coordinatorRoles();
        boolean coordinator = roles.stream()
                .anyMatch(r -> pcRoles.stream().anyMatch(p -> p.trim().equalsIgnoreCase(r)));
        return new Identity(userId, name == null ? userId : name, coordinator);
    }

    private static String text(Object o) {
        return o instanceof String s && !s.isBlank() ? s : null;
    }

    private static List<String> roles(Map<String, Object> claims, String path) {
        if (path == null || path.isBlank()) {
            return List.of();
        }
        Object node = claims;
        for (String part : path.split("\\.")) {
            if (!(node instanceof Map<?, ?> m)) {
                return List.of();
            }
            node = m.get(part);
        }
        List<String> out = new ArrayList<>();
        if (node instanceof Collection<?> list) {
            list.forEach(r -> out.add(String.valueOf(r)));
        } else if (node instanceof String s) {
            for (String r : s.split("[,\\s]+")) {
                if (!r.isBlank()) {
                    out.add(r);
                }
            }
        }
        return out;
    }
}
