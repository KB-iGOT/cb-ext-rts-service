package com.igot.cb;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** All rts.* settings from application.properties. Survey state is always in Redis, shared by all pods. */
@ConfigurationProperties(prefix = "rts")
public record RtsSettings(Ws ws, Room room, Stats stats, Pod pod, Auth auth) {

    public record Ws(String path, int maxFramePayload, Duration pingInterval, Duration idleTimeout, int sendQueue,
            boolean logConnections) {
    }

    /** startOnPcJoin: the PC connecting creates and starts the session (START_SESSION is then a no-op). */
    public record Room(int maxParticipants, Duration pcWaitTimeout, Duration liveStatsInterval,
            Duration deliveryReportDelay, int maxTextChars, boolean startOnPcJoin, Duration ttl,
            Duration ttlAfterEnd) {
    }

    /**
     * heartbeat: how often this pod tells Redis it is alive and checks for dead pods and paused surveys whose PC wait
     * ran out. deadAfter: a pod with no heartbeat for this long is treated as crashed and its sockets are cleaned up.
     */
    public record Pod(Duration heartbeat, Duration deadAfter) {
    }

    /**
     * mode (LLD §8): pass (identity only from a single-use join pass from POST /rts/v1/join) or dev (identity from
     * the URL, no checks: local and load tests only). identity: how POST /rts/v1/join checks the caller: jwt
     * (Keycloak access token) or demo (Bearer demo:userId, laptop demo only). passTtl: life of a join pass.
     * adminKey: X-Admin-Key for PUT /admin/sessions/{code} (empty: disabled). devClaimCoordinator: a code with no
     * roster gets one on the fly, the first user asking to be PC becomes its PC and anyone may join as a learner
     * (demo only). allowedOrigins: browser origins allowed to open the socket (empty: any).
     */
    public record Auth(String mode, String identity, Jwt jwt, Duration passTtl, String adminKey,
            boolean devClaimCoordinator, List<String> allowedOrigins) {

        public boolean passes() {
            return !"dev".equalsIgnoreCase(mode);
        }
    }

    /**
     * Where the Keycloak realm's public key comes from, first found wins:
     * keysDir: a folder with one key file per realm key, named by its kid (accesstoken.publickey.basepath=/keys/, as
     * every iGOT service has it; the token's kid picks the key); publicKey: one key (base64, as Keycloak shows it);
     * jwksUri: the realm's certs URL. issuer: expected iss; if empty, ssoUrl + "realms/" + ssoRealm (sso.url and
     * sso.realm, as the other iGOT services check it); if those are empty too, not checked. roleClaim: where the
     * token keeps the user's roles, as a dot path (e.g. realm_access.roles); coordinatorRoles: any of these makes the
     * user a coordinator (PC).
     */
    public record Jwt(String keysDir, String publicKey, String jwksUri, String issuer, String ssoUrl, String ssoRealm,
            String roleClaim, List<String> coordinatorRoles) {

        /** The iss a token must carry, or null when not checked. */
        public String expectedIssuer() {
            if (issuer != null && !issuer.isBlank()) {
                return issuer.trim();
            }
            if (ssoUrl == null || ssoUrl.isBlank() || ssoRealm == null || ssoRealm.isBlank()) {
                return null;
            }
            String base = ssoUrl.trim();
            return (base.endsWith("/") ? base : base + "/") + "realms/" + ssoRealm.trim();
        }
    }

    /** logInterval: window for the per-second rates in /stats. logEnabled: also print a STATS line each window. */
    public record Stats(Duration logInterval, boolean logEnabled) {
    }
}
