package com.igot.cb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Keycloak-style RS256 access tokens checked against the realm public key. */
class JwtIdentityVerifierTest {

    static final String ISSUER = "https://igot.example/auth/realms/sunbird";
    final KeyPair realm = keys();
    final KeyPair other = keys();
    final JwtIdentityVerifier verifier = verifier(realm, ISSUER);

    @Test
    void userIdNameAndRoleComeFromTheToken() {
        var learner = verifier.verify("Bearer " + token(realm, "f:5a8a3f2b:user-123", ISSUER, 60)).block();
        assertEquals("user-123", learner.userId());
        assertEquals("Asha Rao", learner.name());
        assertFalse(learner.coordinator());

        var pc = verifier.verify("Bearer " + token(realm, "f:5a8a3f2b:user-9", ISSUER, 60,
                List.of("offline_access", "PROGRAM_COORDINATOR"))).block();
        assertTrue(pc.coordinator(), "coordinator role in realm_access.roles");
    }

    @Test
    void refusedTokens() {
        assertUnauthenticated("Bearer " + token(other, "f:x:user-123", ISSUER, 60));     // not signed by the realm
        assertUnauthenticated("Bearer " + token(realm, "f:x:user-123", ISSUER, -120));   // expired
        assertUnauthenticated("Bearer " + token(realm, "f:x:user-123", "https://evil", 60)); // other issuer
        assertUnauthenticated("Bearer not-a-jwt");
        assertUnauthenticated(null);
    }

    /** iGOT setup: /keys/<kid> files (accesstoken.publickey.basepath) and iss = sso.url + "realms/" + sso.realm. */
    @Test
    void keyFolderAndSsoIssuer(@TempDir Path keys) throws Exception {
        String pem = "-----BEGIN PUBLIC KEY-----\n" + Base64.getMimeEncoder().encodeToString(realm.getPublic().getEncoded())
                + "\n-----END PUBLIC KEY-----\n";
        Files.writeString(keys.resolve("kid-realm"), pem);
        Files.writeString(keys.resolve("kid-old"), Base64.getEncoder().encodeToString(other.getPublic().getEncoded()));
        Files.writeString(keys.resolve(".hidden"), "not a key");
        RtsSettings.Jwt jwt = new RtsSettings.Jwt(keys.toString(), "", "", "", "https://igot.example/auth/", "sunbird",
                "realm_access.roles", List.of("PROGRAM_COORDINATOR"));
        assertEquals(ISSUER, jwt.expectedIssuer());
        JwtIdentityVerifier v = verifier(jwt);

        assertEquals("user-7", v.verify("Bearer " + token(realm, "kid-realm", "f:x:user-7", ISSUER, 60)).block().userId());
        // the kid picks the key: right key file, but the token was signed by another key
        assertUnauthenticated(v, "Bearer " + token(realm, "kid-old", "f:x:user-7", ISSUER, 60));
        assertUnauthenticated(v, "Bearer " + token(realm, "kid-unknown", "f:x:user-7", ISSUER, 60));
        assertUnauthenticated(v, "Bearer " + token(realm, "kid-realm", "f:x:user-7", "https://evil/auth/realms/sunbird", 60));
    }

    private void assertUnauthenticated(String authorization) {
        assertUnauthenticated(verifier, authorization);
    }

    private static void assertUnauthenticated(JwtIdentityVerifier verifier, String authorization) {
        RtsError e = assertThrows(RtsError.class, () -> verifier.verify(authorization).block());
        assertEquals("UNAUTHENTICATED", e.code);
    }

    private static String token(KeyPair kp, String sub, String iss, int expiresInSec) {
        return token(kp, sub, iss, expiresInSec, List.of("offline_access"));
    }

    private static String token(KeyPair kp, String sub, String iss, int expiresInSec, List<String> roles) {
        return token(kp, null, sub, iss, expiresInSec, roles);
    }

    private static String token(KeyPair kp, String kid, String sub, String iss, int expiresInSec) {
        return token(kp, kid, sub, iss, expiresInSec, List.of("offline_access"));
    }

    private static String token(KeyPair kp, String kid, String sub, String iss, int expiresInSec, List<String> roles) {
        try {
            JWTClaimsSet claims = new JWTClaimsSet.Builder().subject(sub).issuer(iss).claim("name", "Asha Rao")
                    .claim("realm_access", java.util.Map.of("roles", roles))
                    .expirationTime(new Date(System.currentTimeMillis() + expiresInSec * 1000L)).build();
            SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(kid).build(), claims);
            jwt.sign(new RSASSASigner(kp.getPrivate()));
            return jwt.serialize();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static JwtIdentityVerifier verifier(KeyPair kp, String issuer) {
        String publicKey = Base64.getEncoder().encodeToString(kp.getPublic().getEncoded());
        return verifier(new RtsSettings.Jwt("", publicKey, "", issuer, "", "", "realm_access.roles",
                List.of("PROGRAM_COORDINATOR")));
    }

    private static JwtIdentityVerifier verifier(RtsSettings.Jwt jwt) {
        RtsSettings.Auth auth = new RtsSettings.Auth("pass", "jwt", jwt, null, "", false, List.of());
        try {
            return new JwtIdentityVerifier(new RtsSettings(null, null, null, null, auth));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static KeyPair keys() {
        try {
            KeyPairGenerator g = KeyPairGenerator.getInstance("RSA");
            g.initialize(2048);
            return g.generateKeyPair();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
