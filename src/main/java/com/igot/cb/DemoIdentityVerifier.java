package com.igot.cb;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import com.nimbusds.jwt.PlainJWT;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * Laptop demo only (rts.auth.identity=demo): an unsigned token (alg "none") with the same claims a real iGOT token
 * has (sub, name, roles at rts.auth.jwt.role-claim). Read exactly like a real one, but not checked: anyone can be
 * anyone. The roster still decides who may be PC of a survey.
 */
@Component
@ConditionalOnProperty(name = "rts.auth.identity", havingValue = "demo")
public class DemoIdentityVerifier implements IdentityVerifier {

    private final RtsSettings.Jwt jwt;

    public DemoIdentityVerifier(RtsSettings settings) {
        this.jwt = settings.auth().jwt();
        LoggerFactory.getLogger(DemoIdentityVerifier.class)
                .warn("rts.auth.identity=demo: tokens are NOT verified. Laptop demo only.");
    }

    @Override
    public Mono<Identity> verify(String authorization) {
        String token = IdentityVerifier.bearer(authorization);
        if (token == null) {
            return Mono.error(IdentityVerifier.unauthenticated("Bearer token required"));
        }
        JWTClaimsSet claims;
        try {
            if (!(JWTParser.parse(token) instanceof PlainJWT plain)) {
                return Mono.error(IdentityVerifier.unauthenticated("Demo mode takes an unsigned token (alg none)"));
            }
            claims = plain.getJWTClaimsSet();
        } catch (Exception e) {
            return Mono.error(IdentityVerifier.unauthenticated("Not a token: " + e.getMessage()));
        }
        return Mono.fromCallable(() -> IdentityVerifier.fromClaims(claims, jwt));
    }
}
