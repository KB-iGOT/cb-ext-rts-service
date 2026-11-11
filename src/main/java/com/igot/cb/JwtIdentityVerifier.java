package com.igot.cb;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import com.nimbusds.jose.jwk.JWK;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Keycloak access token (RS256): signature from the realm's public keys (the /keys/ folder every iGOT service has,
 * a single public key, or the JWKS URL), expiry, and the issuer (sso.url + "realms/" + sso.realm) if set.
 * userId, name and role are read from its claims by {@link IdentityVerifier#fromClaims}.
 */
@Component
@ConditionalOnProperty(name = "rts.auth.identity", havingValue = "jwt", matchIfMissing = true)
public class JwtIdentityVerifier implements IdentityVerifier {

    private static final Logger log = LoggerFactory.getLogger(JwtIdentityVerifier.class);

    private final DefaultJWTProcessor<SecurityContext> processor = new DefaultJWTProcessor<>();
    private final boolean configured;
    private final RtsSettings.Jwt jwt;

    public JwtIdentityVerifier(RtsSettings settings) throws Exception {
        RtsSettings.Jwt jwt = settings.auth().jwt();
        this.jwt = jwt;
        JWKSource<SecurityContext> keys = null;
        String source = null;
        List<JWK> fromDir = jwt == null ? List.of() : readKeysDir(jwt.keysDir());
        if (!fromDir.isEmpty()) {
            keys = new ImmutableJWKSet<>(new JWKSet(fromDir));
            source = "key folder " + jwt.keysDir() + " (kid " + fromDir.stream().map(JWK::getKeyID).toList() + ")";
        } else if (jwt != null && jwt.publicKey() != null && !jwt.publicKey().isBlank()) {
            keys = new ImmutableJWKSet<>(new JWKSet(new RSAKey.Builder(rsaKey(jwt.publicKey())).build()));
            source = "public key";
        } else if (jwt != null && jwt.jwksUri() != null && !jwt.jwksUri().isBlank()) {
            keys = JWKSourceBuilder.create(new URL(jwt.jwksUri())).build();
            source = "JWKS " + jwt.jwksUri();
        }
        configured = keys != null;
        if (!configured) {
            // fail on use, not on start, so a pod in url mode or without auth config still starts
            if (settings.auth().passes()) {
                log.error("rts.auth.identity=jwt but no realm key: accesstoken.publickey.basepath has no key file and "
                        + "neither rts.auth.jwt.public-key nor jwks-uri is set: every POST /rts/v1/join will be refused");
            }
            return;
        }
        processor.setJWSKeySelector(new JWSVerificationKeySelector<>(JWSAlgorithm.RS256, keys));
        String issuer = jwt.expectedIssuer();
        log.info("Keycloak token check: {}, issuer {}", source, issuer == null ? "not checked" : issuer);
        JWTClaimsSet exact = issuer == null ? new JWTClaimsSet.Builder().build()
                : new JWTClaimsSet.Builder().issuer(issuer).build();
        processor.setJWTClaimsSetVerifier(new DefaultJWTClaimsVerifier<>(null, exact, Set.of("sub", "exp")));
    }

    @Override
    public Mono<Identity> verify(String authorization) {
        String token = IdentityVerifier.bearer(authorization);
        if (token == null) {
            return Mono.error(IdentityVerifier.unauthenticated("Bearer token required"));
        }
        if (!configured) {
            return Mono.error(IdentityVerifier.unauthenticated("Token verification is not configured"));
        }
        // the JWKS source may fetch over HTTP the first time: not on an event-loop thread
        return Mono.fromCallable(() -> {
            JWTClaimsSet c;
            try {
                c = processor.process(token, null);
            } catch (Exception e) {
                throw IdentityVerifier.unauthenticated("Invalid token: " + e.getMessage());
            }
            return IdentityVerifier.fromClaims(c, jwt);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Every key file in the folder, named by its kid, the way the other iGOT services read accesstoken.publickey.basepath.
     * Hidden entries are skipped (a Kubernetes secret mount also holds ..data links).
     */
    static List<JWK> readKeysDir(String dir) {
        List<JWK> out = new ArrayList<>();
        if (dir == null || dir.isBlank()) {
            return out;
        }
        Path folder = Path.of(dir.trim());
        if (!Files.isDirectory(folder)) {
            log.warn("Key folder {} does not exist", folder);
            return out;
        }
        try (Stream<Path> files = Files.list(folder)) {
            files.filter(f -> !f.getFileName().toString().startsWith(".") && Files.isRegularFile(f)).sorted()
                    .forEach(f -> {
                        try {
                            out.add(new RSAKey.Builder(rsaKey(Files.readString(f, StandardCharsets.UTF_8)))
                                    .keyID(f.getFileName().toString()).build());
                        } catch (Exception e) {
                            log.error("Key file {} is not an RSA public key: {}", f, e.getMessage());
                        }
                    });
        } catch (IOException e) {
            log.error("Cannot read key folder {}: {}", folder, e.getMessage());
        }
        return out;
    }

    /** A base64 X.509 RSA public key, with or without the PEM header lines. */
    static RSAPublicKey rsaKey(String text) throws Exception {
        String b64 = text.replaceAll("-----[A-Z ]+-----", "").replaceAll("\\s", "");
        return (RSAPublicKey) KeyFactory.getInstance("RSA")
                .generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(b64)));
    }
}
