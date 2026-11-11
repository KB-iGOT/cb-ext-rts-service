package com.igot.cb;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * Refuses a WebSocket handshake from a web page whose origin is not in rts.auth.allowed-origins (when set), before
 * the upgrade: a page on another site must not open sockets with a user's pass. The pass itself is checked by
 * {@link RtsWebSocketHandler} after the upgrade, so a bad pass gets close code 4401 (LLD §3.2).
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class OriginFilter implements WebFilter {

    private static final Logger log = LoggerFactory.getLogger(OriginFilter.class);

    private final String path;
    private final List<String> origins;

    public OriginFilter(RtsSettings settings) {
        this.path = settings.ws().path();
        List<String> o = settings.auth().allowedOrigins();
        this.origins = o == null ? List.of() : o.stream().map(String::trim).filter(s -> !s.isEmpty()).toList();
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String origin = exchange.getRequest().getHeaders().getOrigin();
        if (origins.isEmpty() || origin == null || origins.contains(origin)
                || !path.equals(exchange.getRequest().getPath().pathWithinApplication().value())) {
            return chain.filter(exchange);
        }
        log.info("WebSocket refused: origin {} from {}", origin, exchange.getRequest().getRemoteAddress());
        var res = exchange.getResponse();
        res.setStatusCode(HttpStatus.FORBIDDEN);
        res.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        byte[] body = "{\"error\":{\"code\":\"ORIGIN_NOT_ALLOWED\"}}".getBytes(StandardCharsets.UTF_8);
        return res.writeWith(Mono.just(res.bufferFactory().wrap(body)));
    }
}
