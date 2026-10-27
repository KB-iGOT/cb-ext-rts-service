package com.example.wspoc;

import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;
import org.springframework.web.reactive.socket.server.upgrade.ReactorNettyRequestUpgradeStrategy;
import reactor.netty.http.server.WebsocketServerSpec;

@Configuration
public class WebSocketConfig {

    @Bean
    HandlerMapping webSocketMapping(RtsWebSocketHandler handler, RtsSettings settings) {
        // Order -1 so the WebSocket path wins over controllers and static resources.
        return new SimpleUrlHandlerMapping(Map.of(settings.ws().path(), handler), -1);
    }

    @Bean
    WebSocketHandlerAdapter webSocketHandlerAdapter(RtsSettings settings) {
        ReactorNettyRequestUpgradeStrategy strategy = new ReactorNettyRequestUpgradeStrategy(
                () -> WebsocketServerSpec.builder().maxFramePayloadLength(settings.ws().maxFramePayload()));
        return new WebSocketHandlerAdapter(new HandshakeWebSocketService(strategy));
    }
}
