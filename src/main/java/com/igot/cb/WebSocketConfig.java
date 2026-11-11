package com.igot.cb;

import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.config.WebFluxConfigurer;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.server.WebSocketService;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import org.springframework.web.reactive.socket.server.upgrade.ReactorNettyRequestUpgradeStrategy;
import reactor.netty.http.server.WebsocketServerSpec;

@Configuration
public class WebSocketConfig implements WebFluxConfigurer {

    private final RtsSettings settings;

    public WebSocketConfig(RtsSettings settings) {
        this.settings = settings;
    }

    @Bean
    HandlerMapping webSocketMapping(RtsWebSocketHandler handler) {
        // Order -1 so the WebSocket path wins over controllers and static resources.
        return new SimpleUrlHandlerMapping(Map.of(settings.ws().path(), handler), -1);
    }

    /** The WebSocket service of WebFlux's own handler adapter (a separate adapter bean would not be the one used). */
    @Override
    public WebSocketService getWebSocketService() {
        ReactorNettyRequestUpgradeStrategy strategy = new ReactorNettyRequestUpgradeStrategy(
                () -> WebsocketServerSpec.builder().maxFramePayloadLength(settings.ws().maxFramePayload()));
        return new HandshakeWebSocketService(strategy);
    }
}
