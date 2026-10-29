package com.igot.cb;

import java.util.function.Consumer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/** Single pod: an event is delivered straight to this pod's sockets. */
@Component
@ConditionalOnProperty(name = "rts.store", havingValue = "memory")
public class MemorySessionBus implements SessionBus {

    private volatile Consumer<Event> listener = e -> { };

    @Override
    public void onEvent(Consumer<Event> listener) {
        this.listener = listener;
    }

    @Override
    public Mono<Void> subscribe(String code) {
        return Mono.empty();
    }

    @Override
    public void unsubscribe(String code) {
    }

    @Override
    public Mono<Void> publish(Event event) {
        return Mono.fromRunnable(() -> listener.accept(event));
    }
}
