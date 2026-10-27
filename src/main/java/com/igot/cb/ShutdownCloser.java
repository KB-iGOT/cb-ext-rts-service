package com.igot.cb;

import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * On pod shutdown, closes every socket with 1001 (going away) so clients reconnect to another pod. Runs before
 * the web server's graceful shutdown, which would otherwise wait its full timeout for sockets that never end.
 */
@Component
public class ShutdownCloser implements SmartLifecycle {

    private final SessionManager manager;
    private volatile boolean running;

    public ShutdownCloser(SessionManager manager) {
        this.manager = manager;
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        running = false;
        manager.closeAll();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}
