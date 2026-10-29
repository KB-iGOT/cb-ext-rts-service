package com.igot.cb;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** All rts.* settings from application.properties. store: redis (shared by all pods, default) or memory (one pod). */
@ConfigurationProperties(prefix = "rts")
public record RtsSettings(String store, Ws ws, Room room, Stats stats) {

    public record Ws(String path, int maxFramePayload, Duration pingInterval, Duration idleTimeout, int sendQueue,
            boolean logConnections) {
    }

    /** startOnPcJoin: the PC connecting creates and starts the session (START_SESSION is then a no-op). */
    public record Room(int maxParticipants, Duration pcWaitTimeout, Duration liveStatsInterval,
            Duration deliveryReportDelay, int maxTextChars, boolean startOnPcJoin, Duration ttl,
            Duration ttlAfterEnd) {
    }

    /** logInterval: window for the per-second rates in /stats. logEnabled: also print a STATS line each window. */
    public record Stats(Duration logInterval, boolean logEnabled) {
    }
}
