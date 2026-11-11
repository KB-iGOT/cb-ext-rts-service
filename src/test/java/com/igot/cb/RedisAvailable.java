package com.igot.cb;

import java.net.InetSocketAddress;
import java.net.Socket;

/** Redis tests run only when Redis is on localhost:6379 (docker run -d -p 6379:6379 redis:7); else skipped. */
final class RedisAvailable {

    private RedisAvailable() {
    }

    static boolean up() {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("localhost", 6379), 500);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
