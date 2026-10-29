package com.igot.cb;

import java.util.List;
import java.util.Map;

/** Result bodies for RESULTS_SHOWN / LIVE_RESULTS, the same shape from either store. */
final class Results {

    static final int TOP_WORDS = 30;

    private Results() {
    }

    static Map<String, Object> choice(String pushId, long total, long[] counts) {
        return Frames.map("pushId", pushId, "total", total, "counts", counts);
    }

    static Map<String, Object> words(String pushId, long total, List<List<Object>> top) {
        return Frames.map("pushId", pushId, "total", total, "top", top);
    }

    static Map<String, Object> text(String pushId, long total) {
        return Frames.map("pushId", pushId, "total", total);
    }
}
