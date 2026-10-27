package com.example.wspoc;

import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StatsController {

    private final Stats stats;

    public StatsController(Stats stats) {
        this.stats = stats;
    }

    /** Capacity numbers of this pod (sockets, rates, fan-out time, memory per socket, CPU, GC). */
    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return stats.snapshot();
    }

    /** Forces a full GC, then returns /stats: use before reading kbPerConnection. */
    @PostMapping("/stats/gc")
    public Map<String, Object> gc() {
        System.gc();
        return stats.snapshot();
    }
}
