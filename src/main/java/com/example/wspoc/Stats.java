package com.example.wspoc;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.netty.buffer.PooledByteBufAllocator;
import jakarta.annotation.PreDestroy;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;

/**
 * What one pod is doing and what it costs: sockets, frames, fan-out time, memory per socket, CPU cores used, GC.
 * Read it at GET /stats; the same counters are in /actuator/prometheus (rts_*).
 */
@Component
public class Stats {

    private static final Logger log = LoggerFactory.getLogger(Stats.class);

    final LongAdder handshakes = new LongAdder();
    final LongAdder rejected = new LongAdder();
    final LongAdder framesIn = new LongAdder();
    final LongAdder framesOut = new LongAdder();
    final LongAdder bytesOut = new LongAdder();
    final LongAdder emitFailed = new LongAdder();
    final LongAdder slowDropped = new LongAdder();
    final LongAdder idleClosed = new LongAdder();
    final LongAdder pushes = new LongAdder();
    final LongAdder answersRecorded = new LongAdder();
    final LongAdder answersNotRecorded = new LongAdder();
    final LongAdder unexpectedCloses = new LongAdder();
    private final AtomicInteger open = new AtomicInteger();
    private final AtomicInteger peak = new AtomicInteger();
    private final AtomicInteger coordinators = new AtomicInteger();
    private final Timer fanout;

    private final String pod;
    private final long startedAt = System.currentTimeMillis();
    private volatile Supplier<Map<String, Object>> sessions = Map::of;
    private volatile Map<String, Object> rates = Map.of();
    private final Disposable ticker;

    // previous totals for the per-second rates
    private long tickAt = System.nanoTime();
    private long[] prev = new long[6];
    private long prevCpuNanos = cpuNanos();
    private long prevGcMs = gcTimeMs();

    public Stats(MeterRegistry registry, RtsSettings settings, @Value("${pod.name}") String pod) {
        this.pod = pod;
        this.fanout = Timer.builder("rts.fanout")
                .description("Time to queue one broadcast frame to every socket of a survey on this pod")
                .publishPercentiles(0.5, 0.95, 0.99).publishPercentileHistogram().register(registry);
        Gauge.builder("rts.connections", open, AtomicInteger::get).register(registry);
        Gauge.builder("rts.connections.coordinators", coordinators, AtomicInteger::get).register(registry);
        registry.more().counter("rts.frames.in", java.util.List.of(), framesIn);
        registry.more().counter("rts.frames.out", java.util.List.of(), framesOut);
        registry.more().counter("rts.bytes.out", java.util.List.of(), bytesOut);
        registry.more().counter("rts.handshakes", java.util.List.of(), handshakes);
        registry.more().counter("rts.slow.dropped", java.util.List.of(), slowDropped);
        registry.more().counter("rts.idle.closed", java.util.List.of(), idleClosed);
        registry.more().counter("rts.answers.recorded", java.util.List.of(), answersRecorded);
        long every = settings.stats().logInterval().toMillis();
        this.ticker = Schedulers.single().schedulePeriodically(this::tick, every, every, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    void stop() {
        ticker.dispose();
    }

    void sessionSummary(Supplier<Map<String, Object>> s) {
        this.sessions = s;
    }

    void connected(boolean coordinator) {
        peak.accumulateAndGet(open.incrementAndGet(), Math::max);
        if (coordinator) {
            coordinators.incrementAndGet();
        }
    }

    void disconnected(boolean coordinator) {
        open.decrementAndGet();
        if (coordinator) {
            coordinators.decrementAndGet();
        }
    }

    void frameOut(int bytes) {
        framesOut.increment();
        bytesOut.add(bytes);
    }

    void emitFailed() {
        emitFailed.increment();
    }

    void slowDropped() {
        slowDropped.increment();
    }

    void fanout(long nanos) {
        fanout.record(nanos, TimeUnit.NANOSECONDS);
    }

    // ---------------------------------------------------------------- rates and log line

    private synchronized void tick() {
        try {
            long now = System.nanoTime();
            double sec = Math.max(0.001, (now - tickAt) / 1e9);
            long[] cur = {handshakes.sum(), framesIn.sum(), framesOut.sum(), bytesOut.sum(), answersRecorded.sum(),
                    pushes.sum()};
            long cpu = cpuNanos();
            long gc = gcTimeMs();
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("windowSec", round(sec));
            r.put("handshakes", round((cur[0] - prev[0]) / sec));
            r.put("framesIn", round((cur[1] - prev[1]) / sec));
            r.put("framesOut", round((cur[2] - prev[2]) / sec));
            r.put("kbOut", round((cur[3] - prev[3]) / sec / 1024));
            r.put("answers", round((cur[4] - prev[4]) / sec));
            r.put("pushes", round((cur[5] - prev[5]) / sec));
            r.put("cpuCoresUsed", cpu < 0 ? null : round((cpu - prevCpuNanos) / 1e9 / sec));
            r.put("gcMsPerSec", round((gc - prevGcMs) / sec));
            rates = r;
            prev = cur;
            tickAt = now;
            prevCpuNanos = cpu;
            prevGcMs = gc;
            MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            log.info("STATS pod={} conns={} peak={} sessions={} in/s={} out/s={} answers/s={} cpuCores={} "
                    + "heapMb={}/{} directMb={} fanoutP99ms={} slowDropped={} idleClosed={}",
                    pod, open.get(), peak.get(), sessions.get().get("total"), r.get("framesIn"), r.get("framesOut"),
                    r.get("answers"), r.get("cpuCoresUsed"), heap.getUsed() >> 20, heap.getMax() >> 20,
                    directBytes() >> 20, percentile(0.99), slowDropped.sum(), idleClosed.sum());
        } catch (Exception e) {
            log.warn("Stats tick failed: {}", e.toString());
        }
    }

    // ---------------------------------------------------------------- snapshot for GET /stats

    public Map<String, Object> snapshot() {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("pod", pod);
        s.put("uptimeSec", (System.currentTimeMillis() - startedAt) / 1000);
        s.put("cpusVisibleToJvm", Runtime.getRuntime().availableProcessors());
        int conns = open.get();
        s.put("connections", Frames.map("open", conns, "peak", peak.get(), "coordinators", coordinators.get(),
                "participants", conns - coordinators.get()));
        s.put("sessions", sessions.get());
        s.put("ratesPerSec", rates);
        s.put("fanoutMs", Frames.map("p50", percentile(0.5), "p95", percentile(0.95), "p99", percentile(0.99),
                "max", round(fanout.max(TimeUnit.MICROSECONDS) / 1000.0), "count", fanout.count()));
        MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        MemoryUsage nonHeap = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
        long direct = directBytes();
        long netty = PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory();
        s.put("memoryMb", Frames.map("heapUsed", heap.getUsed() >> 20, "heapMax", heap.getMax() >> 20,
                "nonHeapUsed", nonHeap.getUsed() >> 20, "direct", direct >> 20, "nettyPooledDirect", netty >> 20,
                "kbPerConnection", conns == 0 ? null : round((heap.getUsed() + Math.max(direct, netty)) / 1024.0 / conns),
                "note", "kbPerConnection is (heap used + direct) / open sockets; POST /stats/gc first for a clean figure"));
        s.put("gc", Frames.map("collections", gcCount(), "timeMs", gcTimeMs()));
        s.put("threads", ManagementFactory.getThreadMXBean().getThreadCount());
        s.put("totals", Frames.map("handshakes", handshakes.sum(), "rejected", rejected.sum(),
                "framesIn", framesIn.sum(), "framesOut", framesOut.sum(), "mbOut", bytesOut.sum() >> 20,
                "pushes", pushes.sum(), "answersRecorded", answersRecorded.sum(),
                "answersNotRecorded", answersNotRecorded.sum(), "emitFailed", emitFailed.sum(),
                "slowClientsDropped", slowDropped.sum(), "idleClosed", idleClosed.sum(),
                "unexpectedCloses", unexpectedCloses.sum()));
        return s;
    }

    private Double percentile(double p) {
        for (ValueAtPercentile v : fanout.takeSnapshot().percentileValues()) {
            if (v.percentile() == p) {
                return round(v.value(TimeUnit.MICROSECONDS) / 1000.0);
            }
        }
        return null;
    }

    private static long directBytes() {
        for (BufferPoolMXBean b : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
            if ("direct".equals(b.getName())) {
                return b.getMemoryUsed();
            }
        }
        return 0;
    }

    private static long cpuNanos() {
        if (ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean os) {
            return os.getProcessCpuTime();
        }
        return -1;
    }

    private static long gcCount() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
                .mapToLong(GarbageCollectorMXBean::getCollectionCount).sum();
    }

    private static long gcTimeMs() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
                .mapToLong(GarbageCollectorMXBean::getCollectionTime).sum();
    }

    private static double round(double v) {
        return Math.round(v * 100) / 100.0;
    }
}
