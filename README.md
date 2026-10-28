# cb-ext-rts-service

A Real Time Survey (RTS) lets a Program Coordinator ask learners in a classroom session a few quick questions and see
their answers as they come in. The coordinator controls questions, answer timing, and results.

Real Time Survey (RTS) WebSocket server that keeps **everything in one pod's memory (no Redis)**. Its job is to
measure how many sockets and how much survey traffic **one pod** can carry, so the technology and the pod size can
be decided. It speaks the protocol of the HLD (§9). The client-side load generator is in `loadgen/`.
Java 17 · Spring Boot 3.4 · WebFlux on Reactor Netty.

## What it does

| Use case | Messages |
|---|---|
| Learner opens the survey before the PC starts | gets `SESSION_STATE {status: WAITING}` |
| PC starts | `START_SESSION` → everyone waiting gets `SESSION_STARTED` (a second Start does not create a second room) |
| PC pushes a question (optional timer) | `PUSH_QUESTION` → learners get `QUESTION_PUSHED` (no correct answer); the PC gets `PUSH_DELIVERY {delivered, failed}` 2 s later |
| Learner answers | `SUBMIT_ANSWER` → `RECORDED`, or `NOT_RECORDED` with `ANSWERS_STOPPED`, `ALREADY_ANSWERED`, `INVALID_ANSWER`, `NOT_CURRENT_QUESTION`, `NO_ACTIVE_QUESTION` |
| PC live view | `LIVE_STATS` / `LIVE_RESULTS` every second while numbers change; `LIVE_TEXT_ANSWERS` for free text |
| PC controls | `STOP_ANSWERS`, `RESUME_ANSWERS`, `SHOW_RESULTS`, `REVEAL_ANSWER` (graded only) |
| Learner joins a running survey | PC gets `PARTICIPANT_JOINED {userId, name, pod, connected}` (learners who joined while WAITING are counted in the PC's `SESSION_STATE.connected`) |
| Learner leaves or drops | PC gets `PARTICIPANT_LEFT {reason: LEFT / CONNECTION_LOST}` |
| PC's network drops | survey `PAUSED`, learners get `COORDINATOR_AWAY`; PC reconnects → `COORDINATOR_BACK`; no return within `rts.room.pc-wait-timeout` → ended |
| PC ends or leaves | `END_SESSION` / `LEAVE` → everyone gets `SESSION_ENDED`, then the sockets are closed |
| Limits | 200 participants per survey (close 4429); slow reader dropped after 256 queued frames (close 1008); no frame for 50 s → closed as lost; pings every 20 s |
| Pod shutdown | every socket closed with 1001 so clients reconnect |

Identity comes from the URL (no auth, this is a capacity tool):
`ws://host:8080/ws?rtsCode=482913&userId=asha&role=PARTICIPANT|COORDINATOR&name=Asha`.
The old `?room=&clientId=&role=trainer|learner` form still works for `scripts/poc_test.py` and
`scripts/pod_load_test.py` (plain `QUESTION` relay + a `{"type":"CONNECTED","pod":...}` hello).

## Build, test, run

```bash
mvn clean package                      # runs SurveyFlowTest (whole survey over real sockets)
POD_NAME=local-1 PORT=8081 java -jar target/cb-ext-rts-service.jar
```

`POD_NAME` is only a label for logs and `/stats` (default `local-1`). In Kubernetes it is set from the pod name.

## Measuring one pod

| Endpoint | What |
|---|---|
| `GET /stats` | sockets (open, peak), surveys by status, frames in/out per second, CPU cores used, heap/direct memory, **KB per connection**, fan-out time p50/p95/p99, GC, drops |
| `POST /stats/gc` | full GC, then `/stats` (clean KB-per-connection figure) |
| `GET /actuator/prometheus` | the same as `rts_*` metrics plus JVM metrics, for Grafana |
| log | one `STATS ...` line every 10 s |

Load comes from the client-side load generator in [`loadgen/`](loadgen/README.md), a separate Maven project
(not built by the root pom). `scripts/capacity_ramp.py` steps it up and writes one CSV row per step:

```bash
mvn -f loadgen/pom.xml package
# terminal 1: pod-sized server (1 core, 1.5 GB heap)
POD_NAME=pod-1cpu PORT=8080 taskset -c 0 java -Xmx1500m -jar target/cb-ext-rts-service.jar
# terminal 2: load generator (separate JVM)
java -Xmx6g -jar loadgen/target/cb-ext-rts-loadgen.jar
# terminal 3: ramp (reads the pod's /stats through the load generator)
python3 scripts/capacity_ramp.py --loadgen http://localhost:7017 --ws-url ws://localhost:8080/ws \
    --start 20 --step 30 --max 140 --cpu-limit 1
```

A step fails when: connect failures > 0.5 %, any socket dropped, push → learner p95 > 500 ms, CPU > 80 % of the
limit, heap > 85 %, or a slow client was dropped. One source IP gets about 28k client ports to one address; past
that locally add `--source-ip 127.0.0.1 --source-ip 127.0.0.2 ...`, in Kubernetes see below.

Measured on a laptop (28 Oct 2026), 1 core, 1.5 GB heap, a question every 30 s, MIX types:

| Live sessions × 200 | Sockets | push p95 / p99 | answer p95 | CPU cores | Heap used | Verdict |
|---|---|---|---|---|---|---|
| 140 | 28,000 | 4 / 133 ms | 1 ms | 0.17 | 522 MB | OK |
| 260 | 52,000 | 5 / 238 ms | 2 ms | 0.21 | 1,033 MB | OK |
| 320 | 64,000 | 40 / 378 ms | 68 ms | 0.23 | 1,174 MB | OK, latency rising |
| 380 | 76,000 | 220 / 508 ms | 384 ms | 0.34 | 1,232 MB (85 %) | GC 10 % of time |

Memory, not CPU, is the limit: about 17–20 KB per socket, so heap size decides sessions per pod.

## Kubernetes

1. Build and push the two images: the service (Jenkins, as usual) and the load generator
   (`mvn -f loadgen/pom.xml package && docker build -t <registry>/cb-ext-rts-loadgen:poc loadgen/`).
2. `kubectl apply -f k8s/cb-ext-rts-loadgen.yaml` (same namespace as the service).
3. `kubectl port-forward deploy/cb-ext-rts-loadgen 7017:7017` (the only port-forward needed).
4. `python3 scripts/capacity_ramp.py --loadgen http://localhost:7017
   --ws-url ws://cb-ext-rts-service.<namespace>.svc.cluster.local:8080/ws --cpu-limit 1 --csv dev.csv`

`k8s/cb-ext-rts-service.yaml` is a plain manifest for the service itself (1 replica, requests = limits =
1 CPU / 2 Gi, `POD_NAME` from the pod name) if it is not deployed through the igot-deploy chart.
## Files

| File | Role |
|---|---|
| `RtsWebSocketHandler` | one call per socket: read identity, join, frames in order, clean up |
| `SessionManager` | the protocol: state changes under the survey's lock, sends after; timers, live stats, ping sweep |
| `Session`, `Conn` | survey state (status, current question, answers); one socket with its bounded send queue |
| `Frames` | JSON envelope, serialised once per broadcast |
| `Stats`, `StatsController` | capacity numbers |
| `ShutdownCloser` | closes sockets with 1001 on shutdown |
