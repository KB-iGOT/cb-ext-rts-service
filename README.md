# cb-ext-rts-service

Real Time Survey (RTS) WebSocket server that keeps **everything in one pod's memory (no Redis)**. Its job is to
measure how many sockets and how much survey traffic **one pod** can carry, so the technology and the pod size can
be decided. It speaks the same protocol as the HLD (§9) and `rts-realtime-service`, so the same load tools drive both.

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

Load comes from the load generator in `~/IdeaProjects/rts-realtime-service` (same protocol; dashboard + API,
resizable while running). `scripts/capacity_ramp.py` steps it up and writes one CSV row per step:

```bash
# terminal 1: pod-sized server (1 core, 1.5 GB heap)
POD_NAME=pod-1cpu PORT=8083 taskset -c 0 java -Xmx1500m -jar target/cb-ext-rts-service.jar
# terminal 2: load generator (separate JVM)
cd ~/IdeaProjects/rts-realtime-service && java -Xmx4g -jar target/rts-realtime-service.jar --spring.profiles.active=loadgen
# terminal 3: ramp
python3 scripts/capacity_ramp.py --server http://localhost:8083 --loadgen http://localhost:7017 \
    --ws-url ws://localhost:8083/ws --start 20 --step 40 --max 130 --cpu-limit 1
```

A step fails when: connect failures > 0.5 %, any socket dropped, push → learner p95 > 500 ms, CPU > 80 % of the
limit, heap > 85 %, or a slow client was dropped. One load generator on one IP opens about 28k sockets to one
address; locally that is the ceiling, in Kubernetes use `k8s/rts-loadgen.yaml` (wider port range, more replicas).

## Kubernetes

`k8s/cb-ext-rts-service.yaml` (1 replica, requests = limits = 1 CPU / 2 Gi, `POD_NAME` from the pod name) and
`k8s/rts-loadgen.yaml` (load generator inside the cluster). See the steps in the conversation notes / team page.

## Files

| File | Role |
|---|---|
| `RtsWebSocketHandler` | one call per socket: read identity, join, frames in order, clean up |
| `SessionManager` | the protocol: state changes under the survey's lock, sends after; timers, live stats, ping sweep |
| `Session`, `Conn` | survey state (status, current question, answers); one socket with its bounded send queue |
| `Frames` | JSON envelope, serialised once per broadcast |
| `Stats`, `StatsController` | capacity numbers |
| `ShutdownCloser` | closes sockets with 1001 on shutdown |
