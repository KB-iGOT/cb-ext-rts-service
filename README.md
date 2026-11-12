# cb-ext-rts-service

A Real Time Survey (RTS) lets a Program Coordinator ask learners in a classroom session a few quick questions and see
their answers as they come in. The coordinator controls questions, answer timing, and results.

Real Time Survey (RTS) WebSocket server. Survey state lives in **Redis**, shared by every pod, so any pod serves
any PC or learner and a pod restart does not lose a survey. Redis is required: there is no in-memory mode.
It speaks the protocol of the HLD (§9). The client-side load generator is in `loadgen/`.
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

## Who is connecting: join passes (LLD §3)

The role is never taken from the client. `rts.auth.mode=pass` (default):

1. `POST /rts/v1/join {rtsCode}` with `Authorization: Bearer <Keycloak access token>`. Only the code is sent:
   **userId, name and role come from the token** (RS256, realm public key `RTS_AUTH_JWT_PUBLIC_KEY` or JWKS URL
   `RTS_AUTH_JWT_JWKS_URI`, issuer `RTS_AUTH_JWT_ISSUER`). userId = `sub` after the last `:`; name = `name`, else
   `preferred_username`; role = COORDINATOR if the roles at `RTS_AUTH_JWT_ROLE_CLAIM` (default
   `realm_access.roles`) include one of `RTS_AUTH_JWT_COORDINATOR_ROLES` (default `PROGRAM_COORDINATOR`; confirm
   both against a real iGOT token), else PARTICIPANT. Then the survey's roster: a coordinator is PC only of their
   own survey (in another PC's survey they join as a learner, if allowed); a participant must be one of its
   learners. Reply `{pass, path, expiresAt, userId, name, role}`; errors 400 `BAD_REQUEST`, 401 `UNAUTHENTICATED`,
   403 `NOT_ENROLLED`, 404 `RTS_NOT_ENABLED`.
2. `wss://host/ws?pass=...`. The pass is used up when the socket opens (Redis `GETDEL`, any pod); a missing, used or
   expired pass gets `ERROR UNAUTHORIZED`, then close **4401**, so the app asks for a new pass. Identity, survey and
   role come only from the pass.
3. Every connection uses a new pass: a reconnect to the same live survey is step 1 again, and the server matches it
   to the same person by user id (same answers, current state; no new session).

Rosters stand in for the service that owns RTS sessions (PC of the session, learners of its batch):
`PUT /admin/sessions/{code}` with `X-Admin-Key: $RTS_AUTH_ADMIN_KEY` and
`{"coordinators": ["u1"], "participants": ["u2", "u3"], "anyLearner": false}`. Demo settings (docker compose):
`RTS_AUTH_IDENTITY=demo` takes an unsigned token (alg `none`) with the same claims, read the same way but not
checked (the demo page builds it from the name and role you pick), and `RTS_AUTH_DEV_CLAIM_COORDINATOR=true`
makes the first person asking to be PC of an unknown code its PC. `RTS_AUTH_ALLOWED_ORIGINS` limits which web
origins may open a socket.

`rts.auth.mode=dev` (`RTS_AUTH_MODE=dev`) is for local and load tests only: identity from the URL with no checks,
`ws://host:8080/ws?rtsCode=482913&userId=asha&role=PARTICIPANT|COORDINATOR&name=Asha`. The load generator,
`scripts/capacity_ramp.py`, `scripts/k6-rts-load.js` and the old `?room=&clientId=&role=trainer|learner` form
(`scripts/poc_test.py`, `scripts/pod_load_test.py`) need it.

## Try it with friends

`docker compose up --build -d`, then open `http://<your-ip>:8000` (nginx spreading people over two pods; `:8080`/`:8081` are the pods directly): a page to join as the coordinator or a learner.
Step-by-step test with 10+ people (browser or terminal, laptop or dev): [docs/TESTING-WITH-FRIENDS.md](docs/TESTING-WITH-FRIENDS.md).

## Build, test, run

```bash
docker run -d -p 6379:6379 redis:7       # tests and the service need Redis
mvn clean package                      # runs the tests (whole survey over real sockets, on Redis)
RTS_AUTH_IDENTITY=demo RTS_AUTH_DEV_CLAIM_COORDINATOR=true POD_NAME=local-1 PORT=8081 \
    java -jar target/cb-ext-rts-service.jar   # Redis on localhost:6379, demo auth
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
RTS_AUTH_MODE=dev POD_NAME=pod-1cpu PORT=8080 taskset -c 0 java -Xmx1500m -jar target/cb-ext-rts-service.jar
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

## Where the survey state lives: Redis

All survey state, join passes, rosters and pod heartbeats are in Redis, shared by all pods: any pod serves any PC
or learner, and a pod restart or crash does not lose a survey (clients reconnect to any pod and continue).

Set `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD` (else `IGOT_REDIS_HOST`/`IGOT_REDIS_PORT` from the shared iGOT
ConfigMap, else localhost:6379). The pod does not start if Redis is unreachable (log: "Unable to connect to Redis");
if Redis drops while running, joins and answers reply `TRY_AGAIN`. Layout (LLD §4), keys carry the hash tag `{code}`:

| Key | Type | Holds |
|---|---|---|
| `rts:{code}:room` | hash | roomId, status, coordinatorId, pausedAt, pushCount, current question (snapshot), open, endsAt, shown, revealed |
| `rts:{code}:names` | hash | learner userId → name, everyone who joined (the PC's list after a reconnect) |
| `rts:{code}:conn` | hash | learner userId → open sockets; the 200 cap counts its fields (one atomic script). LLD K5 is a set; a hash also tells one tab from two |
| `rts:{code}:pcs` | set | PC socket ids; the survey pauses only when the last one drops |
| `rts:{code}:{pushId}:ans / cnt / words / texts` | set / hash / zset / list | who answered; total + count per option; word-cloud counts; free-text answers (userId, name, text) |
| `rts:{code}:{pushId}:dlv / dlvf` | hash / set | sockets the question was written to (summed over pods); who it failed for |
| `rts:pass:{id}` | string | join pass (userId, name, code, role); expires after 60 s; removed on use |
| `rts:{code}:roster` | hash | coordinators, participants (`*` = anyone) |
| channel `rts:room:{code}` | pub/sub | every survey event; each pod with sockets of the survey writes it to them |
| `rts:pods` | zset | pod instance (name + random suffix) → last heartbeat |
| `rts:pod:{podId}:conns` | hash | socket id → code, userId, name, role of each socket on that pod |
| `rts:paused` | zset | survey code → pausedAt, while PAUSED |

Every check-then-change is one Lua script (start, pause/resume, end, push, stop/resume, timer, mark, submit,
join, leave). Keys expire 12 h after start and 1 h after the survey ends.

Measured on a laptop (28 Oct 2026), 2 pods + 1 Redis, 30 surveys × 200 with every survey split over both pods:
6,000/6,000 sockets, every question to every learner, every answer recorded, push p95 3 ms, Redis ≈ 5,500
ops/s (about one Redis operation per learner per second; scale this to size Redis). `kill -9` of the pod holding
the PC mid-question: the PC and learners reconnected to the other pod and found the same question, still LIVE,
with the answers given on the dead pod kept.

A pod that crashes runs no "left" step for its sockets, so every pod heartbeats (`rts.pod.heartbeat`, 5 s) and
records its sockets in Redis. A pod silent for `rts.pod.dead-after` (20 s) is claimed by one other pod, which counts
its sockets out: the PC gets `PARTICIPANT_LEFT {reason: CONNECTION_LOST}`, and if it held the last PC socket the
survey pauses (`COORDINATOR_AWAY`). A learner who already reconnected elsewhere is not reported as left (the same
holds for closing one of two tabs). The same sweep ends paused surveys whose PC wait ran out, so that no longer
depends on the pod that paused them. A pod that stalls past dead-after and comes back finds it was taken for dead
and closes its sockets so the clients reconnect and are counted again. Keep dead-after the same on every pod.

While the survey runs, anyone can come back and carries on from where the survey is now. A client that reconnects
gets everything in `SESSION_STATE`; events it missed are not replayed and nothing about earlier questions is sent:
the current question, its own answer, and, if the PC showed them meanwhile, the results and (learners) the correct
answer. A PC also gets `participants` (everyone who joined, `connected` or not) and, for a free-text question, its
`textAnswers` so far (kept in `rts:{code}:{pushId}:texts`), which otherwise reach the PC only as live events.

Tests: `SurveyFlowTest`, `PcJoinTest`, `JoinPassTest` and `DevClaimTest` run the flows on Redis;
`RedisTwoPodsTest` runs a survey across two pods and loses one mid-survey; `RedisPodCrashTest` leaves a crashed
pod behind in Redis and checks the clean-up. All of them except
`JwtIdentityVerifierTest` need Redis on localhost:6379 and are skipped without it, so the Jenkins build does not
need Redis (run them locally before pushing).

## Files

| File | Role |
|---|---|
| `RtsWebSocketHandler` | one call per socket: read identity, join, frames in order, clean up |
| `SessionManager` | the protocol: change the state in the store, then announce it on the bus; timers, live stats, ping sweep |
| `SessionStore`: `RedisSessionStore` | survey state in Redis, changed with Lua scripts |
| `SessionBus`: `RedisSessionBus` | survey events to every pod with sockets of the survey |
| `JoinController`, `JoinPasses` | POST /rts/v1/join and rosters; the pass is checked and used up when the socket opens (`RtsWebSocketHandler`, close 4401 if not valid) |
| `OriginFilter` | refuses sockets from web origins not in `rts.auth.allowed-origins` |
| `IdentityVerifier`: `JwtIdentityVerifier`, `DemoIdentityVerifier` | who is calling POST /rts/v1/join (Keycloak token, or demo) |
| `AuthStore`: `RedisAuthStore` | join passes and rosters |
| `PodPresence`: `RedisPodPresence` | pod heartbeat and socket records, so a crashed pod's sockets are counted out |
| `SessionView`, `Results`, `RtsError` | state snapshots, result bodies, request errors |
| `Conn` | one socket on this pod with its bounded send queue |
| `Frames` | JSON envelope, serialised once per broadcast |
| `Stats`, `StatsController` | capacity numbers |
| `ShutdownCloser` | closes sockets with 1001 on shutdown |
