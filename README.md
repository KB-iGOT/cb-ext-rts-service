# cb-ext-rts-service

A Real Time Survey (RTS) lets a Program Coordinator ask the learners in a classroom (offline) session of a Blended Program a few quick questions and see their answers as they come in. Learners answer from their phone or laptop. The coordinator decides when each question appears, when answering stops, and when results and the correct answer are shown. After the session, the coordinator and the MDO can see a report.
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

## Files

| File | Role |
|---|---|
| `RtsWebSocketHandler` | one call per socket: read identity, join, frames in order, clean up |
| `SessionManager` | the protocol: state changes under the survey's lock, sends after; timers, live stats, ping sweep |
| `Session`, `Conn` | survey state (status, current question, answers); one socket with its bounded send queue |
| `Frames` | JSON envelope, serialised once per broadcast |
| `Stats`, `StatsController` | capacity numbers |
| `ShutdownCloser` | closes sockets with 1001 on shutdown |
