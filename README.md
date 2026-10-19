# Distributed Computing Cluster (CS324)

A fault-tolerant distributed computing cluster built with **Java and gRPC**. Six or more
independent worker JVMs form an unstructured network, a neutral **Bootstrap Node** tracks
membership and keeps the **task ledger**, and a distributed **leader election** lets the workers
agree on a **Coordinator** for one term at a time.

Clients submit jobs (`MAX`, `PRIMECOUNT`, `PRIMESUM`) through a **JavaFX + AtlantaFX GUI** (or a
gRPC stub). The coordinator tags each request with a **task ID**, splits the work into contiguous
**segments**, hands every segment to a different least-loaded worker **node** (it never executes a
segment itself), merges the partial results and streams progress/result updates back to the client.

Every component runs **in its own terminal** — the Bootstrap Node does not spawn anything, so each
part of the system can be started, stopped, killed and restarted independently (which is exactly
what you want when demonstrating failure detection and recovery).

The cluster is designed to keep serving jobs while workers die and rejoin:

- killing a **segment worker** mid-job → its un-done segments are retried on live workers;
- killing the **coordinator** mid-job → a failover election promotes a new coordinator, which
  resumes the task from the durable ledger and finishes it;
- killing the **Bootstrap Node** mid-run → the client dashboard reports
  *failure detected → reconnecting → recovered* live, and everything reconnects when it returns;
- adding a **new worker** to a running cluster → it joins the ring, adopts the current
  coordinator and immediately receives segments.

---

## Table of Contents

1. [What the Program Does](#what-the-program-does)
2. [Requirements & Tools](#requirements--tools)
3. [Configuration](#configuration)
4. [Building the Project](#building-the-project)
5. [Running the System (one terminal per component)](#running-the-system-one-terminal-per-component)
6. [The Client GUI (JavaFX + AtlantaFX)](#the-client-gui-javafx--atlantafx)
7. [The Health Dashboard](#the-health-dashboard)
8. [Job Types, Segmentation & Distribution](#job-types-segmentation--distribution)
9. [How the Cluster Works](#how-the-cluster-works)
10. [Fault Tolerance & Dynamic Join](#fault-tolerance--dynamic-join)
11. [Stopping the Cluster](#stopping-the-cluster)
12. [Troubleshooting](#troubleshooting)

---

## What the Program Does

- The **Bootstrap Node** (gRPC server on `localhost:1099`) tracks membership, sequences the
  startup **lottery**, and records every submitted task and its per-segment progress in a durable
  in-file **ledger**. It never runs jobs and never takes part in elections — and it deliberately
  **does not spawn worker processes**: each worker is started in its own terminal.
- **Worker nodes** (IDs `1..N`, gRPC ports `5001..5000+N`) register with the Bootstrap Node, are
  linked into a small neighbour ring, and automatically **elect a coordinator**. The coordinator
  serves **at most 5 client jobs** per term, then a re-election rotates to the least-loaded worker.
- Every gRPC channel is cached per `host:port` and re-used by all components
  (`com.cs324.backend.grpc.GrpcChannels`), with a 30 s keep-alive.
- The **client GUI** watches the whole system with a background health monitor
  (`ClusterMonitor`) and renders server failure detection, reconnect attempts, recovery and
  coordinator failover live on its dashboard.

Main packages:

| Package | Responsibility |
|---------|----------------|
| `com.cs324.backend.bootstrap` | `BootstrapServer` (terminal entry point — prints per-worker start commands, spawns nothing), `BootstrapServiceImpl` (membership, lottery, task/segment ledger) |
| `com.cs324.backend.worker` | `WorkerNode` (election, coordinator duties, segment executor, retry logic), `WorkerServer`, `WorkerClusterService`, `WorkerClientService`, `WorkerClusterLauncher` (optional bulk start/stop tool) |
| `com.cs324.backend.grpc` | `GrpcChannels` (cached `ManagedChannel`s per target, keep-alive policy) |
| `com.cs324.frontend.client` | `ClientGUI` (JavaFX client + dashboard), `ClusterMonitor` (background health probes driving the dashboard) |
| `com.cs324.frontend.ui` | `UITheme` (AtlantaFX `PrimerLight` base theme + project stylesheet layer) |
| `src/main/resources/com/cs324/frontend/ui/client-theme.css` | Project stylesheet: enlarged typography, cards, status pills, event timeline |
| `src/main/proto/distrilab.proto` | gRPC contracts: `BootstrapService`, `ClusterService`, `ClientService`; messages incl. `SegmentRecord` |

---

## Requirements & Tools

- **JDK 17+** (developed and tested on OpenJDK 17–26).
- **Maven 3.8+** (pulls `io.grpc` 1.66, protobuf, JavaFX 21, AtlantaFX 2.0.1).
- A display for the client GUI (X11/Wayland on Linux); servers run headless.
- Any OS with Java + gRPC; all examples assume a shell on `localhost`.

---

## Configuration

All defaults live in the source so nothing needs to be set before the first run:

| Setting | Where | Default |
|---------|-------|---------|
| Bootstrap gRPC port | `BootstrapServer.DEFAULT_PORT` | `1099` |
| Number of workers | `BootstrapServer [port] [count]` (or `-Dcs324.worker.count`) | `6` (minimum `6`) |
| Worker IDs / ports (`5000 + id`) | `WorkerClusterConfig` | `1..N` → `5001..5000+N` |
| Max jobs per coordinator term | `WorkerNode.COORDINATOR_TERM_LIMIT` | `5` |
| Segment retries (killed worker) | `WorkerClusterConfig.MAX_SEGMENT_RETRIES` | `3` |
| Segment call deadline | `WorkerClusterConfig.SEGMENT_CALL_TIMEOUT_MS` | `15 s` |
| Coordinator liveness probe | `WorkerClusterConfig.COORDINATOR_PROBE_TIMEOUT_MS` | `3 s` |
| Bootstrap heartbeat period / freshness timeout | `WorkerClusterConfig` | `2 s` / `8 s` |
| Client health probe interval / deadline | `ClusterMonitor.PROBE_INTERVAL_MS` / `PROBE_DEADLINE_MS` | `2 s` / `1.5 s` |
| PRIMESUM range length per segment | `WorkerNode.SEGMENT_RANGE_LENGTH` | `200` |
| MAX / PRIMECOUNT numbers per segment | `WorkerNode.SEGMENT_NUMBERS` | `3` |
| Startup lottery countdown | `WorkerNode` (Phase 1) | 150–300 ms random |

Fixed port layout (do not run other services on these):

| Component | Address |
|-----------|---------|
| Bootstrap Node | `localhost:1099` |
| Worker *N* | `localhost:5000 + N` (workers 1..N → `5001`…) |

---

## Building the Project

```bash
mvn clean compile            # compiles + regenerates the gRPC stubs from src/main/proto
mvn process-classes          # also writes target/classpath.txt (needed to run the cluster)
```

The cluster needs the gRPC/JavaFX jars on the classpath, so **always launch with**:

```bash
java -cp "target/classes:$(cat target/classpath.txt)" <class>
```

> **Non-Linux hosts:** the JavaFX jars are pulled with the `javafx.platform` classifier
> (default `linux`). Override it when building elsewhere, e.g.
> `mvn clean compile -Djavafx.platform=win` or `-Djavafx.platform=mac` /
> `-Djavafx.platform=mac-aarch64`.

---

## Running the System (one terminal per component)

The Bootstrap Node only **prints** the per-worker start commands — every JVM is started by you,
in its own terminal, so you can kill/restart any single piece independently.

**Terminal 1 — Bootstrap Node:**

```bash
java -cp "target/classes:$(cat target/classpath.txt)" com.cs324.backend.bootstrap.BootstrapServer
# optional:  BootstrapServer [bootstrapPort] [workerCount]
```

On startup it prints the exact `WorkerServer` command for every worker id, e.g.:

```
Bootstrap Node started on gRPC port 1099
Expecting 6 worker node(s) (minimum 6).
Start each worker in its own terminal, e.g.:
  java -cp "target/classes:$(cat target/classpath.txt)" com.cs324.backend.worker.WorkerServer 1 5001
  java -cp "target/classes:$(cat target/classpath.txt)" com.cs324.backend.worker.WorkerServer 2 5002
  ...
```

**Terminals 2…7 — one worker each:**

```bash
java -cp "target/classes:$(cat target/classpath.txt)" com.cs324.backend.worker.WorkerServer 1 5001
java -cp "target/classes:$(cat target/classpath.txt)" com.cs324.backend.worker.WorkerServer 2 5002
# ... up to WorkerServer 6 5006 (and beyond)
```

Optional trailing args: `WorkerServer <workerId> <port> [bootstrapHost] [bootstrapPort] [workerHost]`.

Once all workers are registered the bootstrap declares the cluster complete and the startup
lottery runs; within ~10 s the workers elect a coordinator.

**Terminal N — the client GUI:**

```bash
java -cp "target/classes:$(cat target/classpath.txt)" com.cs324.frontend.client.ClientGUI
# optional:  ClientGUI [bootstrapPort]
```

Click **Connect** — the dashboard starts probing the cluster and resolves the coordinator.

### Optional: bulk launcher (bash convenience, not a GUI)

If you prefer fewer terminals for the workers, `WorkerClusterLauncher` is still available:

```bash
java -cp "target/classes:$(cat target/classpath.txt)" com.cs324.backend.worker.WorkerClusterLauncher start  localhost 1099 6
java -cp "target/classes:$(cat target/classpath.txt)" com.cs324.backend.worker.WorkerClusterLauncher status
java -cp "target/classes:$(cat target/classpath.txt)" com.cs324.backend.worker.WorkerClusterLauncher stop
```

### Dynamic join

Start an **additional** worker against the *running* cluster at any time:

```bash
java -cp "target/classes:$(cat target/classpath.txt)" com.cs324.backend.worker.WorkerServer 7 5007 localhost 1099 localhost
```

The new worker registers, adopts the current coordinator from a peer, the coordinator rebuilds its
neighbour ring on the membership change, and the joiner starts receiving segments immediately.

---

## The Client GUI (JavaFX + AtlantaFX)

The client was rewritten from Swing to **JavaFX**, styled with **AtlantaFX** (`PrimerLight`
base theme) plus a project stylesheet (`client-theme.css`) layered on top — the two libraries
combine into one modern, CSS-driven look:

- **Larger fonts throughout** — 16 px base text, 30 px title/card values, 16 px status pills and
  table cells, 14 px monospaced log (see `client-theme.css`).
- Rounded white **cards** on a soft `#F5F7FA` background with subtle shadows, coloured
  **status pills**, accent/flat AtlantaFX buttons.
- Layout: header with connection bar → dashboard (status cards + full-width event timeline) →
  job submission, task table, live progress + worker drill-down, and a log panel.

**Run it with the plain classpath command** (see above). Modern JDK launchers refuse to start a
*main* class that extends `javafx.application.Application` when JavaFX is only on the classpath
("JavaFX runtime components are missing"), so `ClientGUI.main` is an ordinary launcher that
delegates to the nested `ClientApp` — the documented run command works as-is.

Jobs stream progress over gRPC (`ClientService.SubmitJob`): the task table updates live, the
progress bar and per-worker drill-down follow the selected task, and if the stream breaks because
a term ended or the coordinator died, the client **re-resolves the new coordinator and resumes
the job from the ledger** (up to 5 attempts).

---

## The Health Dashboard

`ClusterMonitor` runs a daemon probe loop (every **2 s**, **1.5 s** RPC deadline, never on the
JavaFX thread) and feeds four dashboard cards plus a timestamped **event timeline**. All GUI
updates are marshalled onto the JavaFX application thread.

| Card | States | Source |
|------|--------|--------|
| **Cluster Server** | `OFFLINE` → `RUNNING` → `FAILURE DETECTED` → `RECONNECTING` (attempt 1, 2, … with outage time) → `RECOVERED` (with outage duration) | probe to the Bootstrap Node |
| **Coordinator** | `UNKNOWN` / `ELECTING` / `ONLINE` / `FAILURE DETECTED` / `RECOVERED` | GUI coordinator resolution + probes to the coordinator worker |
| **Workers Online** | `online/expected` plus registered count | membership snapshot every probe cycle |
| **Jobs Submitted** | total plus `n done · n failed` | streamed job updates |

Every transition is also appended to the **event timeline** (severity-coloured: info / success /
warning / error) and mirrored into the log panel, e.g.:

```
[SERVER ] FAILURE_DETECTED :: failure detected: ... Connection refused
[SERVER ] RECONNECTING     :: reconnect attempt 3: ... (4.0 s since failure)
[SERVER ] RECOVERED        :: recovered after 10.1 s (5 failed probe(s))
[COORD  ] FAILURE_DETECTED :: worker 1 unreachable (...); failover election expected
[COORD  ] RECOVERED        :: worker 1 responding again
[EVENT  ] WARNING          :: Coordinator changed: worker 1 -> worker 4
[WORKERS] 6/6 registered=6
```

**Try it:** connect the GUI, then `Ctrl+C` the Bootstrap Node terminal — the pill flips to
`FAILURE DETECTED`, then `RECONNECTING` with a running attempt counter; restart the Bootstrap
Node and the dashboard reports `RECOVERED` with the outage duration. Kill the coordinator worker
instead and you get `COORDINATOR FAILURE DETECTED`, the workers' failover election, and a
`Coordinator changed` event when the client re-resolves.

---

## Job Types, Segmentation & Distribution

| Job type | Input | Example | Result |
|----------|-------|---------|--------|
| `MAX` | comma-separated integers | `12,5,99,2,17` | largest number |
| `PRIMECOUNT` | comma-separated integers | `2,4,5,8,11` | count of primes |
| `PRIMESUM` | `start,end` inclusive range | `1,1000` | sum of primes in the range |

CSV files: `MAX`/`PRIMECOUNT` = one line of integers; `PRIMESUM` = one line `start,end`.
Invalid input produces a clear task-table error instead of a crash.

**Segmentation:** a task is split into contiguous segments; the number of segments is
`min(onlineAssignableWorkers, ⌈items / perSegment⌉)`, at least 1. The coordinator picks assignees
by **JAC ascending** (JAC = lifetime job-allocation counter), then startup priority descending,
then worker id, and only ever assigns/merges — it never executes a segment itself.

---

## How the Cluster Works

### Phase 1 — Startup & initial election (lottery)

1. All workers boot with `JAC = 0`, `priority = PENDING`, no coordinator.
2. When the bootstrap reports the cluster complete, each worker runs a **random 150–300 ms
   countdown** and claims a startup priority (first claims `N`, last claims `1`, sequenced by the
   bootstrap).
3. The priority-`N` worker declares itself **Initial Coordinator** and notifies the others.

### Phase 2 — Segmented task distribution

4. A client's job gets a unique **task ID**, is recorded in the ledger, split into segments, and
   fanned out to least-loaded workers. Per-segment progress and results stream back to the client
   (`0%…100%`, `TASK_DONE`).
5. After 5 jobs the term expires (see below). A 6th submission during the hand-over is refused
   with a clear message; the client auto-resolves the new coordinator and retries.

### Phase 3 — Term rotation

6. The coordinator serves **at most `COORDINATOR_TERM_LIMIT` (5)** client jobs, then broadcasts
   Term_End with its final JAC table, demotes itself, takes a **demotion tick** (lifting its JAC
   above the highest worker-node JAC so it can't be re-elected forever), and starts a fresh
   election.
7. The new coordinator is the reachable worker with the **lowest JAC** (ties: highest priority,
   then worker id). This genuinely rotates leadership among least-loaded nodes.

**Election mechanics:** any worker can initiate an election; a fixed background monitor starts one
whenever the cluster has no coordinator or the current coordinator stops answering (3 s probe).
An `ElectionMessage` floods the neighbour ring; each election ID is processed once per worker and
an in-progress guard prevents overlap. The winner is broadcast to every participant.

---

## Fault Tolerance & Dynamic Join

### Worker kill (segment loss)

If a segment worker dies mid-job, the coordinator detects the failed segment call (15 s deadline,
up to `MAX_SEGMENT_RETRIES = 3` retries) and re-assigns that segment to a live worker. The task
completes with the correct answer.

*Observed:* killing a worker during a `PRIMESUM 1..1_000_000` job still returned
`37_550_402_023` (PASS).

### Coordinator kill (failover + ledger resume)

Every task and every segment's progress is persisted in the Bootstrap Node ledger (`SegmentRecord`
stores `data_start/data_length/range_start/range_end`). The client's submit call carries a
**deadline (15 s)** so a dead stream surfaces quickly; the client then re-resolves the new
coordinator and re-submits with the **`resume_task_id`**.

On resume the new coordinator rebuilds the remaining segments **from the ledger**, skips finished
segments, preserves partials, and finishes the job. The dashboard reports the coordinator failure,
the failover and the `Coordinator changed` re-resolution while it happens.

*Observed:* killing the coordinator of a running `PRIMESUM 1..8_000_000` streamed
`UNAVAILABLE`, the client resolved the new coordinator (1 resume), and the *same task ID*
completed with `2_080_483_502_248` (PASS).

### Bootstrap Node kill (client-side recovery)

The client's health monitor keeps probing: first failed probe → `FAILURE DETECTED`, each further
failure → `RECONNECTING` with attempt count and outage time, first success → `RECOVERED` with the
outage duration. Job streams interrupted by the outage go through the same re-resolve-and-resume
path as a coordinator failover.

### Dynamic join

A late-joining worker has no startup-priority slot (the bootstrap mints it a negative priority),
so it **adopts the existing coordinator** from a live peer (`adoptCoordinatorFromPeers`) instead
of being stuck at `coordinator = -1`. Meanwhile every worker **rebuilds its neighbour ring** when
the bootstrap's online membership changes (`maybeResyncRingAfterMembershipChange`), so the
coordinator learns about the joiner and starts assigning it segments.

*Observed:* worker 7 joined a running 6-worker cluster, logged
`adopted existing coordinator worker 3 from peer 1`, received segments in the next job (used
workers `[6, 1, 4, 2, 5, 7]`), and the total stayed correct.

---

## Stopping the Cluster

- **Each component is its own process** — press **Ctrl+C** in the terminal you want to stop
  (bootstrap, individual workers, client). Nothing else is affected; that is the point.
- **Launcher-spawned workers**: `WorkerClusterLauncher stop`.
- The dashboard reflects every stop immediately (`FAILURE DETECTED` / `RECONNECTING`, worker
  counts dropping) and recovers when you restart the process.

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `Connection refused` on startup | Start the Bootstrap Node first; check ports (`1099`, `5001..5000+N`). |
| `Worker N is not the coordinator` | A term just ended (or you targeted a segment worker); submit to the resolved coordinator. |
| `Coordinator term ended after 5 jobs` | Expected — resubmit; the client auto-resolves the new coordinator. |
| `ClassNotFoundException: com.cs324.backend.grpc.GrpcChannels` | You ran with `-cp target/classes` only — use `target/classes:$(cat target/classpath.txt)`. |
| `Error: JavaFX runtime components are missing...` | You launched `...ClientApp` (the nested `Application` subclass) directly — launch `com.cs324.frontend.client.ClientGUI` instead; its plain `main` delegates correctly. |
| Client window does not open | No display available — run it on a desktop session (needs `DISPLAY` on X11). |
| `Address already in use` on a worker port | A stale worker process owns the port (check `ps`, kill the old pid). |
| `GOAWAY ENHANCE_YOUR_CALM too_many_pings` in logs | Keep-alive pings must only run during in-flight calls (see `GrpcChannels`) — do not enable `keepAliveWithoutCalls`. |
