# Distributed Computing Cluster (CS324)

A distributed computing cluster built with **gRPC** (Java). At least six independent worker JVMs
(extra nodes can be added at startup) form an unstructured network, a neutral **Bootstrap Node**
tracks membership, and a distributed **leader election** lets the workers agree on a
**Coordinator** for one term at a time. Clients submit jobs
(MAX, PRIMECOUNT, PRIMESUM); the coordinator tags each client request with a **task ID**, splits
the work into a variable number of contiguous segments, hands every segment to the least-loaded
worker **nodes** (never executing a segment itself), merges the partial results, records exactly
which workers processed the task, and returns the compiled answer plus the used-worker list to the
requesting client.

The whole communication layer is defined once in a **Protocol Buffers** contract
(`proto/src/main/proto/cluster.proto`); every component is a separate Maven module and a separate
JVM.

---

## Table of Contents

1. [What the Program Does](#what-the-program-does)
2. [Project Layout](#project-layout)
3. [Requirements & Tools](#requirements--tools)
4. [Configuration](#configuration)
5. [Building the Project](#building-the-project)
6. [Running the Entire System — Option A (One Terminal Command, Recommended)](#running-the-entire-system-option-a-one-terminal-command-recommended)
7. [Running the Entire System — Option B (Manual or Separate Terminals)](#running-the-entire-system-option-b-manual-or-separate-terminals)
8. [Job Types & Input Formats](#job-types--input-formats)
9. [How the Cluster Works (Election, Tasks, Terms)](#how-the-cluster-works)
10. [Stopping the Cluster](#stopping-the-cluster)
11. [Troubleshooting](#troubleshooting)

---

## What the Program Does

- At least six **worker nodes** (IDs `1..N`, six by default, minimum six) each run in their own JVM
  with their own **gRPC server and port**.
- A **Bootstrap Node** keeps the membership list over gRPC — it never runs jobs and never takes part
  in elections.
- The workers **elect a Coordinator** automatically. On startup a randomized timer lottery picks an
  initial coordinator; afterwards the coordinator serves **at most 5 client jobs**, then a new
  election chooses the **lowest-JAC** worker (JAC = lifetime job-allocation counter).
- Clients submit jobs through a GUI. The elected coordinator:
  1. gives the job a unique **task ID**,
  2. splits the input into contiguous **segments**,
  3. distributes them across the least-loaded worker **nodes** (incrementing each worker's JAC; the
     coordinator only assigns and compiles, it never executes a segment),
  4. merges the partial results and returns a `TaskResult` to the client with the task ID, the
     client ID, the segment count, the exact worker list used and the final answer.

There is **no server GUI**. Everything runs from the terminal: starting the Bootstrap Node also
launches every worker node (one JVM each), the workers elect a coordinator on their own, and a
client GUI handles job submission.

---

## Project Layout

The project is a multi-module Maven build. The gRPC/protobuf contract is generated once in `proto`
and consumed by every other module as a normal Maven dependency.

| Module | What it contains | Artifact |
|--------|------------------|----------|
| `proto` | The `.proto` blueprint, generated gRPC stubs, and the shared config/helpers in `com.cs324.common` (`WorkerClusterConfig`, `GrpcSupport`, `ChannelPool`). | `proto/target/proto-1.0-SNAPSHOT.jar` |
| `worker` | A worker node: `WorkerServiceImpl` (election + task execution), `WorkerServer` (entry point), `WorkerClusterLauncher` (spawns N workers as JVMs). | `worker/target/worker.jar` (fat jar) |
| `bootstrap` | The Bootstrap Node: `BootstrapServiceImpl` (membership), `BootstrapServer` (starts the gRPC server and launches all workers). | `bootstrap/target/bootstrap.jar` (fat jar) |
| `client` | The client GUI (`ClientGUI`) plus its Swing theme (`UITheme`). | `client/target/client.jar` (fat jar) |

The `worker`, `bootstrap` and `client` modules are built as **self-contained executable fat jars**
(via `maven-shade-plugin`), so no classpath assembly is needed at run time.

- **gRPC contracts** live in `proto/src/main/proto/cluster.proto` (services `cs324.WorkerService`
  and `cs324.BootstrapService`); generated Java lands under `com.cs324.grpc`.
- Workers register with the Bootstrap Node and are linked into a small neighbour graph; distributed
  election and job messages still reason over **all reachable active workers**.
- The client finds the current coordinator through the Bootstrap Node automatically.

---

---

## Requirements & Tools

- **JDK 17+** (the build targets `--release 17`).
- **Maven 3.8+**. The gRPC/protobuf toolchain (`protoc` and `protoc-gen-grpc-java`) is downloaded
  automatically by the build, so nothing else needs to be installed manually.
- Any OS with Java; all examples assume a shell on `localhost`.

---

## Configuration

All defaults live in the source so nothing needs to be set before the first run. If you want to
change host/ports, edit these files:

| Setting | Where | Default |
|---------|-------|---------|
| Bootstrap gRPC port | `WorkerClusterConfig.DEFAULT_BOOTSTRAP_PORT` (or `BootstrapServer [port] [workerCount]`) | `1099` |
| Number of workers | system property `cs324.worker.count` (or `BootstrapServer [port] [workerCount]`) | `6` (minimum `6`) |
| Worker IDs | `WorkerClusterConfig.FIRST_WORKER_ID` / `LAST_WORKER_ID` | `1` / `N` (N = configured count) |
| Base worker gRPC port (worker *id* → *BASE + id − 1*) | `WorkerClusterConfig.BASE_WORKER_PORT` | `5001` |
| Default host | `WorkerClusterConfig.DEFAULT_HOST` | `localhost` |
| Worker log directory | `WorkerClusterConfig.LOG_DIR` | `logs` |
| Max submitted jobs per coordinator term | `WorkerClusterConfig.COORDINATOR_TERM_LIMIT` | `5` |
| Task segmentation: MAX/PRIMECOUNT items per segment | `WorkerServiceImpl.SEGMENT_NUMBERS` | `3` |
| Task segmentation: PRIMESUM range length per segment | `WorkerServiceImpl.SEGMENT_RANGE_LENGTH` | `200` |
| Startup lottery countdown | `WorkerServiceImpl` (`LOTTERY_MIN_DELAY_MS` / `LOTTERY_SPAN_MS`) | 150–300 ms random |

Port layout used by the cluster (fixed, don't run other services on these):

| Component | Address |
|-----------|---------|
| Bootstrap Node gRPC server | `localhost:1099` |
| Worker 1 | `localhost:5001` |
| Worker 2 | `localhost:5002` |
| Worker 3 | `localhost:5003` |
| Worker 4 | `localhost:5004` |
| Worker 5 | `localhost:5005` |
| Worker 6 | `localhost:5006` |

Extra worker nodes continue the pattern: worker *N* uses `localhost:5000 + N` (e.g. workers 7 and 8
use `5007` and `5008`).

Hosts and ports can also be overridden per-launch via command-line arguments (see the launch
sections below) instead of editing source.

---

## Building the Project

```bash
mvn clean install
```

This builds all four modules in order and installs them into the local repository. The build also
generates the gRPC stubs from `proto/src/main/proto/cluster.proto` and packages the three
executable fat jars:

- `worker/target/worker.jar`
- `bootstrap/target/bootstrap.jar`
- `client/target/client.jar`

All launch commands below use `java -jar <module>/target/<name>.jar`.

---

## Running the Entire System — Option A (One Terminal Command, Recommended)

There is **no server GUI**. The Bootstrap Node itself launches every worker node in its own JVM when
it starts, so **one command** runs the whole cluster (no manual startup order).

### Step 1 — Start the Bootstrap Node (this also starts all workers)

```bash
# default cluster: bootstrap gRPC server on port 1099 + the 6 default workers
java -jar bootstrap/target/bootstrap.jar

# custom bootstrap port (still the 6 default workers)
java -jar bootstrap/target/bootstrap.jar 2099

# spin up extra worker nodes on top of the default 6 (minimum is 6): 8 workers here
java -jar bootstrap/target/bootstrap.jar 1099 8
```

The bootstrap binds its gRPC server, spawns one JVM per worker node (IDs `1..N`, gRPC ports
`5001..5000+N`, logs `logs/worker-<id>.log`), waits until the configured count has registered and
verifies each node over gRPC. Press **Ctrl+C** (or `kill <pid>`) to stop the whole cluster again.

### Step 2 — Wait for the automatic election

There is **no election button**. Within ~10 seconds the workers agree on the coordinator (see the
bootstrap/worker logs):
- On a cold start a randomized 150–300 ms timer lottery mints startup priorities `N..1`; the
  priority-`N` worker declares itself the Initial Coordinator.
- After every 5-job term the coordinator broadcasts Term_End and the cluster re-elects the
  lowest-JAC worker automatically (see [How the Cluster Works](#how-the-cluster-works)).

### Step 3 — Launch one or more Clients

In separate terminals (run any number of client GUIs simultaneously):

```bash
java -jar client/target/client.jar
```

Click **Connect**. The client auto-discovers the coordinator through the Bootstrap Node and shows
`● ONLINE` and `Coordinator: Worker N`.

### Step 4 — Submit jobs

Pick a job type, type (or load) the input, press **Submit**. See
[Job Types & Input Formats](#job-types--input-formats) below for accepted input.

### Step 5 — Watch the results

- **Client GUI** — its task table lists each job: type, submit time, status, coordinator, the
  **worker nodes** the task was split over, the task's short ID and the **merged result**.
- **Worker logs** (`logs/worker-<id>.log`) — per-worker online state, JAC, `jobsThisTerm`,
  agreement on the coordinator, neighbour count and startup priority.
- **Bootstrap log (the terminal)** — membership claims, `Cluster complete: N/N workers registered`,
  startup lottery priorities, unregistrations and the shutdown summary.

---

## Running the Entire System — Option B (Manual or Separate Terminals)

If you prefer raw processes (no single bootstrap command), start each component from a shell.
Everything is configurable via arguments; only the fixed worker port layout lives in code.

### Step 1 — Start the Bootstrap Node alone

The `bootstrap.jar` always launches workers too. If you want the bootstrap **without** auto-launched
workers, start it and immediately stop the workers it spawned with the launcher's `stop` command
(below), or use Option A. To start just the membership server:

```bash
# default gRPC port 1099
java -jar bootstrap/target/bootstrap.jar

# custom port, e.g. 2099
java -jar bootstrap/target/bootstrap.jar 2099
```

### Step 2 — Start the workers (one JVM each)

Run `WorkerServer` manually (IDs `1..N`, gRPC ports `5001..5000+N`):

```bash
java -jar worker/target/worker.jar 1 5001
java -jar worker/target/worker.jar 2 5002
java -jar worker/target/worker.jar 3 5003
java -jar worker/target/worker.jar 4 5004
java -jar worker/target/worker.jar 5 5005
java -jar worker/target/worker.jar 6 5006
# extra nodes beyond the default six:
java -jar worker/target/worker.jar 7 5007
java -jar worker/target/worker.jar 8 5008
```

Optional trailing arguments (only needed when something is not on `localhost`):

```text
WorkerServer <workerId> <port> [bootstrapHost] [bootstrapPort] [workerHost]
```

### Step 3 — Wait for the automatic election

Nothing to do — each worker runs a background check and starts an election on its own once it sees
no coordinator. Start all workers within a few seconds of each other and the cluster self-elects
within ~10 seconds.

### Step 4 — Run the clients

```bash
java -jar client/target/client.jar
```

### Launcher convenience commands (bash, not a GUI)

The launcher lives in the `worker` module; its jar's main class is `WorkerServer`, so invoke the
launcher class explicitly:

```bash
# start the 6 default workers against a bootstrap on localhost:1099
java -cp worker/target/worker.jar com.cs324.backend.worker.WorkerClusterLauncher start

# custom bootstrap
java -cp worker/target/worker.jar com.cs324.backend.worker.WorkerClusterLauncher start localhost 1099

# add extra nodes (here a 9-worker cluster): extra nodes spawn on top of the default 6
java -cp worker/target/worker.jar com.cs324.backend.worker.WorkerClusterLauncher start localhost 1099 9

# stop all workers started by the launcher, reusing its pid files
java -cp worker/target/worker.jar com.cs324.backend.worker.WorkerClusterLauncher stop

# show pid file state for each worker
java -cp worker/target/worker.jar com.cs324.backend.worker.WorkerClusterLauncher status
```

---

## Job Types & Input Formats

| Job type | Input | Example | Result |
|----------|-------|---------|--------|
| `MAX` | comma-separated integers | `12,5,99,2,17` | largest number |
| `PRIMECOUNT` | comma-separated integers | `2,4,5,8,11` | number of primes (duplicates count) |
| `PRIMESUM` | `start,end` (inclusive range) | `1,1000` | sum of primes in the range |

**CSV format** — one line per file:

- `MAX` / `PRIMECOUNT`: one line of comma-separated integers, e.g. `12,5,99,2,17`.
- `PRIMESUM`: one line with two values, e.g. `1,1000`.

Malformed values (non-numeric, empty, `start > end`, `start < 1`) produce a clear error in the task
table instead of crashing.

### Segmentation & distribution

Each task is diffracted into a **variable number of contiguous segments** and spread across the
reachable worker **nodes**:
- MAX / PRIMECOUNT use `⌈n / 3⌉` segments;
- PRIMESUM uses `⌈range / 200⌉` segments;
- the segment count is capped at the number of reachable worker nodes (up to 5, since the
  coordinator is excluded), so a small task uses one or two nodes and a large one fans out across
  all of them.

The coordinator ranks the worker nodes by **JAC** (least-loaded first) and increments each
receiver's JAC for every segment assigned. Partial results are merged back at the coordinator, which
attaches the task ID, client ID and the list of worker nodes used, and returns the compiled answer
to the client.

### Ready-made sample CSV files

The project ships samples in the `csv/` folder — pick the matching job type, press **Load CSV...**,
choose a file and press **Submit**.

| File | Job type | Contents | Expected result |
|------|----------|----------|-----------------|
| `csv/max.csv` | `MAX` | `12,5,99,2,17,45,8,76,31,50,3,88` | `99` |
| `csv/max-small.csv` | `MAX` | `7,3,21,14,9,1,42` | `42` |
| `csv/primecount.csv` | `PRIMECOUNT` | `2,6,11,15,17,20,23,29,31,40,47,53,60,67,71,80` | `10` |
| `csv/primesum.csv` | `PRIMESUM` | `1,1000` | `76127` |
| `csv/primesum-range.csv` | `PRIMESUM` | `100,600` | `28236` |

---

## How the Cluster Works

The protocol runs in three phases.

### Phase 1 — Startup & Initial Leader Election (Timer Lottery)

1. All N workers (N defaults to 6; minimum 6) boot with `JAC = 0` and no coordinator.
2. When the Bootstrap Node reports the cluster complete (N/N registered), every worker starts a
   **randomized countdown of 150–300 ms**.
3. The first worker whose countdown expires claims **startup priority N**, the second **priority
   N−1**, … the last **priority 1** (the Bootstrap Node sequences the claims, so each cold start
   mints priorities `1..N` in a random assignment).
4. The worker holding **priority N** declares itself the **Initial Coordinator** and alerts the
   other N−1 workers.

### Phase 2 — Dynamic Segmented Task Distribution

5. Reachable worker **nodes** are ranked by **JAC ascending** (least-loaded first), then startup
   priority (descending), then worker id.
6. Each submitted task gets a unique **task ID** and is split into a **variable number of
   contiguous segments** (`max(1, min(nodes, ⌈items / maxItemsPerSegment⌉))`).
7. Every segment is handed to a worker **node** — the coordinator is **excluded** and only assigns
   and compiles, and each receiving worker's own JAC is incremented. Because JACs diverge, tasks
   from different clients tend to reach different subsets of worker nodes. The coordinator merges
   the partial results into a `TaskResult` (task ID, client ID, job type, segment count, worker list
   used, merged answer) and returns it to the requesting client.

### Phase 3 — Term Expiration & Lowest-JAC Re-Election

8. A coordinator serves **at most 5 submitted client jobs** per term. Immediately after the 5th job
   it pauses, broadcasts **Term_End** with its final JAC table, demotes itself to `NO_COORDINATOR`
   and starts a fresh election.
9. The new coordinator is the reachable worker with the **lowest JAC**; ties go to the **highest
   startup priority id** (worker id as a final safety net). On step-down the outgoing coordinator
   takes a **demotion tick** that lifts its JAC strictly above the highest worker-node JAC — since
   it never executes segments it would otherwise always hold the lowest JAC and be re-elected
   forever. The next election therefore genuinely rotates to a different, least-loaded node.
10. A 6th submission during the hand-over is refused with a clear message; clients retry
    automatically against the newly elected coordinator.

**Election mechanics (used from Phase 3 onward, and as a fallback whenever no coordinator is
present):** any worker can initiate an election; a fixed background check starts one when the
cluster has no coordinator. An `ElectionMessage` (unique id) floods the neighbour graph; each
worker processes a given election id at most once, and per-worker in-progress guard avoids
overlapping elections. Every participant contributes a `CandidateInfo` (worker id + priority + JAC);
the winner is broadcast to all participants and every worker records the same coordinator.

---

## Stopping the Cluster

- **One-command bootstrap (Option A)**: press **Ctrl+C** in the bootstrap terminal (or
  `kill <pid>`). Its shutdown hook unregisters every worker node and stops each worker JVM it
  launched this session.
- **Launcher**:
  `java -cp worker/target/worker.jar com.cs324.backend.worker.WorkerClusterLauncher stop`.
- **Manual terminals**: `Ctrl+C` the worker terminals, then the Bootstrap terminal.
- Left-over pid/log files are cleaned automatically or re-detected on the next start.

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `UNAVAILABLE` / connection refused on worker/client start | Start the Bootstrap Node first; check the gRPC ports (1099 and 5001+). |
| `Worker N is not the coordinator` | A term just ended and a new election is running; resubmit — the client auto-re-resolves the new coordinator. |
| `Coordinator term ended after 5 jobs ...` | Expected — the term expired; the next submission goes to the new coordinator. |
| Port already in use | Another instance is running (pid files skip it); stop it first. |
| GUI shows `ELECTING...` for a long time | Seconds-long is normal right after a term ends. If it persists, stop and restart the workers so they all re-run their staggered auto-election checks. |
| Client tables look empty | Click **Connect** — the client view auto-refreshes while connected. |
| Build downloads `protoc` on first run | Expected — the gRPC toolchain is fetched from Maven Central the first time only. |
