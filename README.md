# Distributed Systems Project 2026
![Java](https://img.shields.io/badge/Java-ED8B00?style=flat-square&logo=java&logoColor=white)
![Akka](https://img.shields.io/badge/Akka-15A9CE?style=flat-square&logo=akka&logoColor=white)
![Gradle](https://img.shields.io/badge/Gradle-Build-02303A?style=flat-square&logo=gradle&logoColor=white)

## Build and Run

```bash
# One-time setup (if Gradle wrapper is not present)
gradle wrapper --gradle-version 9.2.1

./gradlew build        # compile
./gradlew run          # interactive demo (5 scenarios)
./gradlew test         # run all 33 tests
```

If Gradle is not installed:

```bash
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install gradle 9.2.1
```

---

## Demo Scenarios

`./gradlew run` opens an interactive menu. Choose 1–5:

| # | Title | What it demonstrates |
|---|-------|----------------------|
| 1 | Normal Operation | Two-phase broadcast end-to-end; sequential consistency across any replica |
| 2 | Coordinator Crash and Recovery | Heartbeat timeout → ring election → new coordinator resumes writes |
| 3 | Uniform Agreement | Coordinator crashes after UPDATE phase; new coordinator completes the write |
| 4 | Write Buffered During Election | Non-coordinator buffers write during election, resubmits after SYNC |
| 5 | Quorum Tolerance | Minority crash (3 of 7); writes still succeed; crashed-replica read times out |

Demo configuration (`Main.java`): `MIN_LAT = 50 ms`, `MAX_LAT = 100 ms`, `READ_TIMEOUT = 1 s`, `WRITE_TIMEOUT = 12 s`.

---

## Tests

> A small number of tests are sensitive to JVM scheduling jitter (Akka HashedWheelTimer granularity × 6 protocol hops). They pass consistently on the second/third run on loaded machines.

---

## How It Works

### Normal Operation — Two-Phase Broadcast

Write path (6 hops end-to-end):

1. Client sends `WriteFromClient` to its bound replica.
2. Non-coordinator replica forwards it to the coordinator and arms an `updateTimeout`.
3. Coordinator assigns `UpdateId(epoch, seqnum)` and broadcasts `Update` to all replicas.
4. Each replica stores the update in `pendingUpdates`, sends `Ack`, and arms a `writeOkTimer`.
5. Coordinator collects ACKs; once a quorum **Q = ⌊N/2⌋+1** is reached (counting its own implicit vote), broadcasts `WriteOk` to all.
6. Each replica applies the update to `positions[]`, moves it to `updateHistory`, and fires `callbackOnUpdateApplied`.

Read path: replica reads directly from its local `positions[]` and replies immediately — no coordination needed.

### Coordinator Election

**Detection:** non-coordinators reset a heartbeat timer on every `Heartbeat`. If the timer fires (2× the coordinator beat interval), they start an election.

**Ring algorithm:**
1. Initiating replica enters election mode (freezing its `lastAppliedId`), sends `Election{candidates}` to its ring successor.
2. Each node on the ring adds itself (with its `lastAppliedId`) and forwards.
3. When the message completes a full loop, the initiator selects the best candidate: highest `lastAppliedId`, tie-break: highest replica ID.
4. Winner broadcasts `Synchronization{updateHistory}` to all replicas.
5. Each replica applies any missing committed updates, increments the epoch, fires `callbackOnCoordinatorElected`, and resumes normal operation.

**Robustness mechanisms:**
- `ElectionAck` timeout — if the ring successor does not ACK within `getMaxLatencyPlusTolerance()`, the election message is forwarded to the next node (skipping the silent one).
- `ElectionCompletionTimeout` — if the elected winner crashes before broadcasting `Synchronization`, the election restarts automatically.
- Stale election waves — a replica that has already learned of a newer coordinator (via a concurrent election) ACKs without rejoining.

### Uniform Agreement

If the coordinator broadcasts `Update` to all replicas but crashes before collecting ACKs or sending `WriteOk`:

1. Every replica's `writeOkTimer` fires after `getMaxLatencyPlusTolerance()`.
2. An election starts and a new coordinator is elected.
3. The new coordinator finds the uncommitted update in its `pendingUpdates` and immediately broadcasts `WriteOk` for it before starting the new epoch.

This guarantees that no update received by all replicas is ever silently discarded.

### Write Buffering During Election

If a `WriteFromClient` arrives at a non-coordinator while an election is in progress:
- The write is stored in `pendingWrites`.
- Once `Synchronization` arrives and normal mode resumes, all buffered writes are automatically resubmitted to the new coordinator.

---

## Authors

- Valerii Levchuk — [valerii.levchuk@studenti.unitn.it]
- Yehor Sharevych — [yehor.sharevych@studenti.unitn.it]
