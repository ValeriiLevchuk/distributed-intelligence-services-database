package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.function.BiConsumer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractReplica.InitSystem;

public class Main {

    static final int  MIN_LAT        = 50;
    static final int  MAX_LAT        = 100;
    static final long READ_TIMEOUT   = 1_000;
    static final long WRITE_TIMEOUT  = 12_000;
    static final long WRITE_WAIT = 1_500;
    static final long READ_WAIT =   500;

    public static void main(String[] args) {
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(false);

        String[] menu = {
            "1. Normal Operation",
            "   Baseline: two-phase broadcast, sequential consistency.",
            "",
            "2. Coordinator Crash and Recovery",
            "   Heartbeat timeout -> ring election -> new coordinator.",
            "",
            "3. Uniform Agreement: Coordinator Crashes in Update Phase",
            "   Coordinator becomes unresponsive after broadcasting Updates",
            "   (no WriteOk sent). New coordinator completes the write.",
            "",
            "4. Write Buffered During Election",
            "   Write arrives at non-coordinator during election; buffered",
            "   in pendingWrites and resubmitted after SYNCHRONIZATION.",
            "",
            "5. Non-Coordinator Crash -- Quorum Tolerance",
            "   Minority of replicas crash; writes still succeed;",
            "   client talking to crashed replica times out.",
        };

        System.out.println("=".repeat(66));
        System.out.println("  QUORUM-BASED TOTAL ORDER BROADCAST -- DEMO");
        System.out.println("=".repeat(66));
        System.out.println();
        for (String line : menu) System.out.println("  " + line);
        System.out.println();
        System.out.print("Enter choice (1-5): ");

        Scanner sc = new Scanner(System.in);
        int choice = sc.nextInt();
        sc.close();
        System.out.println();

        switch (choice) {
            case 1: scenario1(); break;
            case 2: scenario2(); break;
            case 3: scenario3(); break;
            case 4: scenario4(); break;
            case 5: scenario5(); break;
            default: System.out.println("Invalid choice."); break;
        }
    }

    // =========================================================================
    // SCENARIO 1 -- Normal Operation
    // =========================================================================
    static void scenario1() {
        banner("SCENARIO 1 -- Normal Operation (no crashes)");
        note("Setup  : 5 replicas, coordinator = 0",
             "Goal   : Show the two-phase broadcast pipeline end-to-end:",
             "         client write -> replica forwards to coordinator ->",
             "         UPDATE to all -> quorum ACKs -> WRITEOK -> all apply.",
             "         Reads from any replica must reflect the committed value.",
             "Expect : WriteResult(true) for every write; ReadResult values",
             "         consistent across all replicas after each write.");
        runScenario("Scenario1", 5, 0, (R, C) -> {
            step("1. Client_1 reads index=0 (initial value = 0)");
            read(C, 1, 0);  sleep(1_200);

            step("2. Client_1 writes index=0 = 42  (replica 1 forwards to coordinator 0)");
            write(C, 1, 0, 42);  sleep(WRITE_WAIT);

            step("3. Client_1 reads index=0 -- expect 42");
            read(C, 1, 0);  sleep(READ_WAIT);

            step("4. Client_3 reads index=0 from replica 3 -- same value (sequential consistency)");
            read(C, 3, 0);  sleep(READ_WAIT);

            step("5. Client_2 writes index=1 = 99  (second distinct position)");
            write(C, 2, 1, 99);  sleep(WRITE_WAIT);

            step("6. Client_4 reads index=1 from replica 4 -- expect 99");
            read(C, 4, 1);  sleep(READ_WAIT);

            step("7. Client_0 reads index=0 from coordinator (replica 0) -- still 42");
            read(C, 0, 0);  sleep(READ_WAIT);
        });
    }

    // =========================================================================
    // SCENARIO 2 -- Coordinator Crash and Recovery
    // =========================================================================
    static void scenario2() {
        banner("SCENARIO 2 -- Coordinator Crash and Recovery");
        note("Setup  : 5 replicas, coordinator = 0",
             "Goal   : Crash the coordinator. Replicas detect it via heartbeat",
             "         timeout (~2 s), run the ring election, elect the replica",
             "         with the most recent committed update (tie-break: highest",
             "         ID). New coordinator broadcasts SYNCHRONIZATION to bring",
             "         everyone up to date, then resumes normal write processing.",
             "Expect : [Replica X] ELECTION STARTED; [Replica Y] NEW COORDINATOR",
             "         elected: Y; subsequent write and reads succeed.");
        runScenario("Scenario2", 5, 0, (R, C) -> {
            step("1. Client_1 writes index=0 = 10  (establish state before crash)");
            write(C, 1, 0, 10);  sleep(WRITE_WAIT);

            step("2. Client_2 reads index=0 -- expect 10");
            read(C, 2, 0);  sleep(READ_WAIT);

            step("3. >>> CRASHING coordinator (replica 0) NOW <<<");
            note("Replicas detect via heartbeat timeout (~2 s).",
                 "Ring election follows (~1 s for 5 nodes).",
                 "Winner = replica with highest lastAppliedId (tie: highest ID).");
            crash(R, 0);  sleep(6_000); // detection + ring + SYNC propagation

            step("4. Client_2 writes index=0 = 55  (to new coordinator after election)");
            write(C, 2, 0, 55);  sleep(WRITE_WAIT);

            step("5. Client_3 reads index=0 from replica 3 -- expect 55");
            read(C, 3, 0);  sleep(READ_WAIT);

            step("6. Client_4 reads index=0 from replica 4 -- expect 55  (all replicas consistent)");
            read(C, 4, 0);  sleep(READ_WAIT);
        });
    }

    // =========================================================================
    // SCENARIO 3 -- Uniform Agreement: Coordinator Crashes in Update Phase
    // =========================================================================
    static void scenario3() {
        banner("SCENARIO 3 -- Uniform Agreement: Coordinator Crashes in Update Phase");
        note("Setup  : 5 replicas, coordinator = 0",
             "Goal   : Coordinator sends UPDATE to all replicas, then becomes",
             "         unresponsive (Crash.Type.Update). It will never collect",
             "         ACKs or send WriteOk. All replicas set a writeOkTimer;",
             "         when it fires, election starts.",
             "         The NEW coordinator finds the update in its pendingUpdates",
             "         and sends WriteOk to complete it -- satisfying Uniform",
             "         Agreement (no update acknowledged by a quorum is ever lost).",
             "Expect : Election starts; new coordinator elected; all replicas",
             "         eventually apply index=0 = 77.");
        runScenario("Scenario3", 5, 0, (R, C) -> {
            step("1. Arm coordinator (replica 0): crash after first Update-phase checkCrash");
            note("Crash.Type = Update, after_n = 1",
                 "Coordinator will send UPDATE to ALL replicas (the current",
                 "message handler completes), then become unresponsive.",
                 "Replicas receive UPDATE, send ACK -- but coordinator ignores",
                 "all ACKs. WriteOk is never sent by old coordinator.");
            arm(R, 0, AbstractReplica.Crash.Type.Update, 1);

            step("2. Client_1 writes index=0 = 77  via replica 1");
            note("Coordinator broadcasts UPDATE to all, then crashes.",
                 "writeOkTimers on all replicas fire (~350 ms) -> election.",
                 "New coordinator finds uid in pendingUpdates, sends WriteOk.");
            write(C, 1, 0, 77);  sleep(5_000); // writeOkTimer + election + new-coordinator WriteOk

            step("3. Reads from replicas 2, 3, 4 -- all should return 77");
            note("Uniform Agreement: every surviving replica applied the update.");
            read(C, 2, 0);  sleep(READ_WAIT);
            read(C, 3, 0);  sleep(READ_WAIT);
            read(C, 4, 0);  sleep(READ_WAIT);
        });
    }

    // =========================================================================
    // SCENARIO 4 -- Write Buffered During Election
    // =========================================================================
    static void scenario4() {
        banner("SCENARIO 4 -- Write Buffered During Election");
        note("Setup  : 5 replicas, coordinator = 0",
             "Goal   : Crash the coordinator, then immediately send a write to",
             "         a non-coordinator replica.",
             "         The replica forwards the write to (now-crashed) coordinator 0",
             "         and starts an updateTimeout. When it fires, the replica enters",
             "         election mode and saves the write in pendingWrites.",
             "         After SYNCHRONIZATION completes, it resubmits automatically.",
             "Expect : WriteResult(true, 0, 88) arrives for Client_2 only after the",
             "         election finishes; reads from all replicas then return 88.");
        runScenario("Scenario4", 5, 0, (R, C) -> {
            step("1. Client_1 writes index=0 = 5  (establish baseline state)");
            write(C, 1, 0, 5);  sleep(WRITE_WAIT);

            step("2. >>> CRASHING coordinator (replica 0) NOW <<<");
            crash(R, 0);  sleep(150); // let crash take effect before write is forwarded

            step("3. Client_2 writes index=0 = 88  (immediately after crash -- will be buffered)");
            note("Replica 2 forwards write to coordinator 0, starts updateTimeout.",
                 "updateTimeout fires (~350 ms) -> election starts.",
                 "Write is stored in pendingWrites; resubmitted after SYNC.");
            write(C, 2, 0, 88);  sleep(7_000); // updateTimeout + election + resubmit + write round-trip

            step("4. Client_3 reads index=0 from replica 3 -- expect 88");
            read(C, 3, 0);  sleep(READ_WAIT);

            step("5. Client_4 reads index=0 from replica 4 -- expect 88");
            read(C, 4, 0);  sleep(READ_WAIT);
        });
    }

    // =========================================================================
    // SCENARIO 5 -- Non-Coordinator Crash: Quorum Tolerance
    // =========================================================================
    static void scenario5() {
        banner("SCENARIO 5 -- Non-Coordinator Crash: Quorum Tolerance");
        note("Setup  : 7 replicas, coordinator = 0",
             "Goal   : Crash 3 non-coordinator replicas (a strict minority).",
             "         Quorum Q = floor(7/2)+1 = 4.",
             "         Survivors: replicas 0,1,2,3 -> quorum met, no election needed.",
             "         Writes to surviving replicas proceed normally.",
             "         A client bound to a crashed replica gets a READ timeout.",
             "Expect : Writes succeed; reads return correct value on replicas 0-3;",
             "         Client_4 READ times out after " + READ_TIMEOUT + " ms.");
        runScenario("Scenario5", 7, 0, (R, C) -> {
            step("1. Client_1 writes index=0 = 20  (all 7 replicas alive)");
            write(C, 1, 0, 20);  sleep(WRITE_WAIT);

            step("2. >>> CRASHING replicas 4, 5, 6  (3 out of 7 -- minority) <<<");
            note("Quorum = 4. Survivors: 0,1,2,3. Coordinator (0) is alive.",
                 "No election triggered -- coordinator is still reachable.");
            crashAll(R, 4, 5, 6);  sleep(READ_WAIT);

            step("3. Client_2 writes index=0 = 99  (quorum of 4 alive -> succeeds)");
            write(C, 2, 0, 99);  sleep(WRITE_WAIT);

            step("4. Client_1 reads index=0 from replica 1 -- expect 99");
            read(C, 1, 0);  sleep(READ_WAIT);

            step("5. Client_3 reads index=0 from replica 3 -- expect 99");
            read(C, 3, 0);  sleep(READ_WAIT);

            step("6. Client_4 reads from crashed replica 4 -- READ TIMEOUT expected");
            note("Replica 4 is in crashed mode (ignores all messages).",
                 "Client read timeout = " + READ_TIMEOUT + " ms.");
            read(C, 4, 0);  sleep(1_800); // comfortably past the 1000 ms read timeout
        });
    }

    // =========================================================================
    // Helpers -- scenario lifecycle
    // =========================================================================

    private static void runScenario(String sysName, int n, int coordId,
                                    BiConsumer<Map<Integer, ActorRef>, Map<Integer, ActorRef>> body) {
        ActorSystem sys = ActorSystem.create(sysName);
        Map<Integer, ActorRef> R = replicas(sys, n, coordId);
        Map<Integer, ActorRef> C = clients(sys, R, n);
        body.accept(R, C);
        sys.terminate();
        footer();
    }

    // =========================================================================
    // Helpers -- system setup
    // =========================================================================

    private static Map<Integer, ActorRef> replicas(ActorSystem sys, int n, int coordId) {
        Map<Integer, ActorRef> map = new HashMap<>();
        for (int i = 0; i < n; i++)
            map.put(i, sys.actorOf(
                Replica.props(i, MIN_LAT, MAX_LAT, AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                "Replica_" + i));
        InitSystem init = new InitSystem(map, coordId);
        for (ActorRef r : map.values()) r.tell(init, ActorRef.noSender());
        return map;
    }

    private static Map<Integer, ActorRef> clients(ActorSystem sys,
                                                   Map<Integer, ActorRef> R, int n) {
        Map<Integer, ActorRef> map = new HashMap<>();
        for (int i = 0; i < n; i++)
            map.put(i, sys.actorOf(
                Client.props(READ_TIMEOUT, WRITE_TIMEOUT, Optional.of(R.get(i))),
                "Client_" + i));
        return map;
    }

    // =========================================================================
    // Helpers -- client operations
    // =========================================================================

    private static void read(Map<Integer, ActorRef> C, int id, int index) {
        C.get(id).tell(new AbstractClient.ReadRequest(index), ActorRef.noSender());
    }

    private static void write(Map<Integer, ActorRef> C, int id, int index, int value) {
        C.get(id).tell(new AbstractClient.WriteRequest(index, value), ActorRef.noSender());
    }

    private static void crash(Map<Integer, ActorRef> R, int id) {
        R.get(id).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0),
                       ActorRef.noSender());
    }

    private static void crashAll(Map<Integer, ActorRef> R, int... ids) {
        for (int id : ids) crash(R, id);
    }

    private static void arm(Map<Integer, ActorRef> R, int id,
                            AbstractReplica.Crash.Type type, int afterN) {
        R.get(id).tell(new AbstractReplica.Crash(type, afterN), ActorRef.noSender());
    }

    // =========================================================================
    // Helpers -- output
    // =========================================================================

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private static void banner(String title) {
        System.out.println("\n" + "=".repeat(66));
        System.out.println("  " + title);
        System.out.println("=".repeat(66) + "\n");
    }

    private static void step(String msg) {
        System.out.println("\n[STEP] " + msg);
    }

    private static void note(String... lines) {
        for (String line : lines) System.out.println("       " + line);
    }

    private static void footer() {
        System.out.println("\n" + "-".repeat(66));
        System.out.println("  Scenario complete.");
        System.out.println("-".repeat(66) + "\n");
    }
}
