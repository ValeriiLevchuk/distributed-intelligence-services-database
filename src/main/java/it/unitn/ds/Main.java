package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractReplica.InitSystem;

public class Main {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("START");
        System.out.println("========================================\n");

        final int N_REPLICAS = 4;
        final int COORDINATOR_ID = 0;
        final ActorSystem system = ActorSystem.create("TestMain");

        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);

        final int MIN_LAT = 50, MAX_LAT = 100;
        Map<Integer, ActorRef> replicas = new HashMap<>(N_REPLICAS);
        for (int i = 0; i < N_REPLICAS; i++) {
            replicas.put(i,
                system.actorOf(
                    Replica.props(i, MIN_LAT, MAX_LAT, AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                    "Replica_" + i
                )
            );
        }

        InitSystem initMsg = new InitSystem(replicas, COORDINATOR_ID);
        for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
            entry.getValue().tell(initMsg, ActorRef.noSender());
        }

        // TODO: Create your clients
        final int N_CLIENTS = 3;
        Map<Integer, ActorRef> clients = new HashMap<>(N_CLIENTS);
        for (int i = 0; i < N_CLIENTS; i++) {
            clients.put(i,
                    system.actorOf(
                            Client.props(30, 30, Optional.of(replicas.get(i))),
                            "Client_" + i
                    )
            );
            Logger.log("Client_" + i + " created");
        }
        
        // TODO: Implement your main logic

        // Phase 1: normal read/write/read via replica 1
        clients.get(1).tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());
        clients.get(1).tell(new AbstractClient.WriteRequest(0, 67), ActorRef.noSender());
        clients.get(1).tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());
        try { Thread.sleep(2000); } catch (InterruptedException e) { throw new RuntimeException(e); }

        // Phase 2: crash the coordinator
        System.out.println("\n>>> CRASHING COORDINATOR (replica 0) <<<\n");
        replicas.get(0).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0), ActorRef.noSender());

        // Phase 3: wait for election to complete (2x heartbeat timeout + ring + buffer)
        try { Thread.sleep(3500); } catch (InterruptedException e) { throw new RuntimeException(e); }

        // Phase 4: send a write via replica 2 (non-coordinator) — new coordinator must handle it
        System.out.println("\n>>> SENDING WRITE AFTER ELECTION <<<\n");
        clients.get(2).tell(new AbstractClient.WriteRequest(0, 99), ActorRef.noSender());
        try { Thread.sleep(2000); } catch (InterruptedException e) { throw new RuntimeException(e); }


        system.terminate();

        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }


}
