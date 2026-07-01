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

        Map<Integer, ActorRef> replicas = new HashMap<>(N_REPLICAS);
        for (int i = 0; i < N_REPLICAS; i++) {
            replicas.put(i,
                system.actorOf(
                    Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL),
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
        clients.get(1).tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());
        clients.get(1).tell(new AbstractClient.WriteRequest(0, 67), ActorRef.noSender());
        clients.get(1).tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


        system.terminate();

        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }


}
