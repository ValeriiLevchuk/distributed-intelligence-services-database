package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Replica extends AbstractReplica {
    private Map<Integer, ActorRef> replicas = new HashMap<>();
    private int coordinatorID;
    private int[] positions = new int[AbstractReplica.POSITIONS_LIST_LENGTH];

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        // TODO: implement
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    @Override
    public int getSystemNumberOfActors() {
        // TODO: implement
        return replicas.size();
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        // TODO: implement
    }

    @Override
    public void initSystem(InitSystem sysInit) {
        // TODO: implement
        this.replicas = new HashMap<>(sysInit.group);
        this.coordinatorID = sysInit.coordinator_id;
    }

    // =================================================================================
    // API Messages
    // =================================================================================

    public static class ReadFromClient {
        ActorRef replica;
        int index;

        public ReadFromClient(int index) {
            this(index, null);
        }

        public ReadFromClient(int index, ActorRef replica) {
            this.replica = replica;
            this.index = index;
        }
    }

    // =================================================================================
    // Wrapper Handlers
    // =================================================================================

    private void onReadFromClient(Replica.ReadFromClient msg) {
        tell(new AbstractClient.ReadResult(true, msg.index, positions[msg.index], this.id), getSender());
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO add your message handlers here .match(, )
                .match(Replica.ReadFromClient.class, this::onReadFromClient)
                .build();
    }

}
