package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

public class Replica extends AbstractReplica {
    private Map<Integer, ActorRef> replicas = new HashMap<>();
    private int coordinatorID;
    private int[] positions = new int[AbstractReplica.POSITIONS_LIST_LENGTH];

    private final Map<UpdateId, Update> pendingUpdates = new HashMap<>();
    private final Map<UpdateId, Integer> ackCount = new HashMap<>();
    private final Map<UpdateId, ActorRef> pendingClients = new HashMap<>();
    private final Map<UpdateId, Update> updateHistory = new HashMap<>();
    private UpdateId lastAppliedId = null;
    private int epoch = 0;
    private int seqnum = 0;
    private AbstractReplica.Crash pendingCrash = null;
    private int crashMessagesRemaining = 0;
    private Cancellable heartbeatSender = null;
    private Cancellable heartbeatTimeout = null;
    private final Map<UpdateId, Cancellable> writeOkTimers = new HashMap<>();
    private Cancellable updateTimeout = null;
    private Cancellable electionAckTimer = null;

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
        if (how_to_crash.type == AbstractReplica.Crash.Type.Now) {
            getContext().become(createCrashedBehavior());
        } else {
            pendingCrash = how_to_crash;
            crashMessagesRemaining = how_to_crash.after_n_messages_of_type;
        }
    }

    @Override
    public void initSystem(InitSystem sysInit) {
        // TODO: implement
        this.replicas = new HashMap<>(sysInit.group);
        this.coordinatorID = sysInit.coordinator_id;
        if (this.id == coordinatorID) {
            heartbeatSender = scheduleTimeout(getCoordinatorBeatInterval(), new SendHeartbeat());
        } else {
            heartbeatTimeout = scheduleTimeout(getCoordinatorBeatInterval() * 2, new HeartbeatTimeout());
        }
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

    public static class WriteFromClient implements Serializable {
        public final int index;
        public final int value;
        public final ActorRef clientRef;
        public final int sourceReplicaId;
        public WriteFromClient(int index, int value, ActorRef clientRef, int sourceReplicaId) {
            this.index = index; this.value = value;
            this.clientRef = clientRef; this.sourceReplicaId = sourceReplicaId;
        }
    }

    public static class UpdateId implements Serializable, Comparable<UpdateId> {
        public final int epoch;
        public final int seqnum;

        public UpdateId(int epoch, int seqnum) {
            this.epoch = epoch;
            this.seqnum = seqnum;
        }

        @Override public boolean equals(Object o) {
            if (!(o instanceof UpdateId)) return false;
            UpdateId other = (UpdateId) o;
            return epoch == other.epoch && seqnum == other.seqnum;
        }

        @Override public int hashCode() {
            return 31 * epoch + seqnum;
        }

        @Override public int compareTo(UpdateId other) {
            if (this.epoch != other.epoch) return Integer.compare(this.epoch, other.epoch);
            return Integer.compare(this.seqnum, other.seqnum);
        }
    }

    public static class Update implements Serializable {
        public final UpdateId id;
        public final int index;
        public final int value;
        public final ActorRef clientRef;
        public final int sourceReplicaId;
        public Update(UpdateId id, int index, int value, ActorRef clientRef, int sourceReplicaId) {
            this.id = id; this.index = index; this.value = value;
            this.clientRef = clientRef; this.sourceReplicaId = sourceReplicaId;
        }
    }

    public static class Ack implements Serializable {
        public final UpdateId id;
        public Ack(UpdateId id) { this.id = id; }
    }

    public static class WriteOk implements Serializable {
        public final UpdateId id;
        public WriteOk(UpdateId id) { this.id = id; }
    }

    public static class Heartbeat implements Serializable {}

    private static class SendHeartbeat implements Serializable {}

    private static class HeartbeatTimeout implements Serializable {}

    private static class WriteOkTimeout implements Serializable {
        final UpdateId id;
        WriteOkTimeout(UpdateId id) { this.id = id; }
    }

    private static class UpdateTimeout implements Serializable {}

    private static class ElectionAckTimeout implements Serializable {
        final Election election;
        final int targetId;
        ElectionAckTimeout(Election election, int targetId) {
            this.election = election; this.targetId = targetId;
        }
    }

    public static class Election implements Serializable {
        public final Map<Integer, UpdateId> candidates;
        public Election(Map<Integer, UpdateId> candidates) {
            this.candidates = Collections.unmodifiableMap(new HashMap<>(candidates));
        }
    }

    public static class ElectionAck implements Serializable {}

    public static class Synchronization implements Serializable {
        public final int newCoordinatorId;
        public final Map<UpdateId, Update> history;
        public Synchronization(int newCoordinatorId, Map<UpdateId, Update> history) {
            this.newCoordinatorId = newCoordinatorId;
            this.history = Collections.unmodifiableMap(new HashMap<>(history));
        }
    }

    // =================================================================================
    // Helpers
    // =================================================================================

    private Cancellable scheduleTimeout(long delayMs, Serializable msg) {
        return getContext().system().scheduler().scheduleOnce(
            Duration.create(delayMs, TimeUnit.MILLISECONDS),
            getSelf(),
            msg,
            getContext().system().dispatcher(),
            ActorRef.noSender()
        );
    }

    private void checkCrash(AbstractReplica.Crash.Type type) {
        if (pendingCrash == null || pendingCrash.type != type) return;
        if (--crashMessagesRemaining <= 0) {
            getContext().become(createCrashedBehavior());
            pendingCrash = null;
        }
    }

    private int nextInRingAfter(int id) {
        List<Integer> sorted = new ArrayList<>(replicas.keySet());
        Collections.sort(sorted);
        int idx = sorted.indexOf(id);
        return sorted.get((idx + 1) % sorted.size());
    }

    private void sendElectionWithAckTimer(Election msg, int targetId) {
        tell(msg, replicas.get(targetId));
        if (electionAckTimer != null) electionAckTimer.cancel();
        electionAckTimer = scheduleTimeout(getMaxLatencyPlusTolerance(), new ElectionAckTimeout(msg, targetId));
    }

    private int bestCandidate(Map<Integer, UpdateId> candidates) {
        int best = -1;
        UpdateId bestUid = null;
        for (Map.Entry<Integer, UpdateId> entry : candidates.entrySet()) {
            int rid = entry.getKey();
            UpdateId uid = entry.getValue();
            if (best == -1) {
                best = rid; bestUid = uid;
            } else {
                boolean challengerWins;
                if (uid == null && bestUid == null) challengerWins = rid > best;
                else if (uid == null)  challengerWins = false;
                else if (bestUid == null) challengerWins = true;
                else { int cmp = uid.compareTo(bestUid); challengerWins = cmp > 0 || (cmp == 0 && rid > best); }
                if (challengerWins) { best = rid; bestUid = uid; }
            }
        }
        return best;
    }

    private Receive createCrashedBehavior() {
        return receiveBuilder()
            .matchAny(msg -> {})
            .build();
    }

    private Receive createElectionBehavior() {
        return receiveBuilder()
            .match(Election.class, this::onElection)
            .match(ElectionAck.class, msg -> {
                if (electionAckTimer != null) { electionAckTimer.cancel(); electionAckTimer = null; }
            })
            .match(ElectionAckTimeout.class, this::onElectionAckTimeout)
            .match(Synchronization.class, this::onSynchronization)
            .matchAny(msg -> {})
            .build();
    }

    private void onElection(Election msg) {
        if (!msg.candidates.containsKey(this.id)) {
            // not yet in this election: add self, forward, ACK sender
            Map<Integer, UpdateId> updated = new HashMap<>(msg.candidates);
            updated.put(this.id, lastAppliedId);
            sendElectionWithAckTimer(new Election(updated), nextInRingAfter(this.id));
            tell(new ElectionAck(), getSender());
            checkCrash(AbstractReplica.Crash.Type.Election);
        } else {
            // message came back around: determine winner
            int winner = bestCandidate(msg.candidates);
            if (winner == this.id) {
                callbackOnCoordinatorElected(this.id);
                for (ActorRef r : replicas.values()) {
                    tell(new Synchronization(this.id, updateHistory), r);
                }
            } else {
                sendElectionWithAckTimer(msg, nextInRingAfter(this.id));
            }
        }
    }

    private void onElectionAckTimeout(ElectionAckTimeout msg) {
        int next = nextInRingAfter(msg.targetId);
        if (next == this.id) {
            // went all the way around with no response: sole survivor
            callbackOnCoordinatorElected(this.id);
            for (ActorRef r : replicas.values()) {
                tell(new Synchronization(this.id, updateHistory), r);
            }
        } else {
            sendElectionWithAckTimer(msg.election, next);
        }
    }

    private void onSynchronization(Synchronization msg) {
        if (electionAckTimer != null) { electionAckTimer.cancel(); electionAckTimer = null; }
        // Apply committed updates from winner's history that we're missing
        for (Map.Entry<UpdateId, Update> entry : msg.history.entrySet()) {
            if (!updateHistory.containsKey(entry.getKey())) {
                Update update = entry.getValue();
                pendingUpdates.remove(entry.getKey());
                Cancellable t = writeOkTimers.remove(entry.getKey());
                if (t != null) t.cancel();
                positions[update.index] = update.value;
                updateHistory.put(entry.getKey(), update);
                if (lastAppliedId == null || entry.getKey().compareTo(lastAppliedId) > 0)
                    lastAppliedId = entry.getKey();
                callbackOnUpdateApplied(update.index, update.value);
            }
        }

        coordinatorID = msg.newCoordinatorId;
        epoch = (lastAppliedId != null ? lastAppliedId.epoch : epoch) + 1;
        callbackOnCoordinatorElected(msg.newCoordinatorId);
        getContext().unbecome();

        if (this.id == msg.newCoordinatorId) {
            // complete any uncommitted pending updates before starting new epoch
            seqnum = 0;
            for (UpdateId uid : new ArrayList<>(pendingUpdates.keySet())) {
                ackCount.remove(uid);
                for (ActorRef r : replicas.values()) {
                    tell(new WriteOk(uid), r);
                }
            }
            if (heartbeatTimeout != null) { heartbeatTimeout.cancel(); heartbeatTimeout = null; }
            if (heartbeatSender != null) heartbeatSender.cancel();
            heartbeatSender = scheduleTimeout(getCoordinatorBeatInterval(), new SendHeartbeat());
        } else {
            if (heartbeatTimeout != null) heartbeatTimeout.cancel();
            heartbeatTimeout = scheduleTimeout(getCoordinatorBeatInterval() * 2, new HeartbeatTimeout());
        }
    }

    // =================================================================================
    // Wrapper Handlers
    // =================================================================================

    private void onReadFromClient(Replica.ReadFromClient msg) {
        tell(new AbstractClient.ReadResult(true, msg.index, positions[msg.index], this.id), getSender());
    }

    private void onSendHeartbeat(SendHeartbeat msg) {
        for (ActorRef r : replicas.values()) {
            tell(new Heartbeat(), r);
        }
        heartbeatSender = scheduleTimeout(getCoordinatorBeatInterval(), new SendHeartbeat());
        checkCrash(AbstractReplica.Crash.Type.Heartbeat);
    }

    private void onHeartbeat(Heartbeat msg) {
        if (heartbeatTimeout != null) heartbeatTimeout.cancel();
        heartbeatTimeout = scheduleTimeout(getCoordinatorBeatInterval() * 2, new HeartbeatTimeout());
    }

    private void onHeartbeatTimeout(HeartbeatTimeout msg) {
        startElection();
    }

    private void startElection() {
        callbackOnElectionStarted(coordinatorID);
        getContext().become(createElectionBehavior()); // switch first: freeze lastAppliedId
        Map<Integer, UpdateId> candidates = new HashMap<>();
        candidates.put(this.id, lastAppliedId);
        sendElectionWithAckTimer(new Election(candidates), nextInRingAfter(this.id));
    }

    private void onWriteOkTimeout(WriteOkTimeout msg) {
        if (pendingUpdates.containsKey(msg.id)) startElection();
    }

    private void onUpdateTimeout(UpdateTimeout msg) {
        startElection();
    }

    private void onAck(Replica.Ack msg) {
        if (!ackCount.containsKey(msg.id))
            return;
        int count = ackCount.get(msg.id) + 1;
        ackCount.put(msg.id, count);
        if (count >= replicas.size() / 2 + 1) {
            ackCount.remove(msg.id);
            for (ActorRef r : replicas.values()) {
                tell(new WriteOk(msg.id), r);
            }
        }
    }

    private void onUpdate(Replica.Update msg) {
        if (pendingUpdates.containsKey(msg.id)) return;
        pendingUpdates.put(msg.id, msg);
        if (msg.sourceReplicaId == this.id) {
            pendingClients.put(msg.id, msg.clientRef);
            if (updateTimeout != null) { updateTimeout.cancel(); updateTimeout = null; }
        }
        tell(new Ack(msg.id), getSender());
        writeOkTimers.put(msg.id, scheduleTimeout(getMaxLatencyPlusTolerance(), new WriteOkTimeout(msg.id)));
        checkCrash(AbstractReplica.Crash.Type.Update);
    }

    private void onWriteOk(Replica.WriteOk msg) {
        Cancellable t = writeOkTimers.remove(msg.id);
        if (t != null) t.cancel();
        Update update = pendingUpdates.remove(msg.id);
        if (update == null) return;
        positions[update.index] = update.value;
        updateHistory.put(msg.id, update);
        if (lastAppliedId == null || msg.id.compareTo(lastAppliedId) > 0) lastAppliedId = msg.id;
        callbackOnUpdateApplied(update.index, update.value);
        ActorRef client = pendingClients.remove(msg.id);
        if (client != null) {
            tell(new AbstractClient.WriteResult(true, update.index, update.value, this.id), client);
        }
        checkCrash(AbstractReplica.Crash.Type.WriteOK);
    }

    private void onWriteFromClient(Replica.WriteFromClient msg) {
        WriteFromClient stamped = msg.sourceReplicaId == -1
            ? new WriteFromClient(msg.index, msg.value, msg.clientRef, this.id)
            : msg;
        if (this.id == coordinatorID) {
            UpdateId uid = new UpdateId(epoch, seqnum++);
            Update update = new Update(uid, stamped.index, stamped.value, stamped.clientRef, stamped.sourceReplicaId);
            pendingUpdates.put(uid, update);
            ackCount.put(uid, 1); // coordinator counts its own implicit vote
            for (ActorRef r : replicas.values()) {
                tell(update, r);
            }
        } else {
            tell(stamped, replicas.get(coordinatorID));
            if (updateTimeout != null) updateTimeout.cancel();
            updateTimeout = scheduleTimeout(getMaxLatencyPlusTolerance(), new UpdateTimeout());
        }
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO add your message handlers here .match(, )
                .match(Replica.ReadFromClient.class, this::onReadFromClient)
                .match(Replica.Ack.class, this::onAck)
                .match(Replica.Update.class, this::onUpdate)
                .match(Replica.WriteOk.class, this::onWriteOk)
                .match(Replica.WriteFromClient.class, this::onWriteFromClient)
                .match(SendHeartbeat.class, this::onSendHeartbeat)
                .match(Heartbeat.class, this::onHeartbeat)
                .match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
                .match(WriteOkTimeout.class, this::onWriteOkTimeout)
                .match(UpdateTimeout.class, this::onUpdateTimeout)
                .build();
    }

}
