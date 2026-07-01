package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Client extends AbstractClient {

    private final Map<ActorRef, ActorRef> channels = new HashMap<>();

    Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
    }

    public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.ofNullable(listener)));
    }

    @Override
    public void sendRead(ActorRef replica, int index) {
        ActorRef channel = channels.computeIfAbsent(replica, r ->
            getContext().actorOf(NetworkChannel.props(r, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY),
                "channel_to_" + r.path().name()));
        channel.tell(new Replica.ReadFromClient(index), getSelf());
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        // TODO: implement
        ActorRef channel = channels.computeIfAbsent(replica, r ->
            getContext().actorOf(NetworkChannel.props(r, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY),
                "channel_to_" + r.path().name()));
        channel.tell(new Replica.WriteFromClient(index, value, getSelf(), -1), getSelf());
    }

    private void onReadResult(AbstractClient.ReadResult result) {
        callbackOnReadResult(result);
    }

    private void onWriteResult(AbstractClient.WriteResult result) {
        callbackOnWriteResult(result);
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO add your message handlers here .match(, )
                .match(AbstractClient.ReadResult.class, this::onReadResult)
                .match(AbstractClient.WriteResult.class, this::onWriteResult)
                .build();
    }

}
