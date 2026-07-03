package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

public class Client extends AbstractClient {

    private final Map<ActorRef, ActorRef> channels = new HashMap<>();
    private Cancellable readTimer = null;
    private Cancellable writeTimer = null;

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
        if (readTimer != null) readTimer.cancel();
        readTimer = getContext().system().scheduler().scheduleOnce(
            Duration.create(getReadTimeoutDelay(), TimeUnit.MILLISECONDS),
            getSelf(),
            new AbstractClient.ReadTimeout(getSelf(), replica, index),
            getContext().system().dispatcher(),
            ActorRef.noSender()
        );
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        ActorRef channel = channels.computeIfAbsent(replica, r ->
            getContext().actorOf(NetworkChannel.props(r, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY),
                "channel_to_" + r.path().name()));
        channel.tell(new Replica.WriteFromClient(index, value, getSelf(), -1), getSelf());
        if (writeTimer != null) writeTimer.cancel();
        writeTimer = getContext().system().scheduler().scheduleOnce(
            Duration.create(getWriteTimeoutDelay(), TimeUnit.MILLISECONDS),
            getSelf(),
            new AbstractClient.WriteTimeout(getSelf(), replica, index, value),
            getContext().system().dispatcher(),
            ActorRef.noSender()
        );
    }

    private void onReadResult(AbstractClient.ReadResult result) {
        if (readTimer != null) { readTimer.cancel(); readTimer = null; }
        callbackOnReadResult(result);
    }

    private void onWriteResult(AbstractClient.WriteResult result) {
        if (writeTimer != null) { writeTimer.cancel(); writeTimer = null; }
        callbackOnWriteResult(result);
    }

    private void onReadTimeout(AbstractClient.ReadTimeout msg) {
        readTimer = null;
        callbackOnReadTimeout(msg);
    }

    private void onWriteTimeout(AbstractClient.WriteTimeout msg) {
        writeTimer = null;
        callbackOnWriteTimeout(msg);
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(AbstractClient.ReadResult.class, this::onReadResult)
                .match(AbstractClient.WriteResult.class, this::onWriteResult)
                .match(AbstractClient.ReadTimeout.class, this::onReadTimeout)
                .match(AbstractClient.WriteTimeout.class, this::onWriteTimeout)
                .build();
    }

}
