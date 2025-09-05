package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Client extends AbstractActor {
  private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
  private final int clientId;
  private final List<ActorRef> replicas;

  public Client(int clientId, List<ActorRef> replicas) {
    this.clientId = clientId;
    this.replicas = replicas;
  }

  static public Props props(int id, List<ActorRef> replicas) {
    return Props.create(Client.class, () -> new Client(id, replicas));
  }

  @Override
  public void preStart() {
    log.info("Client " + clientId + " started successfully");
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(Messages.ReadRequest.class, this::onReadRequest)
        .match(Messages.WriteRequest.class, this::onWriteRequest)
        .match(Messages.ReadResponse.class, this::onReadResponse)
        .match(Messages.WriteResponse.class, this::onWriteResponse)
        .build();
  }

  private void introduceNetworkDelay() {
    try {
      Thread.sleep(ThreadLocalRandom.current().nextInt(10, 50));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void onReadRequest(Messages.ReadRequest msg) {
    ActorRef replica = replicas.get(ThreadLocalRandom.current().nextInt(replicas.size()));
    log.info("Client " + clientId + " read request to " + replica.path().name());
    introduceNetworkDelay();
    replica.tell(msg, getSelf());
  }

  private void onWriteRequest(Messages.WriteRequest msg) {
    ActorRef replica = replicas.get(ThreadLocalRandom.current().nextInt(replicas.size()));
    log.info("Client " + clientId + " write request with value " + msg.value + " to " + replica.path().name());
    introduceNetworkDelay();
    replica.tell(msg, getSelf());
  }

  private void onReadResponse(Messages.ReadResponse msg) {
    log.info("Client " + clientId + " received read response with value " + msg.value + " from "
        + getSender().path().name());
  }

  private void onWriteResponse(Messages.WriteResponse msg) {
    log.info("Client " + clientId + " recieved write response from " + getSender().path().name() + " with outcome: "
        + (msg.success ? "SUCCESS" : "FAILED"));
  }
}
