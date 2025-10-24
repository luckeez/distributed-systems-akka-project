package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import it.unitn.ds1.debug.Colors;
import scala.concurrent.duration.Duration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class Client extends AbstractActorWithStash {
  private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
  private final int clientId;
  private final List<ActorRef> replicas;
  private int requestCounter = 0;
  private Map<Messages.RequestInfo, Cancellable> pendingRequestsTimeouts = new HashMap<>();

  public Client(int clientId, List<ActorRef> replicas) {
    this.clientId = clientId;
    this.replicas = replicas;
  }

  static public Props props(int id, List<ActorRef> replicas) {
    return Props.create(Client.class, () -> new Client(id, replicas));
  }

  @Override
  public void preStart() {
    log.info("Client " + this.clientId + " started successfully");
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

  private void scheduleRequestTimeout(Messages.WriteRequest msg) {

    ActorRef replica = this.replicas.get(ThreadLocalRandom.current().nextInt(this.replicas.size()));
    Cancellable timeout = getContext().system().scheduler().scheduleOnce(
        Duration.create(7, TimeUnit.SECONDS),
        replica, // receiver
        msg,
        getContext().system().dispatcher(),
        getSelf()); // sender
    this.pendingRequestsTimeouts.put(msg.requestInfo, timeout);

  }

  private void cancelRequestTimeout(Messages.RequestInfo requestInfo) {
    Cancellable timeout = this.pendingRequestsTimeouts.remove(requestInfo);
    if (timeout != null) {
      timeout.cancel();
    }
  }

  private void onReadRequest(Messages.ReadRequest msg) {
    ActorRef replica = this.replicas.get(ThreadLocalRandom.current().nextInt(this.replicas.size()));
    log.info(Colors.YELLOW + "Client " + this.clientId + " read req to " + replica.path().name() + Colors.RESET);
    introduceNetworkDelay();
    replica.tell(msg, getSelf());
  }

  private void onWriteRequest(Messages.WriteRequest msg) {
    ActorRef replica = this.replicas.get(ThreadLocalRandom.current().nextInt(this.replicas.size()));
    log.info(Colors.YELLOW + "Client " + this.clientId + " write request with value " + msg.value + " to " + replica.path().name() + Colors.RESET);

    if (!pendingRequestsTimeouts.isEmpty()){
      log.info(Colors.RED + "Client " + this.clientId + " stashing new request for value " + msg.value + Colors.RESET);
      stash();
      return;
    }
    introduceNetworkDelay();
    Messages.WriteRequest req = new Messages.WriteRequest(msg.value,
        new Messages.RequestInfo(getSelf(), this.requestCounter++));
    scheduleRequestTimeout(req);
    replica.tell(req, getSelf());

  }

  private void onReadResponse(Messages.ReadResponse msg) {
    log.info(Colors.GREEN + "Client " + this.clientId + " read done " + msg.value + Colors.RESET);
  }

  private void onWriteResponse(Messages.WriteResponse msg) {
    log.info("Client " + this.clientId + " received write response from " + getSender().path().name() + " with outcome: "
        + (msg.success ? Colors.GREEN + "SUCCESS" + Colors.RESET : Colors.RED + "FAILED" + Colors.RESET));
    cancelRequestTimeout(msg.requestInfo);

    unstashAll();
  }
}
