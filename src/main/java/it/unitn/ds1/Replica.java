package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import it.unitn.ds1.debug.Colors;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class Replica extends AbstractActor {
  private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
  private final int replicaId;
  private int currentValue = 0;
  private int currentEpoch = 0;
  private int currentSequenceNumber = 0;
  private boolean isCoordinator = false;
  private int coordinatorId = 0;
  private final int quorumSize;
  private boolean crashed = false;

  private List<ActorRef> replicas = new ArrayList<>();
  private Map<Messages.UpdateId, Integer> pendingAcks = new HashMap<>();
  private Map<Messages.UpdateId, Messages.Update> pendingUpdates = new HashMap<>();
  private List<Messages.Update> updateHistory = new ArrayList<>();

  private Cancellable heartBeatSchedule;
  private Cancellable updateTimeout;
  private Cancellable heartBeatTimeout;
  private Map<String, Cancellable> replicaTimeouts;
  private Cancellable electionAckTimeout;

  private boolean electionInProgress = false;

  private Messages.CrashPoint crashPoint = null;
  private int crashAfterOperations = 0;
  private Map<Messages.CrashPoint, Integer> operationCounts = new HashMap<>();

  {
    for (Messages.CrashPoint point : Messages.CrashPoint.values()) {
      operationCounts.put(point, 0);
    }
  }

  public Replica(int replicaId, int quorumSize) {
    this.replicaId = replicaId;
    this.quorumSize = quorumSize;
    this.isCoordinator = (replicaId == 0);
    this.coordinatorId = 0;
  }

  static public Props props(int id, int quorumSize) {
    return Props.create(Replica.class, () -> new Replica(id, quorumSize));
  }

  @Override
  public void preStart() {
    if (isCoordinator) {

      getContext().become(coordinator());
      scheduleHeartBeat();
      scheduleReplicaTimeouts();
    }

    scheduleHeartBeatTimeout();

  }

  final AbstractActor.Receive crashed() {
    return receiveBuilder()
        .matchAny(msg -> {
        })
        .build();
  }

  final AbstractActor.Receive coordinator() {
    return receiveBuilder()
        .match(Messages.Initialize.class, this::onInitialize)
        .match(Messages.ReadRequest.class, this::onReadRequest)
        .match(Messages.WriteRequest.class, this::onWriteRequest)
        .match(Messages.Ack.class, this::onAck)
        .match(Messages.HeartBeatAck.class, this::onHeartBeatAck)
        .match(Messages.Crash.class, this::onCrash)
        .match(Messages.GetState.class, this::onGetState)
        .match(Messages.Timeout.class, this::onTimeout)
        .match(Messages.ReplicaTimeout.class, this::onReplicaTimeout)
        .match(Messages.SetCrashPoint.class, this::onSetCrashPoint)
        .match(Messages.Crash.class, this::onCrash)
        .matchAny(msg -> {
          log.info("Coordinator " + replicaId + " ignoring message " + msg.getClass().getSimpleName());
        })
        .build();
  }

  @Override
  public void postStop() {
    cancelTimeouts();
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(Messages.Initialize.class, this::onInitialize)
        .match(Messages.ReadRequest.class, this::onReadRequest)
        .match(Messages.WriteRequest.class, this::onWriteRequest)
        .match(Messages.Update.class, this::onUpdate)
        .match(Messages.WriteOk.class, this::onWriteOk)
        .match(Messages.HeartBeat.class, this::onHeartBeat)
        .match(Messages.HeartBeatTimeout.class, this::onHeartBeatTimeout)
        .match(Messages.DetectedReplicaFailure.class, this::onDetectedReplicaFailure)
        .match(Messages.Election.class, this::onElection)
        .match(Messages.Timeout.class, this::onTimeout)
        .match(Messages.Synchronization.class, this::onSynchronization)
        .match(Messages.Crash.class, this::onCrash)
        .match(Messages.SetCrashPoint.class, this::onSetCrashPoint)
        .match(Messages.GetState.class, this::onGetState)
        // .match(Messages.NewCoordinator.class, this::onNewCoordinator)
        .match(Messages.ElectionAck.class, this::onElectionAck)
        .match(Messages.ElectionAckTimeout.class, this::onElectionAckTimeout)
        .match(Messages.StartElection.class, msg -> startElection())
        .match(Messages.NewCoordinator.class, msg -> becomeCoordinator(msg.knownUpdates))
        .build();
  }

  // HELPERS

  private boolean shouldCrash(Messages.CrashPoint point) {
    if (this.crashed || point == null || this.crashPoint != point || this.replicas.size() <= this.quorumSize) {
      return false;
    }
    int currentCount = this.operationCounts.get(point);
    this.operationCounts.put(point, currentCount + 1);

    if (currentCount >= this.crashAfterOperations) {
      log.info(Colors.RED +
          "Replica " + this.replicaId + " crashing at " + point + " after " + currentCount + " operations"
          + Colors.RESET);
      crash();
      return true;
    }

    return false;
  }

  private void crash() {
    this.crashed = true;
    cancelTimeouts();
    getContext().become(crashed());
  }

  private void introduceNetworkDelay() {
    try {
      Thread.sleep(ThreadLocalRandom.current().nextInt(10, 50));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void scheduleReplicaTimeouts() {
    this.replicaTimeouts = new HashMap<>();
    for (ActorRef replica : this.replicas) {
      String name = replica.path().name();
      if (!replica.equals(getSelf())) {
        resetReplicaTimeout(name);
        // log.info("Coordinator " + this.replicaId + " scheduling timeout for " + name); // necessary? 
      } else {
        if (this.replicaTimeouts.get(name) != null) {
          this.replicaTimeouts.get(name).cancel();
        }
      }
    }
  }

  private void resetReplicaTimeout(String name) {
    if (this.replicaTimeouts.get(name) != null) {
      this.replicaTimeouts.get(name).cancel();
    }
    if (this.isCoordinator) {
      // Schedule a new timeout
      Cancellable timeout = getContext().system().scheduler().scheduleOnce(
          Duration.create(5, TimeUnit.SECONDS),
          getSelf(),
          new Messages.ReplicaTimeout(Integer.parseInt(name.replace("Replica", ""))),
          getContext().system().dispatcher(),
          getSelf());
      // Store the timeout for the replica
      this.replicaTimeouts.put(name, timeout);
    }
  }

  private void scheduleHeartBeatTimeout() {
    if (!this.isCoordinator) {
      resetHeartBeatTimeout();
    }
  }

  private void scheduleHeartBeat() {
    if (this.heartBeatSchedule != null) {
      this.heartBeatSchedule.cancel();
    }

    // send an heartbeat message to each replica
    this.heartBeatSchedule = getContext().getSystem().scheduler().scheduleAtFixedRate(
        Duration.create(1, TimeUnit.SECONDS),
        Duration.create(1, TimeUnit.SECONDS),
        () -> {
          if (!this.crashed && this.isCoordinator) {
            for (ActorRef replica : this.replicas) {
              if (!replica.equals(getSelf())) {
                replica.tell(new Messages.HeartBeat(this.replicaId), getSelf());
              }
            }
          }
        },
        getContext().getDispatcher());
  }

  private void resetHeartBeatTimeout() {
    if (this.heartBeatTimeout != null) {
      this.heartBeatTimeout.cancel();
    }

    if (!this.isCoordinator) {
      this.heartBeatTimeout = getContext().getSystem().scheduler().scheduleOnce(
          Duration.create(3, TimeUnit.SECONDS),
          getSelf(),
          new Messages.HeartBeatTimeout(),
          getContext().getDispatcher(),
          getSelf());
    }
  }

  private void setElectionAckTimeout(Messages.Election msg) {
    if (this.electionAckTimeout != null) {
      this.electionAckTimeout.cancel();
    }

    this.electionAckTimeout = getContext().getSystem().scheduler().scheduleOnce(
        Duration.create(500, TimeUnit.MILLISECONDS),
        getSelf(),
        new Messages.ElectionAckTimeout(msg),
        getContext().getDispatcher(),
        getSelf());
  }

  private void cancelTimeouts() {
    if (this.heartBeatSchedule != null) {
      this.heartBeatSchedule.cancel();
    }

    if (this.heartBeatTimeout != null) {
      this.heartBeatTimeout.cancel();
    }

    if (this.updateTimeout != null) {
      this.updateTimeout.cancel();
    }

    if (this.replicaTimeouts != null) {
      for (Entry<String, Cancellable> entry : this.replicaTimeouts.entrySet()) {
        if (entry.getValue() != null) {
          entry.getValue().cancel();
        }
      }
    }

  }

  private boolean alreadyServed(Messages.RequestInfo requestInfo) {
    for (Messages.Update update : this.updateHistory) {
      if (update.requestInfo != null && update.requestInfo.equals(requestInfo)) {
        return true;
      }
    }
    return false;
  }

  private void startElection() {
    if (this.electionInProgress || shouldCrash(Messages.CrashPoint.DURING_ELECTION))
      return;
    this.electionInProgress = true;
    this.replicas.removeIf(r -> r.path().name().equals("Replica" + this.coordinatorId));
    log.info("Replica " + this.replicaId + " started the election process");
    Set<Messages.Update> knownPendingUpdates = new HashSet<>();
    knownPendingUpdates.addAll(this.pendingUpdates.values());
    Messages.Election msg = new Messages.Election(
        this.replicaId,
        this.replicaId,
        getLastKnownUpdateId(),
        knownPendingUpdates);
    setElectionAckTimeout(msg);
    forwardToNextReplica(msg);

    if (shouldCrash(Messages.CrashPoint.DURING_ELECTION_INITIATOR)) {
      return;
    }
  }

  private void becomeCoordinator(Set<Messages.Update> knownPendingUpdates) {
    if (this.isCoordinator)
      return;

    getContext().become(coordinator());

    log.info(Colors.GREEN + "Replica " + this.replicaId + " becoming the new Coordinator" + Colors.RESET);
    if (shouldCrash(Messages.CrashPoint.BEFORE_SYNCHRONIZATION))
      return;
    this.isCoordinator = true;
    this.coordinatorId = replicaId;
    this.currentEpoch++;
    this.currentSequenceNumber = 0;

    try {
      Thread.sleep(500);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // missed updates for synchronization
    broadcast(new Messages.Synchronization(this.replicaId, this.updateHistory), null);
    scheduleHeartBeat();
    scheduleReplicaTimeouts();

    for (Messages.Update update : knownPendingUpdates) {
      log.info("Coordinator " + this.replicaId + " re-broadcasting pending update " + update.updateId +
          " value " + update.value);
      if (!this.updateHistory.contains(update)) {
        Messages.Update newUpdate = new Messages.Update(new Messages.UpdateId(this.currentEpoch, this.currentSequenceNumber),
            update.value, update.requestInfo);
        this.pendingAcks.put(newUpdate.updateId, 0);
        this.pendingUpdates.put(newUpdate.updateId, newUpdate);
        broadcast(newUpdate, null);
      }
    }
    this.electionInProgress = false;
  }

  private void forwardToCoordinator(Serializable msg) {
    for (ActorRef replica : this.replicas) {
      if (replica.path().name().equals("Replica" + this.coordinatorId)) {
        log.info("Replica " + this.replicaId + " forwarding write request to coordinator " + this.coordinatorId);
        introduceNetworkDelay();
        replica.tell(msg, getSelf());
        return;
      }
    }
  }

  private void broadcast(Serializable msg, Messages.CrashPoint crashPoint) {
    for (ActorRef replica : this.replicas) {
      if (replica == getSelf())
        continue;
      if (shouldCrash(crashPoint))
        return;
      introduceNetworkDelay();
      replica.tell(msg, getSelf());
    }
  }

  private boolean isAlive(int replicaId) {
    for (ActorRef replica : this.replicas) {
      if (replica.path().name().equals("Replica" + replicaId)) {
        return true;
      }
    }
    return false;
  }

  private ActorRef getNextReplica() {
    int nextReplica = 0;
    for (int i = 0; i < this.replicas.size(); i++) {
      if (this.replicas.get(i).equals(getSelf())) {
        nextReplica = (i + 1) % this.replicas.size();
        break;
      }
    }
    return this.replicas.get(nextReplica);
  }

  private void forwardToNextReplica(Serializable msg) {
    introduceNetworkDelay();
    getNextReplica().tell(msg, getSelf());
  }

  private void tellToReplica(Serializable msg, int replicaId) {
    for (ActorRef replica : this.replicas) {
      if (replica.path().name().equals("Replica" + replicaId)) {
        introduceNetworkDelay();
        replica.tell(msg, getSelf());
        return;
      }
    }
  }

  private void applyUpdates(List<Messages.Update> knownUpdates) {
    for (Messages.Update update : knownUpdates) {
      if (!this.updateHistory.contains(update)) {
        this.updateHistory.add(update);
        log.info(Colors.GREEN + "Replica " + this.replicaId + " applied update " + update.updateId + " value " + update.value + Colors.RESET);
      }
    }

    this.updateHistory.sort((u1, u2) -> u1.updateId.compareTo(u2.updateId));
    this.currentValue = this.updateHistory.isEmpty() ? 0 : getLastKnownUpdate().value;
  }

  private void applyUpdate(Messages.Update update) {
    if (this.updateHistory.contains(update))
      return;
    this.updateHistory.add(update);
    this.currentValue = update.value;
    this.currentSequenceNumber++;
    if (this.isCoordinator) {
      log.info("Coordinator" + this.replicaId + " applied update " + update.updateId + " value " + update.value);
    } else {
      log.info("Replica" + this.replicaId + " applied update " + update.updateId + " value " + update.value);
    }
  }

  private Messages.Update getLastKnownUpdate() {
    if (this.updateHistory.isEmpty()) {
      return null;
    } else {
      return this.updateHistory.get(this.updateHistory.size() - 1);
    }
  }

  private Messages.UpdateId getLastKnownUpdateId() {
    Messages.Update lastUpdate = getLastKnownUpdate();
    if (lastUpdate == null) {
      return new Messages.UpdateId(0, -1);
    } else {
      return lastUpdate.updateId;
    }
  }

  // onRS
  private void onInitialize(Messages.Initialize msg) {
    if (this.crashed)
      return;
    this.replicas = new ArrayList<>(msg.replicas);
    log.info(
        Colors.GREEN + "Replica " + this.replicaId + " initialized with " + this.replicas.size() + " replicas" + Colors.RESET);

  }

  private void onReadRequest(Messages.ReadRequest msg) {
    if (this.crashed)
      return;
    log.info("Replica " + this.replicaId + " received read request from " + getSender().path().name());
    introduceNetworkDelay();
    getSender().tell(new Messages.ReadResponse(this.currentValue), getSelf());
  }

  private void onWriteRequest(Messages.WriteRequest msg) {
    if (this.crashed)
      return;

    if (this.isCoordinator) {
      if (alreadyServed(msg.requestInfo)) {
        introduceNetworkDelay();
        msg.requestInfo.client.tell(new Messages.WriteResponse(true, msg.requestInfo), getSelf());
        log.info("Coordinator " + this.replicaId + " already served request with " + msg.requestInfo.toString());
      }

      if (shouldCrash(Messages.CrashPoint.BEFORE_SENDING_UPDATE))
        return;

      Messages.UpdateId updateId = new Messages.UpdateId(this.currentEpoch, this.currentSequenceNumber);
      Messages.Update update = new Messages.Update(updateId, msg.value, msg.requestInfo);

      log.info(Colors.CYAN + "Coordinator " + this.replicaId + " initiating update " + updateId + " value " + update.value +
          Colors.RESET);

      this.pendingUpdates.put(updateId, update);
      this.pendingAcks.put(updateId, 0);

      broadcast(update, Messages.CrashPoint.DURING_SENDING_UPDATE);

      log.info("Coordinator " + this.replicaId + " broadcasted update " + updateId + " value " + update.value);

      if (shouldCrash(Messages.CrashPoint.AFTER_SENDING_UPDATE))
        return;
    } else {
      introduceNetworkDelay();
      forwardToCoordinator(new Messages.WriteRequest(msg.value, msg.requestInfo));
    }
    if (this.updateTimeout != null) {
      this.updateTimeout.cancel();
    }
    // this.updateTimeout = getContext().getSystem().scheduler().scheduleOnce(
    //     Duration.create(2, TimeUnit.SECONDS),
    //     getSelf(),
    //     new Messages.Timeout(),
    //     getContext().getDispatcher(),
    //     getSelf());

  }

  private void onUpdate(Messages.Update msg) {

    if (this.updateTimeout != null) {
      this.updateTimeout.cancel();
    }

    log.info(Colors.CYAN + "Replica " + this.replicaId + " received update " + msg.updateId + " value " + msg.value +
        Colors.RESET);

    if (shouldCrash(Messages.CrashPoint.AFTER_RECEIVING_UPDATE))
      return;
    this.pendingUpdates.putIfAbsent(msg.updateId, msg);

    if (shouldCrash(Messages.CrashPoint.BEFORE_SENDING_ACK))
      return;
    introduceNetworkDelay();
    getSender().tell(new Messages.Ack(msg.updateId), getSelf());

    if (shouldCrash(Messages.CrashPoint.AFTER_SENDING_ACK))
      return;
  }

  private void onElectionAck(Messages.ElectionAck msg) {
    if (this.electionAckTimeout != null) {
      this.electionAckTimeout.cancel();
    }
  }

  private void onAck(Messages.Ack msg) {
    if (this.pendingAcks.containsKey(msg.updateId)) {
      int currentAcks = pendingAcks.get(msg.updateId);
      this.pendingAcks.put(msg.updateId, currentAcks + 1);
      if (currentAcks + 1 >= quorumSize) {
        log.info("Coordinator " + this.replicaId + " received quorum for update " + msg.updateId);

        if (shouldCrash(Messages.CrashPoint.BEFORE_SENDING_WRITEOK))
          return;

        Messages.Update update = this.pendingUpdates.get(msg.updateId);
        applyUpdate(update);
        Messages.WriteOk writeOk = new Messages.WriteOk(msg.updateId);

        broadcast(writeOk, null);

        if (shouldCrash(Messages.CrashPoint.AFTER_SENDING_WRITEOK))
          return;

        // notify the client that the write was successful
        update.requestInfo.client.tell(new Messages.WriteResponse(true, update.requestInfo), getSelf());

        // clean up
        this.pendingAcks.remove(msg.updateId);
        this.pendingUpdates.remove(msg.updateId);

        if (this.updateTimeout != null) {
          this.updateTimeout.cancel();
        }
      }
    }
  }

  private void onWriteOk(Messages.WriteOk msg) {

    Messages.Update update = this.pendingUpdates.get(msg.updateId);
    if (update != null) {
      applyUpdate(update);
      this.pendingUpdates.remove(msg.updateId);
    } else {
      return;
    }

    if (shouldCrash(Messages.CrashPoint.AFTER_RECEIVING_WRITEOK))
      return;

    // Reset heartbeat timeout since we heard from coordinator
    resetHeartBeatTimeout();
  }

  private void onHeartBeat(Messages.HeartBeat msg) {
    resetHeartBeatTimeout();
    introduceNetworkDelay();
    getSender().tell(new Messages.HeartBeatAck(this.replicaId), getSelf());
  }

  private void onHeartBeatAck(Messages.HeartBeatAck msg) {
    String name = "Replica" + msg.replicaId;
    resetReplicaTimeout(name);
  }

  private void onHeartBeatTimeout(Messages.HeartBeatTimeout msg) {
    log.info(Colors.RED + "Replica " + this.replicaId + " detected coordinator failure of replica " + coordinatorId +
        Colors.RESET);

    // Start election
    if (!this.electionInProgress) {
        getContext().getSystem().scheduler().scheduleOnce(
            Duration.create(this.replicaId * 80, TimeUnit.MILLISECONDS),
            getSelf(),
            new Messages.StartElection(),
            getContext().getDispatcher(),
            getSelf()
        );
      // TODO: election should finish within a timeout, otherwise restart it
      // FIX: election is enough robust to handle this case
    }
  }

  private void onReplicaTimeout(Messages.ReplicaTimeout msg) {

    log.warning(Colors.RED + "Coordinator " + this.replicaId + " detected failure of replica " + msg.replicaId +
        Colors.RESET);

    // remove the crashed replica from the group
    this.replicas.removeIf(r -> r.path().name().equals("Replica" + msg.replicaId));

    // notify the detected failure to other replicas
    broadcast(new Messages.DetectedReplicaFailure(msg.replicaId), null);
  }

  private void onElectionAckTimeout(Messages.ElectionAckTimeout msg) {
    ActorRef nextReplica = getNextReplica();
    int nextReplicaInt = Integer.parseInt(nextReplica.path().name().replace("Replica", ""));
    this.replicas.removeIf(r -> r.equals(nextReplica));
    log.info(Colors.RED + "Replica " + this.replicaId + " timeout waiting for election ack from " +
        nextReplica.path().name() + ", assuming it crashed" + Colors.RESET);
    broadcast(new Messages.DetectedReplicaFailure(nextReplicaInt), null);

    if (nextReplicaInt == msg.msg.initiatorId){
      log.info(Colors.BLUE + "Replica " + this.replicaId + " restarting election process since the initiator crashed" + Colors.RESET);
      this.electionInProgress = false;
      startElection();
      return;
    }
    setElectionAckTimeout(msg.msg);
    forwardToNextReplica(msg.msg);
  }

  private void onTimeout(Messages.Timeout msg) {
    if (this.isCoordinator) {
      log.info("Coordinator " + this.replicaId + " timeout waiting for acks");
      // TODO: think about what to do if the coordinator times out waiting for ACKs
    } else {
      log.info("Replica " + this.replicaId + " timeout waiting for update message, starting eletion process...");
      startElection();
    }
  }

  private void onDetectedReplicaFailure(Messages.DetectedReplicaFailure msg) {
    log.warning(Colors.RED +
        "Replica " + this.replicaId + " received ReplicaFailure message, replica" + msg.failedReplicaId
        + " crashed" +
        Colors.RESET);

    // the replica received the failure notication of another replica from the
    // coordinator
    // and proceed to remove the replica from the group
    this.replicas.removeIf(r -> r.path().name().equals("Replica" + msg.failedReplicaId));
  }

  private void onElection(Messages.Election msg) {
    if (shouldCrash(Messages.CrashPoint.DURING_ELECTION))
      return;

    if (!isAlive(msg.initiatorId)) {
      return;
    }
    getSender().tell(new Messages.ElectionAck(this.replicaId), getSelf());
    log.info("Replica " + this.replicaId + " received election message from " + getSender().path().name());

    if (!this.electionInProgress) {
      this.electionInProgress = true;
      this.replicas.removeIf(r -> r.path().name().equals("Replica" + this.coordinatorId));
    }

    if (msg.initiatorId == this.replicaId) {
      if (this.replicaId == msg.bestCoordinator) {
        becomeCoordinator(msg.knownPendingUpdates);
      }

      log.info(Colors.BLUE + "Replica " + this.replicaId + " telling replica " + msg.bestCoordinator + " to be the new coordinator" + Colors.RESET);
      tellToReplica(new Messages.NewCoordinator(msg.bestCoordinator, msg.knownPendingUpdates), msg.bestCoordinator);
      return;
    }

    Messages.Update myLastUpdate = getLastKnownUpdate();
    Messages.UpdateId myLastUpdateId = myLastUpdate == null ? new Messages.UpdateId(0, -1) : myLastUpdate.updateId;

    Set<Messages.Update> knownPendingUpdates = new HashSet<>(msg.knownPendingUpdates);
    knownPendingUpdates.addAll(pendingUpdates.values());
    if (myLastUpdateId.compareTo(msg.bestUpdateId) > 0 ||
        (myLastUpdateId.compareTo(msg.bestUpdateId) == 0 && replicaId > msg.bestCoordinator)) {
      Messages.Election newMsg = new Messages.Election(msg.initiatorId,this.replicaId,myLastUpdateId,knownPendingUpdates);
      setElectionAckTimeout(newMsg);
      forwardToNextReplica(newMsg);
    } else {
      Messages.Election newMsg = new Messages.Election(msg.initiatorId,msg.bestCoordinator,msg.bestUpdateId,knownPendingUpdates);
      setElectionAckTimeout(newMsg);
      forwardToNextReplica(newMsg);
    }
  }

  private void onSynchronization(Messages.Synchronization msg) {
    log.info("Replica " + this.replicaId + " received synchronization message from new coordinator " + msg.newCoordinatorId);
    this.coordinatorId = msg.newCoordinatorId;
    this.isCoordinator = (replicaId == coordinatorId);
    this.currentEpoch++;
    this.currentSequenceNumber = 0;
    this.electionInProgress = false;
    this.pendingUpdates.clear();

    // Apply missed updates
    applyUpdates(msg.missedUpdates);
    if (shouldCrash(Messages.CrashPoint.AFTER_SYNCHRONIZATION))
      return;
    if (!this.isCoordinator) {
      resetHeartBeatTimeout();
    }
  }

  private void onCrash(Messages.Crash msg) {
    log.info(Colors.RED + "Replica " + this.replicaId + " crashing now!" + Colors.RESET);
    crash();
  }

  private void onSetCrashPoint(Messages.SetCrashPoint msg) {
    this.crashPoint = msg.crashPoint;
    this.crashAfterOperations = msg.afterOperations;
    log.info(
        "Replica " + this.replicaId + " set to crash at " + this.crashPoint + " after " + this.crashAfterOperations + " operations");
  }

  private void onGetState(Messages.GetState msg) {
    String status = String.format(
        "Replica %d | Coordinator: %b | Epoch: %d | SeqNum: %d | Value: %d | LastUpdateId: %s | ElectionInProgress: %b | Crashed: %b, GroupSize: %d",
        this.replicaId, this.isCoordinator, this.currentEpoch, this.currentSequenceNumber, this.currentValue,
        this.updateHistory.isEmpty() ? new Messages.UpdateId(0, -1).toString()
            : getLastKnownUpdate().updateId.toString(),
        this.electionInProgress, this.crashed, this.replicas.size());
    log.info(Colors.BLUE + status + Colors.RESET);
  }

  private void onNewCoordinator(Messages.NewCoordinator msg) {
    if (msg.newCoordinatorId == this.replicaId) {
      becomeCoordinator(msg.knownUpdates);
      return;
    } 
    // NOTE: Addirittura fare messaggio collegato direttamente con la funzione becomeCoordinator ??
  }
}
