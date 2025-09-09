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
        .match(Messages.NewCoordinator.class, this::onNewCoordinator)
        .match(Messages.ElectionAck.class, this::onElectionAck)
        .match(Messages.ElectionAckTimeout.class, this::onElectionAckTimeout)
        .match(Messages.StartElection.class, msg -> startElection())
        .build();
  }

  // HELPERS

  private boolean shouldCrash(Messages.CrashPoint point) {
    if (this.crashed || point == null || crashPoint != point || replicas.size() <= quorumSize) {
      return false;
    }
    int currentCount = operationCounts.get(point);
    operationCounts.put(point, currentCount + 1);

    if (currentCount >= crashAfterOperations) {
      log.info(Colors.RED +
          "Replica " + replicaId + " crashing at " + point + " after " + currentCount + " operations"
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
    replicaTimeouts = new HashMap<>();
    for (ActorRef replica : replicas) {
      String name = replica.path().name();
      if (!replica.equals(getSelf())) {
        resetReplicaTimeout(name);
        log.info("Coordinator " + replicaId + " scheduling timeout for " + name);
      } else {
        if (replicaTimeouts.get(name) != null) {
          replicaTimeouts.get(name).cancel();
        }
      }
    }
  }

  private void resetReplicaTimeout(String name) {
    if (replicaTimeouts.get(name) != null) {
      replicaTimeouts.get(name).cancel();
    }
    if (isCoordinator) {
      // Schedule a new timeout
      Cancellable timeout = getContext().system().scheduler().scheduleOnce(
          Duration.create(5, TimeUnit.SECONDS),
          getSelf(),
          new Messages.ReplicaTimeout(Integer.parseInt(name.replace("Replica", ""))),
          getContext().system().dispatcher(),
          getSelf());
      // Store the timeout for the replica
      replicaTimeouts.put(name, timeout);
    }
  }

  private void scheduleHeartBeatTimeout() {
    if (!isCoordinator) {
      resetHeartBeatTimeout();
    }
  }

  private void scheduleHeartBeat() {
    if (heartBeatSchedule != null) {
      heartBeatSchedule.cancel();
    }

    // send an heartbeat message to each replica
    heartBeatSchedule = getContext().getSystem().scheduler().scheduleAtFixedRate(
        Duration.create(1, TimeUnit.SECONDS),
        Duration.create(1, TimeUnit.SECONDS),
        () -> {
          if (!crashed && isCoordinator) {
            for (ActorRef replica : replicas) {
              if (!replica.equals(getSelf())) {
                replica.tell(new Messages.HeartBeat(replicaId), getSelf());
              }
            }
          }
        },
        getContext().getDispatcher());
  }

  private void resetHeartBeatTimeout() {
    if (heartBeatTimeout != null) {
      heartBeatTimeout.cancel();
    }

    if (!isCoordinator) {
      heartBeatTimeout = getContext().getSystem().scheduler().scheduleOnce(
          Duration.create(3, TimeUnit.SECONDS),
          getSelf(),
          new Messages.HeartBeatTimeout(),
          getContext().getDispatcher(),
          getSelf());
    }
  }

  private void setElectionAckTimeout(Messages.Election msg) {
    if (electionAckTimeout != null) {
      electionAckTimeout.cancel();
    }

    electionAckTimeout = getContext().getSystem().scheduler().scheduleOnce(
        Duration.create(500, TimeUnit.MILLISECONDS),
        getSelf(),
        new Messages.ElectionAckTimeout(msg),
        getContext().getDispatcher(),
        getSelf());
  }

  private void cancelTimeouts() {
    if (heartBeatSchedule != null) {
      heartBeatSchedule.cancel();
    }

    if (heartBeatTimeout != null) {
      heartBeatTimeout.cancel();
    }

    if (updateTimeout != null) {
      updateTimeout.cancel();
    }

    if (replicaTimeouts != null) {
      for (Entry<String, Cancellable> entry : replicaTimeouts.entrySet()) {
        if (entry.getValue() != null) {
          entry.getValue().cancel();
        }
      }
    }

  }

  private boolean alreadyServed(Messages.RequestInfo requestInfo) {
    for (Messages.Update update : updateHistory) {
      if (update.requestInfo != null && update.requestInfo.equals(requestInfo)) {
        return true;
      }
    }
    return false;
  }

  private void startElection() {
    if (electionInProgress || shouldCrash(Messages.CrashPoint.DURING_ELECTION))
      return;
    electionInProgress = true;
    replicas.removeIf(r -> r.path().name().equals("Replica" + coordinatorId));
    log.info("Replica " + replicaId + " started the election process");
    Set<Messages.Update> knownPendingUpdates = new HashSet<>();
    knownPendingUpdates.addAll(pendingUpdates.values());
    Messages.Election msg = new Messages.Election(
        replicaId,
        replicaId,
        getLastKnownUpdateId(),
        knownPendingUpdates);
    setElectionAckTimeout(msg);
    forwardToNextReplica(msg);
  }

  private void becomeCoordinator(Set<Messages.Update> knownPendingUpdates) {
    if (isCoordinator)
      return;

    getContext().become(coordinator());

    log.info(Colors.GREEN + "Replica " + replicaId + " becoming the new Coordinator" + Colors.RESET);
    if (shouldCrash(Messages.CrashPoint.BEFORE_SYNCHRONIZATION))
      return;
    isCoordinator = true;
    coordinatorId = replicaId;
    currentEpoch++;
    currentSequenceNumber = 0;

    try {
      Thread.sleep(500);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // missed updates for synchronization
    broadcast(new Messages.Synchronization(replicaId, updateHistory), null);
    scheduleHeartBeat();
    scheduleReplicaTimeouts();

    for (Messages.Update update : knownPendingUpdates) {
      log.info("Coordinator " + replicaId + " re-broadcasting pending update " + update.updateId +
          " value " + update.value);
      if (!this.updateHistory.contains(update)) {
        Messages.Update newUpdate = new Messages.Update(new Messages.UpdateId(currentEpoch, currentSequenceNumber),
            update.value, update.requestInfo);
        pendingAcks.put(newUpdate.updateId, 0);
        pendingUpdates.put(newUpdate.updateId, newUpdate);
        broadcast(newUpdate, null);
      }
    }
    electionInProgress = false;
  }

  private void forwardToCoordinator(Serializable msg) {
    for (ActorRef replica : replicas) {
      if (replica.path().name().equals("Replica" + coordinatorId)) {
        log.info("Replica " + replicaId + " forwarding write request to coordinator " + coordinatorId);
        introduceNetworkDelay();
        replica.tell(msg, getSelf());
        return;
      }
    }
  }

  private void broadcast(Serializable msg, Messages.CrashPoint crashPoint) {
    for (ActorRef replica : replicas) {
      if (replica == getSelf())
        continue;
      if (shouldCrash(crashPoint))
        return;
      introduceNetworkDelay();
      replica.tell(msg, getSelf());
    }
  }

  private boolean isAlive(int replicaId) {
    for (ActorRef replica : replicas) {
      if (replica.path().name().equals("Replica" + replicaId)) {
        return true;
      }
    }
    return false;
  }

  private ActorRef getNextReplica() {
    int nextReplica = 0;
    for (int i = 0; i < replicas.size(); i++) {
      if (replicas.get(i).equals(getSelf())) {
        nextReplica = (i + 1) % replicas.size();
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
      if (!updateHistory.contains(update)) {
        updateHistory.add(update);
        log.info("Replica " + replicaId + " applied update " + update.updateId + " value " + update.value);
      }
    }

    updateHistory.sort((u1, u2) -> u1.updateId.compareTo(u2.updateId));
    currentValue = updateHistory.isEmpty() ? 0 : getLastKnownUpdate().value;
  }

  private void applyUpdate(Messages.Update update) {
    if (updateHistory.contains(update))
      return;
    updateHistory.add(update);
    currentValue = update.value;
    currentSequenceNumber++;
    if (isCoordinator) {
      log.info("Coordinator" + replicaId + " applied update " + update.updateId + " value " + update.value);
    } else {
      log.info("Replica" + replicaId + " applied update " + update.updateId + " value " + update.value);
    }
  }

  private Messages.Update getLastKnownUpdate() {
    if (updateHistory.isEmpty()) {
      return null;
    } else {
      return updateHistory.get(updateHistory.size() - 1);
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
    if (crashed)
      return;
    this.replicas = new ArrayList<>(msg.replicas);
    log.info(
        Colors.GREEN + "Replica " + replicaId + " initialized with " + replicas.size() + " replicas" + Colors.RESET);

  }

  private void onReadRequest(Messages.ReadRequest msg) {
    if (crashed)
      return;
    log.info("Replica " + replicaId + " received read request from " + getSender().path().name());
    introduceNetworkDelay();
    getSender().tell(new Messages.ReadResponse(currentValue), getSelf());
  }

  private void onWriteRequest(Messages.WriteRequest msg) {
    if (crashed)
      return;

    if (isCoordinator) {
      if (alreadyServed(msg.requestInfo)) {
        introduceNetworkDelay();
        msg.requestInfo.client.tell(new Messages.WriteResponse(true, msg.requestInfo), getSelf());
        log.info("Coordinator " + replicaId + " already served request with " + msg.requestInfo.toString());
      }

      if (shouldCrash(Messages.CrashPoint.BEFORE_SENDING_UPDATE))
        return;

      Messages.UpdateId updateId = new Messages.UpdateId(currentEpoch, currentSequenceNumber);
      Messages.Update update = new Messages.Update(updateId, msg.value, msg.requestInfo);

      log.info(Colors.CYAN + "Coordinator " + replicaId + " initiating update " + updateId + " value " + update.value +
          Colors.RESET);

      pendingUpdates.put(updateId, update);
      pendingAcks.put(updateId, 0);

      broadcast(update, Messages.CrashPoint.DURING_SENDING_UPDATE);

      if (shouldCrash(Messages.CrashPoint.AFTER_SENDING_UPDATE))
        return;
    } else {
      introduceNetworkDelay();
      forwardToCoordinator(new Messages.WriteRequest(msg.value, msg.requestInfo));
    }
    if (updateTimeout != null) {
      updateTimeout.cancel();
    }
    updateTimeout = getContext().getSystem().scheduler().scheduleOnce(
        Duration.create(2, TimeUnit.SECONDS),
        getSelf(),
        new Messages.Timeout(),
        getContext().getDispatcher(),
        getSelf());

  }

  private void onUpdate(Messages.Update msg) {

    if (updateTimeout != null) {
      updateTimeout.cancel();
    }

    log.info(Colors.CYAN + "Replica " + replicaId + " received update " + msg.updateId + " value " + msg.value +
        Colors.RESET);

    if (shouldCrash(Messages.CrashPoint.AFTER_RECEIVING_UPDATE))
      return;
    pendingUpdates.putIfAbsent(msg.updateId, msg);

    if (shouldCrash(Messages.CrashPoint.BEFORE_SENDING_ACK))
      return;
    introduceNetworkDelay();
    getSender().tell(new Messages.Ack(msg.updateId), getSelf());

    if (shouldCrash(Messages.CrashPoint.AFTER_SENDING_ACK))
      return;
  }

  private void onElectionAck(Messages.ElectionAck msg) {
    if (electionAckTimeout != null) {
      electionAckTimeout.cancel();
    }
  }

  private void onAck(Messages.Ack msg) {
    if (pendingAcks.containsKey(msg.updateId)) {
      int currentAcks = pendingAcks.get(msg.updateId);
      pendingAcks.put(msg.updateId, currentAcks + 1);
      if (currentAcks + 1 >= quorumSize) {
        log.info("Coordinator " + replicaId + " received quorum for update " + msg.updateId);

        if (shouldCrash(Messages.CrashPoint.BEFORE_SENDING_WRITEOK))
          return;

        Messages.Update update = pendingUpdates.get(msg.updateId);
        applyUpdate(update);
        Messages.WriteOk writeOk = new Messages.WriteOk(msg.updateId);

        broadcast(writeOk, null);

        if (shouldCrash(Messages.CrashPoint.AFTER_SENDING_WRITEOK))
          return;

        // notify the client that the write was successful
        update.requestInfo.client.tell(new Messages.WriteResponse(true, update.requestInfo), getSelf());

        // clean up
        pendingAcks.remove(msg.updateId);
        pendingUpdates.remove(msg.updateId);

        if (updateTimeout != null) {
          updateTimeout.cancel();
        }
      }
    }
  }

  private void onWriteOk(Messages.WriteOk msg) {

    Messages.Update update = pendingUpdates.get(msg.updateId);
    if (update != null) {
      applyUpdate(update);
      pendingUpdates.remove(msg.updateId);
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
    getSender().tell(new Messages.HeartBeatAck(replicaId), getSelf());
  }

  private void onHeartBeatAck(Messages.HeartBeatAck msg) {
    String name = "Replica" + msg.replicaId;
    resetReplicaTimeout(name);
  }

  private void onHeartBeatTimeout(Messages.HeartBeatTimeout msg) {
    log.info(Colors.RED + "Replica " + replicaId + " detected coordinator failure of replica " + coordinatorId +
        Colors.RESET);

    // Start election
    if (!electionInProgress) {
        getContext().getSystem().scheduler().scheduleOnce(
            Duration.create(replicaId * 80, TimeUnit.MILLISECONDS),
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

    log.warning(Colors.RED + "Coordinator " + replicaId + " detected failure of replica " + msg.replicaId +
        Colors.RESET);

    // remove the crashed replica from the group
    replicas.removeIf(r -> r.path().name().equals("Replica" + msg.replicaId));

    // notify the detected failure to other replicas
    broadcast(new Messages.DetectedReplicaFailure(msg.replicaId), null);
  }

  private void onElectionAckTimeout(Messages.ElectionAckTimeout msg) {
    ActorRef nextReplica = getNextReplica();
    this.replicas.removeIf(r -> r.equals(nextReplica));
    log.info(Colors.RED + "Replica " + this.replicaId + " timeout waiting for election ack from " +
        nextReplica.path().name() + ", assuming it crashed" + Colors.RESET);
    broadcast(new Messages.DetectedReplicaFailure(
        Integer.parseInt(nextReplica.path().name().replace("Replica", ""))), null);

    setElectionAckTimeout(msg.msg);
    forwardToNextReplica(msg.msg);
  }

  private void onTimeout(Messages.Timeout msg) {
    if (isCoordinator) {
      log.info("Coordinator " + replicaId + " timeout waiting for acks");
      // TODO: think about what to do if the coordinator times out waiting for ACKs
    } else {
      log.info("Replica " + replicaId + " timeout waiting for update message, starting eletion process...");
      startElection();
    }
  }

  private void onDetectedReplicaFailure(Messages.DetectedReplicaFailure msg) {
    log.warning(Colors.RED +
        "Replica " + replicaId + " received ReplicaFailure message, replica" + msg.failedReplicaId
        + " crashed" +
        Colors.RESET);

    // the replica received the failure notication of another replica from the
    // coordinator
    // and proceed to remove the replica from the group
    replicas.removeIf(r -> r.path().name().equals("Replica" + msg.failedReplicaId));
  }

  private void onElection(Messages.Election msg) {
    if (shouldCrash(Messages.CrashPoint.DURING_ELECTION))
      return;

    if (!isAlive(msg.initiatorId)) {
      return;
    }
    getSender().tell(new Messages.ElectionAck(this.replicaId), getSelf());
    log.info("Replica " + replicaId + " received election message from " + getSender().path().name());

    if (!electionInProgress) {
      electionInProgress = true;
      replicas.removeIf(r -> r.path().name().equals("Replica" + coordinatorId));
    }

    if (msg.initiatorId == replicaId) {
      if (replicaId == msg.bestCoordinator) {
        becomeCoordinator(msg.knownPendingUpdates);
      }
      // } else {
      //   log.info(Colors.BLUE + "Replica " + replicaId + " forwarding NewCoordinator message for replica " +
      //       msg.bestCoordinator + Colors.RESET);
      //   forwardToNextReplica(new Messages.NewCoordinator(msg.bestCoordinator, msg.knownPendingUpdates));
      //   return;
      // }
      log.info(Colors.BLUE + "Replica " + replicaId + " telling replica " +
          msg.bestCoordinator + "to be the new coordinator" + Colors.RESET);
      tellToReplica(new Messages.NewCoordinator(msg.bestCoordinator, msg.knownPendingUpdates), msg.bestCoordinator);
      return;
    }

    Messages.Update myLastUpdate = getLastKnownUpdate();
    Messages.UpdateId myLastUpdateId = myLastUpdate == null ? new Messages.UpdateId(0, -1) : myLastUpdate.updateId;

    Set<Messages.Update> knownPendingUpdates = new HashSet<>(msg.knownPendingUpdates);
    knownPendingUpdates.addAll(pendingUpdates.values());
    if (myLastUpdateId.compareTo(msg.bestUpdateId) > 0 ||
        (myLastUpdateId.compareTo(msg.bestUpdateId) == 0 && replicaId > msg.bestCoordinator)) {
      Messages.Election newMsg = new Messages.Election(msg.initiatorId,replicaId,myLastUpdateId,knownPendingUpdates);
      setElectionAckTimeout(newMsg);
      forwardToNextReplica(newMsg);
    } else {
      Messages.Election newMsg = new Messages.Election(msg.initiatorId,msg.bestCoordinator,msg.bestUpdateId,knownPendingUpdates);
      setElectionAckTimeout(newMsg);
      forwardToNextReplica(newMsg);
    }
  }

  private void onSynchronization(Messages.Synchronization msg) {
    log.info("Replica " + replicaId + " received synchronization message from new coordinator " + msg.newCoordinatorId);
    coordinatorId = msg.newCoordinatorId;
    isCoordinator = (replicaId == coordinatorId);
    currentEpoch++;
    currentSequenceNumber = 0;
    electionInProgress = false;
    pendingUpdates.clear();

    // Apply missed updates
    applyUpdates(msg.missedUpdates);
    if (shouldCrash(Messages.CrashPoint.AFTER_SYNCHRONIZATION))
      return;
    if (!isCoordinator) {
      resetHeartBeatTimeout();
    }
  }

  private void onCrash(Messages.Crash msg) {
    log.info(Colors.RED + "Replica " + replicaId + " crashing now!" + Colors.RESET);
    crash();
  }

  private void onSetCrashPoint(Messages.SetCrashPoint msg) {
    this.crashPoint = msg.crashPoint;
    this.crashAfterOperations = msg.afterOperations;
    log.info(
        "Replica " + replicaId + " set to crash at " + crashPoint + " after " + crashAfterOperations + " operations");
  }

  private void onGetState(Messages.GetState msg) {
    String status = String.format(
        "Replica %d | Coordinator: %b | Epoch: %d | SeqNum: %d | Value: %d | LastUpdateId: %s | ElectionInProgress: %b | Crashed: %b, GroupSize: %d",
        replicaId, isCoordinator, currentEpoch, currentSequenceNumber, currentValue,
        updateHistory.isEmpty() ? new Messages.UpdateId(0, -1).toString()
            : getLastKnownUpdate().updateId.toString(),
        electionInProgress, crashed, replicas.size());
    log.info(Colors.BLUE + status + Colors.RESET);
  }

  private void onNewCoordinator(Messages.NewCoordinator msg) {
    if (msg.newCoordinatorId == replicaId) {
      becomeCoordinator(msg.knownUpdates);
      return;
    }
    // NOTE: Addirittura fare messaggio collegato direttamente con la funzione becomeCoordinator ??


    // } else {
    //   log.info(Colors.BLUE + "Replica " + replicaId + " forwarding NewCoordinator message for replica " +
    //       msg.newCoordinatorId + Colors.RESET);
    //   forwardToNextReplica(msg);
    // }
  }
}
