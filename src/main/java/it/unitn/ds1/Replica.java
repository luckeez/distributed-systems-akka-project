package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Cancellable;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import it.unitn.ds1.debug.Colors;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
  private boolean crashed = false;

  private List<ActorRef> replicas;
  private Map<Messages.UpdateId, Integer> pendingAcks = new HashMap<>();
  private Map<Messages.UpdateId, Messages.Update> pendingUpdates = new HashMap<>();
  private List<Messages.Update> updateHistory = new ArrayList<>();
  private Map<Integer, Messages.UpdateId> lastKnownUpdate = new HashMap<>();

  private Cancellable heartBeatSchedule;
  private Cancellable updateTimeout;
  private Cancellable heartBeatTimeout;
  private Map<Integer, Cancellable> replicaTimeouts;

  private boolean electionInProgress = false;

  private Messages.CrashPoint crashPoint = null;
  private int crashAfterOperations = 0;
  private Map<Messages.CrashPoint, Integer> operationCounts = new HashMap<>();

  {
    for (Messages.CrashPoint point : Messages.CrashPoint.values()) {
      operationCounts.put(point, 0);
    }
  }

  public Replica(int replicaId) {
    this.replicaId = replicaId;
    this.isCoordinator = (replicaId == 0);
  }

  @Override
  public void preStart() {
    if (isCoordinator) {

      scheduleHeartBeat();
      for (int i = 0; i < replicas.size(); i++) {
        if (i != replicaId) {
          resetReplicaTimeout(i);
        }
      }
    }

    scheduleHeartBeatTimeout();

  }

  final AbstractActor.Receive crashed() {
    return receiveBuilder()
        .matchAny(msg -> {
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
        .match(Messages.Initialize.class, this::handleInitialize)
        .match(Messages.ReadRequest.class, this::handleReadRequest)
        .match(Messages.WriteRequest.class, this::handleWriteRequest)
        .match(Messages.Update.class, this::handleUpdate)
        .match(Messages.Ack.class, this::handleAck)
        .match(Messages.WriteOk.class, this::handleWriteOk)
        .match(Messages.HeartBeat.class, this::handleHeartBeat)
        .match(Messages.HeartBeatAck.class, this::handleHeartBeatAck)
        .match(Messages.ReplicaTimeout.class, this::handleReplicaTimeout)
        .match(Messages.DetectedReplicaFailure.class, this::handleDetectedReplicaFailure)
        .match(Messages.HeartBeatTimeout.class, this::handleHeartBeatTimeout)
        .match(Messages.Election.class, this::handleElection)
        .match(Messages.Timeout.class, this::handleTimeout)
        .match(Messages.Synchronization.class, this::handleSynchronization)
        .match(Messages.Crash.class, this::handleCrash)
        .match(Messages.SetCrashPoint.class, this::handleSetCrashPoint)
        .build();
  }

  // HELPERS

  private boolean shouldCrash(Messages.CrashPoint point) {
    if (crashed || crashPoint != point) {
      return false;
    }
    int currentCount = operationCounts.get(point);
    operationCounts.put(point, currentCount + 1);

    if (currentCount + 1 >= crashAfterOperations) {
      log.info(Colors.RED,
          "Replica " + replicaId + " crashing at " + point + " after " + (currentCount + 1) + " operations");
      crashed = true;
      cancelTimeouts();
      return true;
    }
    crashed();

    return false;
  }

  private void introduceNetworkDelay() {
    try {
      Thread.sleep(ThreadLocalRandom.current().nextInt(10, 50));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void resetReplicaTimeout(int replicaId) {
    if (replicaTimeouts.get(replicaId) != null) {
      replicaTimeouts.get(replicaId).cancel();
    }
    if (isCoordinator) {
      // Schedule a new timeout
      Cancellable timeout = getContext().system().scheduler().scheduleOnce(
          Duration.create(2, TimeUnit.SECONDS),
          getSelf(),
          new Messages.ReplicaTimeout(replicaId),
          getContext().system().dispatcher(),
          getSelf());
      // Store the timeout for the replica
      replicaTimeouts.put(replicaId, timeout);
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

    for (Entry<Integer, Cancellable> entry : replicaTimeouts.entrySet()) {
      if (entry.getValue() != null) {
        entry.getValue().cancel();
      }
    }
  }

  private void startElection() {
    if (electionInProgress)
      return;
    electionInProgress = true;
    log.info("Replica " + replicaId + " started the election process");
    Map<Integer, Messages.UpdateId> knownUpdates = new HashMap<>();
    if (!updateHistory.isEmpty()) {
      Messages.Update lastUpdate = updateHistory.get(updateHistory.size() - 1);
      knownUpdates.put(replicaId, lastUpdate.updateId);
      forwardToNextReplica(new Messages.Election(replicaId, knownUpdates));
    }
  }

  private int findBestCoordinator(Map<Integer, Messages.UpdateId> updatedKnowledge) {
    int bestCoordinator = 0;
    Messages.UpdateId bestUpdate = new Messages.UpdateId(0, -1);
    for (Map.Entry<Integer, Messages.UpdateId> entry : updatedKnowledge.entrySet()) {
      int replicaId = entry.getKey();
      Messages.UpdateId updateId = entry.getValue();
      if (updateId.compareTo(bestUpdate) > 0 ||
          (updateId.compareTo(bestUpdate) == 0 && replicaId < bestCoordinator)) {
        bestCoordinator = replicaId;
        bestUpdate = updateId;
      }

    }
    return bestCoordinator;
  }

  private void becomeCoordinator() {
    log.info(Colors.BLUE, "Replica " + replicaId + " becoming the new Coordinator", Colors.RESET);
    if (shouldCrash(Messages.CrashPoint.BEFORE_SYNCHRONIZATION))
      return;
    isCoordinator = true;
    coordinatorId = replicaId;
    currentEpoch++;
    currentSequenceNumber = 0;

    // missed updates for synchronization
    List<Messages.Update> missedUpdates = new ArrayList<>();
    broadcast(new Messages.Synchronization(replicaId, missedUpdates));
    scheduleHeartBeat();
    electionInProgress = false;

  }

  private void broadcast(Serializable msg) {
    for (ActorRef replica : replicas) {
      if (replica == getSelf())
        continue;
      introduceNetworkDelay();
      replica.tell(msg, getSelf());
    }
  }

  private void forwardToNextReplica(Serializable msg) {
    int nextReplica = (replicaId + 1) % replicas.size();
    introduceNetworkDelay();
    replicas.get(nextReplica).tell(msg, getSelf());
  }

  // HANDLERS
  private void handleInitialize(Messages.Initialize msg) {
    if (crashed)
      return;
    this.replicas = new ArrayList<>(msg.replicas);
    log.info("Replica " + replicaId + " initialized with " + replicas.size() + " replicas");

    for (int i = 0; i < replicas.size(); i++) {
      lastKnownUpdate.put(i, new Messages.UpdateId(0, -1));
    }
  }

  private void handleReadRequest(Messages.ReadRequest msg) {
    if (crashed)
      return;
    log.info("Client Read Request to " + replicaId);
    introduceNetworkDelay();
    getSender().tell(new Messages.ReadResponse(currentValue), getSelf());
  }

  private void handleWriteRequest(Messages.WriteRequest msg) {
    if (crashed)
      return;

    if (isCoordinator) {
      if (shouldCrash(Messages.CrashPoint.BEFORE_SENDING_UPDATE))
        return;

      Messages.UpdateId updateId = new Messages.UpdateId(currentEpoch, currentSequenceNumber++);
      Messages.Update update = new Messages.Update(updateId, msg.value);

      log.info(Colors.CYAN, "Coordinator " + replicaId + " initiating update " + updateId + " value " + update.value,
          Colors.RESET);

      pendingUpdates.put(updateId, update);
      pendingAcks.put(updateId, 0);

      for (ActorRef replica : replicas) {
        introduceNetworkDelay();
        replica.tell(update, getSelf());
      }

      if (shouldCrash(Messages.CrashPoint.AFTER_SENDING_UPDATE))
        return;
      if (updateTimeout != null) {
        updateTimeout.cancel();
      }
      updateTimeout = getContext().getSystem().scheduler().scheduleOnce(
          Duration.create(2, TimeUnit.SECONDS),
          getSelf(),
          new Messages.Timeout(),
          getContext().getDispatcher(),
          getSelf());
    } else {
      log.info("Replica " + replicaId + " forwarding write request to coordinator " + coordinatorId);
      if (coordinatorId < replicas.size()) {
        introduceNetworkDelay();
        replicas.get(coordinatorId).tell(
            new Messages.WriteRequest(msg.value, getSender()),
            getSelf());
      }
    }
  }

  private void handleUpdate(Messages.Update msg) {
    if (crashed)
      return;

    log.info(Colors.CYAN, "Replica " + replicaId + " received update " + msg.updateId + " value " + msg.value,
        Colors.RESET);

    if (shouldCrash(Messages.CrashPoint.AFTER_RECEIVING_UPDATE))
      return;
    updateHistory.add(msg);
    lastKnownUpdate.put(replicaId, msg.updateId);

    if (shouldCrash(Messages.CrashPoint.BEFORE_SENDING_ACK))
      return;
    introduceNetworkDelay();
    getSender().tell(new Messages.Ack(msg.updateId, replicaId), getSelf());

    if (shouldCrash(Messages.CrashPoint.AFTER_SENDING_ACK))
      return;
  }

  private void handleAck(Messages.Ack msg) {
    if (crashed || !isCoordinator)
      return;

    if (pendingAcks.containsKey(msg.updateId)) {
      int currentAcks = pendingAcks.get(msg.updateId);
      pendingAcks.put(msg.updateId, currentAcks + 1);
      int quorumSize = (replicas.size() / 2) + 1;
      if (currentAcks + 1 >= quorumSize) {
        log.info("Coordinator " + replicaId + " received quorum for update " + msg.updateId);

        if (shouldCrash(Messages.CrashPoint.BEFORE_SENDING_WRITEOK))
          return;

        Messages.WriteOk writeOk = new Messages.WriteOk(msg.updateId);

        for (ActorRef replica : replicas) {
          replica.tell(writeOk, getSelf());
        }

        if (shouldCrash(Messages.CrashPoint.AFTER_SENDING_WRITEOK))
          return;

        // clean up
        pendingAcks.remove(msg.updateId);
        pendingUpdates.remove(msg.updateId);

        if (updateTimeout != null) {
          updateTimeout.cancel();
        }
      }
    }
  }

  private void handleWriteOk(Messages.WriteOk msg) {
    if (crashed)
      return;

    for (Messages.Update update : updateHistory) {
      if (update.updateId.equals(msg.updateId)) {
        currentValue = update.value;
        log.info("Replica " + replicaId + " update " + update.updateId + " value " + update.value);
        break;
      }
    }

    if (shouldCrash(Messages.CrashPoint.AFTER_RECEIVING_WRITEOK))
      return;

    // Reset heartbeat timeout since we heard from coordinator
    resetHeartBeatTimeout();
  }

  private void handleHeartBeat(Messages.HeartBeat msg) {
    if (crashed)
      return;

    resetHeartBeatTimeout();
  }

  private void handleHeartBeatAck(Messages.HeartBeatAck msg) {
    if (crashed)
      return;
    resetReplicaTimeout(msg.replicaId);
  }

  private void handleHeartBeatTimeout(Messages.HeartBeatTimeout msg) {
    if (crashed || isCoordinator)
      return;

    log.warning(Colors.RED, "Replica " + replicaId + " detected coordinator failure of replica " + coordinatorId,
        Colors.RESET);

    // Start election
    if (!electionInProgress) {
      electionInProgress = true;
      getSelf().tell(new Messages.Election(replicaId, new HashMap<>(lastKnownUpdate)), getSelf());
      replicas.removeIf(r -> r.path().name().equals("replica" + coordinatorId));
    }
  }

  private void handleReplicaTimeout(Messages.ReplicaTimeout msg) {
    if (crashed || !isCoordinator)
      return;

    log.warning(Colors.RED, "Coordinator " + replicaId + " detected failure of replica " + msg.replicaId,
        Colors.RESET);

    replicas.removeIf(r -> r.path().name().equals("replica" + msg.replicaId));
    for (ActorRef replica : replicas) {
      if (replica.equals(getSelf()))
        continue;
      replica.tell(new Messages.DetectedReplicaFailure(msg.replicaId), getSelf());
    }
  }

  private void handleTimeout(Messages.Timeout msg) {
    if (crashed)
      return;
    if (isCoordinator) {
      log.info("Coordinator " + replicaId + " timeout waiting for acks");
      // TODO: think about what to do if the coordinator timeout on writeok acks
    } else {
      log.info("Replica " + replicaId + " timeout waiting for WRITEOK, starting eletion process...");
      startElection();
    }
  }

  private void handleDetectedReplicaFailure(Messages.DetectedReplicaFailure msg) {
    if (crashed)
      return;

    log.warning(Colors.RED,
        "Replica " + replicaId + " received ReplicaFailure message from coordinator " + msg.failedReplicaId,
        Colors.RESET);
    replicas.removeIf(r -> r.path().name().equals("replica" + msg.failedReplicaId));
  }

  private void handleElection(Messages.Election msg) {
    if (crashed)
      return;
    if (shouldCrash(Messages.CrashPoint.DURING_ELECTION))
      return;
    log.info("Replica " + replicaId + " received election message from " + msg.initiatorId);

    // Update our knowledge of other replicas' updates
    for (Map.Entry<Integer, Messages.UpdateId> entry : msg.knownUpdates.entrySet()) {
      Messages.UpdateId current = lastKnownUpdate.get(entry.getKey());
      if (current == null || entry.getValue().compareTo(current) > 0) {
        lastKnownUpdate.put(entry.getKey(), entry.getValue());
      }
    }

    Map<Integer, Messages.UpdateId> updatedKnowledge = new HashMap<>(msg.knownUpdates);
    if (!updateHistory.isEmpty()) {
      Messages.Update lastUpdate = updateHistory.get(updateHistory.size() - 1);
      updatedKnowledge.put(replicaId, lastUpdate.updateId);
    }

    int nextReplica = (replicaId + 1) % replicas.size();
    if (nextReplica != msg.initiatorId) {
      forwardToNextReplica(new Messages.Election(msg.initiatorId, updatedKnowledge));
    } else {
      int bestCoordinator = findBestCoordinator(updatedKnowledge);
      log.info("Replica " + replicaId + " determined new coordinator: " + bestCoordinator);

      if (bestCoordinator == replicaId) {
        becomeCoordinator();
      } else {
        forwardToNextReplica(new Messages.NewCoordinator(bestCoordinator));
      }
    }
  }

  private void handleSynchronization(Messages.Synchronization msg) {
    if (crashed)
      return;
    log.info("Replica " + replicaId + " received synchronization message from new coordinator " + msg.newCoordinatorId);
    coordinatorId = msg.newCoordinatorId;
    isCoordinator = (replicaId == coordinatorId);
    electionInProgress = false;

    // Apply missed updates
    for (Messages.Update update : msg.missedUpdates) {
      if (!updateHistory.contains(update)) {
        updateHistory.add(update);
        currentValue = update.value;
        log.info("Replica " + replicaId + " synchronized update " + update.updateId + " value " + update.value);
      }
    }
    if (shouldCrash(Messages.CrashPoint.AFTER_SYNCHRONIZATION))
      return;
    if (!isCoordinator) {
      resetHeartBeatTimeout();
    } else {
      scheduleHeartBeat();
      for (int i = 0; i < replicas.size(); i++) {
        if (i != replicaId) {
          resetReplicaTimeout(i);
        }
      }
    }
  }

  private void handleCrash(Messages.Crash msg) {
    // TODO: handle crash
  }

  private void handleSetCrashPoint(Messages.SetCrashPoint msg) {
    this.crashPoint = msg.crashPoint;
    this.crashAfterOperations = msg.afterOperations;
    log.info(
        "Replica " + replicaId + " set to crash at " + crashPoint + " after " + crashAfterOperations + " operations");
  }
}
