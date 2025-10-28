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
  // Replica state attributes
  private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
  private final int replicaId;
  private int currentValue = 0;
  private int currentEpoch = 0;
  private int currentSequenceNumber = 0;
  private boolean isCoordinator = false;
  private int coordinatorId = 0;
  private final int quorumSize;
  private boolean crashed = false;
  private boolean electionInProgress = false;

  // Lists and maps to manage replicas, pending acks and updates
  private List<ActorRef> replicas = new ArrayList<>();
  private Map<Messages.UpdateId, Integer> pendingAcks = new HashMap<>();
  private Map<Messages.UpdateId, Messages.Update> pendingUpdates = new HashMap<>();
  private List<Messages.Update> updateHistory = new ArrayList<>();

  // Timeouts
  private Cancellable heartBeatSchedule;
  private Cancellable updateTimeout;
  private Cancellable heartBeatTimeout;
  private Map<String, Cancellable> replicaTimeouts;
  private Cancellable electionAckTimeout;
  private Cancellable electionTimeout;

  // Crash simulation
  private Messages.CrashPoint crashPoint = null;
  private int crashAfterOperations = 0;
  private Map<Messages.CrashPoint, Integer> operationCounts = new HashMap<>();

  // Initialize operation counts
  {
    for (Messages.CrashPoint point : Messages.CrashPoint.values()) {
      operationCounts.put(point, 0);
    }
  }

  // Constructor
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

  // CRASHED STATE: ignore all messages
  final AbstractActor.Receive crashed() {
    return receiveBuilder()
        .matchAny(msg -> {
        })
        .build();
  }

  // COORDINATOR STATE: handle coordinator-specific messages
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

  // RECEIVE METHOD
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
        .match(Messages.ElectionAck.class, this::onElectionAck)
        .match(Messages.ElectionAckTimeout.class, this::onElectionAckTimeout)
        .match(Messages.StartElection.class, msg -> startElection())
        .match(Messages.NewCoordinator.class, msg -> becomeCoordinator(msg.knownUpdates))
        .build();
  }

  // ===================  HELPERS  ===================

  // ------------------- COMMUNICATION  -------------------
  private void introduceNetworkDelay() {
    try {
      Thread.sleep(ThreadLocalRandom.current().nextInt(10, 50));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void forwardToCoordinator(Serializable msg) {
    for (ActorRef replica : this.replicas) {
      if (replica.path().name().equals("Replica" + this.coordinatorId)) {
        introduceNetworkDelay();
        replica.tell(msg, getSelf());
        return;
      }
    }
  }

  private boolean broadcast(Serializable msg, Messages.CrashPoint crashPoint) {
    for (ActorRef replica : this.replicas) {
      if (replica == getSelf())
        continue;
      if (shouldCrash(crashPoint))
        return true;
      introduceNetworkDelay();
      replica.tell(msg, getSelf());
    }
    return false;
  }

  private ActorRef getNextReplica() {
    // find the next replica in the ring
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
    // send a message to replica with given id
    for (ActorRef replica : this.replicas) {
      if (replica.path().name().equals("Replica" + replicaId)) {
        introduceNetworkDelay();
        replica.tell(msg, getSelf());
        return;
      }
    }
  }

  // ------------------- TIMEOUTS  -------------------
  private void scheduleReplicaTimeouts() {
    // coordinator schedules timeouts for each replica to detect crashes
    this.replicaTimeouts = new HashMap<>();
    for (ActorRef replica : this.replicas) {
      String name = replica.path().name();
      if (!replica.equals(getSelf())) {
        resetReplicaTimeout(name);
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

  private void resetElectionTimeout() {
    // reset election timeout
    if (this.electionTimeout != null) {
      this.electionTimeout.cancel();
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
                introduceNetworkDelay();
                replica.tell(new Messages.HeartBeat(this.replicaId), getSelf());
              }
            }
          }
        },
        getContext().getDispatcher());
  }

  private void scheduleHeartBeatTimeout() {
    // schedule heartbeat timeout for replicas to detect coordinator crash
    if (!this.isCoordinator) {
      resetHeartBeatTimeout();
    }
  }

  private void resetHeartBeatTimeout() {
    if (this.heartBeatTimeout != null) {
      this.heartBeatTimeout.cancel();
    }

    if (!this.isCoordinator) {
      // schedule a new heartbeat timeout
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
    // schedule election ack timeout to check if election process is stuck
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

  // -------------------- UPDATE  -------------------
  private boolean alreadyServed(Messages.RequestInfo requestInfo) {
    // check if the client request was already served by checking if requestInfo is already in the update history
    for (Messages.Update update : this.updateHistory) {
      if (update.requestInfo != null && update.requestInfo.equals(requestInfo)) {
        return true;
      }
    }
    return false;
  }

  private void applyUpdates(List<Messages.Update> knownUpdates) {
    // apply all updates that are not already in the update history
    for (Messages.Update update : knownUpdates) {
      if (!this.updateHistory.contains(update)) {
        this.updateHistory.add(update);
        log.info(Colors.GREEN + "Replica " + this.replicaId + " update " + update.updateId + " value " + update.value
            + Colors.RESET);
      }
    }
    // sort the update history by update id
    this.updateHistory.sort((u1, u2) -> u1.updateId.compareTo(u2.updateId));
    this.currentValue = this.updateHistory.isEmpty() ? 0 : getLastKnownUpdate().value;
  }

  private void applyUpdate(Messages.Update update) {
    // apply a single update if not already applied
    if (this.updateHistory.contains(update))
      return;
    this.updateHistory.add(update);
    this.currentValue = update.value;
    this.currentSequenceNumber++;
    if (this.isCoordinator) {
      log.info(Colors.GREEN + "Coordinator " + this.replicaId + " update " + update.updateId + " " + update.value
          + Colors.RESET);
    } else {
      log.info(Colors.GREEN + "Replica " + this.replicaId + " update " + update.updateId + " " + update.value
          + Colors.RESET);
    }
  }

  private Messages.Update getLastKnownUpdate() {
    // retrieve the last update in the update history
    if (this.updateHistory.isEmpty()) {
      return null;
    } else {
      return this.updateHistory.get(this.updateHistory.size() - 1);
    }
  }

  private Messages.UpdateId getLastKnownUpdateId() {
    // retrieve the last update id in the update history
    Messages.Update lastUpdate = getLastKnownUpdate();
    if (lastUpdate == null) {
      return new Messages.UpdateId(0, -1);
    } else {
      return lastUpdate.updateId;
    }
  }

  // ------------------- ELECTION  -------------------
  private void startElection() {
    // start the election process using a ring-based election algorithm
    // if the election is already in progress, skip
    if (this.electionInProgress || shouldCrash(Messages.CrashPoint.DURING_ELECTION))
      return;
    this.electionInProgress = true;

    // remove the current coordinator from the replicas list
    this.replicas.removeIf(r -> r.path().name().equals("Replica" + this.coordinatorId));
    log.info("Replica " + this.replicaId + " started the election process");

    // add known pending updates to the shared set in the election message
    Set<Messages.Update> knownPendingUpdates = new HashSet<>();
    knownPendingUpdates.addAll(this.pendingUpdates.values());

    // create and send election message
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

    if (shouldCrash(Messages.CrashPoint.BEFORE_SYNCHRONIZATION))
      return;
    log.info(Colors.GREEN + "Replica " + this.replicaId + " becoming the new Coordinator" + Colors.RESET);

    // update coordinator state
    this.isCoordinator = true;
    this.coordinatorId = replicaId;
    this.currentEpoch++;
    this.currentSequenceNumber = 0;
    resetElectionTimeout();

    // wait a bit to complete the transition
    try {
      Thread.sleep(500);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // broadcast synchronization message to replicas with coordinator update history
    broadcast(new Messages.Synchronization(this.replicaId, this.updateHistory), null);

    // schedule heartbeats and replica timeouts
    scheduleHeartBeat();
    scheduleReplicaTimeouts();

    // perform update protocol for known pending updates
    int i = 0;
    for (Messages.Update update : knownPendingUpdates) {
      if (!this.updateHistory.contains(update)) {
        log.info("Coordinator " + this.replicaId + " re-broadcasting pending update " + update.updateId + " value "
            + update.value);
        // create the new update
        Messages.Update newUpdate = new Messages.Update(
            new Messages.UpdateId(this.currentEpoch, this.currentSequenceNumber + i),
            update.value, update.requestInfo);
        this.pendingAcks.put(newUpdate.updateId, 0);
        this.pendingUpdates.put(newUpdate.updateId, newUpdate);
        // broadcast the update message
        broadcast(newUpdate, null);
      }
      i++;
    }
    this.electionInProgress = false;

    if (shouldCrash(Messages.CrashPoint.AFTER_SYNCHRONIZATION))
      return;
  }

  // ------------------- CRASH SIMULATION -------------------
  private boolean shouldCrash(Messages.CrashPoint point) {
    // set to crash at the specified crash point
    if (this.crashed || point == null || this.crashPoint != point || this.replicas.size() <= this.quorumSize) {
      return false;
    }
    int currentCount = this.operationCounts.get(point);
    this.operationCounts.put(point, currentCount + 1);

    // set crash after specified number of operations
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
    // simulate crash by setting crashed flag and cancelling timeouts
    this.crashed = true;
    cancelTimeouts();
    getContext().become(crashed());
  }

  private boolean isAlive(int replicaId) {
    // check if a replica with given id is alive in the replicas list
    for (ActorRef replica : this.replicas) {
      if (replica.path().name().equals("Replica" + replicaId)) {
        return true;
      }
    }
    return false;
  }

  // ===================  MESSAGE HANDLERS  ===================

  // ------------------- UTILS  -------------------
  private void onInitialize(Messages.Initialize msg) {
    /*
      Initialize the replica with the list of replicas in the system.
    */
    if (this.crashed)
      return;

    // initialize the replicas list
    this.replicas = new ArrayList<>(msg.replicas);
    log.info(
        Colors.GREEN + "Replica " + this.replicaId + " initialized with " + this.replicas.size() + " replicas"
            + Colors.RESET);

  }

  private void onGetState(Messages.GetState msg) {
    /* 
      print on console the current state of the replica
    */
    String status = String.format(
        "Replica %d | Coordinator: %d | Epoch: %d | SeqNum: %d | Value: %d | LastUpdateId: %s | ElectionInProgress: %b | Crashed: %b, GroupSize: %d",
        this.replicaId, this.coordinatorId, this.currentEpoch, this.currentSequenceNumber, this.currentValue,
        this.updateHistory.isEmpty() ? new Messages.UpdateId(0, -1).toString()
            : getLastKnownUpdate().updateId.toString(),
        this.electionInProgress, this.crashed, this.replicas.size());
    status += "\nUpdate History: ";
    for (Messages.Update update : this.updateHistory) {
      status += "id:  " + update.updateId + ", Value: " + Colors.RESET + update.value + Colors.BLUE + " -- ";
    }
    System.out.println(Colors.BLUE + status + Colors.RESET);
  }

  // ------------------- CLIENT REQUESTS  -------------------
  private void onReadRequest(Messages.ReadRequest msg) {
    /*
      respond to read request with current value
    */
    if (this.crashed)
      return;
    if (!getSender().path().name().equals("Client2"))
      log.info("Replica " + this.replicaId + " received read request from " + getSender().path().name());
    introduceNetworkDelay();
    getSender().tell(new Messages.ReadResponse(this.currentValue), getSelf());
  }

  private void onWriteRequest(Messages.WriteRequest msg) {
    /* 
      handle write request from client
    */
    if (this.crashed)
      return;

    if (this.isCoordinator) {
      // check if the request was already served
      if (alreadyServed(msg.requestInfo)) {
        introduceNetworkDelay();
        msg.requestInfo.client.tell(new Messages.WriteResponse(true, msg.requestInfo), getSelf());
        log.info("Coordinator " + this.replicaId + " already served request with " + msg.requestInfo.toString());
        return;
      }

      if (shouldCrash(Messages.CrashPoint.BEFORE_SENDING_UPDATE))
        return;

      // create new update id
      Messages.UpdateId updateId = new Messages.UpdateId(this.currentEpoch, this.currentSequenceNumber);

      // check if update id already exists (conflict with another pending update)
      if (this.pendingUpdates.containsKey(updateId)) {
        // find the last update in pending updates and increment sequence number
        Messages.UpdateId lastUpdate = pendingUpdates.keySet().stream().max(Messages.UpdateId::compareTo).get();
        updateId = new Messages.UpdateId(lastUpdate.epoch, lastUpdate.sequenceNumber + 1);
      }
      // create new update message
      Messages.Update update = new Messages.Update(updateId, msg.value, msg.requestInfo);

      log.info(
          Colors.CYAN + "Coordinator " + this.replicaId + " initiating update " + updateId + " value " + update.value +
              Colors.RESET);

      this.pendingUpdates.put(updateId, update);
      this.pendingAcks.put(updateId, 0);

      // broadcast the update message to replicas
      log.info("Coordinator " + this.replicaId + " broadcasting update " + updateId + " value " + update.value);
      broadcast(update, Messages.CrashPoint.DURING_SENDING_UPDATE);

      if (shouldCrash(Messages.CrashPoint.AFTER_SENDING_UPDATE))
        return;
    } else {
      // forward write request to coordinator
      introduceNetworkDelay();
      log.info("Replica " + this.replicaId + " forwarding write request to coordinator " + this.coordinatorId);
      forwardToCoordinator(new Messages.WriteRequest(msg.value, msg.requestInfo));
    }
    if (this.updateTimeout != null) {
      this.updateTimeout.cancel();
    }
    // this.updateTimeout = getContext().getSystem().scheduler().scheduleOnce(
    // Duration.create(2, TimeUnit.SECONDS),
    // getSelf(),
    // new Messages.Timeout(),
    // getContext().getDispatcher(),
    // getSelf());

  }

  // ------------------- UPDATES  -------------------
  private void onUpdate(Messages.Update msg) {
    /* 
      When a replica receives an update message, it adds it to the pending updates map and sends an Ack back to the coordinator.
    */
    if (this.updateTimeout != null) {
      this.updateTimeout.cancel();
    }

    log.info(Colors.CYAN + "Replica " + this.replicaId + " received update " + msg.updateId + " value " + msg.value +
        Colors.RESET);

    if (shouldCrash(Messages.CrashPoint.AFTER_RECEIVING_UPDATE))
      return;

    // add the update to pending updates
    this.pendingUpdates.putIfAbsent(msg.updateId, msg);

    if (shouldCrash(Messages.CrashPoint.BEFORE_SENDING_ACK))
      return;
    log.info("Replica " + this.replicaId + " sending Ack for update " + msg.updateId);

    // send Ack back to coordinator
    introduceNetworkDelay();
    getSender().tell(new Messages.Ack(msg.updateId), getSelf());

    if (shouldCrash(Messages.CrashPoint.AFTER_SENDING_ACK))
      return;
  }

  private void onAck(Messages.Ack msg) {
    /*
      Coordinator receives Ack messages from replicas and checks if quorum is reached to send writeoks.
    */

    if (this.pendingAcks.containsKey(msg.updateId)) {
      // increment the number of acks received for the update
      int currentAcks = pendingAcks.get(msg.updateId);
      this.pendingAcks.put(msg.updateId, ++currentAcks);

      // check if quorum is reached. +1 because the coordinator implicitly acks
      if (currentAcks + 1 >= this.quorumSize) {
        log.info("Coordinator " + this.replicaId + " received quorum for update " + msg.updateId);

        if (shouldCrash(Messages.CrashPoint.BEFORE_SENDING_WRITEOK))
          return;

        // apply the update
        Messages.Update update = this.pendingUpdates.get(msg.updateId);
        applyUpdate(update);
        
        // broadcast WriteOk to replicas
        Messages.WriteOk writeOk = new Messages.WriteOk(msg.updateId);
        log.info("Coordinator " + this.replicaId + " broadcasting WriteOk for update " + msg.updateId);
        if (broadcast(writeOk, Messages.CrashPoint.DURING_SENDING_WRITEOK))
          return;

        if (shouldCrash(Messages.CrashPoint.AFTER_SENDING_WRITEOK))
          return;

        // notify the client that the write was successful
        introduceNetworkDelay();
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
    /*
      Replica receives WriteOk messages from coordinator and applies the update.
    */
    Messages.Update update = this.pendingUpdates.get(msg.updateId);
    if (update != null) {
      applyUpdate(update);
      this.pendingUpdates.remove(msg.updateId);
    } else {
      return;
    }

    if (shouldCrash(Messages.CrashPoint.AFTER_RECEIVING_WRITEOK))
      return;
  }

  // ------------------- HEARTBEAT  -------------------
  private void onHeartBeat(Messages.HeartBeat msg) {
    // replica receives heartbeat from coordinator and resets heartbeat timeout
    resetHeartBeatTimeout();
    introduceNetworkDelay();
    getSender().tell(new Messages.HeartBeatAck(this.replicaId), getSelf());
  }

  private void onHeartBeatAck(Messages.HeartBeatAck msg) {
    // coordinator receives heartbeat ack from replica and resets replica timeout
    String name = "Replica" + msg.replicaId;
    resetReplicaTimeout(name);
  }

  // ------------------- CRASH DETECTION  -------------------
  private void onHeartBeatTimeout(Messages.HeartBeatTimeout msg) {
    /*
      Replica detects coordinator failure due to heartbeat timeout and starts election process.
    */
    this.electionInProgress = false;
    log.info(
        Colors.RED + "Replica " + this.replicaId + " detected coordinator failure of replica " + this.coordinatorId +
            Colors.RESET);

    // Start election: incremental delay based on replica id to reduce collisions
    // Upper bound of 2 seconds
    if (!this.electionInProgress) {
      getContext().getSystem().scheduler().scheduleOnce(
          Duration.create(Math.min(this.replicaId * 80, 2000), TimeUnit.MILLISECONDS),
          getSelf(),
          new Messages.StartElection(),
          getContext().getDispatcher(),
          getSelf());
    }
    // Timeout to check if the election was successful
    this.electionTimeout = getContext().getSystem().scheduler().scheduleOnce(
        Duration.create(10, TimeUnit.SECONDS),
        getSelf(),
        new Messages.HeartBeatTimeout(),
        getContext().getDispatcher(),
        getSelf());
  }

  private void onReplicaTimeout(Messages.ReplicaTimeout msg) {
    /*
      Coordinator detects replica failure due to replica timeout and notifies other replicas.
    */

    log.warning(Colors.RED + "Coordinator " + this.replicaId + " detected failure of replica " + msg.replicaId +
        Colors.RESET);

    // remove the crashed replica from the group
    this.replicas.removeIf(r -> r.path().name().equals("Replica" + msg.replicaId));

    // notify the detected failure to other replicas
    broadcast(new Messages.DetectedReplicaFailure(msg.replicaId), null);
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
    /*
      the replica received the failure notication of another replica from the
      coordinator and proceed to remove the replica from the group
    */
    log.warning(Colors.RED +
        "Replica " + this.replicaId + " received ReplicaFailure message, replica" + msg.failedReplicaId
        + " crashed" +
        Colors.RESET);

    this.replicas.removeIf(r -> r.path().name().equals("Replica" + msg.failedReplicaId));
  }

  // ------------------- ELECTION  -------------------
  private void onElection(Messages.Election msg) {
    /*
      Replica receives election message and participates in the ring-based election algorithm.
    */
    if (shouldCrash(Messages.CrashPoint.DURING_ELECTION))
      return;

    if (!isAlive(msg.initiatorId)) {
      return;
    }

    // send election ack back to sender
    introduceNetworkDelay();
    getSender().tell(new Messages.ElectionAck(this.replicaId), getSelf());
    log.info("Replica " + this.replicaId + " received election message from " + getSender().path().name()
        + " with initiator " + msg.initiatorId);

    // mark election as in progress and remove crashed coordinator from replicas list
    if (!this.electionInProgress) {
      this.electionInProgress = true;
      this.replicas.removeIf(r -> r.path().name().equals("Replica" + this.coordinatorId));
    }

    // check if this replica is the initiator of the election -> the ring is completed
    if (msg.initiatorId == this.replicaId) {
      // check who is the best coordinator
      if (this.replicaId == msg.bestCoordinator) {
        becomeCoordinator(msg.knownPendingUpdates);
      } else {
        log.info(Colors.BLUE + "Replica " + this.replicaId + " telling replica " + msg.bestCoordinator
            + " to be the new coordinator" + Colors.RESET);
        
        tellToReplica(new Messages.NewCoordinator(msg.bestCoordinator, msg.knownPendingUpdates), msg.bestCoordinator);
        return;
      }
    }

    // get this replica's last known update id
    Messages.Update myLastUpdate = getLastKnownUpdate();
    Messages.UpdateId myLastUpdateId = myLastUpdate == null ? new Messages.UpdateId(0, -1) : myLastUpdate.updateId;

    // merge known pending updates
    Set<Messages.Update> knownPendingUpdates = new HashSet<>(msg.knownPendingUpdates);
    knownPendingUpdates.addAll(pendingUpdates.values());

    // compare update ids and replica ids to determine the best coordinator
    if (myLastUpdateId.compareTo(msg.bestUpdateId) > 0 ||
        (myLastUpdateId.compareTo(msg.bestUpdateId) == 0 && replicaId > msg.bestCoordinator)) {
      // this replica is a better coordinator, forward election message with its id and update id
      Messages.Election newMsg = new Messages.Election(msg.initiatorId, this.replicaId, myLastUpdateId,
          knownPendingUpdates);
      setElectionAckTimeout(newMsg);
      forwardToNextReplica(newMsg);
    } else {
      // forward election message with the best coordinator and update id received
      Messages.Election newMsg = new Messages.Election(msg.initiatorId, msg.bestCoordinator, msg.bestUpdateId,
          knownPendingUpdates);
      setElectionAckTimeout(newMsg);
      forwardToNextReplica(newMsg);
    }
  }

  private void onElectionAck(Messages.ElectionAck msg) {
    /*
      Replica receives election ack from another replica and cancels the election ack timeout.
    */
    if (this.electionAckTimeout != null) {
      this.electionAckTimeout.cancel();
    }
  }

  private void onElectionAckTimeout(Messages.ElectionAckTimeout msg) {
    /*
      Replica detects timeout waiting for election ack from next replica and assumes it crashed.
      It removes the crashed replica from the group and continues the election process by forwarding 
      the election message to the next replica.
    */
    // remove the crashed replica from the group
    ActorRef nextReplica = getNextReplica();
    int nextReplicaInt = Integer.parseInt(nextReplica.path().name().replace("Replica", ""));
    this.replicas.removeIf(r -> r.equals(nextReplica));

    // log and broadcast detected failure
    log.info(Colors.RED + "Replica " + this.replicaId + " timeout waiting for election ack from " +
        nextReplica.path().name() + ", assuming it crashed" + Colors.RESET);
    broadcast(new Messages.DetectedReplicaFailure(nextReplicaInt), null);

    // check if the crashed replica was the initiator of the election. If so, restart the election process
    if (nextReplicaInt == msg.msg.initiatorId) {
      log.info(Colors.BLUE + "Replica " + this.replicaId + " restarting election process since the initiator crashed"
          + Colors.RESET);
      this.electionInProgress = false;
      startElection();
      return;
    }
    setElectionAckTimeout(msg.msg);
    forwardToNextReplica(msg.msg);
  }

  private void onSynchronization(Messages.Synchronization msg) {
    /*
      Replica receives synchronization message from new coordinator and updates its state.
    */
    log.info(
        "Replica " + this.replicaId + " received synchronization message from new coordinator " + msg.newCoordinatorId);
    this.coordinatorId = msg.newCoordinatorId;
    this.isCoordinator = (replicaId == coordinatorId);
    this.currentEpoch++;
    this.currentSequenceNumber = 0;
    this.electionInProgress = false;
    this.pendingUpdates.clear();
    resetElectionTimeout();

    // Apply missed updates
    applyUpdates(msg.missedUpdates);
    if (shouldCrash(Messages.CrashPoint.AFTER_SYNCHRONIZATION))
      return;
    if (!this.isCoordinator) {
      resetHeartBeatTimeout();
    }
  }

  // ------------------- CRASH SIMULATION -------------------
  private void onCrash(Messages.Crash msg) {
    /*
      Handle crash command to simulate replica crash.
      Ensure that crashing the replica does not violate quorum size.
    */
    if (this.replicas.size() <= this.quorumSize) {
      log.info(
          Colors.RED + "Replica " + this.replicaId + " ignoring crash command to preserve quorum size" + Colors.RESET);
      return;
    } else {
      log.info(Colors.RED + "Replica " + this.replicaId + " crashing now!" + Colors.RESET);
      crash();
    }
  }

  private void onSetCrashPoint(Messages.SetCrashPoint msg) {
    /*
      Set the crash point and number of operations after which the replica should crash.
    */
    this.crashPoint = msg.crashPoint;
    this.crashAfterOperations = msg.afterOperations;
    log.info(
        "Replica " + this.replicaId + " set to crash at " + this.crashPoint + " after " + this.crashAfterOperations
            + " operations");
  }
}
