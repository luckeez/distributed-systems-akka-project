package it.unitn.ds1;

import akka.actor.ActorRef;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class Messages {
  public enum CrashPoint {
    BEFORE_SENDING_UPDATE,
    AFTER_SENDING_UPDATE,
    AFTER_RECEIVING_UPDATE,
    BEFORE_SENDING_ACK,
    AFTER_SENDING_ACK,
    BEFORE_SENDING_WRITEOK,
    AFTER_SENDING_WRITEOK,
    AFTER_RECEIVING_WRITEOK,
    DURING_ELECTION,
    BEFORE_SYNCHRONIZATION,
    AFTER_SYNCHRONIZATION;
  }

  public static class Initialize implements Serializable {
    public final List<ActorRef> replicas;

    public Initialize(List<ActorRef> replicas) {
      this.replicas = replicas;
    }
  }

  public static class ReadRequest implements Serializable {
  }

  public static class WriteRequest implements Serializable {
    public final int value;
    public final ActorRef client;

    public WriteRequest(int value) {
      this.value = value;
      this.client = null;
    }

    public WriteRequest(int value, ActorRef client) {
      this.value = value;
      this.client = client;
    }
  }

  public static class ReadResponse implements Serializable {
    public final int value;

    public ReadResponse(int value) {
      this.value = value;
    }
  }

  public static class WriteResponse implements Serializable {
    public final boolean success;

    public WriteResponse(boolean success) {
      this.success = success;
    }
  }

  public static class Update implements Serializable {
    public final UpdateId updateId;
    public final int value;

    public Update(UpdateId updateId, int value) {
      this.updateId = updateId;
      this.value = value;
    }
  }

  public static class UpdateId implements Serializable, Comparable<UpdateId> {
    public final int epoch;
    public final int sequenceNumber;

    public UpdateId(int epoch, int sequenceNumber) {
      this.epoch = epoch;
      this.sequenceNumber = sequenceNumber;
    }

    @Override
    public int compareTo(UpdateId other) {
      if (this.epoch == other.epoch) {
        return Integer.compare(this.sequenceNumber, other.sequenceNumber);
      }
      return Integer.compare(this.epoch, other.epoch);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (!(obj instanceof UpdateId))
        return false;
      UpdateId other = (UpdateId) obj;
      return epoch == other.epoch && sequenceNumber == other.sequenceNumber;
    }

    @Override
    public int hashCode() {
      // 31 is used to avoid collisions because it is an odd prime number
      return 31 * epoch + sequenceNumber;
    }

    @Override
    public String toString() {
      return epoch + ":" + sequenceNumber;
    }
  }

  public static class Ack implements Serializable {
    public final UpdateId updateId;
    public final int replicaId;

    public Ack(UpdateId updateId, int replicaId) {
      this.updateId = updateId;
      this.replicaId = replicaId;
    }
  }

  public static class WriteOk implements Serializable {
    public final UpdateId updateId;

    public WriteOk(UpdateId updateId) {
      this.updateId = updateId;
    }
  }

  public static class HeartBeat implements Serializable {
    public final int coordinatorId;

    public HeartBeat(int coordinatorId) {
      this.coordinatorId = coordinatorId;
    }
  }

  public static class HeartBeatAck implements Serializable {
    public final int replicaId;

    public HeartBeatAck(int replicaId) {
      this.replicaId = replicaId;
    }
  }

  public static class Election implements Serializable {
    public final int initiatorId;
    public final Map<Integer, UpdateId> knownUpdates;

    public Election(int initiatorId, Map<Integer, UpdateId> knownUpdates) {
      this.initiatorId = initiatorId;
      this.knownUpdates = knownUpdates;
    }
  }

  public static class NewCoordinator implements Serializable {
    public final int newCoordinatorId;

    public NewCoordinator(int newCoordinatorId) {
      this.newCoordinatorId = newCoordinatorId;
    }
  }

  public static class Synchronization implements Serializable {
    public final int newCoordinatorId;
    public final List<Update> missedUpdates;

    public Synchronization(int newCoordinatorId, List<Update> missedUpdates) {
      this.newCoordinatorId = newCoordinatorId;
      this.missedUpdates = missedUpdates;
    }
  }

  public static class ReplicaTimeout implements Serializable {
    public final int replicaId;

    public ReplicaTimeout(int replicaId) {
      this.replicaId = replicaId;
    }
  }

  public static class DetectedReplicaFailure implements Serializable {
    public final int failedReplicaId;

    public DetectedReplicaFailure(int failedReplicaId) {
      this.failedReplicaId = failedReplicaId;
    }
  }

  public static class Timeout implements Serializable {
  }

  public static class HeartBeatTimeout implements Serializable {
  }

  public static class Crash implements Serializable {
  }

  public static class SetCrashPoint implements Serializable {
    public final CrashPoint crashPoint;
    public final int afterOperations;

    public SetCrashPoint(CrashPoint crashPoint, int afterOperations) {
      this.crashPoint = crashPoint;
      this.afterOperations = afterOperations;
    }
  }

  public static class GetState implements Serializable {
  }

  public static class StateResponse implements Serializable {

    public final int replicaId;
    public final boolean isCoordinator;
    public final int currentEpoch;
    public final int currentValue;
    public final UpdateId lastUpdateId;
    public final boolean electionInProgress;
    public final boolean isCrashed;

    public StateResponse(int replicaId, boolean isCoordinator, int currentEpoch, int currentValue,
        UpdateId lastUpdateId, boolean electionInProgress, boolean isCrashed) {
      this.replicaId = replicaId;
      this.isCoordinator = isCoordinator;
      this.currentEpoch = currentEpoch;
      this.currentValue = currentValue;
      this.lastUpdateId = lastUpdateId;
      this.electionInProgress = electionInProgress;
      this.isCrashed = isCrashed;
    }

    @Override
    public String toString() {
      return String.format(
          "Replica %d | Coordinator: %b | Epoch: %d | Value: %d | LastUpdateId: %s | ElectionInProgress: %b | Crashed: %b",
          replicaId, isCoordinator, currentEpoch, currentValue,
          lastUpdateId != null ? lastUpdateId.toString() : "null", electionInProgress, isCrashed);
    }
  }
}
