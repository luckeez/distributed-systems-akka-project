package it.unitn.ds1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Collections;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.actor.ActorRef;




public class Replica extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final int id;
    private int v;
    private List<ActorRef> peers = new ArrayList<>();
    private boolean isCoordinator;
    private final int crashP = 5;
    private final Random rnd;
    private ActorRef coordinator;
    private int epoch;
    private int seqNum;
    private int quorumSize;
    private int ackCount = 0;
    private UpdateMsg pendingUpdate;

    // CONSTRUCTOR
    public Replica(int id, boolean isCoordinator, ActorRef coordinator){
        this.id = id;
        this.isCoordinator = isCoordinator;
        this.rnd = new Random();
        this.quorumSize = (10 / 2) + 1; // Majority quorum
        this.epoch = 0;
        this.seqNum = 0;
        if (isCoordinator && coordinator == null){
            this.coordinator = getSelf();
        } else {
            this.coordinator = coordinator;
        }
    }

    static public Props props(int id, boolean isCoordinator, ActorRef coordinator){
        return Props.create(Replica.class, () -> new Replica(id, isCoordinator, coordinator));
    }


    /*-- Message classes ------------------------------------------------------ */

    // Start message that informs every participant about its peers
    public static class JoinGroupMsg implements Serializable {
        public final List<ActorRef> group;   // an array of group members
        public JoinGroupMsg(List<ActorRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<ActorRef>(group));
        }
    }

    public static class UpdateMsg implements Serializable {
        public final int epoch;
        public final int seqNum;
        public final int newV;

        public UpdateMsg(int epoch, int seqNum, int newV) {
            this.epoch = epoch;
            this.seqNum = seqNum;
            this.newV = newV;
        }
    }

    public static class AckMsg implements Serializable {
        public final int epoch;
        public final int seqNum;

        public AckMsg(int epoch, int seqNum) {
            this.epoch = epoch;
            this.seqNum = seqNum;
        }
    }

    public static class WriteOkMsg implements Serializable {
        public final int epoch;
        public final int seqNum;

        public WriteOkMsg(int epoch, int seqNum) {
            this.epoch = epoch;
            this.seqNum = seqNum;
        }
    }

    public static class ReadRequestMsg implements Serializable {
        public final ActorRef sender;

        public ReadRequestMsg(ActorRef sender) {
            this.sender = sender;
        }
    }

    public static class WriteRequestMsg implements Serializable {
        public final ActorRef sender;
        public final int proposedV;

        public WriteRequestMsg(ActorRef sender, int proposedV) {
            this.sender = sender;
            this.proposedV = proposedV;
        }
    }

    public static class CoordinatorUpdateMsg implements Serializable {
        public final ActorRef newCoordinator;

        public CoordinatorUpdateMsg(ActorRef newCoordinator) {
            this.newCoordinator = newCoordinator;
        }
    }


    /*------------- Actor logic -------------------------------------------- */

    private void onJoinGroupMsg(JoinGroupMsg msg){
        for (ActorRef a : msg.group){
            if (!a.equals(getSelf())){
                this.peers.add(a);  // copy all replicas except for self
            }
        }
        log.info("Replica {} joined group with {} peers", this.id, this.peers.size());
    }

    private void onReadRequestMsg(ReadRequestMsg msg){
        ActorRef to = msg.sender;
        to.tell(new Client.ReadResponseMsg(this.v), getSelf());
        log.info("Replica {} sent value {} to client", this.id, this.v);
    }

    private void onWriteRequestMsg(WriteRequestMsg msg){
        // correctly handle both client messages and replicas messages
        if (this.isCoordinator){
            this.seqNum += 1;
            this.pendingUpdate = new UpdateMsg(this.epoch, this.seqNum, msg.proposedV);
            this.ackCount = 0;

            // Broadcast UPDATE message to all replicas
            for (ActorRef peer: this.peers){
                peer.tell(this.pendingUpdate, getSelf());
            }
            log.info("Coordinator {} broadcasted update message with value {}", this.id, msg.proposedV);
        } else {
            // forward the message to coordinator
            this.coordinator.tell(msg, getSelf());
            log.info("Replica {} forwarded write request to coordinator", this.id);
        }
    }

    private void onUpdateMsg(UpdateMsg msg) {
        this.epoch = Math.max(this.epoch, msg.epoch);
        this.seqNum = Math.max(this.seqNum, msg.seqNum);
        this.pendingUpdate = msg;
        getSender().tell(new AckMsg(msg.epoch, msg.seqNum), getSelf());
        log.info("Replica {} received update message with value {}", this.id, msg.newV);
    }

    private void onAckMsg (AckMsg ack){
        if (this.isCoordinator && ack.epoch == this.epoch && ack.seqNum == this.seqNum) {
            this.ackCount += 1;
            if (this.ackCount >= this.quorumSize) {
                this.ackCount = 0;
                for (ActorRef peer: this.peers){
                    peer.tell(new WriteOkMsg(this.epoch, this.seqNum), getSelf());
                }
                this.v = this.pendingUpdate.newV;
                log.info("Coordinator {} received enough acks, broadcasted write ok message", this.id);
            }
        }
    }

    private void onWriteOkMsg(WriteOkMsg msg) {
        if (msg.epoch == this.epoch && msg.seqNum == this.seqNum) {
            this.v = this.pendingUpdate.newV;
            log.info("Replica {} received write ok message, updated value to {}", this.id, this.v);
        } else {
            log.error("Replica {} received write ok message with wrong epoch or seqNum", this.id);
            log.info("Expected epoch: {}, seqNum: {}, got ({},{})", this.epoch, this.seqNum, msg.epoch, msg.seqNum);
        }
    }

    private void onCoordinatorUpdateMsg(CoordinatorUpdateMsg msg) {
        this.coordinator = msg.newCoordinator;
        log.info("Replica {} updated coordinator to {}", this.id, msg.newCoordinator.path().name());
    }

    private void decideCrash(){
        if (this.rnd.nextInt(100) <= this.crashP){
            crash();
        }
    }


    private void crash(){
        getContext().become(crashed());
    }

    /* -------------------------------------------------------------- */

    // Here we define the mapping between the received message types
    // and our actor methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinGroupMsg.class, this::onJoinGroupMsg)
                .match(ReadRequestMsg.class, this::onReadRequestMsg)
                .match(WriteRequestMsg.class, this::onWriteRequestMsg)
                .match(UpdateMsg.class, this::onUpdateMsg)
                .match(AckMsg.class, this::onAckMsg)
                .match(WriteOkMsg.class, this::onWriteOkMsg)
                .match(CoordinatorUpdateMsg.class, this::onCoordinatorUpdateMsg)
                .build();
    }

    final AbstractActor.Receive crashed(){
        return receiveBuilder()
                .matchAny(msg -> {})
                .build();
    }
}
