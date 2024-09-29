package it.unitn.ds1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Collections;

import akka.actor.AbstractActor;
import akka.actor.Actor;
import akka.actor.Props;
import it.unitn.ds1.Client.ReadRequestMsg;
import it.unitn.ds1.Client.WriteRequestMsg;
import akka.actor.ActorRef;



public class Replica extends AbstractActor {
    private int id;
    private int v;
    private List<ActorRef> peers = new ArrayList<>();
    private boolean isCoordinator;
    private final int crashP = 5;
    private final Random rnd;
    private ActorRef coordinator;
    private int epoch = 0;
    private int seqNum = 0;
    private int quorumSize;
    private int ackCount = 0;
    private UpdateMsg pendingUpdate;

    // CONSTRUCTOR
    public Replica(int id, boolean isCoordinator){
        this.id = id;
        this.isCoordinator = isCoordinator;
        this.rnd = new Random();
        this.quorumSize = (10 / 2) + 1; // Majority quorum
    }

    static public Props props(int id, boolean isCoordinator){
        return Props.create(Replica.class, () -> new Replica(id, isCoordinator));
    }


    /*-- Message classes ------------------------------------------------------ */

    // Start message that informs every participant about its peers
    public static class JoinGroupMsg implements Serializable {
        public final List<ActorRef> group;   // an array of group members
        public JoinGroupMsg(List<ActorRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<ActorRef>(group));
        }
    }

    public static class ReadResponseMsg implements Serializable{
        public final int v;
        public ReadResponseMsg(int v){
            this.v = v;
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


    /*------------- Actor logic -------------------------------------------- */

    private void onJoinGroupMsg(JoinGroupMsg msg){
        for (ActorRef a : msg.group){
            if (!a.equals(getSelf())){
                this.peers.add(a);  // copy all replicas except for self
            }
        }
    }

    private void onReadRequestMsg(ReadRequestMsg msg){
        ActorRef to = msg.sender;
        to.tell(new ReadResponseMsg(this.v), getSelf());
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
        } else {
            // forward the message to coordinator
            this.coordinator.tell(msg, getSelf());
        }
    }

    private void onUpdateMsg(UpdateMsg msg) {
        getSender().tell(new AckMsg(msg.epoch, msg.seqNum), getSelf());
    }

    private void onAckMsg (AckMsg ack){
        if (this.isCoordinator && ack.epoch == this.epoch && ack.seqNum == this.seqNum) {
            this.ackCount += 1;
            if (this.ackCount >= this.quorumSize) {
                for (ActorRef peer: this.peers){
                    peer.tell(new WriteOkMsg(this.epoch, this.seqNum), getSelf());
                }
            }
        }
    }

    private void onWriteOkMsg(WriteOkMsg msg) {
        if (msg.epoch == this.epoch && msg.seqNum == this.seqNum) {
            this.v = this.pendingUpdate.newV;
        }
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
            .build();
    }

    final AbstractActor.Receive crashed(){
        return receiveBuilder()
            .matchAny(msg -> {})
            .build();
    }    
}
