package it.unitn.ds1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Collections;

import akka.actor.AbstractActor;
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

    // CONSTRUCTOR
    public Replica(int id, boolean isCoordinator){
        this.id = id;
        this.isCoordinator = isCoordinator;
        this.rnd = new Random();
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
            // TODO - UPDATE V
        } else {
            // forward the message to coordinator
            this.coordinator.tell(msg, getSelf());
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
        .build();
    }

    final AbstractActor.Receive crashed(){
        return receiveBuilder()
            .matchAny(msg -> {})
            .build();
    }    
}
