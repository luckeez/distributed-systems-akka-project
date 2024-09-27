package it.unitn.ds1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.actor.ActorRef;



public class Replica extends AbstractActor {
    private int id;
    private String v;
    private List<ActorRef> peers = new ArrayList<>();
    private boolean isCoordinator;

    // CONSTRUCTOR
    public Replica(int id, boolean isCoordinator){
        this.id = id;
        this.isCoordinator = isCoordinator;
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



    /*------------- Actor logic -------------------------------------------- */

    private void onJoinGroupMsg(JoinGroupMsg msg){
        for (ActorRef a : msg.group){
            if (!a.equals(getSelf())){
                this.peers.add(a);  // copy all replicas except for self
            }
        }
    }











    // Here we define the mapping between the received message types
    // and our actor methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(JoinGroupMsg.class, this::onJoinGroupMsg)
        .build();
    }
}
