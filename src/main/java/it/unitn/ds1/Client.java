package it.unitn.ds1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.actor.ActorRef;

import it.unitn.ds1.Replica.JoinGroupMsg;


public class Client extends AbstractActor {
    private int id;
    private List<ActorRef> replicas = new ArrayList<>();

    // CONSTRUCTOR
    public Client(int id){
        this.id = id;
    }

    static public Props props(int id){
        return Props.create(Client.class, () -> new Client(id));
    }



    /* ---------------- Client logic ----------------- */

    private void onJoinGroupMsg(JoinGroupMsg msg){
        for (ActorRef a : msg.group){
            if (!a.equals(getSelf())){
                this.replicas.add(a);  // copy all replicas except for self
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
