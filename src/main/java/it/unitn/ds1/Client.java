package it.unitn.ds1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.swing.AbstractAction;

import java.util.Collections;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.actor.ActorRef;

import it.unitn.ds1.Replica.JoinGroupMsg;


public class Client extends AbstractActor {
    private int id;
    private List<ActorRef> replicas = new ArrayList<>();
    private final Random rnd;

    // CONSTRUCTOR
    public Client(int id){
        this.id = id;
        this.rnd = new Random();
    }

    static public Props props(int id){
        return Props.create(Client.class, () -> new Client(id));
    }

    /* ---------------- Message classes ---------------- */

    // Client Read Request
    public static class ReadRequestMsg implements Serializable{
        public final ActorRef sender;
        public ReadRequestMsg(ActorRef sender){
            this.sender = sender;
        }
    }

    // Client Write Request
    public static class WriteRequestMsg implements Serializable{
        public final ActorRef sender;
        public final int proposedV;
        public WriteRequestMsg(ActorRef sender, int proposedV){
            this.sender = sender;
            this.proposedV = proposedV;
        }
    }


    /* ---------------- Client logic ----------------- */

    private void sendReadRequest(){
        int to = rnd.nextInt(this.replicas.size());
        replicas.get(to).tell(new ReadRequestMsg(getSelf()), getSelf());
    }

    private void sendWriteRequest(int proposedV){
        int to = rnd.nextInt(this.replicas.size());
        replicas.get(to).tell(new WriteRequestMsg(getSelf(), proposedV), getSelf());
    }







    private void onJoinGroupMsg(JoinGroupMsg msg){
        for (ActorRef a : msg.group){
            if (!a.equals(getSelf())){
                this.replicas.add(a);  // copy all replicas except for self
            }
        }
    }





    /* --------------------------------------------------------- */

    // Here we define the mapping between the received message types
    // and our actor methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(JoinGroupMsg.class, this::onJoinGroupMsg)
        .build();
    }




}
