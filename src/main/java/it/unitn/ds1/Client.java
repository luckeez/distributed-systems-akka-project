package it.unitn.ds1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.actor.ActorRef;

import akka.event.Logging;
import akka.event.LoggingAdapter;


public class Client extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final int id;
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

    public static class JoinGroupMsg implements Serializable {
        public final List<ActorRef> group;   // an array of group members
        public JoinGroupMsg(List<ActorRef> group){
            this.group = group;
        }
    }

    public static class ReadResponseMsg implements Serializable{
        public final int v;
        public ReadResponseMsg(int v){
            this.v = v;
        }
    }

    /* ---------------- Client logic ----------------- */

    private void onReadRequestMsg(ReadRequestMsg msg){
        int to = rnd.nextInt(this.replicas.size());
        replicas.get(to).tell(new Replica.ReadRequestMsg(getSelf()), getSelf());
        log.info("Client {} sent read request to replica {}", this.id, to);
    }

    private void onWriteRequestMsg(WriteRequestMsg msg){
        int to = rnd.nextInt(this.replicas.size());
        replicas.get(to).tell(new Replica.WriteRequestMsg(getSelf(), msg.proposedV), getSelf());
        log.info("Client {} sent write request to replica {} with value {}", this.id, to, msg.proposedV);
    }

    private void onJoinGroupMsg(JoinGroupMsg msg){
        for (ActorRef a : msg.group){
            if (!a.equals(getSelf())){
                this.replicas.add(a);  // copy all replicas except for self
            }
        }
        log.info("Client {} joined group with {} replicas", this.id, this.replicas.size());
    }

    private void onReadResponseMsg(ReadResponseMsg msg){
        log.info("Client {} received value: {}", this.id, msg.v);
    }

    /* --------------------------------------------------------- */

    // Here we define the mapping between the received message types
    // and our actor methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(JoinGroupMsg.class, this::onJoinGroupMsg)
            .match(ReadResponseMsg.class, this::onReadResponseMsg)
            .match(ReadRequestMsg.class, this::onReadRequestMsg)
            .match(WriteRequestMsg.class, this::onWriteRequestMsg)
            .build();
    }
}