package it.unitn.ds1;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import it.unitn.ds1.debug.Colors;


public class Client extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final int id;
    private List<ActorRef> replicas = new ArrayList<>();
    private final Random rnd;
    private Cancellable requestScheduler;
    private Cancellable readRequestTimeout;
    private Cancellable writeRequestTimeout;

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

    public static class JoinGroupMsg implements Serializable {   ///DOM perchè un nuovo joingroupmsg?
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

    public static class ReadRequestTimeoutMsg implements Serializable {
        public final ActorRef replica;
        public ReadRequestTimeoutMsg(ActorRef replica) {
            this.replica = replica;
        }
    }

    public static class WriteRequestTimeoutMsg implements Serializable {
        public final ActorRef replica;
        public final int proposedV;
        public WriteRequestTimeoutMsg(ActorRef replica, int proposedV){
            this.replica = replica;
            this.proposedV = proposedV;
        }
    }

    /* ---------------- Client logic ----------------- */

    private void onReadRequestMsg(ReadRequestMsg msg){
        int to = rnd.nextInt(this.replicas.size());
        replicas.get(to).tell(new Replica.ReadRequestMsg(getSelf()), getSelf());

        this.readRequestTimeout = getContext().system().scheduler().scheduleOnce(
                Duration.ofSeconds(2),  // duration
                getSelf(),   // receiver
                new ReadRequestTimeoutMsg(replicas.get(to)), // message type
                getContext().system().dispatcher(), // process
                getSelf() // sender
        );

        log.info(Colors.YELLOW +"Client {} read req to replica {}"+Colors.RESET, this.id, to);
    }

    private void onWriteRequestMsg(WriteRequestMsg msg){
        int to = rnd.nextInt(this.replicas.size());
        replicas.get(to).tell(new Replica.WriteRequestMsg(getSelf(), msg.proposedV), getSelf());
        log.info(Colors.YELLOW + "Client {} sent write request to replica {} with value {}"+ Colors.RESET, this.id, to, msg.proposedV);

        // this.writeRequestTimeout = getContext().system().scheduler().scheduleOnce(
        //         Duration.ofSeconds(2),  // duration
        //         getSelf(),   // receiver
        //         new WriteRequestTimeoutMsg(replicas.get(to), msg.proposedV), // message type
        //         getContext().system().dispatcher(), // process
        //         getSelf() // sender
        // );
    }

    private void onJoinGroupMsg(JoinGroupMsg msg){
        for (ActorRef a : msg.group){
            if (!a.equals(getSelf())){
                this.replicas.add(a);  // copy all replicas except for self
            }
        }
        log.info("Client {} joined group with {} replicas", this.id, this.replicas.size());
        // Schedule periodic crash decision
        this.requestScheduler = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.ZERO,
                Duration.ofSeconds(3),
                this::sendRequest,
                getContext().system().dispatcher()
        );
    }

    private void onReadResponseMsg(ReadResponseMsg msg){
        if (readRequestTimeout != null && !readRequestTimeout.isCancelled()) {
            readRequestTimeout.cancel();
        }
        log.info(Colors.GREEN + "Client {} read done: {}" + Colors.RESET, this.id, msg.v);
    }


    private void onReadRequestTimeoutMsg(ReadRequestTimeoutMsg msg){
        log.info(Colors.PURPLE + "Client READ TIMEOUT" + Colors.RESET);
        // When receiving this timeout, assume that the replica has crashed
        if (readRequestTimeout != null && !readRequestTimeout.isCancelled()) {
            readRequestTimeout.cancel();
        }
        // remove from the list the crashed replica and resend a read request
        this.replicas.remove(msg.replica);
        getSelf().tell(new ReadRequestMsg(getSelf()), ActorRef.noSender());
    }

    private void onWriteRequestTimeoutMsg(WriteRequestTimeoutMsg msg){
        log.info(Colors.PURPLE + "Client WRITE TIMEOUT" + Colors.RESET);
        // When receiving this timeout, assume that the replica has crashed
        if (writeRequestTimeout != null && !writeRequestTimeout.isCancelled()) {
            writeRequestTimeout.cancel();
        }
        // remove from the list the crashed replica and resend a write request with the same proposed value
        this.replicas.remove(msg.replica);
        getSelf().tell(new WriteRequestMsg(getSelf(), msg.proposedV), ActorRef.noSender());
    }

    // TODO onAckWriteResponse to know when the write request has not been received by te replica, and to cancel the timeout

    // if (writeRequestTimeout != null && !writeRequestTimeout.isCancelled()) {
    //     writeRequestTimeout.cancel();
    // }


    private void sendRequest(){
        int delay = rnd.nextInt(2000);
        try {
            // introduce random delay between requests
            Thread.sleep(delay);
        } catch (Exception e){
        }
        if (delay%2 == 0){
            // if delay is even, send a read request
            getSelf().tell(new ReadRequestMsg(getSelf()), ActorRef.noSender());
        } else {
            // if delay is odd, send a write request with a random number
            getSelf().tell(new WriteRequestMsg(getSelf(), rnd.nextInt(100)), ActorRef.noSender());
        }

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
            .match(ReadRequestTimeoutMsg.class, this::onReadRequestTimeoutMsg)
            .match(WriteRequestTimeoutMsg.class, this::onWriteRequestTimeoutMsg)
            .build();
    }
    
    @Override
    public void postStop() {
        if (requestScheduler != null && !requestScheduler.isCancelled()){
            requestScheduler.cancel();
        }
        if (readRequestTimeout != null && !readRequestTimeout.isCancelled()) {
            readRequestTimeout.cancel();
        }
        if (writeRequestTimeout != null && !writeRequestTimeout.isCancelled()) {
            writeRequestTimeout.cancel();
        }
    }
}