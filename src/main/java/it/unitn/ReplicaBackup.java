    package it.unitn.ds1;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.MDC;

import akka.actor.Cancellable;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.actor.ActorRef;

import it.unitn.ds1.debug.Colors;

public class Replica extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private final int id;
    private int v = 12;
    private List<ActorRef> peers = new ArrayList<>();
    private boolean isCoordinator;
    private final int crashP = 5;
    private final Random rnd;
    private ActorRef coordinator;
    private int epoch;
    private int seqNum;
    private final int quorumSize;
    private int ackCount = 1;
    private UpdateMsg pendingUpdate;
    private Cancellable writeTimeout;
    private Cancellable heartbeatTimeout;
    private Cancellable crashTimeout;
    private final int heartbeatInterval = 1; // seconds
    private final int heartbeatTimeoutDuration = 3; // seconds
    private HashMap<ActorRef, Cancellable> replicaTimeouts = new HashMap<>();
    private List<UpdateMsg> updates = new ArrayList<>();
    //DEV
    private enum State{
        CRASHED,
        RUNNING
    }
    private State currentState = State.RUNNING;
    // public enum DebugAction{
    //     UPDATE_SENDING,
    //     UPDATE_RECEIVING,
    //     WRITEOK_SENDING,
    //     ELECTION,
    //     DEFAULT
    // }
    // private DebugAction debugAction = DebugAction.DEFAULT;

    // CONSTRUCTOR
    public Replica(int id, boolean isCoordinator, ActorRef coordinator, int N){
        this.id = id;
        // MDC.put("actorName", String.valueOf(this.id));
        this.isCoordinator = isCoordinator;
        this.rnd = new Random();
        this.quorumSize = (N / 2) + 1; // Majority quorum
        this.epoch = 0;
        this.seqNum = 0;
        if (isCoordinator && coordinator == null){
            this.coordinator = getSelf();
            this.heartbeatTimeout = getContext().system().scheduler().scheduleWithFixedDelay(
                    Duration.ZERO,
                    Duration.ofSeconds(heartbeatInterval),
                    this::sendHeartbeat,
                    getContext().system().dispatcher()
            );
        } else {
            this.coordinator = coordinator;
        }
        // Schedule periodic crash decision
        this.crashTimeout = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.ZERO,
                Duration.ofSeconds(2),
                this::decideCrash,
                getContext().system().dispatcher()
        );
    }

    static public Props props(int id, boolean isCoordinator, ActorRef coordinator, int N){
        return Props.create(Replica.class, () -> new Replica(id, isCoordinator, coordinator, N));
    }

    public int getId(){
        return this.id;
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
        //public final int proposedV;
        public int proposedV;

        public WriteRequestMsg(ActorRef sender, int proposedV) {
            this.sender = sender;
            this.proposedV = proposedV;
        }
    }

    public static class HeartbeatMsg implements Serializable {
        public final int epoch;
        public final int seqNum;

        public HeartbeatMsg(int epoch, int seqNum) {
            this.epoch = epoch;
            this.seqNum = seqNum;
        }
    }

    public static class HeartbeatAckMsg implements Serializable {
        public final int replicaId;

        public HeartbeatAckMsg(int replicaId) {
            this.replicaId = replicaId;
        }
    }

    public static class TimeoutMsg implements Serializable {
        public TimeoutMsg() {}
    }

        public static class WriteTimeoutMsg implements Serializable {
        public WriteTimeoutMsg() {}
    }

    public static class ReplicaTimeoutMsg implements Serializable {
        public final ActorRef replica;

        public ReplicaTimeoutMsg(ActorRef replica) {
            this.replica = replica;
        }
    }

    //DEV
    public static class CoordinatorCrashMsg implements Serializable {
        public CoordinatorCrashMsg() {}
    }

    // DEV
    public static class ReplicaCrashMsg implements Serializable {
        public ReplicaCrashMsg() {}
    }

    // // DEV
    // public static class DebugMsg implements Serializable {
    //     public final DebugAction action;

    //     public DebugMsg(DebugAction action) {
    //         this.action = action;
    //     }
    // }

    public static class LeaderElectionMsg implements Serializable {
        public final int epoch;
        public final int seqNum;
        public final ActorRef sender;
        public final int candidateId;
        public final ArrayList<ActorRef> notifiedReplicas;


        public LeaderElectionMsg(int epoch, int seqNum, ActorRef sender, int candidateId, ArrayList<ActorRef> notifiedReplicas) {
            this.epoch = epoch;
            this.seqNum = seqNum;
            this.sender = sender;
            this.candidateId = candidateId;
            this.notifiedReplicas = notifiedReplicas;

        }
    }

    public static class CoordinatorMsg implements Serializable {
        public final ActorRef sender;
        public final int candidateId;

        public CoordinatorMsg (int candidateId, ActorRef sender) {
            this.candidateId = candidateId;
            this.sender = sender;
        }

    }

    public static class SynchronizationMsg implements Serializable {
        public final int epoch;
        public final int seqNum;
        public final ActorRef newCoordinator;
        public final List<UpdateMsg> updates;

        public SynchronizationMsg(int epoch, int seqNum, ActorRef newCoordinator, List<UpdateMsg> updates) {
            this.epoch = epoch;
            this.seqNum = seqNum;
            this.newCoordinator = newCoordinator;
            this.updates = updates;
        }
    }


    // DEV
    public static class GetInfoMsg implements Serializable {
        public GetInfoMsg() {};
    }

    /*------------- Actor logic -------------------------------------------- */

    private void onJoinGroupMsg(JoinGroupMsg msg){
        for (ActorRef a : msg.group){
            this.peers.add(a);  // copy all replicas (also itself because it is checked in multicast)
        }
        log.info("Replica {} joined group with {} peers", this.id, this.peers.size());
    }

    private void onReadRequestMsg(ReadRequestMsg msg){
        ActorRef to = msg.sender;
        //to.tell(new Client.ReadResponseMsg(this.v), getSelf());
        tellToReplica(to, new Client.ReadResponseMsg(this.v));
        log.info("Replica {} sent value {} to client", this.id, this.v);
    }

    private void onWriteRequestMsg(WriteRequestMsg msg){
        // correctly handle both client messages and replicas messages
        if (this.isCoordinator){
            this.seqNum += 1;
            this.pendingUpdate = new UpdateMsg(this.epoch, this.seqNum, msg.proposedV);
            this.ackCount = 1;
            this.updates.add(pendingUpdate);

            // IF crash while writing
            this.writeTimeout = getContext().system().scheduler().scheduleOnce(
                    Duration.ofSeconds(heartbeatTimeoutDuration),
                    getSelf(),
                    new WriteTimeoutMsg(),
                    getContext().system().dispatcher(),
                    getSelf()
            );

            // Broadcast UPDATE message to all replicas
            // for (ActorRef peer: this.peers){
            //     peer.tell(this.pendingUpdate, getSelf());
            // }

            // DEV
            // DEBUG CRASH - during sending of update
            if (msg.proposedV == 9999){
                crash();
                return;
            }
            multicast(this.pendingUpdate, this.peers); // il coordinator deve settare timer quando invia update a tutti?

            // DEV
            // DEBUG CRASH - after sending of update
            if (msg.proposedV == 8888){
                crash();
                return;
            }


            tellToReplica(msg.sender, new Client.WriteAckMsg(msg.proposedV));

            log.info("Coordinator {} broadcasted update message with value {}", this.id, msg.proposedV);
        } else {
            // forward the message to coordinator
            //this.coordinator.tell(msg, getSelf());
            tellToReplica(this.coordinator, msg);
            log.info("Replica {} forwarded write request to coordinator", this.id);
        }
    }

    private void onUpdateMsg(UpdateMsg msg) {
        this.epoch = Math.max(this.epoch, msg.epoch);
        this.seqNum = Math.max(this.seqNum, msg.seqNum);
        this.pendingUpdate = msg;
        this.updates.add(msg);

        // DEV
        // DEBUG CRASH - after receiving of update
        if (msg.newV == 7777){
            crash();
            return;
        }

        //getSender().tell(new AckMsg(msg.epoch, msg.seqNum), getSelf());
        tellToReplica(getSender(), new AckMsg(msg.epoch, msg.seqNum));
        log.info("Replica {} received update message with value {}", this.id, msg.newV);
    }

    private void onAckMsg (AckMsg ack){
        if (this.isCoordinator && ack.epoch == this.epoch && ack.seqNum == this.seqNum) {
            this.ackCount += 1;
            if (this.ackCount >= this.quorumSize) {
                this.ackCount = 1;

                // for (ActorRef peer: this.peers){
                //     peer.tell(new WriteOkMsg(this.epoch, this.seqNum), getSelf());
                // }

                // DEV ???????????
                // DEBUG CRASH - during sending writeoks
                // if (msg.proposedV == 6666){
                //     crash();
                //     return;
                // }

                WriteOkMsg m = new WriteOkMsg(this.epoch, this.seqNum);
                multicast(m, this.peers);
            
                this.v = this.pendingUpdate.newV;
                if (this.writeTimeout != null && !writeTimeout.isCancelled()) {
                    this.writeTimeout.cancel();
                }
                log.info("Coordinator {} received enough acks, broadcasted write ok message", this.id);
            }
        }
    }

    private void onWriteOkMsg(WriteOkMsg msg) {
        if (msg.epoch == this.epoch && msg.seqNum == this.seqNum) {
            this.v = this.pendingUpdate.newV;
            if (this.writeTimeout != null && !writeTimeout.isCancelled()) {
                 this.writeTimeout.cancel();
            }
            log.info(Colors.BLUE + "Replica {} update {}:{} {}" + Colors.RESET, this.id, this.epoch, this.seqNum, this.v);
        } else {
            log.error("Replica {} received write ok message with wrong epoch or seqNum", this.id);
            log.info("Expected : ({},{}) \nGot ({},{})", this.epoch, this.seqNum, msg.epoch, msg.seqNum);
        }
    }

    // DEV
    private void onGetInfoMsg(GetInfoMsg msg){
        if (this.currentState == State.CRASHED){
            String str = Colors.RED + this.currentState + Colors.RESET;
            log.info("REPLICA {} INFO\nValue: {}\nState: {}\n", this.id, this.v, str);
        } else {
            // peers List cleaning
            List<String> peersList = this.peers.stream()
                .map(ref -> ref.path().toString()) 
                .map(path -> path.replaceAll(".*/user/([^#]+).*", "$1")) 
                .toList(); 

            // updates list cleaning
            List<String> updatesList = this.updates.stream()
            .map(Object::toString) // Converte l'oggetto in stringa
            .map(s -> s.substring(s.lastIndexOf('$') + 1)) // Estrai tutto dopo l'ultimo "."
            .toList();

            String state = Colors.GREEN + this.currentState + Colors.RESET;
            String str_info = "Coordinator: " + Colors.CYAN + this.coordinator.toString().replaceAll(".*/user/([^#]+).*", "$1") + Colors.RESET + "\nPeers: " + peersList + "\n" + "Epoch and SeqNum " + this.epoch + " - " + this.seqNum + "\nUpdates " + updatesList;
            log.info(Colors.YELLOW + "\nREPLICA {} INFO" + Colors.RESET + "\nValue: {}\nState: {}\n{}\n", this.id, this.v, state, str_info);
        }
    }

    // DEV
    private void onCoordinatorCrashMsg(CoordinatorCrashMsg msg){
        if (this.isCoordinator){
            log.info("Coordinator {} received console coordinator crash request", this.id);
            crash();
        } else {
            tellToReplica(this.coordinator, msg);
            log.info("Replica {} forwarded console coordinator crash request to coordinator", this.id);
        }
    }

    // DEV
    private void onReplicaCrashMsg(ReplicaCrashMsg msg){
        if (!this.isCoordinator){
            log.info("Replica {} received console replica crash request", this.id);
            crash();
        } else {}
    }

    // // DEV
    // private void onDebugMsg(DebugMsg msg){
    //     switch (msg.action){
    //         case UPDATE_RECEIVING:
    //             if (this.isCoordinator){
    //                 tellToReplica(getSelf(), new WriteRequestMsg(getSelf(), 9999));
    //             } else {
    //                 tellToReplica(this.coordinator, new WriteRequestMsg(getSelf(), 9999));
    //             }
    //             break;
    //         case UPDATE_SENDING:
                
    //             break;
    //         case WRITEOK_SENDING:
                
    //             break;
    //         case ELECTION:
                
    //             break;
    //         case DEFAULT:
    //             break;
    //     }
    // }

    //------------------------- Crash detection system -------------------------\\

    // when replica receives an heartbeat msg, it deletes its timeout -> coordinator is active
    private void onHeartbeatMsg(HeartbeatMsg msg) {
        // log.info("Replica {} received heartbeat message from coordinator", this.id);
        resetHeartbeatTimeout();
        // Send acknowledgment back to the coordinator
        //getSender().tell(new HeartbeatAckMsg(this.id), getSelf());
        tellToReplica(getSender(), new HeartbeatAckMsg(this.id));
    }

    // when coordinator receives heartbeatAckMsg, it deletes its replicaTimeout[replica] -> that replica is active 
    private void onHeartbeatAckMsg(HeartbeatAckMsg msg) {
        // log.info("Coordinator {} received heartbeat acknowledgment from replica {}", this.id, msg.replicaId);
        resetReplicaTimeout(getSender());
    }

    // coordinator sends heartbeat to all replicas
    private void sendHeartbeat(){
        // if (this.isActive) {
        // for (ActorRef peer : this.peers) {
        //     peer.tell(new HeartbeatMsg(this.epoch, this.seqNum), getSelf());
        // }
        HeartbeatMsg m = new HeartbeatMsg(this.epoch, this.seqNum);
        multicast(m, this.peers);
        // }
    }

    // reset replica heartbeat timeout 
    private void resetHeartbeatTimeout(){
        if (this.heartbeatTimeout != null && !this.heartbeatTimeout.isCancelled()){
            this.heartbeatTimeout.cancel();
        }
        this.heartbeatTimeout = getContext().system().scheduler().scheduleOnce(
                Duration.ofSeconds(heartbeatTimeoutDuration),  // duration
                getSelf(),   // receiver
                new TimeoutMsg(), // message type
                getContext().system().dispatcher(), // process
                getSelf() // sender
        );
    }

    // Replica detects a coordinator crash
    private void onTimeoutMsg(TimeoutMsg msg) {
        log.info(Colors.RED + "Replica {} detected a timeout, assuming coordinator {} CRASHED" + Colors.RESET, this.id, this.coordinator);
        peers.remove(this.coordinator);

        // peers list cleaning
        List<String> peersList = this.peers.stream()
        .map(ref -> ref.path().toString()) 
        .map(path -> path.replaceAll(".*/user/([^#]+).*", "$1")) 
        .toList(); 

        log.warning(Colors.CYAN + "SONO {} e la mia lista di repliche è {}" + Colors.RESET, this.id, peersList);
        ActorRef nextReplica = getNextReplica();
        if (nextReplica != null) {
            ArrayList<ActorRef> notifiedReplicas = new ArrayList<>();
            notifiedReplicas.add(getSelf());
            //nextReplica.tell(new LeaderElectionMsg(this.epoch, this.seqNum, getSelf(), this.id, notifiedReplicas), getSelf());
            tellToReplica(nextReplica, new LeaderElectionMsg(this.epoch, this.seqNum, getSelf(), this.id, notifiedReplicas));
        } else {
            // If this is the only replica left, it becomes the coordinator
            selfElection();
        }
    }

    private void onWriteTimeoutMsg(WriteTimeoutMsg msg){
        log.info(Colors.RED + "Coordinator {} detected a timeout while WRITING, assuming replica  CRASHED" + Colors.RESET, this.id);
    }

    private void resetReplicaTimeout(ActorRef replica) {
        if (replicaTimeouts.containsKey(replica)) {
            replicaTimeouts.get(replica).cancel();
        }

        // Schedule a new timeout
        Cancellable timeout = getContext().system().scheduler().scheduleOnce(
            Duration.ofSeconds(heartbeatTimeoutDuration),
            getSelf(),
            new ReplicaTimeoutMsg(replica),
            getContext().system().dispatcher(),
            getSelf()
        );
        // Store the timeout for the replica
        replicaTimeouts.put(replica, timeout);
    }

    private void onReplicaTimeoutMsg(ReplicaTimeoutMsg msg) {
        if (getSelf() == this.coordinator) {
            log.info("Coordinator {} detected a timeout for replica {}, assuming it CRASHED", this.id, msg.replica.path().name());
            peers.remove(msg.replica);

            // for (ActorRef peer: this.peers){
            //     peer.tell(new ReplicaTimeoutMsg(msg.replica), getSelf());
            // }

            ReplicaTimeoutMsg m = new ReplicaTimeoutMsg(msg.replica);
            multicast(m, this.peers);

        } else {
            peers.remove(msg.replica);
        }
    }

    private void decideCrash(){
        
        if (this.isCoordinator && this.rnd.nextInt(100) <= this.crashP && this.peers.size() > this.quorumSize) {
            // crash();
            log.warning(Colors.YELLOW + "I'm {} and I SHOULD CRASH" + Colors.RESET, this.id);
        }
        
        // if (peers.size() > this.quorumSize && this.rnd.nextInt(100) <= this.crashP){
        //     crash();
        // }
    }

    private void crash(){
        if (this.isCoordinator && heartbeatTimeout != null && !heartbeatTimeout.isCancelled()) {
            heartbeatTimeout.cancel();
        }
        if (crashTimeout != null && !crashTimeout.isCancelled()) {
            crashTimeout.cancel();
        }   
        log.error(Colors.RED + "SONO  FOTTUTAMENTE CRASHATO e sono {}" + Colors.RESET, this.id);
        this.currentState = State.CRASHED;
        getContext().become(crashed());
        // this.isActive = false;
    }

    //------------------------- Communication system --------------------------\\

    private void introduceNetworkDelay(){
        try { Thread.sleep(rnd.nextInt(100)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }

    private void multicast(Serializable m, List<ActorRef> multicastGroup){
        CopyOnWriteArrayList<ActorRef> safeMulticastGroup = new CopyOnWriteArrayList<>(multicastGroup);
        Iterator<ActorRef> iterator = safeMulticastGroup.iterator();
        while (iterator.hasNext()){
            ActorRef peer = iterator.next();
            if (!peer.equals(getSelf())){

                // simulate network delay
                introduceNetworkDelay();
                peer.tell(m, getSelf()); 
            }
        }
    }

    private void tellToReplica(ActorRef recipient, Serializable m){
        introduceNetworkDelay();
        recipient.tell(m, getSelf());
    }

    //------------------------- Leader election system -------------------------\\

    private void onLeaderElectionMsg(LeaderElectionMsg msg) {
        log.info(Colors.YELLOW + "Replica {} received ELECTION message from {} with candidateId {}" + Colors.RESET, this.id, msg.sender.path().name(), msg.candidateId);

        // DEV
        // DEBUG CRASH - during election

        // Determine if the current replica should be the new candidate
        boolean isBetterCandidate = (msg.epoch < this.epoch) ||
                (msg.epoch == this.epoch && msg.seqNum < this.seqNum) ||
                (msg.epoch == this.epoch && msg.seqNum == this.seqNum && msg.candidateId < this.id);

        // If I'm the best candidate, use my data, otherwise use msg data
        int newCandidateId = isBetterCandidate ? this.id : msg.candidateId;
        int newEpoch = isBetterCandidate ? this.epoch : msg.epoch;
        int newSeqNum = isBetterCandidate ? this.seqNum : msg.seqNum;

        // Forward the election message to the next replica in the ring
        ActorRef nextReplica = getNextReplica();
        if (nextReplica == null){
            // If this is the only replica left, it becomes the coordinator (it is impossible but avoid warnings)
            selfElection();
        } else if (!msg.notifiedReplicas.contains(getSelf())) {
            msg.notifiedReplicas.add(getSelf());
            //nextReplica.tell(new LeaderElectionMsg(newEpoch, newSeqNum, getSelf(), newCandidateId, new ArrayList<>(msg.notifiedReplicas)), getSelf());
            tellToReplica(nextReplica, new LeaderElectionMsg(newEpoch, newSeqNum, getSelf(), newCandidateId, new ArrayList<>(msg.notifiedReplicas)));
        } else {
           //nextReplica.tell(new CoordinatorMsg(newCandidateId, getSelf()), getSelf());
           tellToReplica(nextReplica, new CoordinatorMsg(newCandidateId, getSelf()));
        }
    }

    private void onCoordinatorMsg(CoordinatorMsg msg) {
        ActorRef nextReplica = getNextReplica();
        if (msg.candidateId == this.id || nextReplica == null) {
            selfElection();
        } else {
            //nextReplica.tell(new CoordinatorMsg(msg.candidateId, getSelf()), getSelf());
            tellToReplica(nextReplica, new CoordinatorMsg(msg.candidateId, getSelf()));
        }
    }

    private void onSynchronizationMsg(SynchronizationMsg msg) {
        log.info("Replica {} received synchronization message from new coordinator {}", this.id, msg.newCoordinator);

        // Synchronize updates
        // update messages before storing new info
        for (UpdateMsg update : msg.updates) {
            if (update.seqNum > this.seqNum) {
                this.seqNum = update.seqNum;
                this.v = update.newV;
                log.info("Replica {} synchronized update with seqNum {} and value {}", this.id, update.seqNum, update.newV);
            }
        }

        this.coordinator = msg.newCoordinator;
        this.epoch = msg.epoch;
        this.seqNum = msg.seqNum;
        this.isCoordinator = (msg.newCoordinator == getSelf());
        resetHeartbeatTimeout();
    }

    private ActorRef getNextReplica() {
        if (this.peers.isEmpty()) return null;
        int nextIndex = (this.peers.indexOf(getSelf()) + 1) % this.peers.size();
        return this.peers.get(nextIndex);
    }

    private void selfElection() {
        if (getSelf() != this.coordinator){
            log.info(Colors.GREEN + "Replica {} becomes the new coordinator" + Colors.RESET, this.id);
            this.isCoordinator = true;
            this.coordinator = getSelf();
            this.epoch = epoch + 1;
    
            // Broadcast SynchronizationMsg to all replicas with the list of updates
            // for (ActorRef peer : this.peers) {
            //     peer.tell(new SynchronizationMsg(this.epoch, this.seqNum, getSelf(), new ArrayList<>(this.updates)), getSelf());
            // }

            SynchronizationMsg m = new SynchronizationMsg(this.epoch, this.seqNum, getSelf(), new ArrayList<>(this.updates));
            multicast(m, this.peers);

            this.heartbeatTimeout = getContext().system().scheduler().scheduleWithFixedDelay(
                    Duration.ZERO,
                    Duration.ofSeconds(heartbeatInterval),
                    this::sendHeartbeat,
                    getContext().system().dispatcher()
            ); 
        }
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
                .match(HeartbeatMsg.class, this::onHeartbeatMsg)
                .match(HeartbeatAckMsg.class, this::onHeartbeatAckMsg)
                .match(TimeoutMsg.class, this::onTimeoutMsg)
                .match(WriteTimeoutMsg.class, this::onWriteTimeoutMsg)
                .match(ReplicaTimeoutMsg.class, this::onReplicaTimeoutMsg)
                .match(LeaderElectionMsg.class, this::onLeaderElectionMsg)
                .match(SynchronizationMsg.class, this::onSynchronizationMsg)
                .match(CoordinatorMsg.class, this::onCoordinatorMsg)
                .match(GetInfoMsg.class, this::onGetInfoMsg) // DEV
                .match(CoordinatorCrashMsg.class, this::onCoordinatorCrashMsg) // DEV
                .match(ReplicaCrashMsg.class, this::onReplicaCrashMsg) // DEV
                // .match(DebugMsg.class, this::onDebugMsg) // DEV
                .build();
    }

    final AbstractActor.Receive crashed(){
        return receiveBuilder()
                .match(GetInfoMsg.class, this::onGetInfoMsg)
                .matchAny(msg -> {})
                .build();
    }

    @Override
    public void postStop() {
        if (writeTimeout != null && !writeTimeout.isCancelled()) {
            writeTimeout.cancel();
        }
        if (heartbeatTimeout != null && !heartbeatTimeout.isCancelled()) {
            heartbeatTimeout.cancel();
        }
        if (crashTimeout != null && !crashTimeout.isCancelled()) {
            crashTimeout.cancel();
        }
        MDC.remove("actorName");
    }
}