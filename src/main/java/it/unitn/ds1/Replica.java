package it.unitn.ds1;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;

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
    private int v;
    private List<ActorRef> peers = new ArrayList<>();
    private boolean isCoordinator;
    private final int crashP = 5;
    private final Random rnd;
    private ActorRef coordinator;
    private int epoch;
    private int seqNum;
    private final int quorumSize;
    private int ackCount = 0;
    private UpdateMsg pendingUpdate;
    private Cancellable writeTimeout;
    private Cancellable heartbeatTimeout;
    private final int heartbeatInterval = 1; // seconds
    private final int heartbeatTimeoutDuration = 2; // seconds
    private HashMap<ActorRef, Cancellable> replicaTimeouts = new HashMap<>();
    private List<UpdateMsg> updates = new ArrayList<>();
    private List<ActorRef> ringTopology;

    // CONSTRUCTOR
    public Replica(int id, boolean isCoordinator, ActorRef coordinator, int N){
        this.id = id;
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
        getContext().system().scheduler().scheduleWithFixedDelay(
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
        public final int proposedV;

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

    public static class ReplicaTimeoutMsg implements Serializable {
        public final ActorRef replica;

        public ReplicaTimeoutMsg(ActorRef replica) {
            this.replica = replica;
        }
    }

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

    /*------------- Actor logic -------------------------------------------- */

    private void onJoinGroupMsg(JoinGroupMsg msg){
        for (ActorRef a : msg.group){
            if (!a.equals(getSelf())){
                this.peers.add(a);  // copy all replicas except for self
            }
        }
        this.ringTopology = new ArrayList<>(msg.group);
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
            this.ackCount = 0;
            this.updates.add(pendingUpdate);

            // IF crash while writing
            this.writeTimeout = getContext().system().scheduler().scheduleOnce(
                    Duration.ofSeconds(heartbeatTimeoutDuration),
                    getSelf(),
                    new TimeoutMsg(),
                    getContext().system().dispatcher(),
                    getSelf()
            );

            // Broadcast UPDATE message to all replicas
            // for (ActorRef peer: this.peers){
            //     peer.tell(this.pendingUpdate, getSelf());
            // }

            multicast(this.pendingUpdate, this.peers);

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

        //getSender().tell(new AckMsg(msg.epoch, msg.seqNum), getSelf());
        tellToReplica(getSender(), new AckMsg(msg.epoch, msg.seqNum));
        log.info("Replica {} received update message with value {}", this.id, msg.newV);
    }

    private void onAckMsg (AckMsg ack){
        if (this.isCoordinator && ack.epoch == this.epoch && ack.seqNum == this.seqNum) {
            this.ackCount += 1;
            if (this.ackCount >= this.quorumSize) {
                this.ackCount = 0;

                // for (ActorRef peer: this.peers){
                //     peer.tell(new WriteOkMsg(this.epoch, this.seqNum), getSelf());
                // }

                WriteOkMsg m = new WriteOkMsg(this.epoch, this.seqNum);
                multicast(m, this.peers);
            
                this.v = this.pendingUpdate.newV;
                if (this.writeTimeout != null) {
                    this.writeTimeout.cancel();
                }
                log.info("Coordinator {} received enough acks, broadcasted write ok message", this.id);
            }
        }
    }

    private void onWriteOkMsg(WriteOkMsg msg) {
        if (msg.epoch == this.epoch && msg.seqNum == this.seqNum) {
            this.v = this.pendingUpdate.newV;
            if (this.writeTimeout != null) {
                 this.writeTimeout.cancel();
            }
            log.info(Colors.BLUE + "Replica {} update {}:{} {}" + Colors.RESET, this.id, this.epoch, this.seqNum, this.v);
        } else {
            log.error("Replica {} received write ok message with wrong epoch or seqNum", this.id);
            log.info("Expected : ({},{}) \nGot ({},{})", this.epoch, this.seqNum, msg.epoch, msg.seqNum);
        }
    }

    //------------------------- Crash detection system -------------------------\\

    private void onHeartbeatMsg(HeartbeatMsg msg) {
        // log.info("Replica {} received heartbeat message from coordinator", this.id);
        resetHeartbeatTimeout();
        // Send acknowledgment back to the coordinator
        //getSender().tell(new HeartbeatAckMsg(this.id), getSelf());
        tellToReplica(getSender(), new HeartbeatAckMsg(this.id));
    }

    private void onHeartbeatAckMsg(HeartbeatAckMsg msg) {
        // log.info("Coordinator {} received heartbeat acknowledgment from replica {}", this.id, msg.replicaId);
        resetReplicaTimeout(getSender());
    }

    private void sendHeartbeat(){
        // if (this.isActive) {
        // for (ActorRef peer : this.peers) {
        //     peer.tell(new HeartbeatMsg(this.epoch, this.seqNum), getSelf());
        // }
        HeartbeatMsg m = new HeartbeatMsg(this.epoch, this.seqNum);
        multicast(m, this.peers);
        // }
    }

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

    private void onTimeoutMsg(TimeoutMsg msg) {
        log.info(Colors.RED + "Replica {} detected a timeout, assuming coordinator {} CRASHED" + Colors.RESET, this.id, this.coordinator);
        peers.remove(this.coordinator);
        this.ringTopology.remove(this.coordinator);
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
        this.ringTopology.remove(msg.replica);
    }

    private void decideCrash(){
        
        if (this.isCoordinator && this.rnd.nextInt(100) <= this.crashP && this.ringTopology.size() > this.quorumSize) {
            crash();
        }
        
        // if (peers.size() > this.quorumSize && this.rnd.nextInt(100) <= this.crashP){
        //     crash();
        // }
    }

    private void crash(){
        if (this.isCoordinator && heartbeatTimeout != null && !heartbeatTimeout.isCancelled()) {
            heartbeatTimeout.cancel();
        }
        getContext().become(crashed());
        // this.isActive = false;
    }

    private void introduceNetworkDelay(){
        try { Thread.sleep(rnd.nextInt(100)); }
        catch (InterruptedException e) { e.printStackTrace(); }
    }

    private void multicast(Serializable m, List<ActorRef> multicastGroup){
        for (ActorRef peer : multicastGroup){

            // if (!peer.equals(getSelf())){      // using this we can avoid having the ringtopology list (?)

            // simulate network delay
            introduceNetworkDelay();  // TODO add in all the single "tell"?

            peer.tell(m, getSelf()); 
            // }
        }
    }

    private void tellToReplica(ActorRef recipient, Serializable m){
        introduceNetworkDelay();
        recipient.tell(m, getSelf());
    }

    //------------------------- Leader election system -------------------------\\

    private void onLeaderElectionMsg(LeaderElectionMsg msg) {
        log.info(Colors.YELLOW + "Replica {} received ELECTION message from {} with candidateId {}" + Colors.RESET, this.id, msg.sender.path().name(), msg.candidateId);

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
        if (this.ringTopology.isEmpty()) return null;
        int nextIndex = (this.ringTopology.indexOf(getSelf()) + 1) % this.ringTopology.size();
        return this.ringTopology.get(nextIndex);
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
                .match(ReplicaTimeoutMsg.class, this::onReplicaTimeoutMsg)
                .match(LeaderElectionMsg.class, this::onLeaderElectionMsg)
                .match(SynchronizationMsg.class, this::onSynchronizationMsg)
                .match(CoordinatorMsg.class, this::onCoordinatorMsg)
                .build();
    }

    final AbstractActor.Receive crashed(){
        return receiveBuilder()
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
    }
}