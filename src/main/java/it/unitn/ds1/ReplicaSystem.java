package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import it.unitn.ds1.Replica.JoinGroupMsg;

import static java.lang.System.exit;

public class ReplicaSystem {
    private static final LoggingAdapter log = Logging.getLogger(ActorSystem.create("replicasystem"), ReplicaSystem.class);
    final static int N = 10;

    public static void getInfo(List<ActorRef> group){
        for (ActorRef peer : group){
            peer.tell(new Replica.GetInfoMsg(), ActorRef.noSender());
        }
    }

    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("replicasystem");

        List<ActorRef> group = new ArrayList<>();

        ActorRef initialCoordinator = system.actorOf(Replica.props(0, true, null, N), "replica0");
        group.add(initialCoordinator);
        log.info("Added initial coordinator: replica0");

        // Add other replicas
        for (int i = 1; i < N; i++) {
            group.add(system.actorOf(Replica.props(i, false, initialCoordinator, N), "replica" + i));
            log.info("Added replica: replica{}", i);
        }

        JoinGroupMsg start = new JoinGroupMsg(group);
        for (ActorRef peer : group) {
            peer.tell(start, ActorRef.noSender());
            log.info("Sent JoinGroupMsg to: {}", peer.path().name());
        }

        // Create a client
        ActorRef client = system.actorOf(Client.props(1), "client1");
        log.info("Created client: client0");   /// MOD CLIENT1
        // Send JoinGroupMsg to client
        client.tell(new Client.JoinGroupMsg(group), ActorRef.noSender());
        log.info("Sent JoinGroupMsg to client1");

    //     // Send read request
    //     client.tell(new Client.ReadRequestMsg(client), ActorRef.noSender());
    //     log.info("Sent ReadRequestMsg from client1");

    //    // Send write request
    //     client.tell(new Client.WriteRequestMsg(client, 42), ActorRef.noSender());
    //     log.info("Sent WriteRequestMsg from client1");

        // try {
        //     Thread.sleep(1000);
        // } catch (Exception e) {
        // }
        // // Send read request
        // client.tell(new Client.ReadRequestMsg(client), ActorRef.noSender());
        // log.info("Sent ReadRequestMsg from client1");

        log.info(">>> Press ENTER to exit <<<");
        while(true){
            try {
                int input = System.in.read();
                char ch = (char) input;
                switch (ch){
                    case 'c':
                    // TODO crash
                        log.info("Console replica crash");
                        break;

                    case 'l':
                        // TODO coordinator crash (leader election)
                        log.warning("Console coordinator crash");
                        break;
                    
                    case 'w':
                        // TODO write
                        log.warning("Console write");
                        break;

                    case 'r':
                        // TODO read
                        log.warning("Console read");
                        break;
                    
                    case 'p':
                        log.warning("Console get info");
                        getInfo(group);
                        break;

                    case 'q':
                        system.terminate();
                        log.info("Actor SYSTEM terminated");
                        exit(0);
                }
                
            } catch (IOException ioe) {
                log.error("IOException occurred", ioe);
            }
        }
    }
}