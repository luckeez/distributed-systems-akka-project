package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.List;
import java.io.IOException;
import java.util.ArrayList;

import it.unitn.ds1.Replica.JoinGroupMsg;

public class ReplicaSystem {
    final static int N = 10;

    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("replicasystem");

        List<ActorRef> group = new ArrayList<>();

        // Add initial coordinatoor
        group.add(system.actorOf(Replica.props(0, true), "replica" + 0));

        // Add other replicas
        for (int i=1;i<N;i++){
            group.add(system.actorOf(Replica.props(i, false), "replica" + i));
        }

        
        JoinGroupMsg start = new JoinGroupMsg(group);
        for (ActorRef peer : group){
            peer.tell(start, ActorRef.noSender());
        }

        try {
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        }
        catch (IOException ioe){}
        system.terminate();
    }
}