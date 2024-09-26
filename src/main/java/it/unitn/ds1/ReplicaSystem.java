package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

public class ReplicaSystem {
    final static int N = 10;

    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("replicasystem");

        List<ActorRef> group = new ArrayList<>();
        for (int i=0;i<N;i++){
            group.add(system.actorOf(Replica.props(i), "replica" + i));
        }
    }
}