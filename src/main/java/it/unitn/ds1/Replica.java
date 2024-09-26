package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.Cancellable;
import akka.actor.Props;
import ch.qos.logback.core.subst.Token;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.lang.Thread;
import java.lang.InterruptedException;
import java.util.Collections;


public class Replica extends AbstractActor {
    private int id;
    private String v;

    // CONSTRUCTOR
    public Replica(int id){
        this.id = id;
    }

    static public Props props(int id){
        return Props.create(Replica.class, () -> new Replica(id));
    }


      // Here we define the mapping between the received message types
  // and our actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .build();
  }

}
