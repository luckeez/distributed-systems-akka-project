package it.unitn.ds1;

import akka.actor.AbstractActor;
import akka.actor.Props;



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
