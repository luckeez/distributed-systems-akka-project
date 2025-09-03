package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import it.unitn.ds1.debug.Colors;

import java.util.*;

public class QuorumSystem {
  private static ActorSystem system = ActorSystem.create("QuorumSystem");
  private static final LoggingAdapter log = Logging.getLogger(system, QuorumSystem.class);
  private static List<ActorRef> clients;
  private static List<ActorRef> replicas;
  private static Scanner scanner;
  private static final int N = 10;

  private static void initializeSystem() {
    replicas = new ArrayList<>();
    int quorumSize = N / 2 + 1;
    for (int i = 0; i < N; i++) {
      replicas.add(system.actorOf(Replica.props(i, quorumSize)));
    }

    clients.add(system.actorOf(Client.props(0, replicas)));
    log.info(Colors.BLUE, "System has been initialized!", Colors.RESET);

  }

  private static void printHelp() {
    System.out.println("\n=== Available Commands ===");
    System.out.println("help                          - Show this help message");
    System.out.println("status                        - Show system status");
    System.out.println("write <clientId> <value>      - Client writes a value (clientId: 0-2)");
    System.out.println("read <clientId> <replicaId>   - Client reads from replica (replicaId: 0-4)");
    System.out.println("crash <replicaId>             - Immediately crash a replica");
    System.out.println("setcrash <replicaId> <point> <count> - Set crash point for replica");
    System.out.println("scenario <n>               - Run predefined scenario");
    System.out.println("reset                         - Reset and restart the system");
    System.out.println("exit                          - Shutdown and exit");
    System.out.println("\n=== Crash Points ===");
    System.out.println("BEFORE_SENDING_UPDATE         AFTER_SENDING_UPDATE");
    System.out.println("AFTER_RECEIVING_UPDATE        BEFORE_SENDING_ACK");
    System.out.println("AFTER_SENDING_ACK             BEFORE_SENDING_WRITEOK");
    System.out.println("AFTER_SENDING_WRITEOK         AFTER_RECEIVING_WRITEOK");
    System.out.println("DURING_ELECTION               BEFORE_SYNCHRONIZATION");
    System.out.println("AFTER_SYNCHRONIZATION");
    System.out.println("\n=== Predefined Scenarios ===");
    System.out.println("scenario basic          - Basic write/read operations");
    System.out.println("scenario coordinator    - Coordinator crash during update");
    System.out.println("scenario election       - Multiple crashes during election");
    System.out.println("scenario recovery       - Recovery and synchronization test");
    System.out.println("scenario stress         - Stress test with multiple operations");
  }

  private static void printStatus() {

  }

  private static void handleReadCommand(String[] parts) {

  }

  private static void handleWriteCommand(String[] parts) {

  }

  private static void handleCrashCommand(String[] parts) {

  }

  private static void handleSetCrashCommand(String[] parts) {

  }

  private static void handleScenarioCommand(String[] parts) {

  }

  private static void resetSystem() {

  }

  public static void main(String[] args) {
    scanner = new Scanner(System.in);

    System.out.println(Colors.PURPLE + "=== Quorum-based Total Order Broadcast Simulator ===" + Colors.RESET);

    log.info("Initializing the system...");

    initializeSystem();

    System.out.println("Try 'help' for availble commands");

    while (true) {
      System.out.print("\n> ");
      String input = scanner.nextLine().trim();
      if (input.isEmpty())
        continue;

      String[] parts = input.split("\\s+");
      String command = parts[0].toLowerCase();

      try {
        switch (command) {
          case "help":
            printHelp();
            break;
          case "status":
            printStatus();
            break;
          case "write":
            handleWriteCommand(parts);
            break;
          case "read":
            handleReadCommand(parts);
            break;
          case "crash":
            handleCrashCommand(parts);
            break;
          case "setcrash":
            handleSetCrashCommand(parts);
            break;
          case "scenario":
            handleScenarioCommand(parts);
            break;
          case "reset":
            resetSystem();
            break;
          case "quit":
          case "exit":
            log.info("Shutting down the system...");
            system.terminate();
            scanner.close();
            System.exit(0);
            break;
          default:
            System.out.println("Unknown command: " + command + ". Type 'help' for available commands.");
            break;
        }
      } catch (Exception e) {
        System.out.println("Error executing command: " + e.getMessage());
      }
    }
  }
}
