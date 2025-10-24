package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.debug.Colors;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class QuorumSystem {
  private static ActorSystem system;
  private static List<ActorRef> clients = new ArrayList<>();
  private static List<ActorRef> replicas = new ArrayList<>();
  private static Scanner scanner;
  private static final int N = 10;
  private static final int N_CLIENTS = 3;

  private static void initializeSystem() {
    replicas = new ArrayList<>();
    int quorumSize = (N / 2) + 1;
    for (int i = 0; i < N; i++) {
      replicas.add(system.actorOf(Replica.props(i, quorumSize), "Replica" + i));
    }

    for (ActorRef replica : replicas) {
      replica.tell(new Messages.Initialize(replicas), ActorRef.noSender());
    }

    for (int i = 0; i < N_CLIENTS; i++){
      clients.add(system.actorOf(Client.props(i, replicas), "Client" + i));
    }
    System.out.println(Colors.BLUE + "System has been initialized!" + Colors.RESET);
  }

  private static void printHelp() {
    System.out.println("\n=== Available Commands ===");
    System.out.println("help                          - Show this help message");
    System.out.println("status                        - Show system status");
    System.out.println("write <clientId> <value>      - Client writes a value (clientId: 0-" + (N_CLIENTS-1) +")");
    System.out.println("read <clientId>               - Client reads from replica (clientId: 0-" + (N_CLIENTS-1) +")");
    System.out.println("crash <replicaId>             - Immediately crash a replica");
    System.out.println("setcrash <replicaId> <point> <count> - Set crash point for replica");
    System.out.println("scenario <n>                  - Run predefined scenario");
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
    System.out.println("scenario election       - Coordinator and replica crash during election");
    System.out.println("scenario election2      - Multiple replica crashes during election");
    System.out.println("scenario electioninit   - Election initiator crashes during election");
    System.out.println("scenario update         - Crash during sending updates");
    System.out.println("scenario beforeupdate   - Crash before sending updates");
    System.out.println("scenario afterupdate    - Crash after sending updates");
    System.out.println("scenario writeok        - Crash during sending WriteOK");
    System.out.println("scenario afterwriteok   - Crash after sending WriteOK");
    System.out.println("scenario recvupdate     - Replica crash after receiving update");
    System.out.println("scenario stress         - Stress test with multiple operations");
  }

  private static void printStatus() {
    System.out.println(Colors.BLUE + "\n=== Status ===" + Colors.RESET);
    for (ActorRef replica : replicas) {
      replica.tell(new Messages.GetState(), ActorRef.noSender());
    }
  }

  private static void handleReadCommand(String[] parts) {
    if (parts.length != 2) {
      System.out.println("Usage: read <clientId>");
      System.out.println("Example: read 0");
      return;
    }

    int clientId = Integer.parseInt(parts[1]);

    if (clientId < 0 || clientId >= clients.size()) {
      System.out.println("Invalid client ID. Use 0-" + (clients.size() - 1));
      return;
    }

    clients.get(clientId).tell(new Messages.ReadRequest(), ActorRef.noSender());
  }

  private static void handleWriteCommand(String[] parts) {
    if (parts.length != 3) {
      System.out.println("Usage: write <clientId> <value>");
      System.out.println("Example: write 0 120");
      return;
    }

    int clientId = Integer.parseInt(parts[1]);
    int value = Integer.parseInt(parts[2]);
    if (clientId >= clients.size()) {
      System.out.println("Invalid client ID. Use 0-" + (clients.size() - 1));
      return;
    }

    clients.get(clientId).tell(new Messages.WriteRequest(value), ActorRef.noSender());

  }

  private static void handleCrashCommand(String[] parts) {
    if (parts.length != 2) {
      System.out.println("Usage: crash <replicaId>");
      System.out.println("Example: crash 0");
      return;
    }

    int replicaId = Integer.parseInt(parts[1]);

    if (replicaId < 0 || replicaId >= replicas.size()) {
      System.out.println("Invalid replica ID. Use 0-" + (replicas.size() - 1));
      return;
    }

    replicas.get(replicaId).tell(new Messages.Crash(), ActorRef.noSender());
  }

  private static void handleSetCrashCommand(String[] parts) {
    if (parts.length != 4) {
      System.out.println("Usage: setcrash <replicaId> <crashPoint> <afterOperations>");
      System.out.println("Example: setcrash 0 AFTER_SENDING_UPDATE 2");
      return;
    }

    int replicaId = Integer.parseInt(parts[1]);
    String crashPointStr = parts[2].toUpperCase();
    int afterOperations = Integer.parseInt(parts[3]);

    if (replicaId < 0 || replicaId >= replicas.size()) {
      System.out.println("Invalid replica ID. Use 0-" + (replicas.size() - 1));
      return;
    }

    try {
      Messages.CrashPoint crashPoint = Messages.CrashPoint.valueOf(crashPointStr);
      System.out.println("Setting crash point for replica " + replicaId + ": " +
          crashPoint + " after " + afterOperations + " operations");
      replicas.get(replicaId).tell(new Messages.SetCrashPoint(crashPoint, afterOperations), ActorRef.noSender());
    } catch (IllegalArgumentException e) {
      System.out.println("Invalid crash point. Type 'help' to see available crash points.");
    }

  }

  private static void handleScenarioCommand(String[] parts) {
    if (parts.length != 2) {
      System.out.println("Usage: scenario <scenarioName>");
      System.out.println("Available scenarios: basic, coordinator, election, recovery, stress");
      return;
    }

    String scenarioName = parts[1].toLowerCase();
    System.out.println("Running scenario: " + scenarioName);

    try {
      switch (scenarioName) {
        case "basic":
          runBasicScenario();
          break;
        case "coordinator":
          runCoordinatorCrashScenario();
          break;
        case "election":
          runElectionScenario();
          break;
        case "stress":
          runStressScenario();
          break;
        case "update":
          runCrashDuringSendingUpdates();
          break;
        case "beforeupdate":
          runCrashBeforeSendingUpdate();
          break;
        case "afterupdate":
          runCrashAfterSendingUpdates();
          break;
        case "election2":
          runElectionMultipleCrashes();
          break;
        case "electioninit":
          runElectionInitiatorCrash();
          break;
        case "writeok":
          runCrashDuringSendingWriteOK();
          break;
        case "afterwriteok":
          runCrashAfterSendingWriteOK();
          break;
        case "recvupdate":
          runCrashAfterReceivingUpdate();
          break;
        default:
          System.out.println("Unknown scenario: " + scenarioName);
          break;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.out.println("Scenario interrupted");
    }
  }

// ===================== SCENARIOS =====================

// --------------------- BASIC scenarios --------------------
  private static void runBasicScenario() throws InterruptedException {
    System.out.println("=== Basic Scenario: Normal Operations ===");

    clients.get(0).tell(new Messages.WriteRequest(10), ActorRef.noSender());
    Thread.sleep(1000);

    replicas.get(0).tell(new Messages.ReadRequest(), clients.get(0));
    Thread.sleep(500);

    clients.get(0).tell(new Messages.WriteRequest(20), ActorRef.noSender());
    Thread.sleep(1000);

    replicas.get(2).tell(new Messages.ReadRequest(), clients.get(0));
    Thread.sleep(500);

    System.out.println("Basic scenario completed");
  }

  private static void runCoordinatorCrashScenario() throws InterruptedException {
    System.out.println("=== Coordinator Crash Scenario ===");

    // Set coordinator to crash after sending first update
    replicas.get(0).tell(new Messages.SetCrashPoint(Messages.CrashPoint.AFTER_SENDING_UPDATE, 0), ActorRef.noSender());
    Thread.sleep(500);

    System.out.println("Sending write request that will trigger coordinator crash...");
    clients.get(0).tell(new Messages.WriteRequest(100), ActorRef.noSender());
    Thread.sleep(5000);

    printStatus();
    System.out.println("Coordinator crash scenario completed");
  }

// --------------------- ELECTION scenarios --------------------
  private static void runElectionScenario() throws InterruptedException {
    System.out.println("=== Election Scenario: Coord. and Replica Crash ===");

    // Set multiple replicas to crash at different points
    replicas.get(7).tell(new Messages.SetCrashPoint(Messages.CrashPoint.DURING_ELECTION, 0), ActorRef.noSender());
    Thread.sleep(500);

    replicas.get(0).tell(new Messages.Crash(), ActorRef.noSender());
    Thread.sleep(7000);

    System.out.println("Election scenario completed");
  }

  private static void runElectionMultipleCrashes() throws InterruptedException {
    System.out.println("=== Election Scenario: Multiple Crashes ===");

    // Set multiple replicas to crash during election
    replicas.get(4).tell(new Messages.SetCrashPoint(Messages.CrashPoint.DURING_ELECTION, 0), ActorRef.noSender());
    replicas.get(3).tell(new Messages.SetCrashPoint(Messages.CrashPoint.DURING_ELECTION, 0), ActorRef.noSender());

    Thread.sleep(500);

    replicas.get(0).tell(new Messages.Crash(), ActorRef.noSender());
    Thread.sleep(7000);

    System.out.println("Election scenario completed");
  }

  private static void runElectionInitiatorCrash() throws InterruptedException {
    System.out.println("=== Election Scenario: Initiator Crash ===");

    // Set the initiator to crash during election
    replicas.get(1).tell(new Messages.SetCrashPoint(Messages.CrashPoint.DURING_ELECTION_INITIATOR, 0), ActorRef.noSender());

    Thread.sleep(500);

    replicas.get(0).tell(new Messages.Crash(), ActorRef.noSender());
    Thread.sleep(7000);

    System.out.println("Election scenario completed");
  }

// --------------------- UPDATE scenarios --------------------
  private static void runCrashBeforeSendingUpdate() throws InterruptedException {
    System.out.println("=== Crash Before Sending Update Scenario ===");

    // Set coordinator to crash before sending updates
    replicas.get(0).tell(new Messages.SetCrashPoint(Messages.CrashPoint.BEFORE_SENDING_UPDATE, 0), ActorRef.noSender());
    Thread.sleep(500);

    System.out.println("Sending write request that will trigger crash before sending updates...");
    clients.get(0).tell(new Messages.WriteRequest(150), ActorRef.noSender());
    Thread.sleep(7000);

    printStatus();
    System.out.println("Crash before sending update scenario completed");
  }

  private static void runCrashDuringSendingUpdates() throws InterruptedException {
    System.out.println("=== Crash During Sending Updates Scenario ===");

    // Set coordinator to crash during sending updates
    replicas.get(0).tell(new Messages.SetCrashPoint(Messages.CrashPoint.DURING_SENDING_UPDATE, 3), ActorRef.noSender());
    Thread.sleep(500);

    System.out.println("Sending write request that will trigger crash during updates...");
    clients.get(0).tell(new Messages.WriteRequest(800), ActorRef.noSender());
    Thread.sleep(7000);

    printStatus();
    System.out.println("Crash during sending updates scenario completed");
  }

  private static void runCrashAfterSendingUpdates() throws InterruptedException {
    System.out.println("=== Crash After Sending Updates Scenario ===");

    // Set coordinator to crash during sending updates
    replicas.get(0).tell(new Messages.SetCrashPoint(Messages.CrashPoint.AFTER_SENDING_UPDATE, 0), ActorRef.noSender());
    Thread.sleep(500);

    System.out.println("Sending write request that will trigger crash after updates...");
    clients.get(0).tell(new Messages.WriteRequest(900), ActorRef.noSender());
    Thread.sleep(7000);

    printStatus();
    System.out.println("Crash after sending updates scenario completed");
  }

// --------------------- WRITEOK scenarios --------------------
  private static void runCrashDuringSendingWriteOK() throws InterruptedException {
    System.out.println("=== Crash Before Sending WriteOK Scenario ===");

    // Set coordinator to crash before sending WriteOK
    replicas.get(0).tell(new Messages.SetCrashPoint(Messages.CrashPoint.DURING_SENDING_WRITEOK, 3), ActorRef.noSender());
    Thread.sleep(500);

    System.out.println("Sending write request that will trigger crash during sending WriteOK...");
    clients.get(0).tell(new Messages.WriteRequest(250), ActorRef.noSender());
    Thread.sleep(7000);

    printStatus();
    System.out.println("Crash during sending WriteOK scenario completed");
  }

  private static void runCrashAfterSendingWriteOK() throws InterruptedException {
    System.out.println("=== Crash After Sending WriteOK Scenario ===");

    // Set coordinator to crash after sending WriteOK
    replicas.get(0).tell(new Messages.SetCrashPoint(Messages.CrashPoint.AFTER_SENDING_WRITEOK, 0), ActorRef.noSender());
    Thread.sleep(500);

    System.out.println("Sending write request that will trigger crash after sending WriteOK...");
    clients.get(0).tell(new Messages.WriteRequest(350), ActorRef.noSender());
    Thread.sleep(7000);

    printStatus();
    System.out.println("Crash after sending WriteOK scenario completed");
  }


// --------------------- RECEIVE UPDATE scenario --------------
  private static void runCrashAfterReceivingUpdate() throws InterruptedException {
    System.out.println("=== Crash After Receiving Update Scenario ===");

    // Set replica to crash after receiving update
    replicas.get(1).tell(new Messages.SetCrashPoint(Messages.CrashPoint.AFTER_RECEIVING_UPDATE, 0), ActorRef.noSender());
    Thread.sleep(500);

    System.out.println("Sending write request that will trigger replica crash after receiving update...");
    clients.get(0).tell(new Messages.WriteRequest(450), ActorRef.noSender());
    Thread.sleep(7000);

    printStatus();
    System.out.println("Crash after receiving update scenario completed");
  }


// --------------------- STRESS scenario --------------------
  private static void runStressScenario() throws InterruptedException {
    System.out.println("=== Stress Scenario: Multiple Concurrent Operations ===");

    // Rapid fire writes from multiple clients
    for (int i = 0; i < 10; i++) {
      int clientId = i % (clients.size()-1);
      // int value = 1000 + 1000*clientId + i;
      int value = 1000 + i;
      clients.get(0).tell(new Messages.WriteRequest(value), ActorRef.noSender());
      i+=1;
      value = 2000 + i;
      clients.get(1).tell(new Messages.WriteRequest(value), ActorRef.noSender());
      Thread.sleep(100);

      // Crash coordinator halfway through
      if (i == 3) {
        replicas.get(0).tell(new Messages.Crash(), ActorRef.noSender());
        System.out.println("Coordinator crashed during stress test!");
      }
    }

    Thread.sleep(10000);
    System.out.println("Stress scenario completed");
  }

  /*
  TODO
  Funzioni per il client di test che legge continuamente finché non gli viene detto di fermarsi
  */

  // private static void keepReading() {
  //   clients.get(2).tell(new Messages.keepReading(), ActorRef.noSender());
  // }

  // private static void stopReading() {
  //   clients.get(2).tell(new Messages.stopReading(), ActorRef.noSender());
  // }

// ==================== SYSTEM RESET =====================
  private static void resetSystem() {
    System.out.println("Resetting system...");

    // Terminate current system
    system.terminate();
    try {
      // Block until termination completes (max 10s)
      system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      e.printStackTrace();
    }

    clients.clear();
    replicas.clear();

    // Create new system
    system = ActorSystem.create("QuorumSystem");
    initializeSystem();

    System.out.println("System reset completed");
  }
// ==================== MAIN =====================
  public static void main(String[] args) {
    system = ActorSystem.create("QuorumSystem");
    scanner = new Scanner(System.in);

    System.out.println(Colors.PURPLE + "=== Quorum-based Total Order Broadcast Simulator ===" + Colors.RESET);

    System.out.println("Initializing the system...");

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
          // TODO per client che continua a leggere
          // case "r":
          //   keepReading();
          //   break;
          // case "s":
          //   stopReading();
            // break;
          case "quit":
          case "exit":
            System.out.println(Colors.YELLOW + "Shutting down the system..." + Colors.RESET);
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
