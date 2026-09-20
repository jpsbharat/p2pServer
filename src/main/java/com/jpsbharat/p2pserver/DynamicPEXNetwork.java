package com.jpsbharat.p2pserver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class DynamicPEXNetwork {
    private final int myPort;
    private final String nodeName;
    private final List<String> localLedger = new CopyOnWriteArrayList<>();
    // Strict Peer Restrictions
    private final int MAX_PEERS = 2;
    private final List<Integer> connectedPeerPorts = new CopyOnWriteArrayList<>();
    private final List<PrintWriter> peerOutputStreams = new CopyOnWriteArrayList<>();

    public DynamicPEXNetwork(String nodeName, int myPort) {
        this.nodeName = nodeName;
        this.myPort = myPort;
        this.localLedger.add("GENESIS_STATE");
    }

    public void startServer() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(myPort)) {
                while (true) {
                    Socket socket = serverSocket.accept();
                    new Thread(new PeerConnectionHandler(socket)).start();
                }
            } catch (Exception ignored) {
            }
        }).start();
    }

    // Connects outbound to a peer if our connection pool isn't full
    public synchronized boolean connectToPeer(int targetPort) {
        if (targetPort == myPort || connectedPeerPorts.contains(targetPort)) return false;
        if (connectedPeerPorts.size() >= MAX_PEERS) {
            System.out.println("[" + nodeName + "] Connection to " + targetPort + " rejected: Peer Limit Reached (" + MAX_PEERS + ").");
            return false;
        }
        try {
            Socket socket = new Socket("localhost", targetPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            peerOutputStreams.add(out);
            connectedPeerPorts.add(targetPort);
            new Thread(new PeerConnectionHandler(socket)).start();
            System.out.println("[" + nodeName + "] Handshake SUCCESS with Port: " + targetPort + " | Active Pool: " + connectedPeerPorts);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // Gossip Protocol: Sends data to our immediate small pool of neighbors
    public synchronized void commitAndGossip(String txData) {
        if (!localLedger.contains(txData)) {
            localLedger.add(txData);
            System.out.println("[" + nodeName + "] Appended block locally: " + txData);
            for (PrintWriter out : peerOutputStreams) {
                out.println("TX:" + txData);
            }
        }
    }

    // Wire Protocol Message Handler
    private class PeerConnectionHandler implements Runnable {
        private final Socket socket;

        public PeerConnectionHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                String rawLine;
                while ((rawLine = in.readLine()) != null) {
// Scenario A: Standard Gossip Transaction Payload
                    if (rawLine.startsWith("TX:")) {
                        String txData = rawLine.substring(3);
                        commitAndGossip(txData);
                    }
// Scenario B: A peer is asking us to share a contact address (PEX Request)
                    else if (rawLine.startsWith("PEX_REQ")) {
                        if (!connectedPeerPorts.isEmpty()) {
// Give them the first peer in our list as a introduction referral
                            int referralPort = connectedPeerPorts.get(0);
                            out.println("PEX_RES:" + referralPort);
                        }
                    }
// Scenario C: Receiving a peer address referral from a neighbor
                    else if (rawLine.startsWith("PEX_RES:")) {
                        int referredPort = Integer.parseInt(rawLine.substring(8));
                        System.out.println("[" + nodeName + "] PEX Referral Received! Attempting connection to discoverable node on port " + referredPort);
// Try to connect to the newly discovered peer automatically
                        connectToPeer(referredPort);
                    }
                }
            } catch (Exception ignored) {
            }
        }
    }

    // Triggers a network-level discovery request to an established neighbor
    public void requestNewPeerFromNeighbor(int neighborPort) {
        System.out.println("\n[" + nodeName + "] Triggering PEX Address discovery query to neighbor on port " + neighborPort + "...");
        try {
            Socket socket = new Socket("localhost", neighborPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            new Thread(new PeerConnectionHandler(socket)).start();
// Send Peer Exchange protocol command over wire
            out.println("PEX_REQ");
        } catch (Exception ignored) {
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== BOOTSTRAPPING DYNAMIC GOSSIP NETWORK (MAX_PEER LIMIT = 2) ===");
        DynamicPEXNetwork nodeA = new DynamicPEXNetwork("Node_Alpha", 9001);
        DynamicPEXNetwork nodeB = new DynamicPEXNetwork("Node_Beta", 9002);
        DynamicPEXNetwork nodeC = new DynamicPEXNetwork("Node_Gamma", 9003);
        nodeA.startServer();
        nodeB.startServer();
        nodeC.startServer();
        Thread.sleep(500);
// Form initial link: Alpha <---> Beta <---> Gamma
        nodeA.connectToPeer(9002);
        nodeB.connectToPeer(9003);
        Thread.sleep(500);
// 1. Boot up Node Delta late. It ONLY knows about Node Alpha (its "Boot Node")
        System.out.println("\n--- Late-Joining Node Delta Enters Network ---");
        DynamicPEXNetwork nodeD = new DynamicPEXNetwork("Node_Delta", 9004);
        nodeD.startServer();
        Thread.sleep(200);
// Delta connects to Alpha. Alpha's pool is now full (connected to Beta and Delta)
        nodeD.connectToPeer(9001);
        Thread.sleep(500);
// 2. Node Delta needs another peer to strengthen its connection to the network mesh.
// It asks Node Alpha for a referral. Alpha points Delta to Beta!
        nodeD.requestNewPeerFromNeighbor(9001);
        Thread.sleep(1000); // Wait for PEX socket request/response to finalize
        System.out.println("\n--- Testing Gossip Routing with Bounded Partial Peer View ---");
// Delta drops a transaction. Delta only connects to Alpha and Beta.
// Gamma will still receive it because Beta will gossip it forward!
        nodeD.commitAndGossip("TX_DECENTRALIZED_PAYLOAD_881");
        Thread.sleep(1000);
        System.out.println("\n=== FINAL INTEGRITY CHECK ===");
        System.out.println("Node Delta (Origin) Ledger: " + nodeD.localLedger);
        System.out.println("Node Gamma (Isolated) Ledger: " + nodeC.localLedger);
    }
}
