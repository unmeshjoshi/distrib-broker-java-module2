package com.dist.simplekafka;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;

import java.util.ArrayList;
import java.util.List;

public class ZkController implements IZkChildListener, IZkDataListener {
    private final ZookeeperClient zookeeperClient;
    private final int brokerId;
    private int currentLeader = -1;
    private List<Broker> liveBrokers = new ArrayList<>();

    public ZkController(ZookeeperClient zookeeperClient, int brokerId) {
        this.zookeeperClient = zookeeperClient;
        this.brokerId = brokerId;
    }

    /**
     * public static void main() {
     * registerBroker();
     * ZkController controller = new ZkController();
     * controller.startup();
     * }
     */

    public void startup() {
        elect();
        zookeeperClient.subscribeControllerChangeListener(this);
    }


    public void shutdown() {
        // Implementation not provided in the original code
    }

    /**
     * Attempts to elect this broker as the controller in the Kafka cluster.
     * This method leverages ZooKeeper's strong consistency guarantees to ensure
     * only one controller exists at any time.
     * <p>
     * ZooKeeper's importance in controller election:
     * 1. Atomic Operations: ZooKeeper ensures the controller path creation is atomic
     * (only one broker can succeed, even if multiple try simultaneously)
     * 2. Distributed Consensus: Using ZooKeeper's ZAB (ZooKeeper Atomic Broadcast) protocol,
     * all servers in the ZooKeeper ensemble agree on the state of the controller
     * 3. High Availability: If the controller fails, ZooKeeper's watch mechanisms can
     * notify other brokers to trigger a new election
     * <p>
     * Fault Tolerance in ZooKeeper:
     * - Quorum-based Updates: Changes are committed only when majority of ZooKeeper
     * servers (2f+1 out of 2f+1 servers, where f is number of allowed failures) acknowledge
     * - Write Ahead Logging: Every state change is persisted to disk before acknowledging
     * - Failure Recovery: Even if some ZooKeeper servers fail, as long as a majority
     * remains available, the controller election state is preserved
     * - Leader Election: ZooKeeper itself uses the ZAB protocol to elect its own leader,
     * ensuring service continues even when some servers fail
     * <p>
     * Without a consensus system like ZooKeeper, distributed systems could end up in a
     * "split-brain" scenario where multiple nodes believe they are the controller,
     * leading to cluster inconsistencies.
     */
    public void elect() {
        try {
            // Attempt to create an ephemeral node in ZooKeeper to become the controller
            // This is an atomic operation - only one broker can succeed
            zookeeperClient.tryCreatingControllerPath(brokerId);

            // If we get here, we successfully became the controller
            this.currentLeader = brokerId;

            // Initialize controller state by loading broker information
            onBecomingLeader();

        } catch (ControllerExistsException e) {
            // Another broker is already the controller
            // Update our local state to recognize the existing controller
            this.currentLeader = e.getControllerId();
        }
    }

    private void onBecomingLeader() {
        liveBrokers.addAll(zookeeperClient.getAllBrokers());
    }


    public int getCurrentLeaderId() {
        return currentLeader;
    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {

    }

    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
        elect();
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {

    }
}
