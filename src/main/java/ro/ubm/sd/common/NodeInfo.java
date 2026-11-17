package ro.ubm.sd.common;

import java.util.Objects;

/**
 * Modelul de date pentru un nod.
 */
public class NodeInfo {
    private final String id;
    private String host;
    private int port;
    private volatile NodeState state;
    private volatile long lastHeartbeat;
    private volatile boolean coordinator;
    private volatile int missedIntervals;
    private volatile boolean receivedInCurrentInterval;
    private volatile long pid;

    public NodeInfo(String id, String host, int port, NodeState state, long lastHeartbeat, boolean coordinator) {
        this(id, host, port, state, lastHeartbeat, coordinator, -1L);
    }

    public NodeInfo(String id, String host, int port, NodeState state, long lastHeartbeat, boolean coordinator, long pid) {
        this.id = Objects.requireNonNull(id, "id nu poate fi null");
        this.host = Objects.requireNonNull(host, "host nu poate fi null");
        this.port = port;
        this.state = Objects.requireNonNull(state, "state nu poate fi null");
        this.lastHeartbeat = lastHeartbeat;
        this.coordinator = coordinator;
        this.missedIntervals = 0;
        this.receivedInCurrentInterval = true;
        this.pid = pid;
    }

    public NodeInfo(String id, String host, int port, boolean coordinator) {
        this(id, host, port, NodeState.ACTIV, System.currentTimeMillis(), coordinator, -1L);
    }

    /**
     * Inregistreaza primirea unui heartbeat in intervalul curent si reseteaza contorul de intervale ratate.
     */
    public synchronized void recordHeartbeat(String host, int port, boolean coordinator, long pid) {
        this.host = Objects.requireNonNull(host, "host nu poate fi null");
        this.port = port;
        this.coordinator = coordinator;
        if (pid > 0) {
            this.pid = pid;
        }
        this.lastHeartbeat = System.currentTimeMillis();
        this.missedIntervals = 0;
        this.receivedInCurrentInterval = true;
        this.state = NodeState.ACTIV;
    }

    /**
     * Consuma flag-ul din intervalul curent. Daca nu s-a primit heartbeat in acest interval,
     * incrementeaza numarul de intervale ratate si returneaza noua valoare.
     */
    public synchronized int advanceInterval() {
        if (receivedInCurrentInterval) {
            receivedInCurrentInterval = false;
            return missedIntervals;
        }
        missedIntervals++;
        return missedIntervals;
    }

    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public NodeState getState() {
        return state;
    }

    public void setState(NodeState state) {
        this.state = Objects.requireNonNull(state, "state nu poate fi null");
    }

    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(long lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    public boolean isCoordinator() {
        return coordinator;
    }

    public void setCoordinator(boolean coordinator) {
        this.coordinator = coordinator;
    }

    public int getMissedIntervals() {
        return missedIntervals;
    }

    public void setMissedIntervals(int missedIntervals) {
        this.missedIntervals = missedIntervals;
    }

    public long getPid() {
        return pid;
    }

    public void setPid(long pid) {
        this.pid = pid;
    }

    public String getAddress() {
        return host + ":" + port;
    }

    @Override
    public String toString() {
        return String.format("NodeInfo{id='%s', address='%s:%d', state=%s, missedIntervals=%d, coordinator=%s, pid=%d, lastHeartbeat=%d}",
                id, host, port, state, missedIntervals, coordinator, pid, lastHeartbeat);
    }
}
