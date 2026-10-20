package ro.ubm.sd.common;

import java.util.Objects;

/**
 * Modelul de date pentru un nod.
 */
public class NodeInfo {
    private final String id;
    private final String host;
    private final int port;
    private NodeState state;
    private long lastHeartbeat;
    private boolean coordinator;

    public NodeInfo(String id, String host, int port, NodeState state, long lastHeartbeat, boolean coordinator) {
        this.id = Objects.requireNonNull(id, "id nu poate fi null");
        this.host = Objects.requireNonNull(host, "host nu poate fi null");
        this.port = port;
        this.state = Objects.requireNonNull(state, "state nu poate fi null");
        this.lastHeartbeat = lastHeartbeat;
        this.coordinator = coordinator;
    }

    public NodeInfo(String id, String host, int port, boolean coordinator) {
        this(id, host, port, NodeState.ACTIV, System.currentTimeMillis(), coordinator);
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

    public String getAddress() {
        return host + ":" + port;
    }

    @Override
    public String toString() {
        return String.format("NodeInfo{id='%s', address='%s:%d', state=%s, coordinator=%s, lastHeartbeat=%d}",
                id, host, port, state, coordinator, lastHeartbeat);
    }
}
