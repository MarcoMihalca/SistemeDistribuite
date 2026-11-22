package ro.ubm.sd.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Metadatele unei singure bucati (chunk) dintr-un fisier distribuit.
 */
public class ChunkMetadata {

    private final int index;
    private final String chunkId;
    private final long offset;
    private final long size;
    private final String checksum;
    private final List<String> replicaNodes;

    public ChunkMetadata(int index, String chunkId, long offset, long size,
                         String checksum, List<String> replicaNodes) {
        if (index < 0) {
            throw new IllegalArgumentException("index nu poate fi negativ: " + index);
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset nu poate fi negativ: " + offset);
        }
        if (size < 0) {
            throw new IllegalArgumentException("size nu poate fi negativ: " + size);
        }
        this.index = index;
        this.chunkId = Objects.requireNonNull(chunkId, "chunkId nu poate fi null").trim();
        if (this.chunkId.isEmpty()) {
            throw new IllegalArgumentException("chunkId nu poate fi gol");
        }
        this.offset = offset;
        this.size = size;
        this.checksum = Objects.requireNonNull(checksum, "checksum nu poate fi null").trim().toLowerCase();
        if (this.checksum.isEmpty()) {
            throw new IllegalArgumentException("checksum nu poate fi gol");
        }
        Objects.requireNonNull(replicaNodes, "replicaNodes nu poate fi null");
        this.replicaNodes = new ArrayList<>(replicaNodes);
        if (this.replicaNodes.isEmpty()) {
            throw new IllegalArgumentException("replicaNodes nu poate fi gol");
        }
    }

    public int getIndex() {
        return index;
    }

    public String getChunkId() {
        return chunkId;
    }

    public long getOffset() {
        return offset;
    }

    public long getSize() {
        return size;
    }

    public String getChecksum() {
        return checksum;
    }

    public List<String> getReplicaNodes() {
        return Collections.unmodifiableList(replicaNodes);
    }

    @Override
    public String toString() {
        return String.format("ChunkMetadata{index=%d, chunkId='%s', offset=%d, size=%d, checksum='%s', replicaNodes=%s}",
                index, chunkId, offset, size, checksum, replicaNodes);
    }
}
