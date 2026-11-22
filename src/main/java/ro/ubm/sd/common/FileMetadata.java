package ro.ubm.sd.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Metadatele unui fisier stocat in sistemul distribuit.
 * Coordonatorul pastreaza aceasta structura in memorie, incluzand lista ordonata de bucati.
 */
public class FileMetadata {
    private final UUID id;
    private final String name;
    private final long size;
    private final String checksum;
    private final long chunkSize;
    private final List<ChunkMetadata> chunks;
    private final List<String> replicaNodes;

    public FileMetadata(UUID id, String name, long size, String checksum, List<String> replicaNodes) {
        this(id, name, size, checksum, size,
                Collections.singletonList(new ChunkMetadata(0, name, 0L, size, checksum, replicaNodes)));
    }

    public FileMetadata(UUID id, String name, long size, String checksum,
                        long chunkSize, List<ChunkMetadata> chunks) {
        this.id = Objects.requireNonNull(id, "id nu poate fi null");
        this.name = Objects.requireNonNull(name, "name nu poate fi null");
        if (size < 0) {
            throw new IllegalArgumentException("size nu poate fi negativ: " + size);
        }
        if (chunkSize < 0) {
            throw new IllegalArgumentException("chunkSize nu poate fi negativ: " + chunkSize);
        }
        this.size = size;
        this.checksum = Objects.requireNonNull(checksum, "checksum nu poate fi null");
        this.chunkSize = chunkSize;

        Objects.requireNonNull(chunks, "chunks nu poate fi null");
        if (chunks.isEmpty()) {
            throw new IllegalArgumentException("chunks nu poate fi gol");
        }
        List<ChunkMetadata> sortedChunks = new ArrayList<>(chunks);
        sortedChunks.sort(Comparator.comparingInt(ChunkMetadata::getIndex));
        this.chunks = Collections.unmodifiableList(sortedChunks);

        Set<String> distinctNodes = new TreeSet<>();
        for (ChunkMetadata chunk : sortedChunks) {
            distinctNodes.addAll(chunk.getReplicaNodes());
        }
        this.replicaNodes = Collections.unmodifiableList(new ArrayList<>(distinctNodes));
    }

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public long getSize() {
        return size;
    }

    public String getChecksum() {
        return checksum;
    }

    public long getChunkSize() {
        return chunkSize;
    }

    public List<ChunkMetadata> getChunks() {
        return chunks;
    }

    public int getChunkCount() {
        return chunks.size();
    }

    public List<String> getReplicaNodes() {
        return replicaNodes;
    }

    @Override
    public String toString() {
        return String.format("FileMetadata{id=%s, name='%s', size=%d, chunkSize=%d, chunks=%d, checksum='%s', replicaNodes=%s}",
                id, name, size, chunkSize, chunks.size(), checksum, replicaNodes);
    }
}
