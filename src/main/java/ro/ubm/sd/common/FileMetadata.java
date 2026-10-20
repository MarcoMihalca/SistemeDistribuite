package ro.ubm.sd.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Metadatele unui fisier stocat in sistemul distribuit.
 * Coordonatorul pastreaza aceasta structura in memorie, iar nodurile pot raporta
 * fisierele detinute.
 */
public class FileMetadata {
    private final UUID id;
    private final String name;
    private final long size;
    private final String checksum;
    private final List<String> replicaNodes;

    public FileMetadata(UUID id, String name, long size, String checksum, List<String> replicaNodes) {
        this.id = Objects.requireNonNull(id, "id nu poate fi null");
        this.name = Objects.requireNonNull(name, "name nu poate fi null");
        this.size = size;
        this.checksum = Objects.requireNonNull(checksum, "checksum nu poate fi null");
        this.replicaNodes = new ArrayList<>(Objects.requireNonNull(replicaNodes, "replicaNodes nu poate fi null"));
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

    public List<String> getReplicaNodes() {
        return Collections.unmodifiableList(replicaNodes);
    }

    @Override
    public String toString() {
        return String.format("FileMetadata{id=%s, name='%s', size=%d, checksum='%s', replicaNodes=%s}",
                id, name, size, checksum, replicaNodes);
    }
}
