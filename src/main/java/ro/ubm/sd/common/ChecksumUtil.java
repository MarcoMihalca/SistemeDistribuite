package ro.ubm.sd.common;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Calculul checksum-ului SHA-256 al fisierelor si al bucatilor (chunks), verificarea de integritate.
 */
public final class ChecksumUtil {

    private static final int BUFFER_SIZE = 64 * 1024;

    private ChecksumUtil() {
    }

    /**
     * Rezumatul hashing-ului intr-o singura trecere: SHA-256 global si SHA-256 per bucata.
     */
    public static final class FileHashSummary {
        private final String fullChecksum;
        private final List<String> chunkChecksums;

        public FileHashSummary(String fullChecksum, List<String> chunkChecksums) {
            this.fullChecksum = fullChecksum;
            this.chunkChecksums = Collections.unmodifiableList(new ArrayList<>(chunkChecksums));
        }

        public String getFullChecksum() {
            return fullChecksum;
        }

        public List<String> getChunkChecksums() {
            return chunkChecksums;
        }
    }

    /**
     * Calculeaza intr-o singura citire secventiala atat SHA-256 pe intregul fisier,
     * cat si SHA-256 pentru fiecare dintre cele {@code chunkCount} bucati.
     */
    public static FileHashSummary sha256WithChunks(Path filePath, long fileSize,
                                                   long chunkSize, int chunkCount) throws IOException {
        if (fileSize < 0 || chunkCount < 1) {
            throw new IllegalArgumentException("Parametri invalizi pentru sha256WithChunks: fileSize="
                    + fileSize + ", chunkCount=" + chunkCount);
        }
        try {
            MessageDigest fullDigest = MessageDigest.getInstance("SHA-256");
            List<String> chunkHashes = new ArrayList<>(chunkCount);

            if (fileSize == 0L) {
                if (chunkCount != 1) {
                    throw new IllegalArgumentException("Un fisier gol trebuie sa aiba o singura bucata");
                }
                if (Files.size(filePath) != 0L) {
                    throw new IOException("Dimensiunea fisierului s-a modificat in timpul hashing-ului");
                }
                String emptyHash = toHex(fullDigest.digest());
                chunkHashes.add(emptyHash);
                return new FileHashSummary(emptyHash, chunkHashes);
            }

            if (chunkSize <= 0L) {
                throw new IllegalArgumentException("chunkSize trebuie sa fie pozitiv: " + chunkSize);
            }

            byte[] buffer = new byte[BUFFER_SIZE];
            long totalRead = 0L;
            try (InputStream in = Files.newInputStream(filePath)) {
                for (int i = 0; i < chunkCount; i++) {
                    MessageDigest chunkDigest = MessageDigest.getInstance("SHA-256");
                    long remaining = ChunkPolicy.chunkLengthAt(fileSize, chunkSize, i);
                    while (remaining > 0) {
                        int toRead = (int) Math.min(buffer.length, remaining);
                        int n = in.read(buffer, 0, toRead);
                        if (n < 0) {
                            throw new EOFException("Fisier trunchiat in timpul hashing-ului la octetul "
                                    + totalRead + " din " + fileSize);
                        }
                        fullDigest.update(buffer, 0, n);
                        chunkDigest.update(buffer, 0, n);
                        remaining -= n;
                        totalRead += n;
                    }
                    chunkHashes.add(toHex(chunkDigest.digest()));
                }
                if (in.read() != -1) {
                    throw new IOException("Dimensiunea fisierului a crescut in timpul hashing-ului");
                }
            }
            return new FileHashSummary(toHex(fullDigest.digest()), chunkHashes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Algoritmul SHA-256 nu este disponibil in JDK", e);
        }
    }

    public static String sha256(Path filePath) throws IOException {
        try (InputStream in = Files.newInputStream(filePath)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesRead);
            }
            return toHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Algoritmul SHA-256 nu este disponibil in JDK", e);
        }
    }

    public static String sha256(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return toHex(digest.digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Algoritmul SHA-256 nu este disponibil in JDK", e);
        }
    }

    public static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }
}
